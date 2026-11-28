//go:build integration

// Package clusterproof runs a three-replica quicraft cluster as three OS
// processes, SIGKILLs a random replica for a number of cycles under
// concurrent writes, restarts it on the same directories, and checks after
// every cycle and at the end that every acknowledged write is on every
// replica, read back through linearizable reads.
//
// Run it with `make test-clusterproof`; CLUSTERPROOF_ARGS passes the flags
// below, e.g. CLUSTERPROOF_ARGS='-clusterproof.cycles=50 -clusterproof.seed=7'.
// A failure prints the seed that reproduces the kill schedule.
package clusterproof

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/test/netport"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

var (
	flagCycles  = flag.Int("clusterproof.cycles", 10, "SIGKILL-restart cycles to run")
	flagSeed    = flag.Uint64("clusterproof.seed", 0, "seed of the kill schedule and the clients; 0 picks one from the clock")
	flagWriters = flag.Int("clusterproof.writers", 4, "concurrent writers")
	flagReaders = flag.Int("clusterproof.readers", 2, "concurrent linearizable readers")
	flagRTT     = flag.Uint64("clusterproof.rtt-ms", 20, "Host RTTMillisecond; elections take electionRTT of these")
	// flagAckEarly is the harness's negative check: writers record a write
	// as acknowledged before the cluster has committed (and so fsynced) it.
	// Writes sent to a replica that is down are then recorded but never
	// stored, and the harness must fail.
	flagAckEarly = flag.Bool("clusterproof.ack-before-commit", false,
		"NEGATIVE CHECK: record each write as acknowledged before it is committed; the run must fail")
)

const (
	replicas = 3
	// serveWait bounds a replica's start (process, WAL replay, election)
	// and a cycle's convergence on the acknowledged writes.
	serveWait = 60 * time.Second
	// downMin and downJitter set how long a killed replica stays down.
	downMin    = 300 * time.Millisecond
	downJitter = 1200 * time.Millisecond
	// warmupAcks is how many writes the cluster must acknowledge before the
	// first kill.
	warmupAcks = 50
)

// replica is one replica process; start runs it again after a kill, with
// the same spec.
type replica struct {
	id       int
	spec     nodeSpec
	specPath string
	logPath  string
	cmd      *exec.Cmd
	exited   chan struct{}
}

// harness owns the replica processes and the clients.
type harness struct {
	t      *testing.T
	seed   uint64
	reps   [replicas]*replica
	http   *http.Client
	acks   *ackLog
	stop   atomic.Bool
	wg     sync.WaitGroup
	puts   atomic.Int64 // acknowledged writes
	reads  atomic.Int64 // answered linearizable reads of acknowledged writes
	failMu sync.Mutex
	// clientErr is the first violation or harness error a client goroutine
	// saw; the main goroutine fails the test with it.
	clientErr error
}

// fatalf fails the test with the seed in the message, after the tails of
// the replicas' logs.
func (h *harness) fatalf(format string, args ...any) {
	h.t.Helper()
	for _, r := range h.reps {
		if r != nil {
			h.t.Logf("replica %d log tail (%s):\n%s", r.id+1, r.logPath, logTail(r.logPath, 30))
		}
	}
	h.t.Fatalf("clusterproof seed=%d: %s\nreproduce: make test-clusterproof CLUSTERPROOF_ARGS='%s'",
		h.seed, fmt.Sprintf(format, args...), reproduceArgs(h.seed))
}

// reproduceArgs is the CLUSTERPROOF_ARGS that rerun this schedule: the seed
// and every other harness flag at its value for this run. The schedule is
// the seed's; timing still varies between runs.
func reproduceArgs(seed uint64) string {
	return fmt.Sprintf("-clusterproof.seed=%d -clusterproof.cycles=%d -clusterproof.writers=%d -clusterproof.readers=%d -clusterproof.rtt-ms=%d -clusterproof.ack-before-commit=%v",
		seed, *flagCycles, *flagWriters, *flagReaders, *flagRTT, *flagAckEarly)
}

func (h *harness) clientFail(err error) {
	h.failMu.Lock()
	defer h.failMu.Unlock()
	if h.clientErr == nil {
		h.clientErr = err
	}
}

func (h *harness) checkClients() {
	h.t.Helper()
	h.failMu.Lock()
	err := h.clientErr
	h.failMu.Unlock()
	if err != nil {
		h.fatalf("%v", err)
	}
}

// start runs replica r's process and returns once it has been started; the
// caller waits for it to serve.
func (h *harness) start(r *replica) {
	h.t.Helper()
	logFile, err := os.OpenFile(r.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		h.t.Fatalf("open replica log: %v", err)
	}
	cmd := exec.CommandContext(h.t.Context(), os.Args[0], "-test.run=^TestClusterproof_Node$", "-test.count=1", "-test.timeout=2h")
	// A race report must stop the child at once: a SIGKILLed child never
	// reaches the exit that -race would otherwise fail.
	gorace := "halt_on_error=1"
	if g := os.Getenv("GORACE"); g != "" {
		gorace = g + " " + gorace
	}
	cmd.Env = append(os.Environ(), nodeChildEnv+"=1", nodeSpecEnv+"="+r.specPath, "GORACE="+gorace)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		h.t.Fatalf("start replica %d: %v", r.id+1, err)
	}
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		logFile.Close()
		close(exited)
	}()
	r.cmd, r.exited = cmd, exited
}

// kill SIGKILLs replica r and waits for the process to be gone.
func (h *harness) kill(r *replica) {
	h.t.Helper()
	if err := r.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		h.fatalf("SIGKILL replica %d: %v", r.id+1, err)
	}
	<-r.exited
}

func (h *harness) url(r *replica, path string, q url.Values) string {
	u := "http://" + r.spec.HTTPAddr + path
	if q != nil {
		u += "?" + q.Encode()
	}
	return u
}

// do sends one request to a replica, bound to the test's context.
func (h *harness) do(method, u string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(h.t.Context(), method, u, nil)
	if err != nil {
		return nil, err
	}
	return h.http.Do(req)
}

// put asks replica r to commit put(k, v); nil means acknowledged.
func (h *harness) put(r *replica, k, v string) error {
	resp, err := h.do(http.MethodPost, h.url(r, "/put", url.Values{"k": {k}, "v": {v}}))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("put %s on replica %d: %s: %s", k, r.id+1, resp.Status, strings.TrimSpace(string(b)))
	}
	return nil
}

// get is a linearizable read of k on replica r. A non-nil error means the
// read was not answered.
func (h *harness) get(r *replica, k string) (string, bool, error) {
	resp, err := h.do(http.MethodGet, h.url(r, "/get", url.Values{"k": {k}}))
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", false, err
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return string(b), true, nil
	case http.StatusNotFound:
		return "", false, nil
	default:
		return "", false, fmt.Errorf("get %s on replica %d: %s: %s", k, r.id+1, resp.Status, strings.TrimSpace(string(b)))
	}
}

// all is a linearizable read of replica r's whole state.
func (h *harness) all(r *replica) (map[string]string, error) {
	resp, err := h.do(http.MethodGet, h.url(r, "/all", nil))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("all on replica %d: %s: %s", r.id+1, resp.Status, strings.TrimSpace(string(b)))
	}
	state := map[string]string{}
	if err := json.Unmarshal(b, &state); err != nil {
		// A 200 answer is a completed read: an undecodable one is a
		// fault of the replica, not unavailability, so it is not retried.
		h.fatalf("all on replica %d: decode a 200 answer: %v", r.id+1, err)
	}
	return state, nil
}

// waitServing waits until replica r answers a linearizable read, which needs
// its process up and a leader it can reach.
func (h *harness) waitServing(r *replica) {
	h.t.Helper()
	deadline := time.Now().Add(serveWait)
	var err error
	for time.Now().Before(deadline) {
		select {
		case <-r.exited:
			h.fatalf("replica %d exited before it served", r.id+1)
		default:
		}
		if _, _, err = h.get(r, "probe"); err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	h.fatalf("replica %d did not serve a linearizable read within %v: %v", r.id+1, serveWait, err)
}

// writer proposes unique keys to random replicas, a dead one included, and
// records each write durably before counting it as acknowledged.
func (h *harness) writer(w int) {
	defer h.wg.Done()
	rng := rand.New(rand.NewPCG(h.seed, uint64(1000+w)))
	for seq := 0; !h.stop.Load(); seq++ {
		k, v := fmt.Sprintf("w%d-%d", w, seq), fmt.Sprintf("%016x", rng.Uint64())
		r := h.reps[rng.IntN(replicas)]
		if *flagAckEarly {
			if err := h.acks.record(k, v); err != nil {
				h.clientFail(fmt.Errorf("ack log: %w", err))
				return
			}
			h.puts.Add(1)
			if h.put(r, k, v) != nil {
				time.Sleep(20 * time.Millisecond)
			}
			continue
		}
		if err := h.put(r, k, v); err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if err := h.acks.record(k, v); err != nil {
			h.clientFail(fmt.Errorf("ack log: %w", err))
			return
		}
		h.puts.Add(1)
	}
}

// reader reads acknowledged writes back through random replicas. Every key
// it picks was acknowledged (and its ack fsynced) before the read started,
// so a linearizable read that answers must return the acknowledged value.
func (h *harness) reader(n int) {
	defer h.wg.Done()
	rng := rand.New(rand.NewPCG(h.seed, uint64(2000+n)))
	for !h.stop.Load() {
		k, want, ok := h.acks.pick(rng.IntN(1 << 30))
		if !ok {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		r := h.reps[rng.IntN(replicas)]
		got, found, err := h.get(r, k)
		if err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if !found || got != want {
			h.clientFail(fmt.Errorf("linearizable read of %s on replica %d returned (%q, found=%v) after the write of %q was acknowledged",
				k, r.id+1, got, found, want))
			return
		}
		h.reads.Add(1)
	}
}

// converge waits until every replica's linearizable state holds every write
// in the durable ack log, and fails naming the lost writes if one does not
// within serveWait. With equal set, it also waits until the replicas'
// states are identical (the clients must be stopped for that).
func (h *harness) converge(what string, equal bool) int {
	h.t.Helper()
	acks, err := loadAcks(h.acks.path)
	if err != nil {
		h.fatalf("%s: read ack log: %v", what, err)
	}
	deadline := time.Now().Add(serveWait)
	states := make([]map[string]string, replicas)
	// The ack set was read before any GET /all below started, and GET /all
	// is a linearizable read: a read that answers and lacks an
	// acknowledged write, or holds another value for it, is a stale read
	// and fails at once. Only an unavailable read (transport error, no
	// leader, any non-200 answer) is retried until the deadline.
	for i, r := range h.reps {
		for {
			state, err := h.all(r)
			if err == nil {
				if err := ackedState(acks, state); err != nil {
					h.fatalf("%s: replica %d %v", what, r.id+1, err)
				}
				states[i] = state
				break
			}
			if time.Now().After(deadline) {
				h.fatalf("%s: replica %d answered no linearizable read within %v: %v", what, r.id+1, serveWait, err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	for equal {
		if maps.Equal(states[0], states[1]) && maps.Equal(states[1], states[2]) {
			break
		}
		if time.Now().After(deadline) {
			h.fatalf("%s: replica states differ after the clients stopped (%d, %d, %d keys)",
				what, len(states[0]), len(states[1]), len(states[2]))
		}
		time.Sleep(100 * time.Millisecond)
		if err := refreshStates(states, acks, func(i int) (map[string]string, error) { return h.all(h.reps[i]) }); err != nil {
			h.fatalf("%s: %v", what, err)
		}
	}
	return len(acks)
}

// refreshStates reads every replica's state again with fetch and stores each
// answered read in states, but only once it holds every acknowledged write:
// a read that answers without one, or with another value for one, is a stale
// linearizable read and is returned as an error at once, not stored. An
// unavailable read (fetch returns an error) leaves states[i] as it was.
func refreshStates(states []map[string]string, acks map[string]string, fetch func(i int) (map[string]string, error)) error {
	for i := range states {
		s, err := fetch(i)
		if err != nil {
			continue
		}
		if err := ackedState(acks, s); err != nil {
			return fmt.Errorf("replica %d %w", i+1, err)
		}
		states[i] = s
	}
	return nil
}

// ackedState reports, naming at most ten, the acknowledged writes state does
// not hold with the acknowledged value.
func ackedState(acks, state map[string]string) error {
	missing := missingAcks(acks, state)
	if len(missing) == 0 {
		return nil
	}
	n := len(missing)
	if n > 10 {
		missing = append(missing[:10], fmt.Sprintf("... %d more", n-10))
	}
	return fmt.Errorf("lost %d of %d acknowledged writes (linearizable GET /all): %s", n, len(acks), strings.Join(missing, ", "))
}

// TestClusterproof_CheckerRefresh: the final convergence retry must check
// every answered replacement state against the acknowledgement set. A read
// that lacks an acknowledged write, or holds another value for it, fails at
// once and is not stored; an unavailable read keeps the previous state.
func TestClusterproof_CheckerRefresh(t *testing.T) {
	acks := map[string]string{"a": "1", "b": "2"}
	good := map[string]string{"a": "1", "b": "2"}
	states := []map[string]string{good, good, good}
	fetch := func(i int) (map[string]string, error) {
		switch i {
		case 0:
			return map[string]string{"a": "1", "b": "2", "c": "3"}, nil
		case 1:
			return nil, fmt.Errorf("unavailable")
		default:
			return map[string]string{"a": "1"}, nil
		}
	}
	err := refreshStates(states, acks, fetch)
	if err == nil || !lostAcksRE.MatchString(err.Error()) || !strings.Contains(err.Error(), "replica 3") {
		t.Fatalf("refreshStates with a stale replica 3 = %v, want replica 3 lost acknowledged writes", err)
	}
	if !maps.Equal(states[2], good) {
		t.Fatalf("refreshStates stored the stale state %v", states[2])
	}
	if !maps.Equal(states[1], good) {
		t.Fatalf("refreshStates replaced an unavailable replica's state with %v", states[1])
	}

	wrong := func(int) (map[string]string, error) { return map[string]string{"a": "1", "b": "9"}, nil }
	if err := refreshStates([]map[string]string{good}, acks, wrong); err == nil || !strings.Contains(err.Error(), `holds "9"`) {
		t.Fatalf("refreshStates with a wrong acknowledged value = %v, want a failure naming it", err)
	}
	if err := refreshStates(states, acks, func(int) (map[string]string, error) { return good, nil }); err != nil {
		t.Fatalf("refreshStates with complete states = %v", err)
	}
}

// logTail returns the last n lines of a file, for a failure report.
func logTail(path string, n int) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return err.Error()
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// probeIdentity is the node identity a Host with mtls has, which the
// replicas need in their shard config before any of them starts.
func probeIdentity(t *testing.T, files testpki.Files) string {
	t.Helper()
	mtls, err := loadMTLS(files.CAFile, files.CertFile, files.KeyFile)
	if err != nil {
		t.Fatalf("load mTLS: %v", err)
	}
	cfg := hostConfig("127.0.0.1:0", t.TempDir(), t.TempDir(), *flagRTT, mtls)
	cfg.NoSyncWAL = true
	h, err := quicraft.NewHost(cfg, quicraft.WithoutEncryption())
	if err != nil {
		t.Fatalf("identity probe host: %v", err)
	}
	defer h.Close()
	return h.NodeIdentity()
}

// TestClusterproof_SIGKILL is the harness: three replica processes with WAL
// fsync on, a random replica SIGKILLed and restarted each cycle under
// concurrent writes and linearizable reads, and every acknowledged write
// checked on every replica after each cycle and at the end.
func TestClusterproof_SIGKILL(t *testing.T) {
	if os.Getenv(nodeChildEnv) == "1" {
		t.Skip("replica process")
	}
	seed := *flagSeed
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
	}
	h := &harness{t: t, seed: seed, http: &http.Client{Timeout: 10 * time.Second}}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("clusterproof FAILED with seed=%d", seed)
		}
	})
	t.Logf("clusterproof: seed=%d cycles=%d writers=%d readers=%d ack-before-commit=%v",
		seed, *flagCycles, *flagWriters, *flagReaders, *flagAckEarly)

	dir := t.TempDir()
	ca := testpki.NewCA(t)
	members, idents := map[uint64]string{}, map[uint64]string{}
	files := make([]testpki.Files, replicas)
	for i := range replicas {
		files[i] = ca.LeafFiles(t, dir)
		members[uint64(i+1)] = netport.MustReserveUDP(t.Fatalf)
		idents[uint64(i+1)] = probeIdentity(t, files[i])
	}
	for i := range replicas {
		rdir := filepath.Join(dir, fmt.Sprintf("replica-%d", i+1))
		r := &replica{
			id: i,
			spec: nodeSpec{
				ReplicaID:      uint64(i + 1),
				RaftAddr:       members[uint64(i+1)],
				HTTPAddr:       netport.MustReserveTCP(t.Fatalf),
				WALDir:         filepath.Join(rdir, "wal"),
				NodeHostDir:    filepath.Join(rdir, "nodehost"),
				CAFile:         files[i].CAFile,
				CertFile:       files[i].CertFile,
				KeyFile:        files[i].KeyFile,
				RTTMillisecond: *flagRTT,
				Members:        members,
				Identities:     idents,
			},
			specPath: filepath.Join(dir, fmt.Sprintf("replica-%d.json", i+1)),
			logPath:  filepath.Join(dir, fmt.Sprintf("replica-%d.log", i+1)),
		}
		b, err := json.Marshal(r.spec)
		if err != nil {
			t.Fatalf("encode spec: %v", err)
		}
		if err := os.WriteFile(r.specPath, b, 0o600); err != nil {
			t.Fatalf("write spec: %v", err)
		}
		h.reps[i] = r
	}
	acks, err := openAckLog(filepath.Join(dir, "acks"))
	if err != nil {
		t.Fatalf("open ack log: %v", err)
	}
	h.acks = acks
	t.Cleanup(func() { _ = acks.close() })

	for _, r := range h.reps {
		h.start(r)
	}
	t.Cleanup(func() {
		h.stop.Store(true)
		h.wg.Wait()
		for _, r := range h.reps {
			select {
			case <-r.exited:
			default:
				_ = r.cmd.Process.Kill()
				<-r.exited
			}
		}
	})
	for _, r := range h.reps {
		h.waitServing(r)
	}

	for w := range *flagWriters {
		h.wg.Add(1)
		go h.writer(w)
	}
	for n := range *flagReaders {
		h.wg.Add(1)
		go h.reader(n)
	}
	deadline := time.Now().Add(serveWait)
	for h.puts.Load() < warmupAcks {
		if time.Now().After(deadline) {
			h.fatalf("the cluster acknowledged %d writes within %v, want %d before the first kill", h.puts.Load(), serveWait, warmupAcks)
		}
		h.checkClients()
		time.Sleep(20 * time.Millisecond)
	}

	rng := rand.New(rand.NewPCG(seed, 0))
	for c := 1; c <= *flagCycles; c++ {
		victim := h.reps[rng.IntN(replicas)]
		down := downMin + time.Duration(rng.Int64N(int64(downJitter)))
		before := h.puts.Load()
		h.kill(victim)
		time.Sleep(down)
		h.start(victim)
		h.waitServing(victim)
		n := h.converge(fmt.Sprintf("cycle %d", c), false)
		h.checkClients()
		during := h.puts.Load() - before
		if during == 0 {
			h.fatalf("cycle %d: no write was acknowledged while replica %d was killed and restarted", c, victim.id+1)
		}
		t.Logf("cycle %d: SIGKILLed replica %d for %v; %d writes acknowledged during the cycle; all %d acknowledged writes on every replica",
			c, victim.id+1, down.Round(time.Millisecond), during, n)
	}

	h.stop.Store(true)
	h.wg.Wait()
	h.checkClients()
	n := h.converge("final", true)
	// With -clusterproof.readers=0 no reader runs, so there is no read to
	// count; the acknowledged writes are still checked through GET /all.
	if *flagReaders > 0 && h.reads.Load() == 0 {
		h.fatalf("no linearizable read of an acknowledged write was answered")
	}
	t.Logf("final: %d acknowledged writes on all %d replicas, replica states identical; %d linearizable reads matched",
		n, replicas, h.reads.Load())
}
