// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build integration

package linearizability_test

// Kill-based restart cases (#1165). Every other restart in this suite stops a
// host with Close, which flushes and syncs the WAL before it returns, so none
// of them proves that an entry acknowledged to a client survives an abrupt
// process death. These cases run the node in a child process (this test
// binary, re-executed with TestKillRestart_Child selected), with WAL fsync
// ON, SIGKILL it, reopen the same directories and check every acknowledged
// entry is still there.
//
// The child appends one line per acknowledged proposal to an ack file and
// fsyncs it before it does anything else, so every line the parent reads
// after the kill names a proposal the engine had acknowledged. A proposal in
// flight when the kill lands may be present or absent after the restart;
// it must never be corrupt.
//
// SIGKILL keeps the page cache: these cases prove the engine acknowledges
// only what it has written and that recovery replays it, not that the
// fdatasync reached the platter.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
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
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/test/netport"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

const (
	// killRestartChildEnv selects the child role; TestKillRestart_Child
	// skips without it.
	killRestartChildEnv = "QUICRAFT_KILLRESTART_CHILD"
	// killRestartSpecEnv names the JSON killChildSpec the parent wrote.
	killRestartSpecEnv = "QUICRAFT_KILLRESTART_SPEC"

	killRestartShardID = 1

	// Child modes.
	killModeAcks     = "acks"     // N sequential acks, then idle
	killModeBatch    = "batch"    // concurrent proposers, never idle
	killModeSnapshot = "snapshot" // concurrent proposers, a snapshot blocks
	killModeServe    = "serve"    // cluster replica: no proposals, dumps state

	// Ack-file lines other than "ack <key> <value>".
	ackLineReady          = "ready"
	ackLineDone           = "done"
	ackLineSnapshotSaved  = "snapshot-saved"
	ackLineSnapshotSaving = "snapshot-saving"

	// killChildWait bounds every wait on a child line; a child that never
	// writes it has failed, and its log is printed.
	killChildWait = 2 * time.Minute

	// killRestartElectionRTT is the cluster case's election timeout in RTTs
	// (testRTTMillisecond each, so 6s). These hosts fsync the WAL, and the
	// stalls recorded at testHostConfig (2.5-4.6s for one fdatasync on a
	// contended disk) must fit inside one election timeout or no candidate
	// can persist its vote in time.
	killRestartElectionRTT  = 120
	killRestartHeartbeatRTT = 6
)

// killChildSpec is everything a child needs to open its host. The parent
// writes it as JSON and passes the path by env.
type killChildSpec struct {
	Mode        string
	WALDir      string
	NodeHostDir string
	Addr        string
	CAFile      string
	CertFile    string
	KeyFile     string
	ReplicaID   uint64
	// Members and Identities are empty for a single-replica shard: the child
	// then names itself with its own identity.
	Members    map[uint64]string
	Identities map[uint64]string
	// Count is the acks-mode proposal count; Workers the batch and snapshot
	// modes' concurrent proposers.
	Count   int
	Workers int
	// SnapshotEntries, when non-zero, overrides the shard's snapshot cadence;
	// BlockSnapshot is the 1-based SaveSnapshot call that blocks (snapshot
	// mode).
	SnapshotEntries uint64
	BlockSnapshot   int64
	AckFile         string
	DumpFile        string
}

// killDump is the serve-mode child's periodic copy of its state machine.
type killDump struct {
	Data map[string]string
}

// killKey and killValue tie every value to its key, so a value read back
// after the restart that is not killValue(key) is corruption, whether or not
// the proposal was acknowledged.
func killKey(worker, seq int) string { return fmt.Sprintf("k-%d-%d", worker, seq) }

func killValue(key string) string { return "val-" + key }

// ackLog is the child's fsynced ack file.
type ackLog struct {
	mu sync.Mutex
	f  *os.File
}

func openAckLog(path string) *ackLog {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		killChildDie("open ack file: %v", err)
	}
	return &ackLog{f: f}
}

// line appends s and fsyncs before returning: a line the parent can read is
// on disk before the child takes its next step.
func (a *ackLog) line(s string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, err := a.f.WriteString(s + "\n"); err != nil {
		killChildDie("write ack file: %v", err)
	}
	if err := a.f.Sync(); err != nil {
		killChildDie("sync ack file: %v", err)
	}
}

// killChildDie ends the child with a non-signal exit code, which the parent
// reports with the child's log. t.Fatalf cannot be used from the child's
// proposer goroutines.
func killChildDie(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "kill-restart child: "+format+"\n", args...)
	os.Exit(3)
}

// killLoadMTLS reads a testpki leaf from the files LeafFiles wrote.
func killLoadMTLS(t testing.TB, caFile, certFile, keyFile string) *config.MTLSConfig {
	t.Helper()
	var m config.MTLSConfig
	for _, f := range []struct {
		path string
		dst  *[]byte
	}{{caFile, &m.CACert}, {certFile, &m.Cert}, {keyFile, &m.Key}} {
		b, err := os.ReadFile(f.path)
		if err != nil {
			t.Fatalf("read %s: %v", f.path, err)
		}
		*f.dst = b
	}
	return &m
}

// killHostConfig is testHostConfig on the given directories with WAL fsync
// explicitly ON: the property under test is what fsync made durable.
func killHostConfig(t *testing.T, addr, walDir, nhDir string, mtls *config.MTLSConfig) config.HostConfig {
	t.Helper()
	cfg := testHostConfig(t, addr, mtls)
	cfg.WALDir = walDir
	cfg.NodeHostDir = nhDir
	cfg.NoSyncWAL = false
	return cfg
}

// killRestartShardConfig is the cluster case's shard config: an election
// timeout sized for real fdatasync (see killRestartElectionRTT).
func killRestartShardConfig(replicaID uint64, ids map[uint64]string) config.Config {
	c := config.Config{
		ShardID:      killRestartShardID,
		ReplicaID:    replicaID,
		ElectionRTT:  killRestartElectionRTT,
		HeartbeatRTT: killRestartHeartbeatRTT,
	}
	c.SetDefaults()
	c.MemberIdentities = ids
	return c
}

// blockingSnapshotKV is kvStateMachine whose blockAt-th SaveSnapshot writes
// the snapshot-saving marker and then blocks until the engine cancels it, so
// the parent's kill lands while a snapshot is being taken.
type blockingSnapshotKV struct {
	*kvStateMachine
	acks    *ackLog
	blockAt int64
	calls   atomic.Int64
}

func (b *blockingSnapshotKV) SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error {
	if b.calls.Add(1) == b.blockAt {
		b.acks.line(ackLineSnapshotSaving)
		<-stopper
		return &SnapshotCancelledError{Op: "save"}
	}
	if err := b.kvStateMachine.SaveSnapshot(ctx, w, stopper); err != nil {
		return err
	}
	b.acks.line(ackLineSnapshotSaved)
	return nil
}

// TestKillRestart_Child is the child process entry point. It opens one host
// on the directories its spec names, starts the shard, runs its mode and
// never returns or calls Close: the parent ends it with SIGKILL.
func TestKillRestart_Child(t *testing.T) {
	if os.Getenv(killRestartChildEnv) != "1" {
		t.Skip("child process entry point; driven by the TestKillRestart cases")
	}
	raw, err := os.ReadFile(os.Getenv(killRestartSpecEnv))
	if err != nil {
		killChildDie("read spec: %v", err)
	}
	var spec killChildSpec
	if err := json.Unmarshal(raw, &spec); err != nil {
		killChildDie("decode spec: %v", err)
	}

	mtls := killLoadMTLS(t, spec.CAFile, spec.CertFile, spec.KeyFile)
	cfg := killHostConfig(t, spec.Addr, spec.WALDir, spec.NodeHostDir, mtls)
	h, err := quicraft.NewHost(cfg, quicraft.WithoutEncryption())
	if err != nil {
		killChildDie("NewHost: %v", err)
	}
	acks := openAckLog(spec.AckFile)

	createFn, tracker := kvCreateFunc()
	if spec.Mode == killModeSnapshot {
		blockAt := spec.BlockSnapshot
		createFn = func(shardID, replicaID uint64) interface{} {
			return &blockingSnapshotKV{
				kvStateMachine: newKVStateMachine(shardID, replicaID),
				acks:           acks,
				blockAt:        blockAt,
			}
		}
	}

	members, ids := spec.Members, spec.Identities
	var scfg config.Config
	if len(members) == 0 {
		members = map[uint64]string{spec.ReplicaID: spec.Addr}
		ids = map[uint64]string{spec.ReplicaID: h.NodeIdentity()}
		scfg = testShardConfig(killRestartShardID, spec.ReplicaID)
		scfg.MemberIdentities = ids
	} else {
		scfg = killRestartShardConfig(spec.ReplicaID, ids)
	}
	if spec.SnapshotEntries > 0 {
		scfg.SnapshotEntries = spec.SnapshotEntries
		scfg.CompactionOverhead = spec.SnapshotEntries / 2
	}
	if err := h.StartShard(members, false, createFn, scfg); err != nil {
		killChildDie("StartShard: %v", err)
	}
	acks.line(ackLineReady)

	switch spec.Mode {
	case killModeServe:
		go killServe(tracker, spec)
	case killModeAcks:
		killWaitLeader(h)
		for seq, acked := 0, 0; acked < spec.Count; seq++ {
			if killProposeOnce(h, acks, 0, seq) {
				acked++
			}
		}
		acks.line(ackLineDone)
	case killModeBatch, killModeSnapshot:
		killWaitLeader(h)
		for w := range spec.Workers {
			go func(worker int) {
				for seq := 0; ; seq++ {
					killProposeOnce(h, acks, worker, seq)
				}
			}(w)
		}
	default:
		killChildDie("unknown mode %q", spec.Mode)
	}
	for {
		time.Sleep(time.Hour)
	}
}

// killWaitLeader waits until the child's single-replica shard has elected
// itself.
func killWaitLeader(h *quicraft.Host) {
	deadline := time.Now().Add(killChildWait)
	for time.Now().Before(deadline) {
		if info, err := h.ShardInfo(killRestartShardID); err == nil && info.IsLeader {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	killChildDie("single-replica shard elected no leader in %v", killChildWait)
}

// killProposeOnce proposes killKey(worker, seq) once and records the ack.
// An error leaves the key in flight: it may or may not be applied, and the
// caller moves on to a fresh key.
func killProposeOnce(h *quicraft.Host, acks *ackLog, worker, seq int) bool {
	key := killKey(worker, seq)
	value := killValue(key)
	ctx, cancel := context.WithTimeout(context.Background(), proposeTimeout)
	defer cancel()
	//quicraft:direct-propose single-voter shard: h is the only replica
	result, err := h.SyncPropose(ctx, killRestartShardID, encodeKVPut(key, value))
	if err != nil {
		time.Sleep(10 * time.Millisecond)
		return false
	}
	if result.Value != uint64(len(value)) {
		killChildDie("put(%s) acknowledged with Value=%d, want %d", key, result.Value, len(value))
	}
	acks.line("ack " + key + " " + value)
	return true
}

// killServe dumps the serve-mode child's state machine every 100ms, written
// to a temporary file and renamed so the parent never reads a torn dump.
func killServe(tracker *smTracker, spec killChildSpec) {
	key := smKey{killRestartShardID, spec.ReplicaID}
	tmp := spec.DumpFile + ".tmp"
	for {
		time.Sleep(100 * time.Millisecond)
		tracker.mu.Lock()
		kv := tracker.machines[key]
		tracker.mu.Unlock()
		if kv == nil {
			continue
		}
		kv.mu.RLock()
		d := killDump{Data: maps.Clone(kv.data)}
		kv.mu.RUnlock()
		b, err := json.Marshal(d)
		if err != nil {
			killChildDie("encode dump: %v", err)
		}
		if err := os.WriteFile(tmp, b, 0o600); err != nil {
			killChildDie("write dump: %v", err)
		}
		if err := os.Rename(tmp, spec.DumpFile); err != nil {
			killChildDie("rename dump: %v", err)
		}
	}
}

// killChild is one running child process.
type killChild struct {
	cmd     *exec.Cmd
	exited  chan struct{}
	logPath string
	ackFile string
}

// startKillChild writes spec and starts the test binary as a child running
// TestKillRestart_Child with it.
func startKillChild(t *testing.T, spec killChildSpec, name string) *killChild {
	t.Helper()
	dir := filepath.Dir(spec.AckFile)
	specPath := filepath.Join(dir, name+".spec.json")
	b, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("encode spec: %v", err)
	}
	if err := os.WriteFile(specPath, b, 0o600); err != nil {
		t.Fatalf("write spec: %v", err)
	}
	logPath := filepath.Join(dir, name+".log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("create child log: %v", err)
	}

	cmd := exec.CommandContext(t.Context(), os.Args[0],
		"-test.run=^TestKillRestart_Child$", "-test.count=1", "-test.timeout=30m")
	cmd.Env = append(os.Environ(), killRestartChildEnv+"=1", killRestartSpecEnv+"="+specPath)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		t.Fatalf("start child: %v", err)
	}
	c := &killChild{cmd: cmd, exited: make(chan struct{}), logPath: logPath, ackFile: spec.AckFile}
	go func() {
		_ = cmd.Wait()
		logFile.Close()
		close(c.exited)
	}()
	t.Cleanup(func() {
		select {
		case <-c.exited:
		default:
			_ = cmd.Process.Kill()
			<-c.exited
		}
	})
	t.Logf("%s: started child pid %d (mode %s)", name, cmd.Process.Pid, spec.Mode)
	return c
}

// logTail is the end of the child's log, for a failure message.
func (c *killChild) logTail() string {
	b, _ := os.ReadFile(c.logPath)
	if len(b) > 4096 {
		b = b[len(b)-4096:]
	}
	return string(b)
}

// ackLines is every complete line of the child's ack file. A line the kill
// cut short has no newline and is dropped: the child had not finished
// recording that ack, so it had not acted on it either.
func (c *killChild) ackLines(t *testing.T) []string {
	t.Helper()
	b, err := os.ReadFile(c.ackFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read ack file: %v", err)
	}
	s := string(b)
	if i := strings.LastIndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	} else {
		return nil
	}
	return strings.Split(s, "\n")
}

// waitLines polls the ack file until pred holds. A child that exits first,
// or never satisfies pred, fails the test with its log.
func (c *killChild) waitLines(t *testing.T, desc string, pred func(lines []string) bool) []string {
	t.Helper()
	deadline := time.Now().Add(killChildWait)
	for {
		lines := c.ackLines(t)
		if pred(lines) {
			return lines
		}
		select {
		case <-c.exited:
			t.Fatalf("child exited (%v) before %s; log:\n%s", c.cmd.ProcessState, desc, c.logTail())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for child: %s; log:\n%s", killChildWait, desc, c.logTail())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// kill SIGKILLs the child and asserts it died by that signal: an exit code
// of -1 and a SIGKILL wait status, never a clean or error exit.
func (c *killChild) kill(t *testing.T, name string) {
	t.Helper()
	if err := c.cmd.Process.Kill(); err != nil {
		t.Fatalf("%s: kill child: %v", name, err)
	}
	select {
	case <-c.exited:
	case <-time.After(30 * time.Second):
		t.Fatalf("%s: child did not exit within 30s of SIGKILL", name)
	}
	ps := c.cmd.ProcessState
	if code := ps.ExitCode(); code != -1 {
		t.Fatalf("%s: child exit code %d, want -1 (killed by signal); log:\n%s", name, code, c.logTail())
	}
	ws, ok := ps.Sys().(syscall.WaitStatus)
	if !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		t.Fatalf("%s: child wait status %v, want killed by SIGKILL", name, ps)
	}
	t.Logf("%s: child pid %d exited by signal %v (exit code %d)", name, ps.Pid(), ws.Signal(), ps.ExitCode())
}

// countAcks is the number of "ack" lines.
func countAcks(lines []string) int {
	n := 0
	for _, l := range lines {
		if strings.HasPrefix(l, "ack ") {
			n++
		}
	}
	return n
}

func hasLine(lines []string, want string) bool {
	for _, l := range lines {
		if l == want {
			return true
		}
	}
	return false
}

// ackedEntries maps every acknowledged key to its value.
func ackedEntries(t *testing.T, lines []string) map[string]string {
	t.Helper()
	acked := make(map[string]string)
	for _, l := range lines {
		if !strings.HasPrefix(l, "ack ") {
			continue
		}
		f := strings.Fields(l)
		if len(f) != 3 {
			t.Fatalf("malformed ack line %q", l)
		}
		acked[f[1]] = f[2]
	}
	return acked
}

// singleReplicaSpec lays out a single-replica child under a fresh directory.
func singleReplicaSpec(t *testing.T, mode string) killChildSpec {
	t.Helper()
	dir := t.TempDir()
	ca := testpki.NewCA(t)
	files := ca.LeafFiles(t, dir)
	return killChildSpec{
		Mode:        mode,
		WALDir:      filepath.Join(dir, "wal"),
		NodeHostDir: filepath.Join(dir, "nodehost"),
		Addr:        netport.MustReserveUDP(t.Fatalf),
		CAFile:      files.CAFile,
		CertFile:    files.CertFile,
		KeyFile:     files.KeyFile,
		ReplicaID:   1,
		AckFile:     filepath.Join(dir, "acks"),
	}
}

// verifySingleReplicaRestart reopens the killed child's directories in this
// process and checks every acknowledged key reads back with its value, and
// that every other key the state machine holds (a proposal in flight at the
// kill) carries its own value.
// killReadBack reads key linearizably from the restarted single replica,
// retrying while the new leader has not yet committed an entry in its term
// (ReadIndex is refused until then).
func killReadBack(h *quicraft.Host, key string) (interface{}, error) {
	deadline := time.Now().Add(killChildWait)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
		//quicraft:direct-read single-voter shard: h is the only replica
		got, err := h.SyncRead(ctx, killRestartShardID, encodeKVGet(key))
		cancel()
		if err == nil || time.Now().After(deadline) {
			return got, err
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func verifySingleReplicaRestart(t *testing.T, spec killChildSpec, acked map[string]string) {
	t.Helper()
	if len(acked) == 0 {
		t.Fatal("the child acknowledged no proposal before the kill")
	}
	mtls := killLoadMTLS(t, spec.CAFile, spec.CertFile, spec.KeyFile)
	cfg := killHostConfig(t, spec.Addr, spec.WALDir, spec.NodeHostDir, mtls)
	h, err := quicraft.NewHost(cfg, quicraft.WithoutEncryption())
	if err != nil {
		t.Fatalf("reopen host on the killed child's directories: %v", err)
	}
	defer h.Close()

	createFn, tracker := kvCreateFunc()
	scfg := testShardConfig(killRestartShardID, spec.ReplicaID)
	scfg.MemberIdentities = map[uint64]string{spec.ReplicaID: h.NodeIdentity()}
	if spec.SnapshotEntries > 0 {
		scfg.SnapshotEntries = spec.SnapshotEntries
		scfg.CompactionOverhead = spec.SnapshotEntries / 2
	}
	if err := h.StartShard(map[uint64]string{spec.ReplicaID: spec.Addr}, false, createFn, scfg); err != nil {
		t.Fatalf("StartShard after the kill: %v", err)
	}
	waitForCondition(t, killChildWait, "restarted single replica elects itself", func() bool {
		info, err := h.ShardInfo(killRestartShardID)
		return err == nil && info.IsLeader
	})

	missing := 0
	for key, want := range acked {
		got, err := killReadBack(h, key)
		if err != nil {
			t.Fatalf("read acknowledged key %s after the restart: %v", key, err)
		}
		if got != want {
			missing++
			if missing <= 20 {
				t.Errorf("acknowledged key %s read back %q after the restart, want %q", key, got, want)
			}
		}
	}
	if missing > 0 {
		t.Fatalf("%d of %d acknowledged entries lost or changed by the kill", missing, len(acked))
	}

	tracker.mu.Lock()
	kv := tracker.machines[smKey{killRestartShardID, spec.ReplicaID}]
	tracker.mu.Unlock()
	if kv == nil {
		t.Fatal("restarted host created no state machine")
	}
	inFlight := 0
	kv.mu.RLock()
	for k, v := range kv.data {
		if v != killValue(k) {
			t.Errorf("key %s holds %q after the restart, want %q (corrupt)", k, v, killValue(k))
		}
		if _, ok := acked[k]; !ok {
			inFlight++
		}
	}
	kv.mu.RUnlock()
	t.Logf("after restart: %d acknowledged entries checked, all present with their values; "+
		"%d unacknowledged in-flight entries applied, none corrupt", len(acked), inFlight)
}

// TestKillRestart_AfterAcks kills a single-replica child after it has
// acknowledged a fixed number of sequential proposals, with none in flight.
func TestKillRestart_AfterAcks(t *testing.T) {
	const name = "after-acks"
	start := time.Now()
	spec := singleReplicaSpec(t, killModeAcks)
	spec.Count = 50
	child := startKillChild(t, spec, name)
	child.waitLines(t, "all acks recorded", func(l []string) bool { return hasLine(l, ackLineDone) })
	child.kill(t, name)

	lines := child.ackLines(t)
	if n := countAcks(lines); n != spec.Count {
		t.Fatalf("child recorded %d acks, want %d", n, spec.Count)
	}
	verifySingleReplicaRestart(t, spec, ackedEntries(t, lines))
	t.Logf("%s: wall time %v", name, time.Since(start).Round(time.Millisecond))
}

// TestKillRestart_MidBatch kills a single-replica child while several
// concurrent proposers keep proposals in flight, so the kill lands between
// a batch's WAL write and its acknowledgements.
func TestKillRestart_MidBatch(t *testing.T) {
	const name = "mid-batch"
	start := time.Now()
	spec := singleReplicaSpec(t, killModeBatch)
	spec.Workers = 8
	child := startKillChild(t, spec, name)
	child.waitLines(t, "60 acks recorded", func(l []string) bool { return countAcks(l) >= 60 })
	child.kill(t, name)

	verifySingleReplicaRestart(t, spec, ackedEntries(t, child.ackLines(t)))
	t.Logf("%s: wall time %v", name, time.Since(start).Round(time.Millisecond))
}

// TestKillRestart_DuringSnapshot kills a single-replica child while its
// state machine is saving a snapshot: the first snapshot completes (so the
// log has been compacted behind it) and the second blocks after writing a
// marker the parent kills on. Proposals keep running throughout.
func TestKillRestart_DuringSnapshot(t *testing.T) {
	const name = "during-snapshot"
	start := time.Now()
	spec := singleReplicaSpec(t, killModeSnapshot)
	spec.Workers = 4
	spec.SnapshotEntries = 16
	spec.BlockSnapshot = 2
	child := startKillChild(t, spec, name)
	child.waitLines(t, "snapshot-saving marker", func(l []string) bool { return hasLine(l, ackLineSnapshotSaving) })
	child.kill(t, name)

	lines := child.ackLines(t)
	if !hasLine(lines, ackLineSnapshotSaved) {
		t.Fatalf("the first snapshot never completed before the blocking one")
	}
	verifySingleReplicaRestart(t, spec, ackedEntries(t, lines))
	t.Logf("%s: wall time %v", name, time.Since(start).Round(time.Millisecond))
}

// killIdentityOf is the node identity a host built on mtls reports. The
// founders' MemberIdentities must name the child before it has started, so
// a throwaway host on the same leaf reads it.
func killIdentityOf(t *testing.T, mtls *config.MTLSConfig) string {
	t.Helper()
	h, err := quicraft.NewHost(testHostConfig(t, kernelAddr, mtls), quicraft.WithoutEncryption())
	if err != nil {
		t.Fatalf("identity probe host: %v", err)
	}
	defer h.Close()
	return h.NodeIdentity()
}

// killLeaderID is the leader replica the in-process hosts report, 0 if none.
func killLeaderID(c *testCluster) uint64 {
	c.hostsMu.RLock()
	defer c.hostsMu.RUnlock()
	for i, h := range c.hosts {
		if h == nil || c.stopped[i].Load() {
			continue
		}
		if info, err := h.ShardInfo(killRestartShardID); err == nil && info.LeaderID != 0 {
			return info.LeaderID
		}
	}
	return 0
}

// killMoveLeaderToChild tries to move leadership to replica 3 (the child)
// and reports the role it holds when it is killed.
func killMoveLeaderToChild(t *testing.T, c *testCluster) string {
	t.Helper()
	const child = 3
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		lead := killLeaderID(c)
		if lead == child {
			return "leader"
		}
		if lead != 0 {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := c.hosts[lead-1].RequestLeaderTransfer(ctx, killRestartShardID, child)
			cancel()
			if err != nil {
				t.Logf("leader transfer to replica %d: %v", child, err)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return "follower"
}

// killReplicaData copies the in-process replica's state machine data.
func killReplicaData(c *testCluster, idx int) map[string]string {
	c.hostsMu.RLock()
	tr := c.trackers[idx]
	c.hostsMu.RUnlock()
	tr.mu.Lock()
	kv := tr.machines[smKey{killRestartShardID, uint64(idx + 1)}]
	tr.mu.Unlock()
	if kv == nil {
		return nil
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return maps.Clone(kv.data)
}

// TestKillRestart_3Node_Linearizable runs a 3-replica shard, two replicas in
// this process and the third in a child, under client load. The child is
// SIGKILLed (as leader when leadership can be moved to it, else as a
// follower), restarted as a new child on the same directories while the load
// keeps running, and then the history must be linearizable and the restarted
// replica's state machine must match the survivors'.
func TestKillRestart_3Node_Linearizable(t *testing.T) {
	const (
		name       = "3node"
		numClients = 4
		numKeys    = 10
		testDur    = 40 * time.Second
	)
	start := time.Now()
	dir := t.TempDir()
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, 2)
	childFiles := ca.LeafFiles(t, dir)
	childID := killIdentityOf(t, killLoadMTLS(t, childFiles.CAFile, childFiles.CertFile, childFiles.KeyFile))
	childAddr := netport.MustReserveUDP(t.Fatalf)

	timeline := newLeaderTimeline()
	hosts := make([]*quicraft.Host, 3)
	addrs := make([]string, 3)
	configs := make([]config.HostConfig, 3)
	ids := make([]string, 3)
	for i := range 2 {
		cfg := testHostConfig(t, kernelAddr, leaves[i])
		cfg.NoSyncWAL = false
		cfg.EventListener = timeline.listener()
		h, err := quicraft.NewHost(cfg, quicraft.WithoutEncryption())
		if err != nil {
			t.Fatalf("NewHost[%d]: %v", i, err)
		}
		t.Cleanup(func() { h.Close() })
		cfg.RaftAddress = h.RaftAddress()
		cfg.ListenAddress = cfg.RaftAddress
		hosts[i], addrs[i], configs[i], ids[i] = h, cfg.RaftAddress, cfg, h.NodeIdentity()
	}
	addrs[2], ids[2] = childAddr, childID
	cluster := &testCluster{
		hosts:    hosts,
		addrs:    addrs,
		configs:  configs,
		trackers: make([]*smTracker, 3),
		stopped:  make([]atomic.Bool, 3),
		ca:       ca,
		ids:      ids,
		t:        t,
		timeline: timeline,
	}
	// Replica 3 lives in the child: the clients never route to it directly.
	cluster.stopped[2].Store(true)
	members, idents := cluster.members(), cluster.identities()

	spec := killChildSpec{
		Mode:        killModeServe,
		WALDir:      filepath.Join(dir, "wal"),
		NodeHostDir: filepath.Join(dir, "nodehost"),
		Addr:        childAddr,
		CAFile:      childFiles.CAFile,
		CertFile:    childFiles.CertFile,
		KeyFile:     childFiles.KeyFile,
		ReplicaID:   3,
		Members:     members,
		Identities:  idents,
		AckFile:     filepath.Join(dir, "acks-1"),
		DumpFile:    filepath.Join(dir, "dump.json"),
	}
	child := startKillChild(t, spec, name+"-first")
	child.waitLines(t, "child shard started", func(l []string) bool { return hasLine(l, ackLineReady) })

	for i := range 2 {
		createFn, tr := kvCreateFunc()
		cluster.trackers[i] = tr
		if err := hosts[i].StartShard(members, false, createFn, killRestartShardConfig(uint64(i+1), idents)); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}
	waitForCondition(t, killChildWait, "3-replica shard elects a leader", func() bool {
		return killLeaderID(cluster) != 0
	})

	recorder := &opRecorder{}
	wait := runClients(t, cluster, killRestartShardID, numClients, numKeys, testDur, recorder)

	time.Sleep(3 * time.Second)
	role := killMoveLeaderToChild(t, cluster)
	t.Logf("killing replica 3 (child) as %s under load", role)
	child.kill(t, name+"-first")

	time.Sleep(4 * time.Second)
	spec.AckFile = filepath.Join(dir, "acks-2")
	restarted := startKillChild(t, spec, name+"-restarted")
	restarted.waitLines(t, "restarted child shard started", func(l []string) bool { return hasLine(l, ackLineReady) })
	t.Logf("replica 3 restarted on the same directories as a new child")

	wait()
	checkLinearizability(t, recorder, floorFor(numClients, 10*time.Second))

	// With the load stopped, the restarted replica must converge on exactly
	// the survivors' state: every write the cluster acknowledged, including
	// those acknowledged before the kill, and nothing else.
	var keys int
	waitForCondition(t, killChildWait, "restarted replica matches the survivors", func() bool {
		b, err := os.ReadFile(spec.DumpFile)
		if err != nil {
			return false
		}
		var d killDump
		if json.Unmarshal(b, &d) != nil {
			return false
		}
		r1, r2 := killReplicaData(cluster, 0), killReplicaData(cluster, 1)
		if len(r1) == 0 || !maps.Equal(r1, r2) || !maps.Equal(r1, d.Data) {
			return false
		}
		keys = len(d.Data)
		return true
	})
	restarted.kill(t, name+"-restarted")
	t.Logf("after restart: replica 3 holds all %d keys the survivors hold, with equal values", keys)
	t.Logf("%s: wall time %v", name, time.Since(start).Round(time.Millisecond))
}
