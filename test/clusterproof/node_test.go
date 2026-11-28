//go:build integration

package clusterproof

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/test/proposer"
)

const (
	// nodeChildEnv marks a test binary started as a replica process; the
	// spec's path is in nodeSpecEnv.
	nodeChildEnv = "QUICRAFT_CLUSTERPROOF_NODE"
	nodeSpecEnv  = "QUICRAFT_CLUSTERPROOF_SPEC"

	shardID = 1

	// electionRTT and heartbeatRTT are in RTTMillisecond ticks.
	electionRTT  = 50
	heartbeatRTT = 5

	// snapshotEntries is small so restarts recover from a snapshot plus a
	// compacted log, not only from a full log replay.
	snapshotEntries = 500

	// nodeOpTimeout bounds one put or read the node serves, retries included.
	nodeOpTimeout = 3 * time.Second
)

// nodeSpec is everything a replica process needs. The parent writes it as
// JSON and the child reads it back; a restart reuses the same spec, so the
// replica comes back on the same directories and addresses.
type nodeSpec struct {
	ReplicaID      uint64
	RaftAddr       string
	HTTPAddr       string
	WALDir         string
	NodeHostDir    string
	CAFile         string
	CertFile       string
	KeyFile        string
	RTTMillisecond uint64
	Members        map[uint64]string
	Identities     map[uint64]string
}

// loadMTLS reads a replica's CA, certificate and key files.
func loadMTLS(caFile, certFile, keyFile string) (*config.MTLSConfig, error) {
	var m config.MTLSConfig
	for _, f := range []struct {
		path string
		dst  *[]byte
	}{{caFile, &m.CACert}, {certFile, &m.Cert}, {keyFile, &m.Key}} {
		b, err := os.ReadFile(f.path)
		if err != nil {
			return nil, err
		}
		*f.dst = b
	}
	return &m, nil
}

// hostConfig is a replica's Host configuration: the WAL fsyncs every write
// (NoSyncWAL false), so an acknowledged entry is on disk on a quorum.
func hostConfig(addr, walDir, nhDir string, rtt uint64, mtls *config.MTLSConfig) config.HostConfig {
	return config.HostConfig{
		WALDir:          walDir,
		NodeHostDir:     nhDir,
		RaftAddress:     addr,
		ListenAddress:   addr,
		RTTMillisecond:  rtt,
		DeploymentID:    1,
		NumWorkers:      1,
		MaxApplyWorkers: 1,
		NoSyncWAL:       false,
		TransportConfig: config.TransportConfig{MTLSConfig: mtls},
	}
}

// shardConfig is the shard's configuration on every replica.
func shardConfig(replicaID uint64, ids map[uint64]string) config.Config {
	c := config.Config{
		ShardID:      shardID,
		ReplicaID:    replicaID,
		ElectionRTT:  electionRTT,
		HeartbeatRTT: heartbeatRTT,
	}
	c.SetDefaults()
	c.SnapshotEntries = snapshotEntries
	c.CompactionOverhead = snapshotEntries / 2
	c.MemberIdentities = ids
	return c
}

// nodeDie reports a fatal child error on stderr (the child's log) and exits.
func nodeDie(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "clusterproof node: FATAL: "+format+"\n", args...)
	os.Exit(2)
}

// TestClusterproof_Node is the replica process's entry point. The harness
// starts the test binary with nodeChildEnv set; run any other way it skips.
// It never returns: the harness SIGKILLs it.
func TestClusterproof_Node(t *testing.T) {
	if os.Getenv(nodeChildEnv) != "1" {
		t.Skip("replica process entry point; started by TestClusterproof_SIGKILL")
	}
	raw, err := os.ReadFile(os.Getenv(nodeSpecEnv))
	if err != nil {
		nodeDie("read spec: %v", err)
	}
	var spec nodeSpec
	if err := json.Unmarshal(raw, &spec); err != nil {
		nodeDie("decode spec: %v", err)
	}
	mtls, err := loadMTLS(spec.CAFile, spec.CertFile, spec.KeyFile)
	if err != nil {
		nodeDie("load mTLS: %v", err)
	}
	h, err := quicraft.NewHost(hostConfig(spec.RaftAddr, spec.WALDir, spec.NodeHostDir, spec.RTTMillisecond, mtls),
		quicraft.WithoutEncryption())
	if err != nil {
		nodeDie("NewHost: %v", err)
	}
	if err := h.StartShard(spec.Members, false, newKVCreate(), shardConfig(spec.ReplicaID, spec.Identities)); err != nil {
		nodeDie("StartShard: %v", err)
	}

	// The parent is the only client. A replica orphaned by a parent that
	// died without killing it exits on its own.
	parent := os.Getppid()
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			if os.Getppid() != parent {
				os.Exit(3)
			}
		}
	}()

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", spec.HTTPAddr)
	if err != nil {
		nodeDie("listen %s: %v", spec.HTTPAddr, err)
	}
	srv := &http.Server{Handler: nodeHandler(h), ReadHeaderTimeout: 5 * time.Second}
	nodeDie("serve: %v", srv.Serve(ln))
}

// nodeHandler serves the harness's three requests:
//
//	POST /put?k=K&v=V  commits put(K, V) through this replica; 200 only once
//	                   the entry is committed and applied
//	GET  /get?k=K      a linearizable read of K: 200 with the value, 404 when
//	                   K is absent
//	GET  /all          a linearizable read of the whole state, as JSON
//
// Any error is a 503 with the error text; the harness never counts one as an
// acknowledgement.
func nodeHandler(h *quicraft.Host) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /put", func(w http.ResponseWriter, r *http.Request) {
		k, v := r.URL.Query().Get("k"), r.URL.Query().Get("v")
		// Pinned: the harness addresses this replica on purpose (it may be
		// a follower, which forwards to the leader), and Temporary retries
		// only what clears on its own, an election in flight included.
		res, err := proposer.TryWith(proposer.Pinned(h), shardID, encodePut(k, v), nodeOpTimeout, quicraft.Temporary)
		if err == nil && res.Value != uint64(len(v)) {
			err = fmt.Errorf("put(%s) acknowledged with Value=%d, want %d", k, res.Value, len(v))
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /get", func(w http.ResponseWriter, r *http.Request) {
		out, err := proposer.TryReadWith(proposer.Pinned(h), shardID, r.URL.Query().Get("k"), nodeOpTimeout, quicraft.Temporary)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		res, ok := out.(lookupResult)
		if !ok {
			http.Error(w, fmt.Sprintf("read answered %T", out), http.StatusInternalServerError)
			return
		}
		if !res.Found {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(res.Value))
	})
	mux.HandleFunc("GET /all", func(w http.ResponseWriter, _ *http.Request) {
		out, err := proposer.TryReadWith(proposer.Pinned(h), shardID, allKeys{}, nodeOpTimeout, quicraft.Temporary)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		data, ok := out.(map[string]string)
		if !ok {
			http.Error(w, fmt.Sprintf("read answered %T", out), http.StatusInternalServerError)
			return
		}
		b, err := json.Marshal(data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(b)
	})
	return mux
}
