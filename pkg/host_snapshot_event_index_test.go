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

package quicraft

import (
	"log/slog"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
)

// TestOnLogCompacted_PanickingListenerKeepsLogDBCompacted: OnLogCompacted
// and OnLogDBCompacted are separate user listeners. A panic in the first
// must not suppress the second; both are delivered, in that order, with the
// same index.
func TestOnLogCompacted_PanickingListenerKeepsLogDBCompacted(t *testing.T) {
	cfg := config.HostConfig{
		WALDir:        t.TempDir(),
		NodeHostDir:   t.TempDir(),
		RaftAddress:   "127.0.0.1:5000",
		ListenAddress: "127.0.0.1:5000",
		DeploymentID:  1,
	}
	cfg.SetDefaults()
	eng := engine.NewEngine(cfg, memdb.New(), &noopSender{}, nil, nil, nil)
	eng.Start()
	t.Cleanup(eng.Stop)

	var (
		mu    sync.Mutex
		order []string
		got   []config.EventShardInfo
	)
	cfg.EventListener = &config.EventListener{
		OnLogCompacted: func(config.EventShardInfo) {
			mu.Lock()
			order = append(order, "OnLogCompacted")
			mu.Unlock()
			panic("listener panic")
		},
		OnLogDBCompacted: func(info config.EventShardInfo) {
			mu.Lock()
			order = append(order, "OnLogDBCompacted")
			got = append(got, info)
			mu.Unlock()
		},
	}
	h := &Host{cfg: cfg, engine: eng, logger: slog.Default()}
	cb := &hostEngineCallback{h: h}

	cb.OnLogCompacted(7, 1, engine.LogCompaction{Index: 42, Reader: true, LogDB: true})
	flushListeners(t, cb)

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "OnLogCompacted" || order[1] != "OnLogDBCompacted" {
		t.Fatalf("listener order = %v, want [OnLogCompacted OnLogDBCompacted]", order)
	}
	want := config.EventShardInfo{ShardID: 7, ReplicaID: 1, Index: 42}
	if len(got) != 1 || got[0] != want {
		t.Fatalf("OnLogDBCompacted got %+v, want exactly one %+v", got, want)
	}
}
