// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dragonboat

import (
	"context"
	"errors"
	"testing"
	"time"
)

const (
	unknownShardRunningID = uint64(1)
	unknownShardID        = uint64(99)
	unknownShardTimeout   = 5 * time.Second
)

// unknownShardCase names one NodeHost call on shard 99 and checks its
// result; check returns a non-empty description when the result is wrong.
type unknownShardCase struct {
	name  string
	check func(ctx context.Context, nh *NodeHost) string
}

// wantShardNotFound reports a mismatch when err is not ErrShardNotFound.
func wantShardNotFound(ctx context.Context, err error) string {
	if ctx.Err() != nil {
		return "deadline expired before the call returned: " + ctx.Err().Error()
	}
	if !errors.Is(err, ErrShardNotFound) {
		if err == nil {
			return "err = nil, want ErrShardNotFound"
		}
		return "err = " + err.Error() + ", want ErrShardNotFound"
	}
	return ""
}

func unknownShardCases() []unknownShardCase {
	return []unknownShardCase{
		// (a) calls that return ErrShardNotFound.
		{"SyncPropose", func(ctx context.Context, nh *NodeHost) string {
			_, err := nh.SyncPropose(ctx, nh.GetNoOPSession(unknownShardID), []byte("x"))
			return wantShardNotFound(ctx, err)
		}},
		{"Propose", func(ctx context.Context, nh *NodeHost) string {
			rs, err := nh.Propose(nh.GetNoOPSession(unknownShardID), []byte("x"), unknownShardTimeout)
			if rs != nil {
				rs.Release()
			}
			return wantShardNotFound(ctx, err)
		}},
		{"SyncRead", func(ctx context.Context, nh *NodeHost) string {
			_, err := nh.SyncRead(ctx, unknownShardID, nil)
			return wantShardNotFound(ctx, err)
		}},
		{"ReadIndex", func(ctx context.Context, nh *NodeHost) string {
			rs, err := nh.ReadIndex(unknownShardID, unknownShardTimeout)
			if rs != nil {
				rs.Release()
			}
			return wantShardNotFound(ctx, err)
		}},
		{"StaleRead", func(ctx context.Context, nh *NodeHost) string {
			_, err := nh.StaleRead(unknownShardID, nil)
			return wantShardNotFound(ctx, err)
		}},
		{"GetShardMembership", func(ctx context.Context, nh *NodeHost) string {
			_, err := nh.GetShardMembership(unknownShardID)
			return wantShardNotFound(ctx, err)
		}},
		{"SyncGetShardMembership", func(ctx context.Context, nh *NodeHost) string {
			_, err := nh.SyncGetShardMembership(ctx, unknownShardID)
			return wantShardNotFound(ctx, err)
		}},
		{"GetLeaderID", func(ctx context.Context, nh *NodeHost) string {
			_, _, _, err := nh.GetLeaderID(unknownShardID)
			return wantShardNotFound(ctx, err)
		}},
		{"RequestLeaderTransfer", func(ctx context.Context, nh *NodeHost) string {
			return wantShardNotFound(ctx, nh.RequestLeaderTransfer(unknownShardID, 1))
		}},
		{"RequestSnapshot", func(ctx context.Context, nh *NodeHost) string {
			rs, err := nh.RequestSnapshot(unknownShardID, DefaultSnapshotOption, unknownShardTimeout)
			if rs != nil {
				rs.Release()
			}
			return wantShardNotFound(ctx, err)
		}},
		{"SyncRequestSnapshot", func(ctx context.Context, nh *NodeHost) string {
			_, err := nh.SyncRequestSnapshot(ctx, unknownShardID, DefaultSnapshotOption)
			return wantShardNotFound(ctx, err)
		}},
		{"StopShard", func(ctx context.Context, nh *NodeHost) string {
			return wantShardNotFound(ctx, nh.StopShard(unknownShardID))
		}},
		{"GetNodeUser", func(ctx context.Context, nh *NodeHost) string {
			_, err := nh.GetNodeUser(unknownShardID)
			return wantShardNotFound(ctx, err)
		}},
		// (b) calls whose doc comment names another result.
		{"RequestCompaction", func(_ context.Context, nh *NodeHost) string {
			// Doc: a shard that is not loaded is treated as already
			// retired via RemoveData; the call succeeds.
			op, err := nh.RequestCompaction(unknownShardID, 1)
			if err != nil {
				return "err = " + err.Error() + ", want nil (unloaded shard treated as retired)"
			}
			if op == nil {
				return "SysOpState = nil, want a completed SysOpState"
			}
			select {
			case <-op.ResultC():
			default:
				return "SysOpState not completed"
			}
			return ""
		}},
		{"GetLogReader", func(_ context.Context, nh *NodeHost) string {
			if _, err := nh.GetLogReader(unknownShardID); !errors.Is(err, ErrLogDBNotCreatedOrClosed) {
				return "err = " + errString(err) + ", want ErrLogDBNotCreatedOrClosed"
			}
			return ""
		}},
		{"GetShardInfo", func(_ context.Context, nh *NodeHost) string {
			// Doc: the bool is false when the shard is not loaded.
			info, ok, err := nh.GetShardInfo(unknownShardID)
			if err != nil || ok || info != nil {
				return "GetShardInfo = (" + boolString(info != nil) + " info, ok=" +
					boolString(ok) + ", err=" + errString(err) + "), want (nil, false, nil)"
			}
			return ""
		}},
		{"HasNodeInfo", func(_ context.Context, nh *NodeHost) string {
			// Doc: true only if the LogDB holds data for the pair.
			if nh.HasNodeInfo(unknownShardID, 1) {
				return "HasNodeInfo = true, want false"
			}
			return ""
		}},
	}
}

func errString(err error) string {
	if err == nil {
		return "nil"
	}
	return err.Error()
}

func boolString(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// requireRunningShardServes checks that shard 1 still answers a
// SyncPropose and a SyncRead that observes it.
func requireRunningShardServes(ctx context.Context, t *testing.T, nh *NodeHost) {
	t.Helper()
	payload := []byte("still serving")
	if _, err := nh.SyncPropose(ctx, nh.GetNoOPSession(unknownShardRunningID), payload); err != nil {
		t.Fatalf("SyncPropose shard %d: %v", unknownShardRunningID, err)
	}
	got, err := nh.SyncRead(ctx, unknownShardRunningID, nil)
	if err != nil {
		t.Fatalf("SyncRead shard %d: %v", unknownShardRunningID, err)
	}
	if b, ok := got.([]byte); !ok || string(b) != string(payload) {
		t.Fatalf("SyncRead shard %d = %v, want %q", unknownShardRunningID, got, payload)
	}
}

// TestNodeHost_UnknownShard drives every NodeHost call that names a shard
// with shard 99, which this host does not run, and checks each returns
// ErrShardNotFound or the result its doc comment names; shard 1 keeps
// serving afterwards.
func TestNodeHost_UnknownShard(t *testing.T) {
	nh := newTestNodeHost(t)
	startTestShard(t, nh, unknownShardRunningID, 1)

	for _, tc := range unknownShardCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), unknownShardTimeout)
			defer cancel()
			if msg := tc.check(ctx, nh); msg != "" {
				t.Errorf("%s(shard %d): %s", tc.name, unknownShardID, msg)
			}
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), unknownShardTimeout)
	defer cancel()
	requireRunningShardServes(ctx, t, nh)
}
