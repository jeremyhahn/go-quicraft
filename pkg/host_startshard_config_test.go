package quicraft

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// startShardConfigMembers is the founding membership of every shard these
// tests start: the single local replica at the test host's address.
func startShardConfigMembers() map[uint64]string {
	return map[uint64]string{1: "127.0.0.1:5000"}
}

// newStartShardConfigHost returns a transport-less test host whose tick is
// rttMillisecond long.
func newStartShardConfigHost(_ context.Context, t *testing.T, rttMillisecond uint64) *Host {
	t.Helper()
	cfg := testHostConfig(t)
	cfg.RTTMillisecond = rttMillisecond
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost(RTTMillisecond=%d) = %v", rttMillisecond, err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

// requireStartShardConfigRefused starts cfg's shard and requires a
// *config.ValidationError naming field, then requires that no shard was left
// running under cfg.ShardID.
func requireStartShardConfigRefused(ctx context.Context, t *testing.T, h *Host, cfg config.Config, field string) {
	t.Helper()
	err := h.StartShard(startShardConfigMembers(), false, testCreateFunc, cfg)
	if err == nil {
		t.Fatalf("StartShard(shard %d) = nil, want a *config.ValidationError for %s", cfg.ShardID, field)
	}
	var ve *config.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("StartShard(shard %d) = %T (%v), want *config.ValidationError", cfg.ShardID, err, err)
	}
	if ve.Field != field {
		t.Fatalf("StartShard(shard %d) ValidationError.Field = %q (%v), want %q", cfg.ShardID, ve.Field, err, field)
	}
	requireStartShardConfigAbsent(ctx, t, h, cfg.ShardID)
}

// requireStartShardConfigAbsent requires that shardID is not loaded on h:
// ShardInfo and SyncPropose report ErrShardNotFound and HealthCheck lists it
// nowhere.
func requireStartShardConfigAbsent(ctx context.Context, t *testing.T, h *Host, shardID uint64) {
	t.Helper()
	if info, err := h.ShardInfo(shardID); !errors.Is(err, ErrShardNotFound) {
		t.Fatalf("ShardInfo(%d) = (%+v, %v), want ErrShardNotFound", shardID, info, err)
	}
	hs := h.HealthCheck()
	for _, s := range hs.Shards {
		if s.ShardID == shardID {
			t.Fatalf("HealthCheck lists refused shard %d: %+v", shardID, s)
		}
	}
	pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := h.SyncPropose(pctx, shardID, []byte("refused")); !errors.Is(err, ErrShardNotFound) {
		t.Fatalf("SyncPropose(%d) after refusal = %v, want ErrShardNotFound", shardID, err)
	}
}

// requireStartShardConfigStarts starts cfg's shard, waits for it to elect
// itself and requires a proposal to be applied.
func requireStartShardConfigStarts(ctx context.Context, t *testing.T, h *Host, cfg config.Config) {
	t.Helper()
	if err := h.StartShard(startShardConfigMembers(), false, testCreateFunc, cfg); err != nil {
		t.Fatalf("StartShard(shard %d) with corrected config = %v", cfg.ShardID, err)
	}
	if _, err := h.ShardInfo(cfg.ShardID); err != nil {
		t.Fatalf("ShardInfo(%d) after start = %v", cfg.ShardID, err)
	}
	waitForStartShardConfigLeader(ctx, t, h, cfg.ShardID)
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := h.SyncPropose(pctx, cfg.ShardID, []byte("accepted")); err != nil {
		t.Fatalf("SyncPropose(%d) after corrected start = %v", cfg.ShardID, err)
	}
}

// waitForStartShardConfigLeader waits until ctx is done for shardID's local
// replica to lead and to have applied its current-term no-op, the condition
// waitForLeader waits for, without waitForLeader's fixed two-second limit: a
// host with a multi-second tick needs tens of seconds to elect.
func waitForStartShardConfigLeader(ctx context.Context, t *testing.T, h *Host, shardID uint64) {
	t.Helper()
	for {
		engNode := h.engine.GetNode(shardID)
		if engNode != nil && engNode.Peer() != nil && engNode.Peer().IsLeader() {
			h.updateNodeLeader(shardID, engNode.Peer().LeaderID(), 0)
			if v, ok := h.nodes.Load(shardID); ok {
				if ns, nsOK := v.(*nodeState); nsOK && ns.lastApplied.Load() >= 2 {
					return
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("shard %d did not elect a leader: %v", shardID, ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestStartShardConfig_RefusesInvalidFieldAndStartsCorrected covers the
// Validate rules StartShard enforces: each invalid config is refused with a
// ValidationError naming the field config.Validate names, leaves no shard
// behind, and the same shard ID then starts with the corrected config.
func TestStartShardConfig_RefusesInvalidFieldAndStartsCorrected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h := newStartShardConfigHost(ctx, t, 1)

	cases := []struct {
		name       string
		shardID    uint64
		field      string
		invalidate func(c *config.Config)
		fix        func(c *config.Config)
	}{
		{
			name:       "ElectionRTTBelowTwiceHeartbeatRTT",
			shardID:    1001,
			field:      "ElectionRTT",
			invalidate: func(c *config.Config) { c.HeartbeatRTT = 4; c.ElectionRTT = 7 },
			fix:        func(c *config.Config) { c.HeartbeatRTT = 1; c.ElectionRTT = 10 },
		},
		{
			name:       "ObserverAndWitness",
			shardID:    1002,
			field:      "IsObserver",
			invalidate: func(c *config.Config) { c.IsObserver = true; c.IsWitness = true },
			fix:        func(c *config.Config) { c.IsObserver = false; c.IsWitness = false },
		},
		{
			name:       "RelativeDataDir",
			shardID:    1003,
			field:      "DataDir",
			invalidate: func(c *config.Config) { c.DataDir = "relative/shard-data" },
			fix:        func(c *config.Config) { c.DataDir = "" },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testShardConfig(tc.shardID, 1)
			tc.invalidate(&cfg)
			requireStartShardConfigRefused(ctx, t, h, cfg, tc.field)
			tc.fix(&cfg)
			requireStartShardConfigStarts(ctx, t, h, cfg)
		})
	}
}

// TestStartShardConfig_RefusesOverflowingElectionTimeout covers
// ValidateElectionTimeout: an ElectionRTT that Validate accepts on its own but
// whose product with the host's RTTMillisecond overflows time.Duration is
// refused with a ValidationError naming ElectionRTT, and the same shard ID
// then starts with an ElectionRTT the host can represent.
func TestStartShardConfig_RefusesOverflowingElectionTimeout(t *testing.T) {
	// 4295 ms is the shortest tick on which an ElectionRTT within Validate's
	// MaxInt32 bound can overflow time.Duration; the corrected shard elects
	// after 5 to 10 such ticks, so the deadline is generous.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const rttMillisecond = 4295
	h := newStartShardConfigHost(ctx, t, rttMillisecond)

	cfg := testShardConfig(2001, 1)
	cfg.HeartbeatRTT = 1
	cfg.ElectionRTT = math.MaxInt32
	// The ElectionRTT passes Validate's own bound; only the product overflows.
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil: the case must reach ValidateElectionTimeout", err)
	}
	if cfg.ElectionRTT <= uint64(math.MaxInt64)/uint64(time.Millisecond)/rttMillisecond {
		t.Fatalf("ElectionRTT %d * %d ms does not overflow time.Duration", cfg.ElectionRTT, rttMillisecond)
	}
	requireStartShardConfigRefused(ctx, t, h, cfg, "ElectionRTT")

	cfg.ElectionRTT = 5
	if err := cfg.ValidateElectionTimeout(rttMillisecond); err != nil {
		t.Fatalf("ValidateElectionTimeout(%d) for the corrected ElectionRTT = %v", rttMillisecond, err)
	}
	requireStartShardConfigStarts(ctx, t, h, cfg)
}
