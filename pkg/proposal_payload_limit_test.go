package quicraft

import (
	"context"
	"errors"
	"testing"
	"time"
)

// payloadLimitEntryPoint is one exported *Host method that submits a
// proposal. submit proposes cmd on shard 1 and, when the method is
// asynchronous, waits for the result.
type payloadLimitEntryPoint struct {
	name   string
	submit func(ctx context.Context, t *testing.T, h *Host, cmd []byte) error
}

func payloadLimitEntryPoints() []payloadLimitEntryPoint {
	newSession := func(ctx context.Context, t *testing.T, h *Host) *Session {
		t.Helper()
		s, err := h.NewSession(ctx, 1)
		if err != nil {
			t.Fatalf("NewSession: %v", err)
		}
		return s
	}
	return []payloadLimitEntryPoint{
		{"Propose", func(ctx context.Context, t *testing.T, h *Host, cmd []byte) error {
			rs, err := h.Propose(ctx, 1, cmd)
			if err != nil {
				return err
			}
			defer rs.Release()
			_, err = awaitResult(ctx, rs)
			return err
		}},
		{"SyncPropose", func(ctx context.Context, t *testing.T, h *Host, cmd []byte) error {
			_, err := h.SyncPropose(ctx, 1, cmd)
			return err
		}},
		{"ProposeWithSession", func(ctx context.Context, t *testing.T, h *Host, cmd []byte) error {
			rs, err := h.ProposeWithSession(ctx, newSession(ctx, t, h), cmd)
			if err != nil {
				return err
			}
			defer rs.Release()
			_, err = awaitResult(ctx, rs)
			return err
		}},
		{"SyncProposeWithSession", func(ctx context.Context, t *testing.T, h *Host, cmd []byte) error {
			_, err := h.SyncProposeWithSession(ctx, newSession(ctx, t, h), cmd)
			return err
		}},
	}
}

// TestPayloadLimit_EveryEntryPoint checks that every exported proposal entry
// point rejects a payload one byte over effectiveMaxPayload with
// *EntryTooLargeError (errors.Is ErrEntryTooLarge) and accepts a payload of
// exactly that size, whichever of MaxProposalPayloadSize and MaxEntrySize is
// the binding limit. Earlier single-method coverage:
// TestPropose_EntryTooLarge and TestProposeWithSession_EntryTooLarge
// (over MaxEntrySize only) and TestPropose_EntryTooLarge_MaxEntrySize, all in
// host_test.go; none covers SyncPropose, SyncProposeWithSession or the
// at-limit success.
func TestPayloadLimit_EveryEntryPoint(t *testing.T) {
	limits := []struct {
		name          string
		maxProposal   uint64
		maxEntry      uint64
		wantEffective uint64
	}{
		{"proposal limit binds", 64, 128, 64},
		{"entry limit binds", 128, 64, 64},
		{"equal limits", 96, 96, 96},
	}
	for _, lim := range limits {
		t.Run(lim.name, func(t *testing.T) {
			if got := effectiveMaxPayload(lim.maxProposal, lim.maxEntry); got != lim.wantEffective {
				t.Fatalf("effectiveMaxPayload(%d, %d) = %d, want %d",
					lim.maxProposal, lim.maxEntry, got, lim.wantEffective)
			}
			h := newTestHost(t)
			scfg := testShardConfig(1, 1)
			scfg.MaxProposalPayloadSize = lim.maxProposal
			scfg.MaxEntrySize = lim.maxEntry
			members := map[uint64]string{1: "127.0.0.1:5000"}
			if err := h.StartShard(members, false, testCreateFunc, scfg); err != nil {
				t.Fatalf("StartShard: %v", err)
			}
			waitForLeader(t, h, 1)

			for _, ep := range payloadLimitEntryPoints() {
				t.Run(ep.name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()

					err := ep.submit(ctx, t, h, make([]byte, lim.wantEffective+1))
					if !errors.Is(err, ErrEntryTooLarge) {
						t.Fatalf("over limit: err = %v, want ErrEntryTooLarge", err)
					}
					var etl *EntryTooLargeError
					if !errors.As(err, &etl) {
						t.Fatalf("over limit: err = %T, want *EntryTooLargeError", err)
					}
					if etl.Size != lim.wantEffective+1 || etl.MaxSize != lim.wantEffective {
						t.Errorf("over limit: EntryTooLargeError{Size: %d, MaxSize: %d}, want {%d, %d}",
							etl.Size, etl.MaxSize, lim.wantEffective+1, lim.wantEffective)
					}

					if err := ep.submit(ctx, t, h, make([]byte, lim.wantEffective)); err != nil {
						t.Fatalf("at limit: err = %v, want nil", err)
					}
				})
			}
		})
	}
}
