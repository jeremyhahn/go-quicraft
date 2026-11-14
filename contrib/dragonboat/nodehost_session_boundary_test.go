package dragonboat

import (
	"context"
	"errors"
	"testing"
	"time"

	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
)

// respondedSession returns a tracked session on a running shard whose
// current series has already been responded to (RespondedTo == SeriesID),
// the state dragonboat's ValidForProposal refuses.
func respondedSession(ctx context.Context, t *testing.T) (*NodeHost, *dbClient.Session) {
	t.Helper()
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	startTestShard(t, nh, shardID, 1)
	session, err := nh.SyncGetSession(ctx, shardID)
	if err != nil {
		t.Fatalf("SyncGetSession: %v", err)
	}
	session.Inner().SetRespondedTo(session.SeriesID())
	if session.Inner().RespondedTo() != session.SeriesID() {
		t.Fatalf("RespondedTo = %d, want SeriesID %d", session.Inner().RespondedTo(), session.SeriesID())
	}
	return nh, session
}

func TestSessionBoundary_SyncPropose_RespondedSeriesRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	nh, session := respondedSession(ctx, t)

	_, err := nh.SyncPropose(ctx, session, []byte("responded"))
	if !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("SyncPropose with RespondedTo == SeriesID: err = %v, want ErrInvalidSession", err)
	}
}

func TestSessionBoundary_Propose_RespondedSeriesRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	nh, session := respondedSession(ctx, t)

	rs, err := nh.Propose(session, []byte("responded"), 5*time.Second)
	if rs != nil {
		rs.Release()
	}
	if !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("Propose with RespondedTo == SeriesID: err = %v, want ErrInvalidSession", err)
	}
}

func TestSessionBoundary_SyncPropose_AdvancedSeriesAccepted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	nh, session := respondedSession(ctx, t)

	session.ProposalCompleted()
	if _, err := nh.SyncPropose(ctx, session, []byte("advanced")); err != nil {
		t.Fatalf("SyncPropose after ProposalCompleted: %v", err)
	}
}

func TestSessionBoundary_Propose_WrongShardRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	nh, session := respondedSession(ctx, t)

	session.ProposalCompleted()
	session.ShardID = 2
	rs, err := nh.Propose(session, []byte("wrong-shard"), 5*time.Second)
	if rs != nil {
		rs.Release()
	}
	if !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("Propose with foreign ShardID: err = %v, want ErrInvalidSession", err)
	}
}
