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

package seal

import (
	"context"
	"errors"
	"testing"
	"time"
)

// --- ShamirConfig ---

func TestShamirConfig_Fields(t *testing.T) {
	cfg := ShamirConfig{
		Threshold:   3,
		TotalShares: 5,
		QuorumTTL:   10 * time.Minute,
	}
	if cfg.Threshold != 3 {
		t.Fatalf("expected threshold=3, got %d", cfg.Threshold)
	}
	if cfg.TotalShares != 5 {
		t.Fatalf("expected total=5, got %d", cfg.TotalShares)
	}
	if cfg.QuorumTTL != 10*time.Minute {
		t.Fatalf("expected TTL=10m, got %v", cfg.QuorumTTL)
	}
}

func TestShamirConfig_ZeroValueDefaults(t *testing.T) {
	cfg := ShamirConfig{}
	if cfg.Threshold != 0 {
		t.Fatalf("expected zero threshold, got %d", cfg.Threshold)
	}
	if cfg.TotalShares != 0 {
		t.Fatalf("expected zero total, got %d", cfg.TotalShares)
	}
	if cfg.QuorumTTL != 0 {
		t.Fatalf("expected zero TTL, got %v", cfg.QuorumTTL)
	}
}

// --- ShamirInitResult ---

func TestShamirInitResult_Fields(t *testing.T) {
	result := ShamirInitResult{
		Shares:      []string{"share1", "share2", "share3"},
		Threshold:   2,
		TotalShares: 3,
	}
	if len(result.Shares) != 3 {
		t.Fatalf("expected 3 shares, got %d", len(result.Shares))
	}
	if result.Threshold != 2 {
		t.Fatalf("expected threshold=2, got %d", result.Threshold)
	}
	if result.TotalShares != 3 {
		t.Fatalf("expected total=3, got %d", result.TotalShares)
	}
}

func TestShamirInitResult_EmptyShares(t *testing.T) {
	result := ShamirInitResult{}
	if result.Shares != nil {
		t.Fatal("expected nil shares for zero value")
	}
}

// --- QuorumProgress ---

func TestQuorumProgress_Complete(t *testing.T) {
	progress := QuorumProgress{
		Required:  3,
		Submitted: 3,
		Complete:  true,
	}
	if !progress.Complete {
		t.Fatal("expected complete=true")
	}
	if progress.Required != 3 {
		t.Fatalf("expected required=3, got %d", progress.Required)
	}
}

func TestQuorumProgress_Incomplete(t *testing.T) {
	progress := QuorumProgress{
		Required:  3,
		Submitted: 1,
		Complete:  false,
	}
	if progress.Complete {
		t.Fatal("expected complete=false")
	}
	if progress.Submitted != 1 {
		t.Fatalf("expected submitted=1, got %d", progress.Submitted)
	}
}

// --- DefaultQuorumTTL ---

func TestDefaultQuorumTTL(t *testing.T) {
	if DefaultQuorumTTL != 5*time.Minute {
		t.Fatalf("expected 5m, got %v", DefaultQuorumTTL)
	}
}

// --- Mocks ---

// mockSplitter is a test ShareSplitter.
type mockSplitter struct {
	splitErr error
}

func (m *mockSplitter) Split(secret []byte, threshold, total int) ([]string, error) {
	if m.splitErr != nil {
		return nil, m.splitErr
	}
	shares := make([]string, total)
	for i := range shares {
		shares[i] = "mock-share"
	}
	return shares, nil
}

// mockAccumulator is a test ShareAccumulator.
type mockAccumulator struct {
	shares    []string
	threshold int
	expired   bool
	addErr    error
}

func (m *mockAccumulator) AddShare(_ context.Context, share string) (*QuorumProgress, error) {
	if m.addErr != nil {
		return nil, m.addErr
	}
	m.shares = append(m.shares, share)
	complete := len(m.shares) >= m.threshold
	return &QuorumProgress{
		Required:  m.threshold,
		Submitted: len(m.shares),
		Complete:  complete,
	}, nil
}

func (m *mockAccumulator) Reconstruct(_ context.Context) ([]byte, error) {
	if len(m.shares) < m.threshold {
		return nil, errors.New("threshold not met")
	}
	return []byte("reconstructed-key"), nil
}

func (m *mockAccumulator) Reset() {
	m.shares = nil
}

func (m *mockAccumulator) Expired() bool {
	return m.expired
}

func (m *mockAccumulator) GetProgress() *QuorumProgress {
	return &QuorumProgress{
		Required:  m.threshold,
		Submitted: len(m.shares),
		Complete:  len(m.shares) >= m.threshold,
	}
}

// --- Barrier.InitializeShamir ---

func TestBarrier_InitializeShamir(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	cfg := &ShamirConfig{
		Threshold:   2,
		TotalShares: 3,
	}
	splitter := &mockSplitter{}
	accFactory := func(threshold, total int) ShareAccumulator {
		return &mockAccumulator{threshold: threshold}
	}

	b.SetShamirSupport(cfg, splitter, accFactory)

	result, err := b.InitializeShamir(context.Background(), s, Credentials{})
	if err != nil {
		t.Fatalf("InitializeShamir failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if len(result.Shares) != 3 {
		t.Fatalf("expected 3 shares, got %d", len(result.Shares))
	}
	if result.Threshold != 2 {
		t.Fatalf("expected threshold=2, got %d", result.Threshold)
	}
	if result.TotalShares != 3 {
		t.Fatalf("expected total=3, got %d", result.TotalShares)
	}

	// Verify the sealed root key has Shamir metadata.
	sealed := b.SealedRootKeyData()
	if sealed.ShamirThreshold != 2 {
		t.Fatalf("expected ShamirThreshold=2, got %d", sealed.ShamirThreshold)
	}
	if sealed.ShamirTotal != 3 {
		t.Fatalf("expected ShamirTotal=3, got %d", sealed.ShamirTotal)
	}
}

func TestBarrier_InitializeShamir_NotConfigured(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	_, err := b.InitializeShamir(context.Background(), s, Credentials{})
	if !errors.Is(err, ErrShamirNotConfigured) {
		t.Fatalf("expected ErrShamirNotConfigured, got %v", err)
	}
}

func TestBarrier_InitializeShamir_InvalidThreshold(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	// Threshold < 2
	cfg := &ShamirConfig{
		Threshold:   1,
		TotalShares: 3,
	}
	b.SetShamirSupport(cfg, &mockSplitter{}, func(threshold, total int) ShareAccumulator {
		return &mockAccumulator{threshold: threshold}
	})

	_, err := b.InitializeShamir(context.Background(), s, Credentials{})
	if !errors.Is(err, ErrShamirThresholdInvalid) {
		t.Fatalf("expected ErrShamirThresholdInvalid, got %v", err)
	}
}

func TestBarrier_InitializeShamir_ThresholdGreaterThanTotal(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	cfg := &ShamirConfig{
		Threshold:   5,
		TotalShares: 3,
	}
	b.SetShamirSupport(cfg, &mockSplitter{}, func(threshold, total int) ShareAccumulator {
		return &mockAccumulator{threshold: threshold}
	})

	_, err := b.InitializeShamir(context.Background(), s, Credentials{})
	if !errors.Is(err, ErrShamirThresholdInvalid) {
		t.Fatalf("expected ErrShamirThresholdInvalid, got %v", err)
	}
}

func TestBarrier_InitializeShamir_SplitterError(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	cfg := &ShamirConfig{
		Threshold:   2,
		TotalShares: 3,
	}
	splitErr := errors.New("splitter failure")
	b.SetShamirSupport(cfg, &mockSplitter{splitErr: splitErr}, func(threshold, total int) ShareAccumulator {
		return &mockAccumulator{threshold: threshold}
	})

	_, err := b.InitializeShamir(context.Background(), s, Credentials{})
	if err == nil {
		t.Fatal("expected error from splitter")
	}
	if !errors.Is(err, splitErr) {
		t.Fatalf("expected splitter error, got %v", err)
	}
}

// --- Barrier.UnsealWithShare ---

func TestBarrier_UnsealWithShare_NotConfigured(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	_, err := b.UnsealWithShare(context.Background(), "share", s, Credentials{})
	if !errors.Is(err, ErrShamirNotConfigured) {
		t.Fatalf("expected ErrShamirNotConfigured, got %v", err)
	}
}

func TestBarrier_UnsealWithShare_IncrementalProgress(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	// Initialize with software strategy, then seal.
	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	// Configure Shamir with threshold=2.
	cfg := &ShamirConfig{
		Threshold:   2,
		TotalShares: 3,
	}
	b.SetShamirSupport(cfg, &mockSplitter{}, func(threshold, total int) ShareAccumulator {
		return &mockAccumulator{threshold: threshold}
	})

	// First share: should not complete the quorum.
	progress, err := b.UnsealWithShare(context.Background(), "share-1", s, Credentials{})
	if err != nil {
		t.Fatalf("UnsealWithShare (share 1) failed: %v", err)
	}
	if progress.Complete {
		t.Fatal("expected incomplete progress after first share")
	}
	if progress.Submitted != 1 {
		t.Fatalf("expected submitted=1, got %d", progress.Submitted)
	}
	if progress.Required != 2 {
		t.Fatalf("expected required=2, got %d", progress.Required)
	}

	// Barrier should still be sealed.
	if !b.IsSealed() {
		t.Fatal("barrier should still be sealed after one share")
	}
}

func TestBarrier_UnsealWithShare_QuorumComplete(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	// Initialize, then seal.
	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	cfg := &ShamirConfig{
		Threshold:   2,
		TotalShares: 3,
	}
	b.SetShamirSupport(cfg, &mockSplitter{}, func(threshold, total int) ShareAccumulator {
		return &mockAccumulator{threshold: threshold}
	})

	// Submit shares until quorum is met.
	progress1, err := b.UnsealWithShare(context.Background(), "share-1", s, Credentials{})
	if err != nil {
		t.Fatalf("UnsealWithShare (share 1) failed: %v", err)
	}
	if progress1.Complete {
		t.Fatal("should not be complete after 1 share")
	}

	progress2, err := b.UnsealWithShare(context.Background(), "share-2", s, Credentials{})
	if err != nil {
		t.Fatalf("UnsealWithShare (share 2) failed: %v", err)
	}
	if !progress2.Complete {
		t.Fatal("expected complete after 2 shares (threshold=2)")
	}

	// Barrier should now be unsealed.
	if b.IsSealed() {
		t.Fatal("barrier should be unsealed after quorum completion")
	}
}

func TestBarrier_UnsealWithShare_AddShareError(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	addErr := errors.New("add share failed")
	cfg := &ShamirConfig{
		Threshold:   2,
		TotalShares: 3,
	}
	b.SetShamirSupport(cfg, &mockSplitter{}, func(threshold, total int) ShareAccumulator {
		return &mockAccumulator{threshold: threshold, addErr: addErr}
	})

	_, err := b.UnsealWithShare(context.Background(), "share", s, Credentials{})
	if !errors.Is(err, addErr) {
		t.Fatalf("expected add share error, got %v", err)
	}
}

func TestBarrier_UnsealWithShare_ExpiredAccumulator(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	callCount := 0
	cfg := &ShamirConfig{
		Threshold:   2,
		TotalShares: 3,
	}
	b.SetShamirSupport(cfg, &mockSplitter{}, func(threshold, total int) ShareAccumulator {
		callCount++
		return &mockAccumulator{threshold: threshold}
	})

	// First share creates the accumulator.
	progress, err := b.UnsealWithShare(context.Background(), "share-1", s, Credentials{})
	if err != nil {
		t.Fatalf("first share failed: %v", err)
	}
	if progress.Complete {
		t.Fatal("should not complete with one share")
	}

	// Mark the accumulator as expired so the next call creates a new one.
	accRaw := b.accumulator.Load()
	acc := accRaw.(*mockAccumulator)
	acc.expired = true

	// Second share should create a new accumulator (reset shares).
	progress, err = b.UnsealWithShare(context.Background(), "share-2", s, Credentials{})
	if err != nil {
		t.Fatalf("second share (after expiry) failed: %v", err)
	}
	// New accumulator starts fresh: 1 share submitted, not complete.
	if progress.Complete {
		t.Fatal("should not complete (new accumulator after expiry)")
	}
	if progress.Submitted != 1 {
		t.Fatalf("expected submitted=1 (fresh accumulator), got %d", progress.Submitted)
	}

	// Factory should have been called twice.
	if callCount != 2 {
		t.Fatalf("expected accFactory called 2 times, got %d", callCount)
	}
}

// --- Barrier.UnsealWithShares ---

func TestBarrier_UnsealWithShares_NotConfigured(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	err := b.UnsealWithShares(context.Background(), []string{"s1"}, s, Credentials{})
	if !errors.Is(err, ErrShamirNotConfigured) {
		t.Fatalf("expected ErrShamirNotConfigured, got %v", err)
	}
}

func TestBarrier_UnsealWithShares_AllAtOnce(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	cfg := &ShamirConfig{
		Threshold:   2,
		TotalShares: 3,
	}
	b.SetShamirSupport(cfg, &mockSplitter{}, func(threshold, total int) ShareAccumulator {
		return &mockAccumulator{threshold: threshold}
	})

	err := b.UnsealWithShares(context.Background(), []string{"s1", "s2"}, s, Credentials{})
	if err != nil {
		t.Fatalf("UnsealWithShares failed: %v", err)
	}

	if b.IsSealed() {
		t.Fatal("barrier should be unsealed after UnsealWithShares")
	}
}

func TestBarrier_UnsealWithShares_InsufficientShares(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	cfg := &ShamirConfig{
		Threshold:   3,
		TotalShares: 5,
	}
	b.SetShamirSupport(cfg, &mockSplitter{}, func(threshold, total int) ShareAccumulator {
		return &mockAccumulator{threshold: threshold}
	})

	// Submit only 2 shares when 3 are needed.
	err := b.UnsealWithShares(context.Background(), []string{"s1", "s2"}, s, Credentials{})
	if !errors.Is(err, ErrShamirNotConfigured) {
		t.Fatalf("expected ErrShamirNotConfigured for insufficient shares, got %v", err)
	}

	// Barrier should still be sealed.
	if !b.IsSealed() {
		t.Fatal("barrier should remain sealed with insufficient shares")
	}
}

// --- Barrier.GetQuorumProgress ---

func TestBarrier_GetQuorumProgress_NoAccumulator(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	progress := b.GetQuorumProgress()
	if progress != nil {
		t.Fatal("expected nil progress for uninitialized barrier")
	}
}

func TestBarrier_GetQuorumProgress_WithAccumulator(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	cfg := &ShamirConfig{
		Threshold:   2,
		TotalShares: 3,
	}
	b.SetShamirSupport(cfg, &mockSplitter{}, func(threshold, total int) ShareAccumulator {
		return &mockAccumulator{threshold: threshold}
	})

	// Submit a share to create the accumulator.
	_, err := b.UnsealWithShare(context.Background(), "share-1", s, Credentials{})
	if err != nil {
		t.Fatalf("UnsealWithShare failed: %v", err)
	}

	// GetQuorumProgress should return progress with 1 share submitted.
	progress := b.GetQuorumProgress()
	if progress == nil {
		t.Fatal("expected non-nil progress from GetQuorumProgress")
	}
	if progress.Required != 2 {
		t.Fatalf("expected Required=2, got %d", progress.Required)
	}
	if progress.Submitted != 1 {
		t.Fatalf("expected Submitted=1, got %d", progress.Submitted)
	}
	if progress.Complete {
		t.Fatal("expected Complete=false")
	}
}

// --- SetShamirSupport ---

func TestBarrier_SetShamirSupport(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))

	cfg := &ShamirConfig{
		Threshold:   3,
		TotalShares: 5,
	}
	splitter := &mockSplitter{}
	factory := func(threshold, total int) ShareAccumulator {
		return &mockAccumulator{threshold: threshold}
	}

	b.SetShamirSupport(cfg, splitter, factory)

	b.mu.RLock()
	if b.shamirConfig == nil {
		t.Fatal("expected shamirConfig to be set")
	}
	if b.shamirConfig.Threshold != 3 {
		t.Fatalf("expected threshold=3, got %d", b.shamirConfig.Threshold)
	}
	if b.splitter == nil {
		t.Fatal("expected splitter to be set")
	}
	if b.accFactory == nil {
		t.Fatal("expected accFactory to be set")
	}
	b.mu.RUnlock()
}

func TestBarrier_SetShamirSupport_Overwrite(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))

	cfg1 := &ShamirConfig{Threshold: 2, TotalShares: 3}
	b.SetShamirSupport(cfg1, &mockSplitter{}, func(threshold, total int) ShareAccumulator {
		return &mockAccumulator{threshold: threshold}
	})

	cfg2 := &ShamirConfig{Threshold: 4, TotalShares: 7}
	b.SetShamirSupport(cfg2, &mockSplitter{}, func(threshold, total int) ShareAccumulator {
		return &mockAccumulator{threshold: threshold}
	})

	b.mu.RLock()
	if b.shamirConfig.Threshold != 4 {
		t.Fatalf("expected threshold=4 after overwrite, got %d", b.shamirConfig.Threshold)
	}
	if b.shamirConfig.TotalShares != 7 {
		t.Fatalf("expected totalShares=7 after overwrite, got %d", b.shamirConfig.TotalShares)
	}
	b.mu.RUnlock()
}
