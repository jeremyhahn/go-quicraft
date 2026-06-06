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
