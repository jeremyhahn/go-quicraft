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
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/supervise"
)

// stalledLogHandler is a slog handler that blocks every record until release
// closes, signalling entered on the first.
type stalledLogHandler struct {
	entered chan<- struct{}
	release <-chan struct{}
}

func (h stalledLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h stalledLogHandler) Handle(context.Context, slog.Record) error {
	select {
	case h.entered <- struct{}{}:
	default:
	}
	<-h.release
	return nil
}
func (h stalledLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h stalledLogHandler) WithGroup(string) slog.Handler      { return h }

// TestHealthCheck_ReportsDiagnosticsDropped: diagnostics dropped because the
// slog handler stopped taking them are visible in HealthCheck, and leave the
// health verdict alone.
func TestHealthCheck_ReportsDiagnosticsDropped(t *testing.T) {
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()
	before := h.HealthCheck()

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	// Cleanups run last-in first-out: the handler is replaced before it is
	// released, so the records still queued drain into the previous one.
	t.Cleanup(func() { close(release) })
	installDefaultLogger(t, stalledLogHandler{entered: entered, release: release})
	for range 2 * supervise.DiagnosticQueueCapacity {
		supervise.LogContained(func() { slog.Info("diagnostic") })
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the stalled handler never received a diagnostic")
	}

	after := h.HealthCheck()
	if want := before.DiagnosticsDropped + supervise.DiagnosticQueueCapacity - 1; after.DiagnosticsDropped < want {
		t.Fatalf("DiagnosticsDropped = %d, want at least %d", after.DiagnosticsDropped, want)
	}
	if after.Status != before.Status {
		t.Fatalf("Status = %q after dropped diagnostics, want %q unchanged", after.Status, before.Status)
	}
}
