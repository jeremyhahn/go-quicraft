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

package quicraft_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm/kv"
)

// startObservedShard starts a single-node Host with the given options and
// shard 1 on an in-memory key-value state machine, and waits until the shard
// serves reads. It is startExampleShard with host options, as the
// observability guide (docs/observability/user.md) passes WithMetrics and
// WithLogger.
func startObservedShard(ctx context.Context, dir string, opts ...quicraft.HostOption) (*quicraft.Host, error) {
	opts = append(opts, quicraft.WithoutTransport(), quicraft.WithoutEncryption())
	h, err := quicraft.NewHost(exampleHostConfig(dir), opts...)
	if err != nil {
		return nil, err
	}
	members := map[uint64]string{1: "127.0.0.1:63100"}
	shardCfg := config.Config{ShardID: 1, ReplicaID: 1}
	if err := h.StartShard(members, false, kv.NewMemoryCreateFunc(), shardCfg); err != nil {
		_ = h.Close()
		return nil, err
	}
	if err := waitReadReady(ctx, h, 1); err != nil {
		_ = h.Close()
		return nil, err
	}
	return h, nil
}

// proposeOne commits one put on shard 1, so the commit and apply paths
// report to the Host's collector.
func proposeOne(ctx context.Context, h *quicraft.Host) error {
	cmd, err := kv.EncodePut([]byte("k"), []byte("v"))
	if err != nil {
		return err
	}
	_, err = h.SyncPropose(ctx, 1, cmd)
	return err
}

// scrape fetches url and returns the exposition lines whose metric name is
// one of names, in the order served.
func scrape(ctx context.Context, url string, names ...string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("scrape: status %d", resp.StatusCode)
	}
	var out []string
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		for _, n := range names {
			if line == n || strings.HasPrefix(line, n+" ") || strings.HasPrefix(line, n+"{") {
				out = append(out, line)
				break
			}
		}
	}
	return out, sc.Err()
}

// exampleTempDir makes the data directory an example runs in.
func exampleTempDir() string {
	dir, err := os.MkdirTemp("", "quicraft-example-")
	if err != nil {
		log.Fatal(err)
	}
	return dir
}

// ExampleNewPrometheusMetricsCollector follows "Turn on metrics" in
// docs/observability/user.md: the collector goes to the Host through
// WithMetrics, and the application serves its dedicated Registry.
func ExampleNewPrometheusMetricsCollector() {
	dir := exampleTempDir()
	defer os.RemoveAll(dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c := quicraft.NewPrometheusMetricsCollector()
	h, err := startObservedShard(ctx, dir, quicraft.WithMetrics(c))
	if err != nil {
		log.Fatal(err)
	}
	defer h.Close()
	if err := proposeOne(ctx, h); err != nil {
		log.Fatal(err)
	}

	// The library serves nothing. Expose the registry yourself.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(c.Registry, promhttp.HandlerOpts{}))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	lines, err := scrape(ctx, srv.URL+"/metrics",
		"quicraft_metrics_per_shard_enabled", "quicraft_raft_elections_total")
	if err != nil {
		log.Fatal(err)
	}
	for _, l := range lines {
		// The election count is not fixed; print the series, not its value.
		if strings.HasPrefix(l, "quicraft_raft_elections_total") {
			l = l[:strings.LastIndexByte(l, ' ')]
		}
		fmt.Println(l)
	}
	// Output:
	// quicraft_metrics_per_shard_enabled 0
	// quicraft_raft_elections_total{shard_id="1"}
}

// ExampleNewPrometheusMetricsCollectorWithConfig turns on the per-shard
// breakdown, and the quicraft_metrics_per_shard_enabled gauge reports it.
func ExampleNewPrometheusMetricsCollectorWithConfig() {
	dir := exampleTempDir()
	defer os.RemoveAll(dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c := quicraft.NewPrometheusMetricsCollectorWithConfig(config.MetricsConfig{
		PerShardMetrics: true,
	})
	h, err := startObservedShard(ctx, dir, quicraft.WithMetrics(c))
	if err != nil {
		log.Fatal(err)
	}
	defer h.Close()

	srv := httptest.NewServer(promhttp.HandlerFor(c.Registry, promhttp.HandlerOpts{}))
	defer srv.Close()
	lines, err := scrape(ctx, srv.URL, "quicraft_metrics_per_shard_enabled")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(strings.Join(lines, "\n"))
	// Output:
	// quicraft_metrics_per_shard_enabled 1
}

// ExampleNewSampledMetricsCollector wraps a collector so that 1 in 100
// commit- and apply-latency observations is recorded, and passes the wrapper
// to WithMetrics.
func ExampleNewSampledMetricsCollector() {
	dir := exampleTempDir()
	defer os.RemoveAll(dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c := quicraft.NewPrometheusMetricsCollector()
	s := quicraft.NewSampledMetricsCollector(c, 100) // record 1 in 100
	h, err := startObservedShard(ctx, dir, quicraft.WithMetrics(s))
	if err != nil {
		log.Fatal(err)
	}
	defer h.Close()
	if err := proposeOne(ctx, h); err != nil {
		log.Fatal(err)
	}
	fmt.Println("proposal committed through the sampled collector")
	// Output:
	// proposal committed through the sampled collector
}

// shardCounter is the guide's "Write your own collector": it embeds
// NoOpMetricsCollector and overrides only RegisterShard.
type shardCounter struct {
	quicraft.NoOpMetricsCollector // override only what you need
	last                          atomic.Uint64
}

// RegisterShard records the shard the Host registered. A real collector
// pre-binds its per-shard metrics here.
func (m *shardCounter) RegisterShard(shardID uint64) {
	m.last.Store(shardID)
}

// ExampleNoOpMetricsCollector plugs a collector that overrides one method
// into a Host.
func ExampleNoOpMetricsCollector() {
	dir := exampleTempDir()
	defer os.RemoveAll(dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	m := &shardCounter{}
	h, err := startObservedShard(ctx, dir, quicraft.WithMetrics(m))
	if err != nil {
		log.Fatal(err)
	}
	defer h.Close()
	fmt.Printf("registered shard %d\n", m.last.Load())
	// Output:
	// registered shard 1
}

// ExampleWithLogger follows "Configure logging": a JSON logger built by
// config.NewLogger goes to the Host through WithLogger.
func ExampleWithLogger() {
	dir := exampleTempDir()
	defer os.RemoveAll(dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger := config.NewLogger(config.LogConfig{
		Format: config.LogFormatJSON,
		Level:  config.LogLevelInfo,
	})
	_, isJSON := logger.Handler().(*slog.JSONHandler)
	fmt.Println("json handler:", isJSON)

	h, err := startObservedShard(ctx, dir, quicraft.WithLogger(logger))
	if err != nil {
		log.Fatal(err)
	}
	if err := h.Close(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("host ran with the JSON logger")
	// Output:
	// json handler: true
	// host ran with the JSON logger
}
