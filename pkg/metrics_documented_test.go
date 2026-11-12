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
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// metricsDocDir holds the observability documents, relative to this package.
const metricsDocDir = "../docs/observability"

// metricsReferenceDoc is the document whose "Metric reference" table lists
// every family the reference collector registers.
const metricsReferenceDoc = "engineering.md"

// perShardModeLabel is the table's Labels cell for a family that carries
// shard_id only when PerShardMetrics is on.
const perShardModeLabel = "per-shard mode"

type documentedMetric struct {
	typ    string
	labels string // the raw Labels cell
}

// readMetricReference parses the "Metric reference" table into name -> row.
func readMetricReference(t *testing.T) map[string]documentedMetric {
	t.Helper()
	path := filepath.Join(metricsDocDir, metricsReferenceDoc)
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	rows := make(map[string]documentedMetric)
	inSection := false
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if strings.HasPrefix(line, "## ") {
			inSection = strings.Contains(line, "Metric reference")
			continue
		}
		if !inSection || !strings.HasPrefix(line, "| `quicraft_") {
			continue
		}
		cells := strings.Split(strings.Trim(line, "|"), "|")
		if len(cells) != 3 {
			t.Fatalf("%s: malformed metric row %q", path, line)
		}
		name := strings.Trim(strings.TrimSpace(cells[0]), "`")
		if _, dup := rows[name]; dup {
			t.Errorf("%s: metric %s listed twice", path, name)
		}
		rows[name] = documentedMetric{
			typ:    strings.TrimSpace(cells[1]),
			labels: strings.TrimSpace(cells[2]),
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if len(rows) == 0 {
		t.Fatalf("%s: no Metric reference table found", path)
	}
	return rows
}

// documentedLabels returns the label names a row promises for the given mode.
func documentedLabels(row documentedMetric, perShard bool) []string {
	switch row.labels {
	case "none":
		return nil
	case perShardModeLabel:
		if perShard {
			return []string{"shard_id"}
		}
		return nil
	}
	var out []string
	for _, l := range strings.Split(row.labels, ",") {
		out = append(out, strings.Trim(strings.TrimSpace(l), "`"))
	}
	sort.Strings(out)
	return out
}

// exercisedFamilies builds the reference collector on its own fresh registry,
// touches every metric so each family has a child series, and gathers.
func exercisedFamilies(t *testing.T, perShard bool) []*dto.MetricFamily {
	t.Helper()
	c := NewPrometheusMetricsCollectorWithConfig(config.MetricsConfig{PerShardMetrics: perShard})
	c.EnablePerMessageMetrics = true
	const shard = 7
	c.RegisterShard(shard)

	d := time.Millisecond
	c.ObserveProposalLatency(shard, d)
	c.ObserveCommitLatency(shard, d)
	c.ObserveApplyLatency(shard, d)
	c.ObserveReadIndexLatency(shard, d)
	c.IncProposalDropped(shard)
	c.ObserveProposalBatchSize(shard, 4)
	c.ObserveCommitBatchSize(4)
	c.IncElection(shard)
	c.IncSnapshot(shard)
	c.IncPiggybackCommit(shard)
	c.ObserveWriteLatency(d)
	c.ObserveFsyncLatency(d)
	c.ObserveCompactionLatency(d)
	c.IncWriteBytes(64)
	c.ObserveSendLatency(d)
	c.IncMessagesSent(shard, 2, 3)
	c.IncMessagesReceived(shard, 2, 3)
	c.IncSnapshotsSent(shard, 1024, d)
	c.IncSnapshotsReceived(shard, 1024, d)
	c.IncSnapshotsRejected(shard, "policy")
	c.IncMessagesDropped(shard, 2, "queue_full")
	c.IncUnauthorizedMessageDropped(shard, 2, "unknown_member")
	c.IncUnauthorizedSnapshotDropped(shard, 2)

	families, err := c.Registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	return families
}

func promTypeName(mt dto.MetricType) string {
	switch mt {
	case dto.MetricType_COUNTER:
		return "counter"
	case dto.MetricType_GAUGE:
		return "gauge"
	case dto.MetricType_HISTOGRAM:
		return "histogram"
	case dto.MetricType_SUMMARY:
		return "summary"
	}
	return mt.String()
}

func familyLabels(mf *dto.MetricFamily) []string {
	if len(mf.GetMetric()) == 0 {
		return nil
	}
	var out []string
	for _, lp := range mf.GetMetric()[0].GetLabel() {
		out = append(out, lp.GetName())
	}
	sort.Strings(out)
	return out
}

// TestMetricsDocumented_ReferenceTableMatchesRegistry asserts, in both label
// modes, that every registered family is in the metric reference with its
// type and label names, and that every listed metric is registered.
func TestMetricsDocumented_ReferenceTableMatchesRegistry(t *testing.T) {
	rows := readMetricReference(t)
	for _, perShard := range []bool{false, true} {
		name := "aggregated"
		if perShard {
			name = "per_shard"
		}
		t.Run(name, func(t *testing.T) {
			registered := make(map[string]bool)
			for _, mf := range exercisedFamilies(t, perShard) {
				registered[mf.GetName()] = true
				row, ok := rows[mf.GetName()]
				if !ok {
					t.Errorf("%s is registered but not in %s §Metric reference", mf.GetName(), metricsReferenceDoc)
					continue
				}
				if got := promTypeName(mf.GetType()); got != row.typ {
					t.Errorf("%s: registered type %s, documented %s", mf.GetName(), got, row.typ)
				}
				got := strings.Join(familyLabels(mf), ",")
				want := strings.Join(documentedLabels(row, perShard), ",")
				if got != want {
					t.Errorf("%s: registered labels [%s], documented [%s] (cell %q)", mf.GetName(), got, want, row.labels)
				}
			}
			for n := range rows {
				if !registered[n] {
					t.Errorf("%s is documented but not registered (or not exercised)", n)
				}
			}
		})
	}
}

var documentedMetricName = regexp.MustCompile(`quicraft_[a-z0-9_]+`)

// TestMetricsDocumented_EveryMentionedNameIsRegistered asserts that every
// quicraft_* metric name any observability document mentions is a family the
// reference collector registers, so a renamed metric cannot linger in prose.
func TestMetricsDocumented_EveryMentionedNameIsRegistered(t *testing.T) {
	registered := make(map[string]bool)
	for _, perShard := range []bool{false, true} {
		for _, mf := range exercisedFamilies(t, perShard) {
			registered[mf.GetName()] = true
		}
	}
	docs, err := filepath.Glob(filepath.Join(metricsDocDir, "*.md"))
	if err != nil || len(docs) == 0 {
		t.Fatalf("no observability documents under %s: %v", metricsDocDir, err)
	}
	for _, path := range docs {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		for _, m := range documentedMetricName.FindAllString(string(b), -1) {
			base := m
			for _, sfx := range []string{"_bucket", "_sum", "_count"} {
				if trimmed := strings.TrimSuffix(m, sfx); trimmed != m && registered[trimmed] {
					base = trimmed
				}
			}
			if !registered[base] {
				t.Errorf("%s mentions %s, which the collector does not register", filepath.Base(path), m)
			}
		}
	}
}
