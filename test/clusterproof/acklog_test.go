//go:build integration

package clusterproof

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
)

// ackLog is the writers' durable record of acknowledged writes. A writer
// appends a write and fsyncs the file BEFORE it counts the write as
// acknowledged, so the record never claims less than the writers saw, and
// the checker reads its expectations back from the file, not from memory.
type ackLog struct {
	mu   sync.Mutex
	f    *os.File
	path string
	// acked mirrors the file for the readers, which pick keys from it; it
	// only ever holds lines that are already fsynced.
	acked map[string]string
	order []string
}

func openAckLog(path string) (*ackLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	return &ackLog{f: f, path: path, acked: map[string]string{}}, nil
}

// record durably logs put(key, value) as acknowledged: the line is written
// and fsynced before record returns, and only then is it visible to the
// readers.
func (l *ackLog) record(key, value string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := fmt.Fprintf(l.f, "ack %s %s\n", key, value); err != nil {
		return err
	}
	if err := l.f.Sync(); err != nil {
		return err
	}
	l.acked[key] = value
	l.order = append(l.order, key)
	return nil
}

// pick returns the i-th acknowledged write modulo the count, and false when
// there is none yet.
func (l *ackLog) pick(i int) (string, string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.order) == 0 {
		return "", "", false
	}
	k := l.order[i%len(l.order)]
	return k, l.acked[k], true
}

func (l *ackLog) close() error { return l.f.Close() }

// loadAcks reads an ack log back. Each write is acknowledged at most once
// (keys are unique), so a repeated key with a different value is corruption.
func loadAcks(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	acks := map[string]string{}
	sc := bufio.NewScanner(f)
	for n := 1; sc.Scan(); n++ {
		fields := strings.Fields(sc.Text())
		if len(fields) != 3 || fields[0] != "ack" {
			return nil, fmt.Errorf("%s:%d: malformed ack line %q", path, n, sc.Text())
		}
		if old, ok := acks[fields[1]]; ok && old != fields[2] {
			return nil, fmt.Errorf("%s:%d: key %s acknowledged as %q and %q", path, n, fields[1], old, fields[2])
		}
		acks[fields[1]] = fields[2]
	}
	return acks, sc.Err()
}

// missingAcks lists, sorted, every acknowledged write that state does not
// hold with the acknowledged value. Keys are unique per write, so a replica
// holding a different value has lost or corrupted the acknowledged one.
func missingAcks(acks, state map[string]string) []string {
	var out []string
	for k, v := range acks {
		got, ok := state[k]
		switch {
		case !ok:
			out = append(out, k+" (absent)")
		case got != v:
			out = append(out, fmt.Sprintf("%s (holds %q, acknowledged %q)", k, got, v))
		}
	}
	sort.Strings(out)
	return out
}

// TestClusterproof_Checker pins the checker the harness relies on: a lost or
// altered acknowledged write is reported, and an unacknowledged extra write
// (one that committed after its client gave up) is not.
func TestClusterproof_Checker(t *testing.T) {
	path := t.TempDir() + "/acks"
	l, err := openAckLog(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		if err := l.record(kv[0], kv[1]); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.close(); err != nil {
		t.Fatal(err)
	}
	acks, err := loadAcks(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(acks) != 3 {
		t.Fatalf("loaded %d acks, want 3", len(acks))
	}
	if m := missingAcks(acks, map[string]string{"a": "1", "b": "2", "c": "3", "d": "4"}); len(m) != 0 {
		t.Fatalf("complete state reported missing %v", m)
	}
	m := missingAcks(acks, map[string]string{"a": "1", "c": "9"})
	want := []string{"b (absent)", `c (holds "9", acknowledged "3")`}
	if fmt.Sprint(m) != fmt.Sprint(want) {
		t.Fatalf("missingAcks = %v, want %v", m, want)
	}
}
