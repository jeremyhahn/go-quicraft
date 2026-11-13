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

package config

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

// captureSafetyFlagWarnings swaps slog's default logger for one writing JSON
// records at Warn level and above to a buffer, restoring the previous logger
// with t.Cleanup. Tests that call it must not run in parallel.
func captureSafetyFlagWarnings(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// safetyFlagWarnings returns the message of every WARN record in buf.
func safetyFlagWarnings(t *testing.T, buf *bytes.Buffer) []string {
	t.Helper()
	var msgs []string
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec struct {
			Level string `json:"level"`
			Msg   string `json:"msg"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("decode log record %q: %v", line, err)
		}
		if rec.Level == slog.LevelWarn.String() {
			msgs = append(msgs, rec.Msg)
		}
	}
	return msgs
}

func TestSafetyFlag_GetPreVote(t *testing.T) {
	cases := []struct {
		name string
		v    *bool
		want bool
	}{
		{"nil", nil, true},
		{"true", BoolPtr(true), true},
		{"false", BoolPtr(false), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := Config{PreVote: tc.v}
			if got := c.GetPreVote(); got != tc.want {
				t.Fatalf("GetPreVote() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSafetyFlag_GetOrderedConfigChange(t *testing.T) {
	cases := []struct {
		name string
		v    *bool
		want bool
	}{
		{"nil", nil, true},
		{"true", BoolPtr(true), true},
		{"false", BoolPtr(false), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := Config{OrderedConfigChange: tc.v}
			if got := c.GetOrderedConfigChange(); got != tc.want {
				t.Fatalf("GetOrderedConfigChange() = %v, want %v", got, tc.want)
			}
		})
	}
}

// safetyFlagConfig returns a valid Config with the three safety flags set to
// the given pointers.
func safetyFlagConfig(checkQuorum, preVote, ordered *bool) Config {
	c := validConfig()
	c.CheckQuorum = checkQuorum
	c.PreVote = preVote
	c.OrderedConfigChange = ordered
	return c
}

func TestSafetyFlag_ValidateNoWarningWhenEnabled(t *testing.T) {
	cases := []struct {
		name string
		v    func() *bool
	}{
		{"nil", func() *bool { return nil }},
		{"true", func() *bool { return BoolPtr(true) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := captureSafetyFlagWarnings(t)
			c := safetyFlagConfig(tc.v(), tc.v(), tc.v())
			if err := c.Validate(); err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
			if got := safetyFlagWarnings(t, buf); len(got) != 0 {
				t.Fatalf("Validate() warned %q, want no warning", got)
			}
		})
	}
}

func TestSafetyFlag_ValidateWarnsPerDisabledFlag(t *testing.T) {
	off := func() *bool { return BoolPtr(false) }
	cases := []struct {
		name  string
		cfg   Config
		flags []string
	}{
		{"CheckQuorum", safetyFlagConfig(off(), nil, nil), []string{"CheckQuorum"}},
		{"PreVote", safetyFlagConfig(nil, off(), nil), []string{"PreVote"}},
		{"OrderedConfigChange", safetyFlagConfig(nil, nil, off()), []string{"OrderedConfigChange"}},
		{"all", safetyFlagConfig(off(), off(), off()), []string{"CheckQuorum", "PreVote", "OrderedConfigChange"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := captureSafetyFlagWarnings(t)
			if err := tc.cfg.Validate(); err != nil {
				t.Fatalf("Validate() = %v, want nil: a disabled flag is a warning, not an error", err)
			}
			got := safetyFlagWarnings(t, buf)
			if len(got) != len(tc.flags) {
				t.Fatalf("Validate() warned %d times %q, want %d", len(got), got, len(tc.flags))
			}
			for _, flag := range tc.flags {
				n := 0
				for _, msg := range got {
					if strings.Contains(msg, flag) {
						n++
					}
				}
				if n != 1 {
					t.Fatalf("%d warnings name %s in %q, want exactly 1", n, flag, got)
				}
			}
		})
	}
}

func TestSafetyFlag_HostConfigNoSyncWAL(t *testing.T) {
	cases := []struct {
		name      string
		noSyncWAL bool
		want      int
	}{
		{"true", true, 1},
		{"false", false, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := captureSafetyFlagWarnings(t)
			hc := validHostConfig()
			hc.NoSyncWAL = tc.noSyncWAL
			if err := hc.Validate(); err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
			got := safetyFlagWarnings(t, buf)
			if len(got) != tc.want {
				t.Fatalf("Validate() warned %q, want %d warning(s)", got, tc.want)
			}
			for _, msg := range got {
				if !strings.Contains(msg, "NoSyncWAL") {
					t.Fatalf("warning %q does not name NoSyncWAL", msg)
				}
			}
		})
	}
}
