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

package perfprofile

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"strings"
)

// NotCaptured is what a field renders as when capture-env.sh could not obtain
// it on the target. A field that is absent must look absent: a blank table cell
// reads as "nothing there", and a defaulted zero reads as a measurement.
const NotCaptured = "**not captured**"

// EnvFile is the name of the captured machine description inside a profile.
const EnvFile = "env.json"

// Env is the machine a profile's numbers were measured on, captured on the
// target by capture-env.sh at run time rather than written down by hand, so the
// specification table beside a result can never go stale.
//
// Every field is a string, including the counts. A field that could not be
// obtained is the empty string and renders as NotCaptured; had the core count
// been an int, an uncaptured value and a genuine zero would be the same byte on
// disk, and the table would state a hardware fact nobody measured.
type Env struct {
	// Profile is the operator-supplied profile name this capture belongs to.
	// It cannot be inferred from the hardware, so it is passed in.
	Profile string `json:"profile"`
	// CapturedAt is the RFC3339 date-time the capture ran on the target.
	CapturedAt string `json:"captured_at"`
	Hostname   string `json:"hostname"`

	CPUModel      string `json:"cpu_model"`
	Sockets       string `json:"sockets"`
	PhysicalCores string `json:"physical_cores"`
	Threads       string `json:"threads"`
	NUMANodes     string `json:"numa_nodes"`
	MemoryTotal   string `json:"memory_total"`

	OSName    string `json:"os_name"`
	OSVersion string `json:"os_version"`
	Kernel    string `json:"kernel"`

	DockerVersion string `json:"docker_version"`
	GoVersion     string `json:"go_version"`
}

// envField binds a display label to the field it reads. Rendering and the
// missing-field report share this one list, so a field can never be shown in
// the table but omitted from the completeness check.
type envField struct {
	label string
	value func(*Env) string
}

var envFields = []envField{
	{"Profile", func(e *Env) string { return e.Profile }},
	{"Hostname", func(e *Env) string { return e.Hostname }},
	{"CPU", func(e *Env) string { return e.CPUModel }},
	{"Sockets", func(e *Env) string { return e.Sockets }},
	{"Physical cores", func(e *Env) string { return e.PhysicalCores }},
	{"Threads", func(e *Env) string { return e.Threads }},
	{"NUMA nodes", func(e *Env) string { return e.NUMANodes }},
	{"Memory", func(e *Env) string { return e.MemoryTotal }},
	{"OS", func(e *Env) string { return e.OSName }},
	{"OS version", func(e *Env) string { return e.OSVersion }},
	{"Kernel", func(e *Env) string { return e.Kernel }},
	{"Docker", func(e *Env) string { return e.DockerVersion }},
	{"Go", func(e *Env) string { return e.GoVersion }},
	{"Specs captured", func(e *Env) string { return e.CapturedAt }},
}

// LoadEnv reads a captured machine description. A file that is not there is
// reported as *EnvNotCapturedError so a caller can distinguish "this profile
// has never been captured" from "this profile's capture is unreadable".
func LoadEnv(path string) (*Env, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, &EnvNotCapturedError{Path: path}
		}
		return nil, err
	}
	var env Env
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, &EnvParseError{Path: path, Detail: err.Error()}
	}
	return &env, nil
}

// Save writes the machine description as indented JSON.
func (e *Env) Save(path string) error {
	data, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0644)
}

// Missing returns the labels of every field the capture did not obtain, in
// table order. An empty slice means the machine is fully described.
func (e *Env) Missing() []string {
	var missing []string
	for _, f := range envFields {
		if strings.TrimSpace(f.value(e)) == "" {
			missing = append(missing, f.label)
		}
	}
	return missing
}

// MarkdownTable renders the machine description as a two-column table, with
// every uncaptured field spelled out as NotCaptured rather than left blank.
func (e *Env) MarkdownTable() string {
	var sb strings.Builder
	sb.WriteString("| Component | Value |\n|---|---|\n")
	for _, f := range envFields {
		value := strings.TrimSpace(f.value(e))
		if value == "" {
			value = NotCaptured
		}
		sb.WriteString("| " + f.label + " | " + value + " |\n")
	}
	return sb.String()
}

// Summary is the one-line hardware description carried inline by every
// generated document, so a reader never has to click through to learn which
// machine produced a number.
func (e *Env) Summary() string {
	parts := []string{
		field(e.CPUModel, "CPU"),
		field(e.Sockets, "sockets") + " socket(s)",
		field(e.Threads, "threads") + " threads",
		field(e.MemoryTotal, "memory"),
		strings.TrimSpace(field(e.OSName, "OS") + " " + field(e.OSVersion, "OS version")),
		"kernel " + field(e.Kernel, "kernel"),
	}
	return strings.Join(parts, ", ")
}

// field returns the value, or a visible marker naming what is absent.
func field(value, what string) string {
	if strings.TrimSpace(value) == "" {
		return "(" + what + " not captured)"
	}
	return strings.TrimSpace(value)
}

// UncapturedSummary is the stand-in used by a generated document when the
// profile has no env.json at all.
const UncapturedSummary = "**machine specifications were never captured for this profile**"
