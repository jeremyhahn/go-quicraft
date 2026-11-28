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
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/writemode"
)

// durabilityWeakeningFields lists every HostConfig bool, by dotted field path,
// that lets a write be acknowledged before a quorum has fsynced it. Each must
// default to false, so weakening durability is always opt-in (#1303);
// docs/durable-by-default/README.md says what each gives up.
var durabilityWeakeningFields = map[string]string{
	"NoSyncWAL":    "WAL writes are not fsynced; refused outside test binaries",
	"NotifyCommit": "CommittedC fires after the local WAL persist, before quorum commit",
}

// durabilityNeutralFields lists every other HostConfig bool. A bool in neither
// map fails TestHostConfig_EveryBoolIsClassified until someone decides, and
// records here, whether it weakens durability.
var durabilityNeutralFields = map[string]string{
	"AllowZeroDeploymentID":                          "cluster identity check",
	"TransportConfig.Enable0RTT":                     "handshake latency; early data is refused",
	"TransportConfig.DisableCompression":             "wire compression",
	"TransportConfig.TransportDisabled":              "no transport at all",
	"TransportConfig.RevocationConfig.AllowSoftFail": "certificate revocation",
}

// hostConfigFields walks t and returns the dotted path of every bool field and
// of every writemode.Mode field reachable through nested structs and pointers
// to structs of this package.
func hostConfigFields(t reflect.Type, prefix string, seen map[reflect.Type]bool, bools, modes *[]string) {
	if seen[t] {
		return
	}
	seen[t] = true
	defer delete(seen, t)
	modeType := reflect.TypeOf(writemode.Mode(0))
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		path := prefix + f.Name
		ft := f.Type
		if ft == modeType {
			*modes = append(*modes, path)
			continue
		}
		if ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		switch ft.Kind() {
		case reflect.Bool:
			*bools = append(*bools, path)
		case reflect.Struct:
			// Only this package's own config structs: a foreign type such
			// as tls.Certificate carries no quicraft setting.
			if ft.PkgPath() != t.PkgPath() {
				continue
			}
			hostConfigFields(ft, path+".", seen, bools, modes)
		}
	}
}

func hostConfigFieldPaths() (bools, modes []string) {
	hostConfigFields(reflect.TypeOf(HostConfig{}), "", map[reflect.Type]bool{}, &bools, &modes)
	sort.Strings(bools)
	sort.Strings(modes)
	return bools, modes
}

// fieldByPath reads a dotted field path from v. ok is false when a nil pointer
// on the path means the field is unset.
func fieldByPath(v reflect.Value, path string) (reflect.Value, bool) {
	for _, name := range strings.Split(path, ".") {
		if v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return reflect.Value{}, false
			}
			v = v.Elem()
		}
		v = v.FieldByName(name)
	}
	return v, true
}

// TestHostConfig_EveryBoolIsClassified is the reflection guard: a new
// HostConfig bool, at any depth, must be listed as durability-weakening or
// durability-neutral, and a listed path must still exist.
func TestHostConfig_EveryBoolIsClassified(t *testing.T) {
	bools, _ := hostConfigFieldPaths()
	present := map[string]bool{}
	for _, path := range bools {
		present[path] = true
		_, weak := durabilityWeakeningFields[path]
		_, neutral := durabilityNeutralFields[path]
		if weak == neutral {
			t.Errorf("HostConfig bool %s is classified weakening=%v neutral=%v; "+
				"list it in exactly one of durabilityWeakeningFields or "+
				"durabilityNeutralFields", path, weak, neutral)
		}
	}
	for _, m := range []map[string]string{durabilityWeakeningFields, durabilityNeutralFields} {
		for path := range m {
			if !present[path] {
				t.Errorf("classified path %s is not a HostConfig bool", path)
			}
		}
	}
}

// TestHostConfig_DefaultIsQuorumDurable pins durable-by-default (#1303): the
// zero HostConfig, before and after SetDefaults, enables no durability-
// weakening bool and carries only quorum-durable write modes.
func TestHostConfig_DefaultIsQuorumDurable(t *testing.T) {
	_, modes := hostConfigFieldPaths()
	zero := HostConfig{}
	defaulted := HostConfig{}
	defaulted.SetDefaults()
	valid := validHostConfig()
	for name, hc := range map[string]*HostConfig{
		"zero": &zero, "SetDefaults": &defaulted, "validHostConfig": &valid,
	} {
		v := reflect.ValueOf(hc).Elem()
		for path, gives := range durabilityWeakeningFields {
			if f, ok := fieldByPath(v, path); ok && f.Bool() {
				t.Errorf("%s HostConfig enables %s (%s); it must default to off",
					name, path, gives)
			}
		}
		for _, path := range modes {
			f, ok := fieldByPath(v, path)
			if !ok {
				continue
			}
			if m := writemode.Mode(f.Uint()); !m.RequiresQuorum() {
				t.Errorf("%s HostConfig.%s = %v, which does not require a quorum",
					name, path, m)
			}
		}
	}
	if zero.NoSyncWAL || defaulted.NoSyncWAL {
		t.Fatal("default HostConfig enables NoSyncWAL")
	}
}
