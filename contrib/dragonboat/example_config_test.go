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

package dragonboat_test

import (
	"path/filepath"
	"testing"

	dragonboat "github.com/jeremyhahn/go-quicraft/contrib/dragonboat"
	"github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

// TestExampleConfig_NewNodeHost builds the config ExampleNewNodeHost shows,
// with certificate files generated in a temporary directory in place of the
// example's placeholder paths, and checks that NewNodeHost accepts it. The
// address is kernel-assigned so the test cannot collide with another
// process on the example's fixed port.
func TestExampleConfig_NewNodeHost(t *testing.T) {
	dir := t.TempDir()
	tls := testpki.NewCA(t).LeafFiles(t, t.TempDir())

	nh, err := dragonboat.NewNodeHost(config.NodeHostConfig{
		DeploymentID:   1,
		WALDir:         filepath.Join(dir, "wal"),
		NodeHostDir:    filepath.Join(dir, "data"),
		RaftAddress:    "127.0.0.1:0",
		ListenAddress:  "127.0.0.1:0",
		RTTMillisecond: 200,
		MutualTLS:      true,
		CAFile:         tls.CAFile,
		CertFile:       tls.CertFile,
		KeyFile:        tls.KeyFile,
	})
	if err != nil {
		t.Fatalf("NewNodeHost: %v", err)
	}
	if nh.RaftAddress() == "" {
		t.Error("RaftAddress is empty")
	}
	if nh.NodeIdentity() == "" {
		t.Error("NodeIdentity is empty")
	}
	nh.Close()
}
