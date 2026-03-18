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
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestConfig_ToQuicraft_DefaultValues(t *testing.T) {
	c := Config{
		ShardID:   1,
		ReplicaID: 1,
	}

	qc := c.ToQuicraft()

	if qc.ShardID != 1 {
		t.Errorf("ShardID: got %d, want 1", qc.ShardID)
	}
	if qc.ReplicaID != 1 {
		t.Errorf("ReplicaID: got %d, want 1", qc.ReplicaID)
	}
	if qc.IsObserver {
		t.Error("IsObserver should be false by default")
	}
	if qc.IsWitness {
		t.Error("IsWitness should be false by default")
	}
	if qc.CheckQuorum == nil {
		t.Fatal("CheckQuorum should not be nil")
	}
	if *qc.CheckQuorum {
		t.Error("CheckQuorum: default dragonboat Config has CheckQuorum=false, expected BoolPtr(false)")
	}
	if qc.PreVote == nil {
		t.Fatal("PreVote should not be nil")
	}
	if *qc.PreVote {
		t.Error("PreVote: default dragonboat Config has PreVote=false, expected BoolPtr(false)")
	}
	if qc.OrderedConfigChange == nil {
		t.Fatal("OrderedConfigChange should not be nil")
	}
	if *qc.OrderedConfigChange {
		t.Error("OrderedConfigChange: default dragonboat Config has OrderedConfigChange=false, expected BoolPtr(false)")
	}
}

func TestConfig_ToQuicraft_AllFieldsMapped(t *testing.T) {
	c := Config{
		ShardID:                 42,
		ReplicaID:               7,
		CheckQuorum:             true,
		PreVote:                 true,
		ElectionRTT:             20,
		HeartbeatRTT:            2,
		SnapshotEntries:         10000,
		CompactionOverhead:      1000,
		OrderedConfigChange:     true,
		MaxInMemLogSize:         1024 * 1024,
		SnapshotCompressionType: Snappy,
		EntryCompressionType:    Snappy,
		Quiesce:                 true,
		MaxEntrySize:            4 * 1024 * 1024,
		IsWitness:               true,
	}

	qc := c.ToQuicraft()

	if qc.ShardID != 42 {
		t.Errorf("ShardID: got %d, want 42", qc.ShardID)
	}
	if qc.ReplicaID != 7 {
		t.Errorf("ReplicaID: got %d, want 7", qc.ReplicaID)
	}
	if qc.CheckQuorum == nil || !*qc.CheckQuorum {
		t.Error("CheckQuorum should be BoolPtr(true)")
	}
	if qc.PreVote == nil || !*qc.PreVote {
		t.Error("PreVote should be BoolPtr(true)")
	}
	if qc.ElectionRTT != 20 {
		t.Errorf("ElectionRTT: got %d, want 20", qc.ElectionRTT)
	}
	if qc.HeartbeatRTT != 2 {
		t.Errorf("HeartbeatRTT: got %d, want 2", qc.HeartbeatRTT)
	}
	if qc.SnapshotEntries != 10000 {
		t.Errorf("SnapshotEntries: got %d, want 10000", qc.SnapshotEntries)
	}
	if qc.CompactionOverhead != 1000 {
		t.Errorf("CompactionOverhead: got %d, want 1000", qc.CompactionOverhead)
	}
	if qc.OrderedConfigChange == nil || !*qc.OrderedConfigChange {
		t.Error("OrderedConfigChange should be BoolPtr(true)")
	}
	if qc.MaxInMemLogSize != 1024*1024 {
		t.Errorf("MaxInMemLogSize: got %d, want %d", qc.MaxInMemLogSize, 1024*1024)
	}
	if qc.SnapshotCompressionType != Snappy {
		t.Errorf("SnapshotCompressionType: got %v, want Snappy", qc.SnapshotCompressionType)
	}
	if qc.EntryCompressionType != Snappy {
		t.Errorf("EntryCompressionType: got %v, want Snappy", qc.EntryCompressionType)
	}
	if !qc.Quiesce {
		t.Error("Quiesce should be true")
	}
	if qc.MaxEntrySize != 4*1024*1024 {
		t.Errorf("MaxEntrySize: got %d, want %d", qc.MaxEntrySize, 4*1024*1024)
	}
	if !qc.IsWitness {
		t.Error("IsWitness should be true")
	}
}

func TestConfig_ToQuicraft_IsNonVotingMapsToObserver(t *testing.T) {
	tests := []struct {
		name        string
		isNonVoting bool
		isObserver  bool
		wantResult  bool
	}{
		{"IsNonVoting=true", true, false, true},
		{"IsObserver=true", false, true, true},
		{"BothTrue", true, true, true},
		{"BothFalse", false, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Config{
				ShardID:     1,
				ReplicaID:   1,
				IsNonVoting: tt.isNonVoting,
				IsObserver:  tt.isObserver,
			}
			qc := c.ToQuicraft()
			if qc.IsObserver != tt.wantResult {
				t.Errorf("IsObserver: got %v, want %v", qc.IsObserver, tt.wantResult)
			}
		})
	}
}

func TestConfig_ToQuicraft_DisableAutoCompactionsSetsSnapshotEntriesToZero(t *testing.T) {
	c := Config{
		ShardID:                1,
		ReplicaID:              1,
		SnapshotEntries:        10000,
		DisableAutoCompactions: true,
	}

	qc := c.ToQuicraft()
	if qc.SnapshotEntries != 0 {
		t.Errorf("SnapshotEntries: got %d, want 0 when DisableAutoCompactions=true", qc.SnapshotEntries)
	}
}

func TestConfig_ToQuicraft_DisableAutoCompactionsFalsePreservesSnapshotEntries(t *testing.T) {
	c := Config{
		ShardID:                1,
		ReplicaID:              1,
		SnapshotEntries:        10000,
		DisableAutoCompactions: false,
	}

	qc := c.ToQuicraft()
	if qc.SnapshotEntries != 10000 {
		t.Errorf("SnapshotEntries: got %d, want 10000", qc.SnapshotEntries)
	}
}

func TestConfig_Validate_ValidConfig(t *testing.T) {
	c := Config{
		ShardID:      1,
		ReplicaID:    1,
		ElectionRTT:  10,
		HeartbeatRTT: 1,
	}
	if err := c.Validate(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestConfig_Validate_ZeroShardID(t *testing.T) {
	c := Config{ReplicaID: 1}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for zero ShardID")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
	if ve.Field != "ShardID" {
		t.Errorf("Field: got %q, want %q", ve.Field, "ShardID")
	}
}

func TestConfig_Validate_ZeroReplicaID(t *testing.T) {
	c := Config{ShardID: 1}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for zero ReplicaID")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
	if ve.Field != "ReplicaID" {
		t.Errorf("Field: got %q, want %q", ve.Field, "ReplicaID")
	}
}

func TestConfig_Validate_ElectionRTTNotGreaterThanTwiceHeartbeat(t *testing.T) {
	c := Config{
		ShardID:      1,
		ReplicaID:    1,
		ElectionRTT:  4,
		HeartbeatRTT: 2,
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error when ElectionRTT <= 2 * HeartbeatRTT")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
	if ve.Field != "ElectionRTT" {
		t.Errorf("Field: got %q, want %q", ve.Field, "ElectionRTT")
	}
}

func TestConfig_Validate_IsNonVotingAndIsWitnessMutuallyExclusive(t *testing.T) {
	c := Config{
		ShardID:      1,
		ReplicaID:    1,
		HeartbeatRTT: 1,
		ElectionRTT:  10,
		IsNonVoting:  true,
		IsWitness:    true,
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error when IsNonVoting and IsWitness are both true")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
}

func TestConfig_Validate_IsObserverAndIsWitnessMutuallyExclusive(t *testing.T) {
	c := Config{
		ShardID:      1,
		ReplicaID:    1,
		HeartbeatRTT: 1,
		ElectionRTT:  10,
		IsObserver:   true,
		IsWitness:    true,
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error when IsObserver and IsWitness are both true")
	}
}

func TestConfig_Validate_SnapshotEntriesLessThanCompactionOverhead(t *testing.T) {
	c := Config{
		ShardID:            1,
		ReplicaID:          1,
		HeartbeatRTT:       1,
		ElectionRTT:        10,
		SnapshotEntries:    100,
		CompactionOverhead: 200,
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error when SnapshotEntries <= CompactionOverhead")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
	if ve.Field != "SnapshotEntries" {
		t.Errorf("Field: got %q, want %q", ve.Field, "SnapshotEntries")
	}
}

func TestNodeHostConfig_ToQuicraft_BasicFields(t *testing.T) {
	c := NodeHostConfig{
		DeploymentID:   99,
		WALDir:         "/tmp/wal",
		NodeHostDir:    "/tmp/nodehost",
		RTTMillisecond: 100,
		RaftAddress:    "localhost:63001",
		ListenAddress:  "0.0.0.0:63001",
		NotifyCommit:   true,
		Expert: ExpertConfig{
			Engine: EngineConfig{
				ExecShards:  8,
				ApplyShards: 4,
			},
		},
	}

	hc, err := c.ToQuicraft()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if hc.WALDir != "/tmp/wal" {
		t.Errorf("WALDir: got %q, want %q", hc.WALDir, "/tmp/wal")
	}
	if hc.NodeHostDir != "/tmp/nodehost" {
		t.Errorf("NodeHostDir: got %q, want %q", hc.NodeHostDir, "/tmp/nodehost")
	}
	if hc.RaftAddress != "localhost:63001" {
		t.Errorf("RaftAddress: got %q, want %q", hc.RaftAddress, "localhost:63001")
	}
	if hc.ListenAddress != "0.0.0.0:63001" {
		t.Errorf("ListenAddress: got %q, want %q", hc.ListenAddress, "0.0.0.0:63001")
	}
	if hc.RTTMillisecond != 100 {
		t.Errorf("RTTMillisecond: got %d, want 100", hc.RTTMillisecond)
	}
	if hc.DeploymentID != 99 {
		t.Errorf("DeploymentID: got %d, want 99", hc.DeploymentID)
	}
	if !hc.NotifyCommit {
		t.Error("NotifyCommit should be true")
	}
	if hc.NumWorkers != 8 {
		t.Errorf("NumWorkers: got %d, want 8 (from ExecShards)", hc.NumWorkers)
	}
	if hc.MaxApplyWorkers != 4 {
		t.Errorf("MaxApplyWorkers: got %d, want 4 (from ApplyShards)", hc.MaxApplyWorkers)
	}
	if hc.TransportConfig.MTLSConfig != nil {
		t.Error("MTLSConfig should be nil when MutualTLS is false")
	}
}

func TestNodeHostConfig_ToQuicraft_MutualTLS(t *testing.T) {
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")

	caData := []byte("--- CA CERT ---")
	certData := []byte("--- CERT ---")
	keyData := []byte("--- KEY ---")

	if err := os.WriteFile(caFile, caData, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certFile, certData, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyData, 0600); err != nil {
		t.Fatal(err)
	}

	c := NodeHostConfig{
		WALDir:        dir,
		NodeHostDir:   dir,
		RaftAddress:   "localhost:63001",
		ListenAddress: "0.0.0.0:63001",
		MutualTLS:     true,
		CAFile:        caFile,
		CertFile:      certFile,
		KeyFile:       keyFile,
	}

	hc, err := c.ToQuicraft()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if hc.TransportConfig.MTLSConfig == nil {
		t.Fatal("MTLSConfig should not be nil when MutualTLS is true")
	}
	if string(hc.TransportConfig.MTLSConfig.CACert) != string(caData) {
		t.Errorf("CACert mismatch")
	}
	if string(hc.TransportConfig.MTLSConfig.Cert) != string(certData) {
		t.Errorf("Cert mismatch")
	}
	if string(hc.TransportConfig.MTLSConfig.Key) != string(keyData) {
		t.Errorf("Key mismatch")
	}
}

func TestNodeHostConfig_ToQuicraft_MutualTLSMissingFile(t *testing.T) {
	c := NodeHostConfig{
		WALDir:        "/tmp/wal",
		NodeHostDir:   "/tmp/nodehost",
		RaftAddress:   "localhost:63001",
		ListenAddress: "0.0.0.0:63001",
		MutualTLS:     true,
		CAFile:        "/nonexistent/ca.pem",
		CertFile:      "/nonexistent/cert.pem",
		KeyFile:       "/nonexistent/key.pem",
	}

	_, err := c.ToQuicraft()
	if err == nil {
		t.Fatal("expected error for missing TLS files")
	}
	var fre *FileReadError
	if !errors.As(err, &fre) {
		t.Fatalf("expected *FileReadError, got %T: %v", err, err)
	}
}

func TestNodeHostConfig_ToQuicraft_ZeroExpertConfig(t *testing.T) {
	c := NodeHostConfig{
		WALDir:        "/tmp/wal",
		NodeHostDir:   "/tmp/nodehost",
		RaftAddress:   "localhost:63001",
		ListenAddress: "0.0.0.0:63001",
	}

	hc, err := c.ToQuicraft()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// When ExecShards and ApplyShards are 0, NumWorkers and
	// MaxApplyWorkers should remain at their zero values (quicraft
	// SetDefaults will fill them later).
	if hc.NumWorkers != 0 {
		t.Errorf("NumWorkers: got %d, want 0 (let quicraft SetDefaults handle it)", hc.NumWorkers)
	}
	if hc.MaxApplyWorkers != 0 {
		t.Errorf("MaxApplyWorkers: got %d, want 0 (let quicraft SetDefaults handle it)", hc.MaxApplyWorkers)
	}
}

func TestNodeHostConfig_Validate_ValidConfig(t *testing.T) {
	c := NodeHostConfig{
		WALDir:         "/tmp/wal",
		NodeHostDir:    "/tmp/nodehost",
		RaftAddress:    "localhost:63001",
		ListenAddress:  "0.0.0.0:63001",
		RTTMillisecond: 100,
	}
	if err := c.Validate(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestNodeHostConfig_Validate_ZeroRTTMillisecond(t *testing.T) {
	c := NodeHostConfig{
		WALDir:      "/tmp/wal",
		NodeHostDir: "/tmp/nodehost",
		RaftAddress: "localhost:63001",
		// RTTMillisecond is zero.
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for zero RTTMillisecond")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
	if ve.Field != "RTTMillisecond" {
		t.Errorf("Field: got %q, want %q", ve.Field, "RTTMillisecond")
	}
}

func TestNodeHostConfig_Validate_PositiveRTTMillisecond(t *testing.T) {
	c := NodeHostConfig{
		WALDir:         "/tmp/wal",
		NodeHostDir:    "/tmp/nodehost",
		RaftAddress:    "localhost:63001",
		RTTMillisecond: 200,
	}
	if err := c.Validate(); err != nil {
		t.Errorf("unexpected error for valid RTTMillisecond: %v", err)
	}
}

func TestNodeHostConfig_Validate_EmptyWALDir(t *testing.T) {
	c := NodeHostConfig{
		NodeHostDir:   "/tmp/nodehost",
		RaftAddress:   "localhost:63001",
		ListenAddress: "0.0.0.0:63001",
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for empty WALDir")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
	if ve.Field != "WALDir" {
		t.Errorf("Field: got %q, want %q", ve.Field, "WALDir")
	}
}

func TestNodeHostConfig_Validate_EmptyNodeHostDir(t *testing.T) {
	c := NodeHostConfig{
		WALDir:        "/tmp/wal",
		RaftAddress:   "localhost:63001",
		ListenAddress: "0.0.0.0:63001",
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for empty NodeHostDir")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
	if ve.Field != "NodeHostDir" {
		t.Errorf("Field: got %q, want %q", ve.Field, "NodeHostDir")
	}
}

func TestNodeHostConfig_Validate_EmptyRaftAddress(t *testing.T) {
	c := NodeHostConfig{
		WALDir:        "/tmp/wal",
		NodeHostDir:   "/tmp/nodehost",
		ListenAddress: "0.0.0.0:63001",
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for empty RaftAddress")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
	if ve.Field != "RaftAddress" {
		t.Errorf("Field: got %q, want %q", ve.Field, "RaftAddress")
	}
}

func TestNodeHostConfig_Validate_EmptyListenAddress(t *testing.T) {
	c := NodeHostConfig{
		WALDir:         "/tmp/wal",
		NodeHostDir:    "/tmp/nodehost",
		RaftAddress:    "localhost:63001",
		RTTMillisecond: 100,
	}
	if err := c.Validate(); err != nil {
		t.Errorf("empty ListenAddress should be valid (defaults to RaftAddress): %v", err)
	}
}

func TestNodeHostConfig_Validate_MutualTLSMissingCAFile(t *testing.T) {
	c := NodeHostConfig{
		WALDir:         "/tmp/wal",
		NodeHostDir:    "/tmp/nodehost",
		RaftAddress:    "localhost:63001",
		ListenAddress:  "0.0.0.0:63001",
		RTTMillisecond: 100,
		MutualTLS:      true,
		CertFile:       "/tmp/cert.pem",
		KeyFile:        "/tmp/key.pem",
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for empty CAFile with MutualTLS=true")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
	if ve.Field != "CAFile" {
		t.Errorf("Field: got %q, want %q", ve.Field, "CAFile")
	}
}

func TestNodeHostConfig_Validate_MutualTLSMissingCertFile(t *testing.T) {
	c := NodeHostConfig{
		WALDir:         "/tmp/wal",
		NodeHostDir:    "/tmp/nodehost",
		RaftAddress:    "localhost:63001",
		ListenAddress:  "0.0.0.0:63001",
		RTTMillisecond: 100,
		MutualTLS:      true,
		CAFile:         "/tmp/ca.pem",
		KeyFile:        "/tmp/key.pem",
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for empty CertFile with MutualTLS=true")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
	if ve.Field != "CertFile" {
		t.Errorf("Field: got %q, want %q", ve.Field, "CertFile")
	}
}

func TestNodeHostConfig_Validate_MutualTLSMissingKeyFile(t *testing.T) {
	c := NodeHostConfig{
		WALDir:         "/tmp/wal",
		NodeHostDir:    "/tmp/nodehost",
		RaftAddress:    "localhost:63001",
		ListenAddress:  "0.0.0.0:63001",
		RTTMillisecond: 100,
		MutualTLS:      true,
		CAFile:         "/tmp/ca.pem",
		CertFile:       "/tmp/cert.pem",
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for empty KeyFile with MutualTLS=true")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
	if ve.Field != "KeyFile" {
		t.Errorf("Field: got %q, want %q", ve.Field, "KeyFile")
	}
}

// ---------------------------------------------------------------------------
// Config: Witness with SnapshotEntries (matching dragonboat patterns)
// ---------------------------------------------------------------------------

func TestConfig_Validate_WitnessWithSnapshotEntries(t *testing.T) {
	// Witness replicas do not maintain a state machine, so SnapshotEntries
	// must be 0 per dragonboat v4 validation.
	c := Config{
		ShardID:         1,
		ReplicaID:       1,
		HeartbeatRTT:    1,
		ElectionRTT:     10,
		IsWitness:       true,
		SnapshotEntries: 1000,
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for witness with SnapshotEntries > 0")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
	if ve.Field != "SnapshotEntries" {
		t.Errorf("Field: got %q, want %q", ve.Field, "SnapshotEntries")
	}
}

func TestConfig_Validate_WitnessAndNonVotingConflict(t *testing.T) {
	c := Config{
		ShardID:      1,
		ReplicaID:    1,
		HeartbeatRTT: 1,
		ElectionRTT:  10,
		IsWitness:    true,
		IsNonVoting:  true,
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error when IsWitness and IsNonVoting are both true")
	}
}

// ---------------------------------------------------------------------------
// NodeHostConfig: ListenAddress mapping
// ---------------------------------------------------------------------------

func TestNodeHostConfig_ToQuicraft_ListenAddress(t *testing.T) {
	c := NodeHostConfig{
		WALDir:        "/tmp/wal",
		NodeHostDir:   "/tmp/nodehost",
		RaftAddress:   "localhost:63001",
		ListenAddress: "0.0.0.0:63002",
	}

	hc, err := c.ToQuicraft()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hc.ListenAddress != "0.0.0.0:63002" {
		t.Errorf("ListenAddress: got %q, want %q", hc.ListenAddress, "0.0.0.0:63002")
	}
}

func TestNodeHostConfig_GetListenAddress_FallsBackToRaftAddress(t *testing.T) {
	c := &NodeHostConfig{
		RaftAddress: "localhost:63001",
		// ListenAddress not set.
	}
	if c.GetListenAddress() != "localhost:63001" {
		t.Errorf("GetListenAddress() = %q, want %q", c.GetListenAddress(), "localhost:63001")
	}
}

func TestNodeHostConfig_GetListenAddress_UsesListenAddress(t *testing.T) {
	c := &NodeHostConfig{
		RaftAddress:   "localhost:63001",
		ListenAddress: "0.0.0.0:63002",
	}
	if c.GetListenAddress() != "0.0.0.0:63002" {
		t.Errorf("GetListenAddress() = %q, want %q", c.GetListenAddress(), "0.0.0.0:63002")
	}
}

// ---------------------------------------------------------------------------
// NodeHostConfig: DeploymentID mapping
// ---------------------------------------------------------------------------

func TestNodeHostConfig_GetDeploymentID(t *testing.T) {
	c := &NodeHostConfig{DeploymentID: 42}
	if c.GetDeploymentID() != 42 {
		t.Errorf("GetDeploymentID() = %d, want 42", c.GetDeploymentID())
	}
}

// ---------------------------------------------------------------------------
// NodeHostConfig: Prepare is alias for Validate
// ---------------------------------------------------------------------------

func TestNodeHostConfig_Prepare_IsValidateAlias(t *testing.T) {
	// Valid config should pass Prepare.
	c := NodeHostConfig{
		WALDir:         "/tmp/wal",
		NodeHostDir:    "/tmp/nodehost",
		RaftAddress:    "localhost:63001",
		ListenAddress:  "0.0.0.0:63001",
		RTTMillisecond: 100,
	}
	if err := c.Prepare(); err != nil {
		t.Errorf("Prepare() unexpected error: %v", err)
	}

	// Invalid config should fail Prepare.
	invalid := NodeHostConfig{}
	if err := invalid.Prepare(); err == nil {
		t.Error("Prepare() should fail for invalid config")
	}
}

// ---------------------------------------------------------------------------
// NodeHostConfig: NodeRegistryEnabled
// ---------------------------------------------------------------------------

func TestNodeHostConfig_NodeRegistryEnabled(t *testing.T) {
	c := &NodeHostConfig{DefaultNodeRegistryEnabled: true}
	if !c.NodeRegistryEnabled() {
		t.Error("NodeRegistryEnabled() should return true")
	}

	c2 := &NodeHostConfig{DefaultNodeRegistryEnabled: false}
	if c2.NodeRegistryEnabled() {
		t.Error("NodeRegistryEnabled() should return false")
	}
}

// ---------------------------------------------------------------------------
// NodeHostConfig: NotifyCommit mapping
// ---------------------------------------------------------------------------

func TestNodeHostConfig_ToQuicraft_NotifyCommit(t *testing.T) {
	c := NodeHostConfig{
		WALDir:        "/tmp/wal",
		NodeHostDir:   "/tmp/nodehost",
		RaftAddress:   "localhost:63001",
		ListenAddress: "0.0.0.0:63001",
		NotifyCommit:  true,
	}
	hc, err := c.ToQuicraft()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hc.NotifyCommit {
		t.Error("NotifyCommit should be true")
	}
}

// ---------------------------------------------------------------------------
// Config: ElectionRTT == 0 and HeartbeatRTT == 0 (both zero is valid)
// ---------------------------------------------------------------------------

func TestConfig_Validate_ZeroHeartbeatRTT(t *testing.T) {
	c := Config{
		ShardID:     1,
		ReplicaID:   1,
		ElectionRTT: 10,
		// HeartbeatRTT zero is rejected per dragonboat v4.
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for zero HeartbeatRTT")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
	if ve.Field != "HeartbeatRTT" {
		t.Errorf("Field: got %q, want %q", ve.Field, "HeartbeatRTT")
	}
}

func TestConfig_Validate_ZeroElectionRTT(t *testing.T) {
	c := Config{
		ShardID:      1,
		ReplicaID:    1,
		HeartbeatRTT: 1,
		// ElectionRTT zero is rejected per dragonboat v4.
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for zero ElectionRTT")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
	if ve.Field != "ElectionRTT" {
		t.Errorf("Field: got %q, want %q", ve.Field, "ElectionRTT")
	}
}

// ---------------------------------------------------------------------------
// GossipConfig validation
// ---------------------------------------------------------------------------

func TestGossipConfig_IsEmpty(t *testing.T) {
	g := GossipConfig{}
	if !g.IsEmpty() {
		t.Error("empty GossipConfig should report IsEmpty=true")
	}
}

func TestGossipConfig_IsNotEmpty(t *testing.T) {
	g := GossipConfig{BindAddress: "0.0.0.0:7946"}
	if g.IsEmpty() {
		t.Error("non-empty GossipConfig should report IsEmpty=false")
	}
}

func TestGossipConfig_Validate_Empty(t *testing.T) {
	g := GossipConfig{}
	if err := g.Validate(); err != nil {
		t.Errorf("empty gossip config should validate: %v", err)
	}
}

func TestGossipConfig_Validate_MissingBindAddress(t *testing.T) {
	g := GossipConfig{AdvertiseAddress: "1.2.3.4:7946"}
	err := g.Validate()
	if err == nil {
		t.Fatal("expected error for gossip config with AdvertiseAddress but no BindAddress")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
}

func TestNodeHostConfig_Validate_InvalidGossip(t *testing.T) {
	c := NodeHostConfig{
		WALDir:         "/tmp/wal",
		NodeHostDir:    "/tmp/nodehost",
		RaftAddress:    "localhost:63001",
		ListenAddress:  "0.0.0.0:63001",
		RTTMillisecond: 100,
		Gossip: GossipConfig{
			AdvertiseAddress: "1.2.3.4:7946",
			// BindAddress missing.
		},
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for invalid gossip config")
	}
}

// ---------------------------------------------------------------------------
// IsValidAddress
// ---------------------------------------------------------------------------

func TestIsValidAddress_Valid(t *testing.T) {
	tests := []string{
		"localhost:8080",
		"0.0.0.0:9090",
		"192.168.1.1:63001",
		"[::1]:8080",
	}
	for _, addr := range tests {
		if !IsValidAddress(addr) {
			t.Errorf("IsValidAddress(%q) = false, want true", addr)
		}
	}
}

func TestIsValidAddress_Invalid(t *testing.T) {
	tests := []string{
		"",
		"localhost",
		":8080",
	}
	for _, addr := range tests {
		if addr == ":8080" {
			// ":8080" is actually valid per net.SplitHostPort (empty host).
			continue
		}
		if IsValidAddress(addr) {
			t.Errorf("IsValidAddress(%q) = true, want false", addr)
		}
	}
}

// ---------------------------------------------------------------------------
// Default config functions
// ---------------------------------------------------------------------------

func TestGetDefaultEngineConfig(t *testing.T) {
	ec := GetDefaultEngineConfig()
	if ec.ExecShards == 0 {
		t.Error("ExecShards should be non-zero")
	}
	if ec.IsEmpty() {
		t.Error("default engine config should not be empty")
	}
}

func TestGetDefaultExpertConfig(t *testing.T) {
	ec := GetDefaultExpertConfig()
	if ec.Engine.IsEmpty() {
		t.Error("default expert config should have non-empty engine")
	}
}

func TestGetDefaultLogDBConfig(t *testing.T) {
	lc := GetDefaultLogDBConfig()
	if lc.Shards == 0 {
		t.Error("default LogDB config should have non-zero Shards")
	}
	if lc.IsEmpty() {
		t.Error("default LogDB config should not be empty")
	}
}

func TestLogDBConfig_IsEmpty(t *testing.T) {
	empty := LogDBConfig{}
	if !empty.IsEmpty() {
		t.Error("zero-value LogDBConfig should be empty")
	}

	nonEmpty := LogDBConfig{Shards: 1}
	if nonEmpty.IsEmpty() {
		t.Error("LogDBConfig with Shards=1 should not be empty")
	}
}

func TestLogDBConfig_MemorySizeMB(t *testing.T) {
	lc := GetDefaultLogDBConfig()
	// Placeholder always returns 0.
	if lc.MemorySizeMB() != 0 {
		t.Errorf("MemorySizeMB() = %d, want 0", lc.MemorySizeMB())
	}
}

func TestEngineConfig_IsEmpty(t *testing.T) {
	empty := EngineConfig{}
	if !empty.IsEmpty() {
		t.Error("zero-value EngineConfig should be empty")
	}
}

func TestEngineConfig_Validate(t *testing.T) {
	ec := GetDefaultEngineConfig()
	if err := ec.Validate(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Memory LogDB config presets
// ---------------------------------------------------------------------------

func TestLogDBConfigPresets(t *testing.T) {
	presets := []struct {
		name string
		fn   func() LogDBConfig
	}{
		{"Tiny", GetTinyMemLogDBConfig},
		{"Small", GetSmallMemLogDBConfig},
		{"Medium", GetMediumMemLogDBConfig},
		{"Large", GetLargeMemLogDBConfig},
	}
	for _, p := range presets {
		t.Run(p.name, func(t *testing.T) {
			lc := p.fn()
			if lc.IsEmpty() {
				t.Error("preset LogDB config should not be empty")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ValidationError format
// ---------------------------------------------------------------------------

func TestValidationError_ErrorFormat(t *testing.T) {
	ve := &ValidationError{Field: "ShardID", Message: "must be > 0"}
	msg := ve.Error()
	if msg != "dragonboat/config: invalid ShardID: must be > 0" {
		t.Errorf("unexpected error message: %s", msg)
	}
}

// ---------------------------------------------------------------------------
// FileReadError format and Unwrap
// ---------------------------------------------------------------------------

func TestFileReadError_ErrorFormat(t *testing.T) {
	inner := errors.New("permission denied")
	fre := &FileReadError{Path: "/tmp/cert.pem", Err: inner}
	msg := fre.Error()
	if msg == "" {
		t.Fatal("expected non-empty error message")
	}
	if fre.Unwrap() != inner {
		t.Error("Unwrap should return inner error")
	}
}

func TestFileReadError_Is(t *testing.T) {
	inner := os.ErrPermission
	fre := &FileReadError{Path: "/tmp/cert.pem", Err: inner}
	if !errors.Is(fre, os.ErrPermission) {
		t.Error("FileReadError should unwrap to os.ErrPermission")
	}
}

// ---------------------------------------------------------------------------
// GossipConfig: valid non-empty config (covers Validate return nil at end)
// ---------------------------------------------------------------------------

func TestGossipConfig_Validate_ValidNonEmpty(t *testing.T) {
	g := GossipConfig{
		BindAddress: "0.0.0.0:7946",
		Seed:        []string{"10.0.0.1:7946"},
	}
	if err := g.Validate(); err != nil {
		t.Errorf("valid gossip config should not error: %v", err)
	}
}

func TestGossipConfig_IsEmpty_SeedOnly(t *testing.T) {
	g := GossipConfig{Seed: []string{"10.0.0.1:7946"}}
	if g.IsEmpty() {
		t.Error("GossipConfig with Seed should not be empty")
	}
}

// ---------------------------------------------------------------------------
// NodeHostConfig: Validate with valid gossip propagates nil
// ---------------------------------------------------------------------------

func TestNodeHostConfig_Validate_ValidGossip(t *testing.T) {
	c := NodeHostConfig{
		WALDir:         "/tmp/wal",
		NodeHostDir:    "/tmp/nodehost",
		RaftAddress:    "localhost:63001",
		ListenAddress:  "0.0.0.0:63001",
		RTTMillisecond: 100,
		Gossip: GossipConfig{
			BindAddress: "0.0.0.0:7946",
			Seed:        []string{"10.0.0.1:7946"},
		},
	}
	if err := c.Validate(); err != nil {
		t.Errorf("valid config with gossip should not error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// NodeHostConfig: ToQuicraft MutualTLS individual file read errors
// ---------------------------------------------------------------------------

func TestNodeHostConfig_ToQuicraft_MutualTLSMissingCertFile(t *testing.T) {
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caFile, []byte("--- CA CERT ---"), 0600); err != nil {
		t.Fatal(err)
	}

	c := NodeHostConfig{
		WALDir:        dir,
		NodeHostDir:   dir,
		RaftAddress:   "localhost:63001",
		ListenAddress: "0.0.0.0:63001",
		MutualTLS:     true,
		CAFile:        caFile,
		CertFile:      filepath.Join(dir, "nonexistent_cert.pem"),
		KeyFile:       filepath.Join(dir, "nonexistent_key.pem"),
	}

	_, err := c.ToQuicraft()
	if err == nil {
		t.Fatal("expected error for missing CertFile")
	}
	var fre *FileReadError
	if !errors.As(err, &fre) {
		t.Fatalf("expected *FileReadError, got %T: %v", err, err)
	}
	if fre.Path != c.CertFile {
		t.Errorf("FileReadError.Path = %q, want %q", fre.Path, c.CertFile)
	}
}

func TestNodeHostConfig_ToQuicraft_MutualTLSMissingKeyFile(t *testing.T) {
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	certFile := filepath.Join(dir, "cert.pem")
	if err := os.WriteFile(caFile, []byte("--- CA CERT ---"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certFile, []byte("--- CERT ---"), 0600); err != nil {
		t.Fatal(err)
	}

	c := NodeHostConfig{
		WALDir:        dir,
		NodeHostDir:   dir,
		RaftAddress:   "localhost:63001",
		ListenAddress: "0.0.0.0:63001",
		MutualTLS:     true,
		CAFile:        caFile,
		CertFile:      certFile,
		KeyFile:       filepath.Join(dir, "nonexistent_key.pem"),
	}

	_, err := c.ToQuicraft()
	if err == nil {
		t.Fatal("expected error for missing KeyFile")
	}
	var fre *FileReadError
	if !errors.As(err, &fre) {
		t.Fatalf("expected *FileReadError, got %T: %v", err, err)
	}
	if fre.Path != c.KeyFile {
		t.Errorf("FileReadError.Path = %q, want %q", fre.Path, c.KeyFile)
	}
}
