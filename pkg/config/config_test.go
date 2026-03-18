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
	"testing"
)

// validConfig returns a Config with all required fields populated and
// defaults applied, suitable as a baseline for validation tests.
func validConfig() Config {
	c := Config{
		ShardID:   1,
		ReplicaID: 1,
	}
	c.SetDefaults()
	return c
}

func TestConfig_SetDefaults(t *testing.T) {
	t.Run("fills all zero fields", func(t *testing.T) {
		var c Config
		c.SetDefaults()

		if c.ElectionRTT != DefaultElectionRTT {
			t.Errorf("ElectionRTT = %d, want %d", c.ElectionRTT, DefaultElectionRTT)
		}
		if c.HeartbeatRTT != DefaultHeartbeatRTT {
			t.Errorf("HeartbeatRTT = %d, want %d", c.HeartbeatRTT, DefaultHeartbeatRTT)
		}
		if c.CompactionOverhead != DefaultCompactionOverhead {
			t.Errorf("CompactionOverhead = %d, want %d", c.CompactionOverhead, DefaultCompactionOverhead)
		}
		if c.MaxInMemLogSize != 0 {
			t.Errorf("MaxInMemLogSize = %d, want 0 (disabled by default)", c.MaxInMemLogSize)
		}
		if c.MaxEntrySize != DefaultMaxEntrySize {
			t.Errorf("MaxEntrySize = %d, want %d", c.MaxEntrySize, DefaultMaxEntrySize)
		}
		if c.MaxApplyEntrySize != DefaultMaxApplyEntrySize {
			t.Errorf("MaxApplyEntrySize = %d, want %d", c.MaxApplyEntrySize, DefaultMaxApplyEntrySize)
		}
		if c.MaxSnapshotSize != DefaultMaxSnapshotSize {
			t.Errorf("MaxSnapshotSize = %d, want %d", c.MaxSnapshotSize, DefaultMaxSnapshotSize)
		}
		if c.MaxSessionsPerClient != DefaultMaxSessionsPerClient {
			t.Errorf("MaxSessionsPerClient = %d, want %d", c.MaxSessionsPerClient, DefaultMaxSessionsPerClient)
		}
		if c.MaxTotalSessions != DefaultMaxTotalSessions {
			t.Errorf("MaxTotalSessions = %d, want %d", c.MaxTotalSessions, DefaultMaxTotalSessions)
		}
		if c.CheckQuorum == nil || !*c.CheckQuorum {
			t.Error("CheckQuorum should default to true")
		}
		if c.PreVote == nil || !*c.PreVote {
			t.Error("PreVote should default to true")
		}
		if c.OrderedConfigChange == nil || !*c.OrderedConfigChange {
			t.Error("OrderedConfigChange should default to true")
		}
	})

	t.Run("preserves non-zero fields", func(t *testing.T) {
		c := Config{
			ElectionRTT:          20,
			HeartbeatRTT:         2,
			CompactionOverhead:   10000,
			MaxEntrySize:         16 * 1024 * 1024,
			MaxSessionsPerClient: 256,
			MaxTotalSessions:     32768,
		}
		c.SetDefaults()

		if c.ElectionRTT != 20 {
			t.Errorf("ElectionRTT = %d, want 20", c.ElectionRTT)
		}
		if c.HeartbeatRTT != 2 {
			t.Errorf("HeartbeatRTT = %d, want 2", c.HeartbeatRTT)
		}
		if c.CompactionOverhead != 10000 {
			t.Errorf("CompactionOverhead = %d, want 10000", c.CompactionOverhead)
		}
		if c.MaxEntrySize != 16*1024*1024 {
			t.Errorf("MaxEntrySize = %d, want %d", c.MaxEntrySize, 16*1024*1024)
		}
		if c.MaxSessionsPerClient != 256 {
			t.Errorf("MaxSessionsPerClient = %d, want 256", c.MaxSessionsPerClient)
		}
		if c.MaxTotalSessions != 32768 {
			t.Errorf("MaxTotalSessions = %d, want 32768", c.MaxTotalSessions)
		}
	})
}

func TestConfig_Validate_Valid(t *testing.T) {
	t.Run("valid config after SetDefaults", func(t *testing.T) {
		c := validConfig()
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("valid config with snapshots enabled", func(t *testing.T) {
		c := validConfig()
		c.SnapshotEntries = 10000
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("valid config with observer", func(t *testing.T) {
		c := validConfig()
		c.IsObserver = true
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("valid config with witness", func(t *testing.T) {
		c := validConfig()
		c.IsWitness = true
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("valid config with snappy compression", func(t *testing.T) {
		c := validConfig()
		c.SnapshotCompressionType = Snappy
		c.EntryCompressionType = Snappy
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestConfig_Validate_ShardID(t *testing.T) {
	t.Run("zero ShardID", func(t *testing.T) {
		c := validConfig()
		c.ShardID = 0
		err := c.Validate()
		assertValidationError(t, err, "ShardID")
	})
}

func TestConfig_Validate_ReplicaID(t *testing.T) {
	t.Run("zero ReplicaID", func(t *testing.T) {
		c := validConfig()
		c.ReplicaID = 0
		err := c.Validate()
		assertValidationError(t, err, "ReplicaID")
	})
}

func TestConfig_Validate_HeartbeatRTT(t *testing.T) {
	t.Run("zero HeartbeatRTT", func(t *testing.T) {
		c := validConfig()
		c.HeartbeatRTT = 0
		err := c.Validate()
		assertValidationError(t, err, "HeartbeatRTT")
	})

	t.Run("minimum valid HeartbeatRTT", func(t *testing.T) {
		c := validConfig()
		c.HeartbeatRTT = 1
		c.ElectionRTT = 10
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestConfig_Validate_ElectionRTT(t *testing.T) {
	t.Run("below minimum", func(t *testing.T) {
		c := validConfig()
		c.ElectionRTT = 4
		err := c.Validate()
		assertValidationError(t, err, "ElectionRTT")
	})

	t.Run("at minimum boundary", func(t *testing.T) {
		c := validConfig()
		c.ElectionRTT = 5
		c.HeartbeatRTT = 1
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("equal to HeartbeatRTT", func(t *testing.T) {
		c := validConfig()
		c.ElectionRTT = 5
		c.HeartbeatRTT = 5
		err := c.Validate()
		assertValidationError(t, err, "ElectionRTT")
	})

	t.Run("less than HeartbeatRTT", func(t *testing.T) {
		c := validConfig()
		c.ElectionRTT = 5
		c.HeartbeatRTT = 6
		err := c.Validate()
		// HeartbeatRTT >= ElectionRTT triggers ElectionRTT error
		assertValidationError(t, err, "ElectionRTT")
	})
}

func TestConfig_Validate_ObserverWitness(t *testing.T) {
	t.Run("both set", func(t *testing.T) {
		c := validConfig()
		c.IsObserver = true
		c.IsWitness = true
		err := c.Validate()
		assertValidationError(t, err, "IsObserver")
	})

	t.Run("neither set", func(t *testing.T) {
		c := validConfig()
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestConfig_Validate_SnapshotEntries(t *testing.T) {
	t.Run("zero is valid (disabled)", func(t *testing.T) {
		c := validConfig()
		c.SnapshotEntries = 0
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("equal to CompactionOverhead", func(t *testing.T) {
		c := validConfig()
		c.SnapshotEntries = c.CompactionOverhead
		err := c.Validate()
		assertValidationError(t, err, "SnapshotEntries")
	})

	t.Run("less than CompactionOverhead", func(t *testing.T) {
		c := validConfig()
		c.SnapshotEntries = c.CompactionOverhead - 1
		err := c.Validate()
		assertValidationError(t, err, "SnapshotEntries")
	})

	t.Run("greater than CompactionOverhead", func(t *testing.T) {
		c := validConfig()
		c.SnapshotEntries = c.CompactionOverhead + 1
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestConfig_Validate_MaxEntrySize(t *testing.T) {
	t.Run("zero MaxEntrySize", func(t *testing.T) {
		c := validConfig()
		c.MaxEntrySize = 0
		err := c.Validate()
		assertValidationError(t, err, "MaxEntrySize")
	})

	t.Run("valid MaxEntrySize", func(t *testing.T) {
		c := validConfig()
		c.MaxEntrySize = 1
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestConfig_Validate_Sessions(t *testing.T) {
	t.Run("zero MaxSessionsPerClient", func(t *testing.T) {
		c := validConfig()
		c.MaxSessionsPerClient = 0
		err := c.Validate()
		assertValidationError(t, err, "MaxSessionsPerClient")
	})

	t.Run("zero MaxTotalSessions", func(t *testing.T) {
		c := validConfig()
		c.MaxTotalSessions = 0
		err := c.Validate()
		assertValidationError(t, err, "MaxTotalSessions")
	})
}

func TestConfig_Validate_CompressionType(t *testing.T) {
	t.Run("invalid SnapshotCompressionType", func(t *testing.T) {
		c := validConfig()
		c.SnapshotCompressionType = CompressionType(99)
		err := c.Validate()
		assertValidationError(t, err, "SnapshotCompressionType")
	})

	t.Run("invalid EntryCompressionType", func(t *testing.T) {
		c := validConfig()
		c.EntryCompressionType = CompressionType(99)
		err := c.Validate()
		assertValidationError(t, err, "EntryCompressionType")
	})
}

func TestConfig_Validate_MaxApplyEntrySize(t *testing.T) {
	t.Run("zero MaxApplyEntrySize", func(t *testing.T) {
		c := validConfig()
		c.MaxApplyEntrySize = 0
		err := c.Validate()
		assertValidationError(t, err, "MaxApplyEntrySize")
	})

	t.Run("valid MaxApplyEntrySize", func(t *testing.T) {
		c := validConfig()
		c.MaxApplyEntrySize = 1
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestConfig_Validate_SafetyBooleans_AllowedFalse(t *testing.T) {
	// Safety booleans default to true via SetDefaults() but explicit false
	// is allowed. Validate() warns but does not reject.
	t.Run("CheckQuorum false passes validation", func(t *testing.T) {
		c := validConfig()
		c.CheckQuorum = BoolPtr(false)
		if err := c.Validate(); err != nil {
			t.Errorf("Validate should not reject CheckQuorum=false: %v", err)
		}
	})

	t.Run("PreVote false passes validation", func(t *testing.T) {
		c := validConfig()
		c.PreVote = BoolPtr(false)
		if err := c.Validate(); err != nil {
			t.Errorf("Validate should not reject PreVote=false: %v", err)
		}
	})

	t.Run("OrderedConfigChange false passes validation", func(t *testing.T) {
		c := validConfig()
		c.OrderedConfigChange = BoolPtr(false)
		if err := c.Validate(); err != nil {
			t.Errorf("Validate should not reject OrderedConfigChange=false: %v", err)
		}
	})
}

func TestConfig_SetDefaults_SafetyBooleans(t *testing.T) {
	t.Run("nil CheckQuorum defaults to true", func(t *testing.T) {
		c := Config{}
		c.SetDefaults()
		if c.CheckQuorum == nil || !*c.CheckQuorum {
			t.Error("SetDefaults must default nil CheckQuorum to true")
		}
	})

	t.Run("nil PreVote defaults to true", func(t *testing.T) {
		c := Config{}
		c.SetDefaults()
		if c.PreVote == nil || !*c.PreVote {
			t.Error("SetDefaults must default nil PreVote to true")
		}
	})

	t.Run("nil OrderedConfigChange defaults to true", func(t *testing.T) {
		c := Config{}
		c.SetDefaults()
		if c.OrderedConfigChange == nil || !*c.OrderedConfigChange {
			t.Error("SetDefaults must default nil OrderedConfigChange to true")
		}
	})

	t.Run("explicit false CheckQuorum preserved", func(t *testing.T) {
		c := Config{CheckQuorum: BoolPtr(false)}
		c.SetDefaults()
		if c.CheckQuorum == nil || *c.CheckQuorum {
			t.Error("SetDefaults must not overwrite explicit CheckQuorum=false")
		}
	})

	t.Run("explicit false PreVote preserved", func(t *testing.T) {
		c := Config{PreVote: BoolPtr(false)}
		c.SetDefaults()
		if c.PreVote == nil || *c.PreVote {
			t.Error("SetDefaults must not overwrite explicit PreVote=false")
		}
	})

	t.Run("explicit false OrderedConfigChange preserved", func(t *testing.T) {
		c := Config{OrderedConfigChange: BoolPtr(false)}
		c.SetDefaults()
		if c.OrderedConfigChange == nil || *c.OrderedConfigChange {
			t.Error("SetDefaults must not overwrite explicit OrderedConfigChange=false")
		}
	})

	t.Run("explicit true CheckQuorum preserved", func(t *testing.T) {
		c := Config{CheckQuorum: BoolPtr(true)}
		c.SetDefaults()
		if c.CheckQuorum == nil || !*c.CheckQuorum {
			t.Error("SetDefaults must not overwrite explicit CheckQuorum=true")
		}
	})

	t.Run("explicit true PreVote preserved", func(t *testing.T) {
		c := Config{PreVote: BoolPtr(true)}
		c.SetDefaults()
		if c.PreVote == nil || !*c.PreVote {
			t.Error("SetDefaults must not overwrite explicit PreVote=true")
		}
	})

	t.Run("explicit true OrderedConfigChange preserved", func(t *testing.T) {
		c := Config{OrderedConfigChange: BoolPtr(true)}
		c.SetDefaults()
		if c.OrderedConfigChange == nil || !*c.OrderedConfigChange {
			t.Error("SetDefaults must not overwrite explicit OrderedConfigChange=true")
		}
	})
}

func TestConfig_GetCheckQuorum(t *testing.T) {
	t.Run("nil returns true", func(t *testing.T) {
		c := Config{}
		if !c.GetCheckQuorum() {
			t.Error("GetCheckQuorum should return true for nil")
		}
	})

	t.Run("explicit false returns false", func(t *testing.T) {
		c := Config{CheckQuorum: BoolPtr(false)}
		if c.GetCheckQuorum() {
			t.Error("GetCheckQuorum should return false when explicitly set")
		}
	})

	t.Run("explicit true returns true", func(t *testing.T) {
		c := Config{CheckQuorum: BoolPtr(true)}
		if !c.GetCheckQuorum() {
			t.Error("GetCheckQuorum should return true when explicitly set")
		}
	})
}

func TestConfig_GetPreVote(t *testing.T) {
	t.Run("nil returns true", func(t *testing.T) {
		c := Config{}
		if !c.GetPreVote() {
			t.Error("GetPreVote should return true for nil")
		}
	})

	t.Run("explicit false returns false", func(t *testing.T) {
		c := Config{PreVote: BoolPtr(false)}
		if c.GetPreVote() {
			t.Error("GetPreVote should return false when explicitly set")
		}
	})

	t.Run("explicit true returns true", func(t *testing.T) {
		c := Config{PreVote: BoolPtr(true)}
		if !c.GetPreVote() {
			t.Error("GetPreVote should return true when explicitly set")
		}
	})
}

func TestConfig_GetOrderedConfigChange(t *testing.T) {
	t.Run("nil returns true", func(t *testing.T) {
		c := Config{}
		if !c.GetOrderedConfigChange() {
			t.Error("GetOrderedConfigChange should return true for nil")
		}
	})

	t.Run("explicit false returns false", func(t *testing.T) {
		c := Config{OrderedConfigChange: BoolPtr(false)}
		if c.GetOrderedConfigChange() {
			t.Error("GetOrderedConfigChange should return false when explicitly set")
		}
	})

	t.Run("explicit true returns true", func(t *testing.T) {
		c := Config{OrderedConfigChange: BoolPtr(true)}
		if !c.GetOrderedConfigChange() {
			t.Error("GetOrderedConfigChange should return true when explicitly set")
		}
	})
}

func TestBoolPtr(t *testing.T) {
	t.Run("true", func(t *testing.T) {
		p := BoolPtr(true)
		if p == nil || !*p {
			t.Error("BoolPtr(true) should return pointer to true")
		}
	})

	t.Run("false", func(t *testing.T) {
		p := BoolPtr(false)
		if p == nil || *p {
			t.Error("BoolPtr(false) should return pointer to false")
		}
	})

	t.Run("returns distinct pointers", func(t *testing.T) {
		p1 := BoolPtr(true)
		p2 := BoolPtr(true)
		if p1 == p2 {
			t.Error("BoolPtr should return distinct pointers on each call")
		}
	})
}

func TestConfig_Validate_MaxProposalPayloadSize(t *testing.T) {
	t.Run("default is valid (proto.MaxEntrySize)", func(t *testing.T) {
		c := validConfig()
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
		if c.MaxProposalPayloadSize != 8*1024*1024 {
			t.Errorf("MaxProposalPayloadSize = %d, want %d", c.MaxProposalPayloadSize, 8*1024*1024)
		}
	})

	t.Run("lower than protocol maximum is valid", func(t *testing.T) {
		c := validConfig()
		c.MaxProposalPayloadSize = 1024 * 1024 // 1 MB
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("equal to protocol maximum is valid", func(t *testing.T) {
		c := validConfig()
		c.MaxProposalPayloadSize = 8 * 1024 * 1024
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("exceeds protocol maximum", func(t *testing.T) {
		c := validConfig()
		c.MaxProposalPayloadSize = 16 * 1024 * 1024 // 16 MB > 8 MB protocol max
		err := c.Validate()
		if err == nil {
			t.Fatal("expected error for MaxProposalPayloadSize > proto.MaxEntrySize, got nil")
		}
		var exceeded *MaxProposalPayloadSizeExceededError
		if !errors.As(err, &exceeded) {
			t.Fatalf("expected *MaxProposalPayloadSizeExceededError, got %T: %v", err, err)
		}
		if exceeded.Configured != 16*1024*1024 {
			t.Errorf("Configured = %d, want %d", exceeded.Configured, 16*1024*1024)
		}
		if exceeded.ProtocolMaximum != 8*1024*1024 {
			t.Errorf("ProtocolMaximum = %d, want %d", exceeded.ProtocolMaximum, 8*1024*1024)
		}
	})

	t.Run("zero after SetDefaults gets protocol default", func(t *testing.T) {
		c := Config{ShardID: 1, ReplicaID: 1}
		c.SetDefaults()
		if c.MaxProposalPayloadSize != 8*1024*1024 {
			t.Errorf("MaxProposalPayloadSize = %d, want %d", c.MaxProposalPayloadSize, 8*1024*1024)
		}
	})

	t.Run("zero without SetDefaults fails validation", func(t *testing.T) {
		c := validConfig()
		c.MaxProposalPayloadSize = 0
		err := c.Validate()
		assertValidationError(t, err, "MaxProposalPayloadSize")
	})
}

func TestConfig_Validate_SessionExpiryEntries(t *testing.T) {
	t.Run("zero is valid (disabled)", func(t *testing.T) {
		c := validConfig()
		c.SessionExpiryEntries = 0
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("at minimum boundary (100)", func(t *testing.T) {
		c := validConfig()
		c.SessionExpiryEntries = 100
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("above minimum is valid", func(t *testing.T) {
		c := validConfig()
		c.SessionExpiryEntries = 10000
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("below minimum (non-zero)", func(t *testing.T) {
		c := validConfig()
		c.SessionExpiryEntries = 99
		err := c.Validate()
		if err == nil {
			t.Fatal("expected error for SessionExpiryEntries < 100, got nil")
		}
		var tooSmall *SessionExpiryEntriesTooSmallError
		if !errors.As(err, &tooSmall) {
			t.Fatalf("expected *SessionExpiryEntriesTooSmallError, got %T: %v", err, err)
		}
		if tooSmall.Configured != 99 {
			t.Errorf("Configured = %d, want 99", tooSmall.Configured)
		}
		if tooSmall.Minimum != 100 {
			t.Errorf("Minimum = %d, want 100", tooSmall.Minimum)
		}
	})

	t.Run("value of 1 is rejected", func(t *testing.T) {
		c := validConfig()
		c.SessionExpiryEntries = 1
		err := c.Validate()
		if err == nil {
			t.Fatal("expected error for SessionExpiryEntries = 1, got nil")
		}
		var tooSmall *SessionExpiryEntriesTooSmallError
		if !errors.As(err, &tooSmall) {
			t.Fatalf("expected *SessionExpiryEntriesTooSmallError, got %T: %v", err, err)
		}
	})

	t.Run("exceeds SnapshotEntries warns but passes", func(t *testing.T) {
		c := validConfig()
		c.SnapshotEntries = 10000
		c.SessionExpiryEntries = 20000
		// Should not return error, just warn
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil (warning only)", err)
		}
	})
}

func TestConfig_Validate_LeaseReadRequiresCheckQuorum(t *testing.T) {
	t.Run("LeaseRead true with CheckQuorum false is rejected", func(t *testing.T) {
		c := validConfig()
		c.LeaseRead = true
		c.CheckQuorum = BoolPtr(false)
		err := c.Validate()
		if err == nil {
			t.Fatal("expected error for LeaseRead=true with CheckQuorum=false, got nil")
		}
		if !errors.Is(err, ErrLeaseReadRequiresCheckQuorum) {
			t.Fatalf("expected ErrLeaseReadRequiresCheckQuorum, got %T: %v", err, err)
		}
		var lrErr *LeaseReadRequiresCheckQuorumError
		if !errors.As(err, &lrErr) {
			t.Fatalf("expected *LeaseReadRequiresCheckQuorumError, got %T: %v", err, err)
		}
	})

	t.Run("LeaseRead true with CheckQuorum true passes validation", func(t *testing.T) {
		c := validConfig()
		c.LeaseRead = true
		c.CheckQuorum = BoolPtr(true)
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("LeaseRead true with CheckQuorum nil passes (defaults to true)", func(t *testing.T) {
		c := validConfig()
		c.LeaseRead = true
		// CheckQuorum is already BoolPtr(true) from SetDefaults, set to nil
		// to verify GetCheckQuorum returns true for nil.
		c.CheckQuorum = nil
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil (nil CheckQuorum defaults to true)", err)
		}
	})

	t.Run("LeaseRead false with CheckQuorum false passes validation", func(t *testing.T) {
		c := validConfig()
		c.LeaseRead = false
		c.CheckQuorum = BoolPtr(false)
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil (LeaseRead disabled)", err)
		}
	})
}

func TestConfig_SetDefaults_MaxInMemLogSizeZeroNotOverridden(t *testing.T) {
	c := Config{}
	c.SetDefaults()
	if c.MaxInMemLogSize != 0 {
		t.Errorf("MaxInMemLogSize = %d, want 0 (should not be overridden)", c.MaxInMemLogSize)
	}
}

// assertValidationError checks that err is a *ValidationError
// with the expected field name.
func assertValidationError(t *testing.T, err error, expectedField string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error for field %q, got nil", expectedField)
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
	if ve.Field != expectedField {
		t.Errorf("error field = %q, want %q", ve.Field, expectedField)
	}
}
