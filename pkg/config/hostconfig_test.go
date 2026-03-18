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
	"runtime"
	"testing"
	"time"
)

// validHostConfig returns a HostConfig with all required fields populated
// and defaults applied, suitable as a baseline for validation tests.
func validHostConfig() HostConfig {
	hc := HostConfig{
		WALDir:        "/tmp/wal",
		NodeHostDir:   "/tmp/node",
		ListenAddress: "127.0.0.1:26001",
		RaftAddress:   "127.0.0.1:26001",
		DeploymentID:  1,
	}
	hc.SetDefaults()
	// Transport is disabled for unit tests that validate host config
	// fields without requiring mTLS certificate material.
	hc.TransportConfig.TransportDisabled = true
	return hc
}

func TestHostConfig_SetDefaults(t *testing.T) {
	t.Run("fills all zero fields", func(t *testing.T) {
		hc := HostConfig{
			WALDir:        "/tmp/wal",
			NodeHostDir:   "/tmp/node",
			ListenAddress: "127.0.0.1:26001",
			DeploymentID:  1,
		}
		hc.SetDefaults()

		if hc.RTTMillisecond != DefaultRTTMillisecond {
			t.Errorf("RTTMillisecond = %d, want %d", hc.RTTMillisecond, DefaultRTTMillisecond)
		}
		expectedWorkers := uint64(runtime.NumCPU())
		if hc.NumWorkers != expectedWorkers {
			t.Errorf("NumWorkers = %d, want %d", hc.NumWorkers, expectedWorkers)
		}
		if hc.MaxApplyWorkers != runtime.NumCPU() {
			t.Errorf("MaxApplyWorkers = %d, want %d", hc.MaxApplyWorkers, runtime.NumCPU())
		}
		if hc.CommitCBufferSize != DefaultCommitCBufferSize {
			t.Errorf("CommitCBufferSize = %d, want %d", hc.CommitCBufferSize, DefaultCommitCBufferSize)
		}
		if hc.ShutdownTimeout != DefaultShutdownTimeout {
			t.Errorf("ShutdownTimeout = %v, want %v", hc.ShutdownTimeout, DefaultShutdownTimeout)
		}
		if hc.MaxApplyRetries != DefaultMaxApplyRetries {
			t.Errorf("MaxApplyRetries = %d, want %d", hc.MaxApplyRetries, DefaultMaxApplyRetries)
		}
		if hc.MaxTotalInMemLogSize != 0 {
			t.Errorf("MaxTotalInMemLogSize = %d, want 0 (disabled by default)", hc.MaxTotalInMemLogSize)
		}
	})

	t.Run("preserves non-zero fields", func(t *testing.T) {
		hc := HostConfig{
			RTTMillisecond:       50,
			NumWorkers:           8,
			MaxApplyWorkers:      4,
			CommitCBufferSize:    128,
			ShutdownTimeout:      10 * time.Second,
			MaxApplyRetries:      50,
			MaxTotalInMemLogSize: 2 * 1024 * 1024 * 1024,
		}
		hc.SetDefaults()

		if hc.RTTMillisecond != 50 {
			t.Errorf("RTTMillisecond = %d, want 50", hc.RTTMillisecond)
		}
		if hc.NumWorkers != 8 {
			t.Errorf("NumWorkers = %d, want 8", hc.NumWorkers)
		}
		if hc.MaxApplyWorkers != 4 {
			t.Errorf("MaxApplyWorkers = %d, want 4", hc.MaxApplyWorkers)
		}
		if hc.CommitCBufferSize != 128 {
			t.Errorf("CommitCBufferSize = %d, want 128", hc.CommitCBufferSize)
		}
		if hc.ShutdownTimeout != 10*time.Second {
			t.Errorf("ShutdownTimeout = %v, want 10s", hc.ShutdownTimeout)
		}
		if hc.MaxApplyRetries != 50 {
			t.Errorf("MaxApplyRetries = %d, want 50", hc.MaxApplyRetries)
		}
		if hc.MaxTotalInMemLogSize != 2*1024*1024*1024 {
			t.Errorf("MaxTotalInMemLogSize = %d, want %d", hc.MaxTotalInMemLogSize, 2*1024*1024*1024)
		}
	})

	t.Run("sets transport defaults", func(t *testing.T) {
		var hc HostConfig
		hc.SetDefaults()

		if hc.TransportConfig.StreamPoolSize != DefaultStreamPoolSize {
			t.Errorf("TransportConfig.StreamPoolSize = %d, want %d",
				hc.TransportConfig.StreamPoolSize, DefaultStreamPoolSize)
		}
		if hc.TransportConfig.MaxStreamPoolSize != DefaultMaxStreamPoolSize {
			t.Errorf("TransportConfig.MaxStreamPoolSize = %d, want %d",
				hc.TransportConfig.MaxStreamPoolSize, DefaultMaxStreamPoolSize)
		}
	})
}

func TestHostConfig_Validate_Valid(t *testing.T) {
	t.Run("valid config after SetDefaults", func(t *testing.T) {
		hc := validHostConfig()
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("valid with AllowZeroDeploymentID", func(t *testing.T) {
		hc := validHostConfig()
		hc.DeploymentID = 0
		hc.AllowZeroDeploymentID = true
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("valid with MaxWALDiskSize unlimited", func(t *testing.T) {
		hc := validHostConfig()
		hc.MaxWALDiskSize = 0
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestHostConfig_Validate_WALDir(t *testing.T) {
	t.Run("empty WALDir", func(t *testing.T) {
		hc := validHostConfig()
		hc.WALDir = ""
		err := hc.Validate()
		assertValidationError(t, err, "WALDir")
	})

	t.Run("non-empty WALDir", func(t *testing.T) {
		hc := validHostConfig()
		hc.WALDir = "/data/wal"
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestHostConfig_Validate_NodeHostDir(t *testing.T) {
	t.Run("empty NodeHostDir", func(t *testing.T) {
		hc := validHostConfig()
		hc.NodeHostDir = ""
		err := hc.Validate()
		assertValidationError(t, err, "NodeHostDir")
	})

	t.Run("non-empty NodeHostDir", func(t *testing.T) {
		hc := validHostConfig()
		hc.NodeHostDir = "/data/node"
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestHostConfig_Validate_RaftAddress(t *testing.T) {
	t.Run("empty RaftAddress", func(t *testing.T) {
		hc := validHostConfig()
		hc.RaftAddress = ""
		err := hc.Validate()
		assertValidationError(t, err, "RaftAddress")
	})

	t.Run("non-empty RaftAddress", func(t *testing.T) {
		hc := validHostConfig()
		hc.RaftAddress = "10.0.0.1:26001"
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestHostConfig_Validate_ListenAddress(t *testing.T) {
	t.Run("empty ListenAddress", func(t *testing.T) {
		hc := validHostConfig()
		hc.ListenAddress = ""
		err := hc.Validate()
		assertValidationError(t, err, "ListenAddress")
	})

	t.Run("non-empty ListenAddress", func(t *testing.T) {
		hc := validHostConfig()
		hc.ListenAddress = "0.0.0.0:26001"
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestHostConfig_Validate_RTTMillisecond(t *testing.T) {
	t.Run("zero RTTMillisecond", func(t *testing.T) {
		hc := validHostConfig()
		hc.RTTMillisecond = 0
		err := hc.Validate()
		assertValidationError(t, err, "RTTMillisecond")
	})

	t.Run("valid RTTMillisecond", func(t *testing.T) {
		hc := validHostConfig()
		hc.RTTMillisecond = 1
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestHostConfig_Validate_DeploymentID(t *testing.T) {
	t.Run("zero without override", func(t *testing.T) {
		hc := validHostConfig()
		hc.DeploymentID = 0
		hc.AllowZeroDeploymentID = false
		err := hc.Validate()
		assertValidationError(t, err, "DeploymentID")
	})

	t.Run("zero with override", func(t *testing.T) {
		hc := validHostConfig()
		hc.DeploymentID = 0
		hc.AllowZeroDeploymentID = true
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("non-zero", func(t *testing.T) {
		hc := validHostConfig()
		hc.DeploymentID = 42
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestHostConfig_Validate_NumWorkers(t *testing.T) {
	t.Run("zero NumWorkers", func(t *testing.T) {
		hc := validHostConfig()
		hc.NumWorkers = 0
		err := hc.Validate()
		assertValidationError(t, err, "NumWorkers")
	})

	t.Run("valid NumWorkers", func(t *testing.T) {
		hc := validHostConfig()
		hc.NumWorkers = 4
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestHostConfig_Validate_MaxApplyWorkers(t *testing.T) {
	t.Run("zero MaxApplyWorkers", func(t *testing.T) {
		hc := validHostConfig()
		hc.MaxApplyWorkers = 0
		err := hc.Validate()
		assertValidationError(t, err, "MaxApplyWorkers")
	})

	t.Run("valid MaxApplyWorkers", func(t *testing.T) {
		hc := validHostConfig()
		hc.MaxApplyWorkers = 2
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestHostConfig_Validate_CommitCBufferSize(t *testing.T) {
	t.Run("zero CommitCBufferSize", func(t *testing.T) {
		hc := validHostConfig()
		hc.CommitCBufferSize = 0
		err := hc.Validate()
		assertValidationError(t, err, "CommitCBufferSize")
	})

	t.Run("valid CommitCBufferSize", func(t *testing.T) {
		hc := validHostConfig()
		hc.CommitCBufferSize = 1
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestHostConfig_Validate_ShutdownTimeout(t *testing.T) {
	t.Run("zero ShutdownTimeout", func(t *testing.T) {
		hc := validHostConfig()
		hc.ShutdownTimeout = 0
		err := hc.Validate()
		assertValidationError(t, err, "ShutdownTimeout")
	})

	t.Run("negative ShutdownTimeout", func(t *testing.T) {
		hc := validHostConfig()
		hc.ShutdownTimeout = -1
		err := hc.Validate()
		assertValidationError(t, err, "ShutdownTimeout")
	})

	t.Run("valid ShutdownTimeout", func(t *testing.T) {
		hc := validHostConfig()
		hc.ShutdownTimeout = 5 * time.Second
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestHostConfig_Validate_MaxApplyRetries(t *testing.T) {
	t.Run("zero MaxApplyRetries", func(t *testing.T) {
		hc := validHostConfig()
		hc.MaxApplyRetries = 0
		err := hc.Validate()
		assertValidationError(t, err, "MaxApplyRetries")
	})

	t.Run("valid MaxApplyRetries", func(t *testing.T) {
		hc := validHostConfig()
		hc.MaxApplyRetries = 1
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestHostConfig_Validate_MaxTotalInMemLogSize(t *testing.T) {
	t.Run("zero MaxTotalInMemLogSize is valid (disabled)", func(t *testing.T) {
		hc := validHostConfig()
		hc.MaxTotalInMemLogSize = 0
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil (0 means disabled)", err)
		}
	})

	t.Run("non-zero MaxTotalInMemLogSize is valid", func(t *testing.T) {
		hc := validHostConfig()
		hc.MaxTotalInMemLogSize = 1
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestHostConfig_Validate_TransportConfig(t *testing.T) {
	t.Run("invalid transport config propagates", func(t *testing.T) {
		hc := validHostConfig()
		hc.TransportConfig.StreamPoolSize = -1
		err := hc.Validate()
		if err == nil {
			t.Fatal("expected error for invalid TransportConfig, got nil")
		}
		var ve *ValidationError
		if !errors.As(err, &ve) {
			t.Fatalf("expected *ValidationError, got %T: %v", err, err)
		}
	})
}

func TestHostConfig_Validate_EventListener(t *testing.T) {
	t.Run("nil EventListener is valid", func(t *testing.T) {
		hc := validHostConfig()
		hc.EventListener = nil
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("non-nil EventListener is valid", func(t *testing.T) {
		hc := validHostConfig()
		hc.EventListener = &EventListener{
			OnLeaderUpdated: func(info LeaderInfo) {},
		}
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestHostConfig_SetDefaults_LogConfig(t *testing.T) {
	t.Run("fills LogConfig defaults", func(t *testing.T) {
		hc := HostConfig{}
		hc.SetDefaults()

		if hc.LogConfig.Format != LogFormatText {
			t.Errorf("LogConfig.Format = %q, want %q", hc.LogConfig.Format, LogFormatText)
		}
		if hc.LogConfig.Level != LogLevelInfo {
			t.Errorf("LogConfig.Level = %q, want %q", hc.LogConfig.Level, LogLevelInfo)
		}
	})

	t.Run("preserves non-zero LogConfig", func(t *testing.T) {
		hc := HostConfig{
			LogConfig: LogConfig{
				Format: LogFormatJSON,
				Level:  LogLevelDebug,
			},
		}
		hc.SetDefaults()

		if hc.LogConfig.Format != LogFormatJSON {
			t.Errorf("LogConfig.Format = %q, want %q", hc.LogConfig.Format, LogFormatJSON)
		}
		if hc.LogConfig.Level != LogLevelDebug {
			t.Errorf("LogConfig.Level = %q, want %q", hc.LogConfig.Level, LogLevelDebug)
		}
	})
}

func TestHostConfig_Validate_LogConfig(t *testing.T) {
	t.Run("valid LogConfig passes", func(t *testing.T) {
		hc := validHostConfig()
		hc.LogConfig = LogConfig{Format: LogFormatJSON, Level: LogLevelWarn}
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("invalid LogConfig format propagates", func(t *testing.T) {
		hc := validHostConfig()
		hc.LogConfig = LogConfig{Format: "invalid", Level: LogLevelInfo}
		err := hc.Validate()
		if err == nil {
			t.Fatal("expected error for invalid LogConfig.Format, got nil")
		}
		var logErr *LogValidationError
		if !errors.As(err, &logErr) {
			t.Fatalf("expected *LogValidationError, got %T: %v", err, err)
		}
		if logErr.Field != "Format" {
			t.Errorf("Field = %q, want %q", logErr.Field, "Format")
		}
	})

	t.Run("invalid LogConfig level propagates", func(t *testing.T) {
		hc := validHostConfig()
		hc.LogConfig = LogConfig{Format: LogFormatText, Level: "verbose"}
		err := hc.Validate()
		if err == nil {
			t.Fatal("expected error for invalid LogConfig.Level, got nil")
		}
		var logErr *LogValidationError
		if !errors.As(err, &logErr) {
			t.Fatalf("expected *LogValidationError, got %T: %v", err, err)
		}
		if logErr.Field != "Level" {
			t.Errorf("Field = %q, want %q", logErr.Field, "Level")
		}
	})
}

func TestHostConfig_Validate_KeyRotationInterval(t *testing.T) {
	t.Run("zero is valid (disabled)", func(t *testing.T) {
		hc := validHostConfig()
		hc.KeyRotationInterval = 0
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("exactly 1 second is valid", func(t *testing.T) {
		hc := validHostConfig()
		hc.KeyRotationInterval = time.Second
		hc.MaxRetainedEpochs = 5
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("24 hours is valid", func(t *testing.T) {
		hc := validHostConfig()
		hc.KeyRotationInterval = 24 * time.Hour
		hc.MaxRetainedEpochs = 5
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("below 1 second is rejected", func(t *testing.T) {
		hc := validHostConfig()
		hc.KeyRotationInterval = 500 * time.Millisecond
		err := hc.Validate()
		if err == nil {
			t.Fatal("expected error for KeyRotationInterval < 1s, got nil")
		}
		var tooSmall *KeyRotationIntervalTooSmallError
		if !errors.As(err, &tooSmall) {
			t.Fatalf("expected *KeyRotationIntervalTooSmallError, got %T: %v", err, err)
		}
		if tooSmall.Configured != 500*time.Millisecond {
			t.Errorf("Configured = %v, want 500ms", tooSmall.Configured)
		}
		if tooSmall.Minimum != time.Second {
			t.Errorf("Minimum = %v, want 1s", tooSmall.Minimum)
		}
	})

	t.Run("1 nanosecond is rejected", func(t *testing.T) {
		hc := validHostConfig()
		hc.KeyRotationInterval = time.Nanosecond
		err := hc.Validate()
		if err == nil {
			t.Fatal("expected error for KeyRotationInterval = 1ns, got nil")
		}
		var tooSmall *KeyRotationIntervalTooSmallError
		if !errors.As(err, &tooSmall) {
			t.Fatalf("expected *KeyRotationIntervalTooSmallError, got %T: %v", err, err)
		}
	})

	t.Run("rotation with zero MaxRetainedEpochs warns but passes", func(t *testing.T) {
		hc := validHostConfig()
		hc.KeyRotationInterval = 24 * time.Hour
		hc.MaxRetainedEpochs = 0
		// Should not return error, just warn
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil (warning only)", err)
		}
	})
}

func TestHostConfig_MaxRetainedEpochs(t *testing.T) {
	t.Run("defaults to zero (no purging)", func(t *testing.T) {
		hc := validHostConfig()
		if hc.MaxRetainedEpochs != 0 {
			t.Fatalf("expected MaxRetainedEpochs=0 by default, got %d", hc.MaxRetainedEpochs)
		}
	})

	t.Run("accepts configured value", func(t *testing.T) {
		hc := validHostConfig()
		hc.MaxRetainedEpochs = 5
		if err := hc.Validate(); err != nil {
			t.Fatalf("Validate failed with MaxRetainedEpochs=5: %v", err)
		}
		if hc.MaxRetainedEpochs != 5 {
			t.Fatalf("expected MaxRetainedEpochs=5, got %d", hc.MaxRetainedEpochs)
		}
	})

	t.Run("zero is valid (backward compatibility)", func(t *testing.T) {
		hc := validHostConfig()
		hc.MaxRetainedEpochs = 0
		if err := hc.Validate(); err != nil {
			t.Fatalf("Validate failed with MaxRetainedEpochs=0: %v", err)
		}
	})

	t.Run("SetDefaults does not override configured value", func(t *testing.T) {
		hc := HostConfig{
			WALDir:            "/tmp/wal",
			NodeHostDir:       "/tmp/node",
			ListenAddress:     "127.0.0.1:26001",
			DeploymentID:      1,
			MaxRetainedEpochs: 10,
		}
		hc.SetDefaults()
		if hc.MaxRetainedEpochs != 10 {
			t.Fatalf("SetDefaults overwrote MaxRetainedEpochs: expected 10, got %d",
				hc.MaxRetainedEpochs)
		}
	})
}
