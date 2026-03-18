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
	"log/slog"
	"testing"
)

func TestNewLogger_TextFormat(t *testing.T) {
	cfg := LogConfig{Format: LogFormatText, Level: LogLevelInfo}
	logger := NewLogger(cfg)

	var buf bytes.Buffer
	// Replace the handler's writer with a buffer for verification.
	textHandler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	testLogger := slog.New(textHandler)
	testLogger.Info("test message", "key", "value")

	if buf.Len() == 0 {
		t.Fatal("expected text handler to produce output")
	}

	// Verify the returned logger uses a text handler by checking type.
	handler := logger.Handler()
	if _, ok := handler.(*slog.TextHandler); !ok {
		t.Errorf("expected *slog.TextHandler, got %T", handler)
	}
}

func TestNewLogger_JSONFormat(t *testing.T) {
	cfg := LogConfig{Format: LogFormatJSON, Level: LogLevelInfo}
	logger := NewLogger(cfg)

	handler := logger.Handler()
	if _, ok := handler.(*slog.JSONHandler); !ok {
		t.Errorf("expected *slog.JSONHandler, got %T", handler)
	}
}

func TestNewLogger_DefaultsToTextOnUnknownFormat(t *testing.T) {
	// If an unvalidated format slips through, NewLogger falls back to text.
	cfg := LogConfig{Format: "unknown", Level: LogLevelInfo}
	logger := NewLogger(cfg)

	handler := logger.Handler()
	if _, ok := handler.(*slog.TextHandler); !ok {
		t.Errorf("expected *slog.TextHandler for unknown format, got %T", handler)
	}
}

func TestNewLogger_DefaultsToInfoOnUnknownLevel(t *testing.T) {
	// If an unvalidated level slips through, NewLogger falls back to info.
	cfg := LogConfig{Format: LogFormatText, Level: "unknown"}
	logger := NewLogger(cfg)

	// The handler should be enabled for info but the exact level
	// is verified indirectly: debug should be disabled.
	if logger.Handler().Enabled(nil, slog.LevelDebug) {
		t.Error("expected debug to be disabled at default info level")
	}
	if !logger.Handler().Enabled(nil, slog.LevelInfo) {
		t.Error("expected info to be enabled at default info level")
	}
}

func TestNewLogger_LevelFiltering(t *testing.T) {
	tests := []struct {
		level        LogLevel
		enabledDebug bool
		enabledInfo  bool
		enabledWarn  bool
		enabledError bool
	}{
		{LogLevelDebug, true, true, true, true},
		{LogLevelInfo, false, true, true, true},
		{LogLevelWarn, false, false, true, true},
		{LogLevelError, false, false, false, true},
	}

	for _, tt := range tests {
		t.Run(string(tt.level), func(t *testing.T) {
			cfg := LogConfig{Format: LogFormatText, Level: tt.level}
			logger := NewLogger(cfg)
			h := logger.Handler()

			if got := h.Enabled(nil, slog.LevelDebug); got != tt.enabledDebug {
				t.Errorf("debug enabled = %v, want %v", got, tt.enabledDebug)
			}
			if got := h.Enabled(nil, slog.LevelInfo); got != tt.enabledInfo {
				t.Errorf("info enabled = %v, want %v", got, tt.enabledInfo)
			}
			if got := h.Enabled(nil, slog.LevelWarn); got != tt.enabledWarn {
				t.Errorf("warn enabled = %v, want %v", got, tt.enabledWarn)
			}
			if got := h.Enabled(nil, slog.LevelError); got != tt.enabledError {
				t.Errorf("error enabled = %v, want %v", got, tt.enabledError)
			}
		})
	}
}

func TestSetDefaultLogger(t *testing.T) {
	// Save and restore the original default logger.
	original := slog.Default()
	defer slog.SetDefault(original)

	cfg := LogConfig{Format: LogFormatJSON, Level: LogLevelWarn}
	SetDefaultLogger(cfg)

	defaultLogger := slog.Default()
	handler := defaultLogger.Handler()

	if _, ok := handler.(*slog.JSONHandler); !ok {
		t.Errorf("expected default logger to use *slog.JSONHandler, got %T", handler)
	}
	if handler.Enabled(nil, slog.LevelInfo) {
		t.Error("expected info to be disabled at warn level")
	}
	if !handler.Enabled(nil, slog.LevelWarn) {
		t.Error("expected warn to be enabled at warn level")
	}
}

func TestLogConfig_SetDefaults(t *testing.T) {
	t.Run("fills zero-valued fields", func(t *testing.T) {
		var lc LogConfig
		lc.SetDefaults()

		if lc.Format != LogFormatText {
			t.Errorf("Format = %q, want %q", lc.Format, LogFormatText)
		}
		if lc.Level != LogLevelInfo {
			t.Errorf("Level = %q, want %q", lc.Level, LogLevelInfo)
		}
	})

	t.Run("preserves non-zero fields", func(t *testing.T) {
		lc := LogConfig{Format: LogFormatJSON, Level: LogLevelDebug}
		lc.SetDefaults()

		if lc.Format != LogFormatJSON {
			t.Errorf("Format = %q, want %q", lc.Format, LogFormatJSON)
		}
		if lc.Level != LogLevelDebug {
			t.Errorf("Level = %q, want %q", lc.Level, LogLevelDebug)
		}
	})
}

func TestLogConfig_Validate(t *testing.T) {
	t.Run("valid text/info", func(t *testing.T) {
		lc := LogConfig{Format: LogFormatText, Level: LogLevelInfo}
		if err := lc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("valid json/debug", func(t *testing.T) {
		lc := LogConfig{Format: LogFormatJSON, Level: LogLevelDebug}
		if err := lc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("valid text/warn", func(t *testing.T) {
		lc := LogConfig{Format: LogFormatText, Level: LogLevelWarn}
		if err := lc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("valid json/error", func(t *testing.T) {
		lc := LogConfig{Format: LogFormatJSON, Level: LogLevelError}
		if err := lc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("invalid format", func(t *testing.T) {
		lc := LogConfig{Format: "yaml", Level: LogLevelInfo}
		err := lc.Validate()
		if err == nil {
			t.Fatal("expected error for invalid format, got nil")
		}
		logErr, ok := err.(*LogValidationError)
		if !ok {
			t.Fatalf("expected *LogValidationError, got %T: %v", err, err)
		}
		if logErr.Field != "Format" {
			t.Errorf("Field = %q, want %q", logErr.Field, "Format")
		}
		if logErr.Value != "yaml" {
			t.Errorf("Value = %q, want %q", logErr.Value, "yaml")
		}
	})

	t.Run("invalid level", func(t *testing.T) {
		lc := LogConfig{Format: LogFormatText, Level: "trace"}
		err := lc.Validate()
		if err == nil {
			t.Fatal("expected error for invalid level, got nil")
		}
		logErr, ok := err.(*LogValidationError)
		if !ok {
			t.Fatalf("expected *LogValidationError, got %T: %v", err, err)
		}
		if logErr.Field != "Level" {
			t.Errorf("Field = %q, want %q", logErr.Field, "Level")
		}
		if logErr.Value != "trace" {
			t.Errorf("Value = %q, want %q", logErr.Value, "trace")
		}
	})

	t.Run("empty format is invalid", func(t *testing.T) {
		lc := LogConfig{Format: "", Level: LogLevelInfo}
		err := lc.Validate()
		if err == nil {
			t.Fatal("expected error for empty format, got nil")
		}
	})

	t.Run("empty level is invalid", func(t *testing.T) {
		lc := LogConfig{Format: LogFormatText, Level: ""}
		err := lc.Validate()
		if err == nil {
			t.Fatal("expected error for empty level, got nil")
		}
	})
}
