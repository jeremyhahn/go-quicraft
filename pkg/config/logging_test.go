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
	"log/slog"
	"testing"
)

func TestNewLogger_TextFormat(t *testing.T) {
	cfg := LogConfig{Format: LogFormatText, Level: LogLevelInfo}
	logger := NewLogger(cfg)

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

func TestNewLogger_DefaultsToInfoOnUnknownLevel(t *testing.T) {
	// If an unvalidated level slips through, NewLogger falls back to info.
	cfg := LogConfig{Format: LogFormatText, Level: "unknown"}
	logger := NewLogger(cfg)

	// The handler should be enabled for info but the exact level
	// is verified indirectly: debug should be disabled.
	if logger.Handler().Enabled(t.Context(), slog.LevelDebug) {
		t.Error("expected debug to be disabled at default info level")
	}
	if !logger.Handler().Enabled(t.Context(), slog.LevelInfo) {
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

			if got := h.Enabled(t.Context(), slog.LevelDebug); got != tt.enabledDebug {
				t.Errorf("debug enabled = %v, want %v", got, tt.enabledDebug)
			}
			if got := h.Enabled(t.Context(), slog.LevelInfo); got != tt.enabledInfo {
				t.Errorf("info enabled = %v, want %v", got, tt.enabledInfo)
			}
			if got := h.Enabled(t.Context(), slog.LevelWarn); got != tt.enabledWarn {
				t.Errorf("warn enabled = %v, want %v", got, tt.enabledWarn)
			}
			if got := h.Enabled(t.Context(), slog.LevelError); got != tt.enabledError {
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
	if handler.Enabled(t.Context(), slog.LevelInfo) {
		t.Error("expected info to be disabled at warn level")
	}
	if !handler.Enabled(t.Context(), slog.LevelWarn) {
		t.Error("expected warn to be enabled at warn level")
	}
}
