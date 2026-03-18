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
	"os"
)

// logLevelMap maps LogLevel string values to slog.Level constants.
var logLevelMap = map[LogLevel]slog.Level{
	LogLevelDebug: slog.LevelDebug,
	LogLevelInfo:  slog.LevelInfo,
	LogLevelWarn:  slog.LevelWarn,
	LogLevelError: slog.LevelError,
}

// NewLogger creates a configured *slog.Logger from the given LogConfig.
// Text format uses slog.NewTextHandler, JSON format uses slog.NewJSONHandler.
// Both write to os.Stderr.
func NewLogger(cfg LogConfig) *slog.Logger {
	level, ok := logLevelMap[cfg.Level]
	if !ok {
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: level}

	var handler slog.Handler
	if cfg.Format == LogFormatJSON {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		handler = slog.NewTextHandler(os.Stderr, opts)
	}

	return slog.New(handler)
}

// SetDefaultLogger creates a logger from the given LogConfig and installs
// it as the slog global default. All subsequent slog.Info/Warn/Error calls
// use the configured format and level.
func SetDefaultLogger(cfg LogConfig) {
	slog.SetDefault(NewLogger(cfg))
}
