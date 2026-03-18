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

package seal

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ShareStore persists share metadata and sealed shares for distributed unsealing.
// Implementations must be safe for concurrent use by multiple goroutines.
type ShareStore interface {
	// SaveMetadata persists metadata about a single Shamir share.
	SaveMetadata(ctx context.Context, meta *ShareMetadata) error

	// GetMetadata retrieves share metadata by share index.
	GetMetadata(ctx context.Context, index int) (*ShareMetadata, error)

	// ListMetadata returns metadata for all persisted shares, ordered by index.
	ListMetadata(ctx context.Context) ([]*ShareMetadata, error)

	// SaveSealedShare persists a sealed (encrypted) share for a given node.
	SaveSealedShare(ctx context.Context, nodeID uint64, sealedShare []byte) error

	// GetSealedShare retrieves the sealed share for a given node.
	GetSealedShare(ctx context.Context, nodeID uint64) ([]byte, error)

	// SaveVersion persists the current seal version number.
	SaveVersion(ctx context.Context, version uint64) error

	// GetVersion retrieves the current seal version number.
	GetVersion(ctx context.Context) (uint64, error)
}

// ShareMetadata holds metadata about a single Shamir share.
type ShareMetadata struct {
	// Index is the share index (1-based, matching Shamir share numbering).
	Index int `json:"index"`

	// NodeID is the cluster node that holds this share.
	NodeID uint64 `json:"node_id"`

	// CreatedAt is the time the share was generated.
	CreatedAt time.Time `json:"created_at"`

	// Version is the seal version when this share was created.
	Version uint64 `json:"version"`
}

// FileShareStore is a filesystem-backed ShareStore that persists share metadata
// and sealed shares as individual files under a configurable data directory.
//
// Directory layout:
//
//	<dataDir>/seal/metadata_{index}.json   - JSON-encoded ShareMetadata
//	<dataDir>/seal/share_{nodeID}.sealed   - raw sealed share bytes
//	<dataDir>/seal/version                 - ASCII seal version number
type FileShareStore struct {
	sealDir string
	mu      sync.RWMutex
}

// NewFileShareStore creates a FileShareStore rooted at dataDir. A "seal"
// subdirectory is created under dataDir if it does not already exist.
func NewFileShareStore(dataDir string) (*FileShareStore, error) {
	if dataDir == "" {
		return nil, &ShareStoreError{Op: "new", Err: ErrShareStoreInvalidDir}
	}

	sealDir := filepath.Join(dataDir, "seal")
	if err := os.MkdirAll(sealDir, 0o700); err != nil {
		return nil, &ShareStoreError{Op: "new", Err: err}
	}

	return &FileShareStore{sealDir: sealDir}, nil
}

// SaveMetadata persists share metadata as a JSON file named metadata_{index}.json.
func (s *FileShareStore) SaveMetadata(_ context.Context, meta *ShareMetadata) error {
	if meta == nil {
		return &ShareStoreError{Op: "save_metadata", Err: ErrShareStoreNilMetadata}
	}
	if meta.Index < 1 {
		return &ShareStoreError{Op: "save_metadata", Err: ErrShareStoreInvalidIndex}
	}

	data, err := json.Marshal(meta)
	if err != nil {
		return &ShareStoreError{Op: "save_metadata", Err: err}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureDataDir(); err != nil {
		return &ShareStoreError{Op: "save_metadata", Err: err}
	}

	path := filepath.Join(s.sealDir, fmt.Sprintf("metadata_%d.json", meta.Index))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return &ShareStoreError{Op: "save_metadata", Err: err}
	}

	return nil
}

// GetMetadata reads share metadata for the given share index.
func (s *FileShareStore) GetMetadata(_ context.Context, index int) (*ShareMetadata, error) {
	if index < 1 {
		return nil, &ShareStoreError{Op: "get_metadata", Err: ErrShareStoreInvalidIndex}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	path := filepath.Join(s.sealDir, fmt.Sprintf("metadata_%d.json", index))
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &ShareStoreError{Op: "get_metadata", Err: ErrShareStoreNotFound}
		}
		return nil, &ShareStoreError{Op: "get_metadata", Err: err}
	}

	var meta ShareMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, &ShareStoreError{Op: "get_metadata", Err: err}
	}

	return &meta, nil
}

// ListMetadata returns metadata for all persisted shares, sorted by index.
func (s *FileShareStore) ListMetadata(_ context.Context) ([]*ShareMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries, err := os.ReadDir(s.sealDir)
	if err != nil {
		return nil, &ShareStoreError{Op: "list_metadata", Err: err}
	}

	var results []*ShareMetadata
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "metadata_") || !strings.HasSuffix(name, ".json") {
			continue
		}

		data, err := os.ReadFile(filepath.Join(s.sealDir, name))
		if err != nil {
			return nil, &ShareStoreError{Op: "list_metadata", Err: err}
		}

		var meta ShareMetadata
		if err := json.Unmarshal(data, &meta); err != nil {
			return nil, &ShareStoreError{Op: "list_metadata", Err: err}
		}

		results = append(results, &meta)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Index < results[j].Index
	})

	return results, nil
}

// SaveSealedShare persists a sealed share as a binary file named share_{nodeID}.sealed.
func (s *FileShareStore) SaveSealedShare(_ context.Context, nodeID uint64, sealedShare []byte) error {
	if sealedShare == nil {
		return &ShareStoreError{Op: "save_sealed_share", Err: ErrShareStoreNilShare}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureDataDir(); err != nil {
		return &ShareStoreError{Op: "save_sealed_share", Err: err}
	}

	path := filepath.Join(s.sealDir, fmt.Sprintf("share_%d.sealed", nodeID))
	if err := os.WriteFile(path, sealedShare, 0o600); err != nil {
		return &ShareStoreError{Op: "save_sealed_share", Err: err}
	}

	return nil
}

// GetSealedShare reads the sealed share for the given node.
func (s *FileShareStore) GetSealedShare(_ context.Context, nodeID uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := filepath.Join(s.sealDir, fmt.Sprintf("share_%d.sealed", nodeID))
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &ShareStoreError{Op: "get_sealed_share", Err: ErrShareStoreNotFound}
		}
		return nil, &ShareStoreError{Op: "get_sealed_share", Err: err}
	}

	return data, nil
}

// SaveVersion persists the seal version as an ASCII number in the "version" file.
func (s *FileShareStore) SaveVersion(_ context.Context, version uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureDataDir(); err != nil {
		return &ShareStoreError{Op: "save_version", Err: err}
	}

	path := filepath.Join(s.sealDir, "version")
	data := []byte(strconv.FormatUint(version, 10))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return &ShareStoreError{Op: "save_version", Err: err}
	}

	return nil
}

// GetVersion reads the persisted seal version number.
func (s *FileShareStore) GetVersion(_ context.Context) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := filepath.Join(s.sealDir, "version")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, &ShareStoreError{Op: "get_version", Err: ErrShareStoreNotFound}
		}
		return 0, &ShareStoreError{Op: "get_version", Err: err}
	}

	version, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, &ShareStoreError{Op: "get_version", Err: err}
	}

	return version, nil
}

// ensureDataDir creates the seal directory if it does not exist. Must be called
// with s.mu held in write mode.
func (s *FileShareStore) ensureDataDir() error {
	return os.MkdirAll(s.sealDir, 0o700)
}
