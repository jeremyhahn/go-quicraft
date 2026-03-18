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
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileShareStore_SaveGetMetadata(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileShareStore(dir)
	if err != nil {
		t.Fatalf("NewFileShareStore: %v", err)
	}

	ctx := context.Background()
	now := time.Now().Truncate(time.Millisecond)

	meta := &ShareMetadata{
		Index:     1,
		NodeID:    100,
		CreatedAt: now,
		Version:   5,
	}

	if err := store.SaveMetadata(ctx, meta); err != nil {
		t.Fatalf("SaveMetadata: %v", err)
	}

	got, err := store.GetMetadata(ctx, 1)
	if err != nil {
		t.Fatalf("GetMetadata: %v", err)
	}

	if got.Index != meta.Index {
		t.Errorf("Index: got %d, want %d", got.Index, meta.Index)
	}
	if got.NodeID != meta.NodeID {
		t.Errorf("NodeID: got %d, want %d", got.NodeID, meta.NodeID)
	}
	if !got.CreatedAt.Equal(meta.CreatedAt) {
		t.Errorf("CreatedAt: got %v, want %v", got.CreatedAt, meta.CreatedAt)
	}
	if got.Version != meta.Version {
		t.Errorf("Version: got %d, want %d", got.Version, meta.Version)
	}

	// Verify file was created with restrictive permissions.
	path := filepath.Join(dir, "seal", "metadata_1.json")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat metadata file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("file permissions: got %o, want 600", perm)
	}
}

func TestFileShareStore_SaveMetadata_Overwrite(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileShareStore(dir)
	if err != nil {
		t.Fatalf("NewFileShareStore: %v", err)
	}

	ctx := context.Background()

	original := &ShareMetadata{Index: 1, NodeID: 100, Version: 1}
	if err := store.SaveMetadata(ctx, original); err != nil {
		t.Fatalf("SaveMetadata (original): %v", err)
	}

	updated := &ShareMetadata{Index: 1, NodeID: 200, Version: 2}
	if err := store.SaveMetadata(ctx, updated); err != nil {
		t.Fatalf("SaveMetadata (updated): %v", err)
	}

	got, err := store.GetMetadata(ctx, 1)
	if err != nil {
		t.Fatalf("GetMetadata: %v", err)
	}
	if got.NodeID != 200 {
		t.Errorf("NodeID after overwrite: got %d, want 200", got.NodeID)
	}
	if got.Version != 2 {
		t.Errorf("Version after overwrite: got %d, want 2", got.Version)
	}
}

func TestFileShareStore_ListMetadata(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileShareStore(dir)
	if err != nil {
		t.Fatalf("NewFileShareStore: %v", err)
	}

	ctx := context.Background()

	// Save shares in reverse order to verify sorting.
	entries := []*ShareMetadata{
		{Index: 3, NodeID: 300, Version: 1},
		{Index: 1, NodeID: 100, Version: 1},
		{Index: 2, NodeID: 200, Version: 1},
	}
	for _, meta := range entries {
		if err := store.SaveMetadata(ctx, meta); err != nil {
			t.Fatalf("SaveMetadata index=%d: %v", meta.Index, err)
		}
	}

	results, err := store.ListMetadata(ctx)
	if err != nil {
		t.Fatalf("ListMetadata: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("ListMetadata: got %d entries, want 3", len(results))
	}

	// Verify results are sorted by index.
	for i, want := range []int{1, 2, 3} {
		if results[i].Index != want {
			t.Errorf("ListMetadata[%d].Index: got %d, want %d", i, results[i].Index, want)
		}
	}
}

func TestFileShareStore_ListMetadata_Empty(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileShareStore(dir)
	if err != nil {
		t.Fatalf("NewFileShareStore: %v", err)
	}

	results, err := store.ListMetadata(context.Background())
	if err != nil {
		t.Fatalf("ListMetadata: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("ListMetadata on empty store: got %d entries, want 0", len(results))
	}
}

func TestFileShareStore_SaveGetSealedShare(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileShareStore(dir)
	if err != nil {
		t.Fatalf("NewFileShareStore: %v", err)
	}

	ctx := context.Background()
	nodeID := uint64(42)
	shareData := []byte("encrypted-share-payload-abc123")

	if err := store.SaveSealedShare(ctx, nodeID, shareData); err != nil {
		t.Fatalf("SaveSealedShare: %v", err)
	}

	got, err := store.GetSealedShare(ctx, nodeID)
	if err != nil {
		t.Fatalf("GetSealedShare: %v", err)
	}

	if string(got) != string(shareData) {
		t.Errorf("GetSealedShare: got %q, want %q", got, shareData)
	}

	// Verify file was created with restrictive permissions.
	path := filepath.Join(dir, "seal", "share_42.sealed")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat sealed share file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("file permissions: got %o, want 600", perm)
	}
}

func TestFileShareStore_SaveGetVersion(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileShareStore(dir)
	if err != nil {
		t.Fatalf("NewFileShareStore: %v", err)
	}

	ctx := context.Background()

	if err := store.SaveVersion(ctx, 7); err != nil {
		t.Fatalf("SaveVersion: %v", err)
	}

	got, err := store.GetVersion(ctx)
	if err != nil {
		t.Fatalf("GetVersion: %v", err)
	}
	if got != 7 {
		t.Errorf("GetVersion: got %d, want 7", got)
	}

	// Overwrite with a new version.
	if err := store.SaveVersion(ctx, 42); err != nil {
		t.Fatalf("SaveVersion (overwrite): %v", err)
	}

	got, err = store.GetVersion(ctx)
	if err != nil {
		t.Fatalf("GetVersion (overwrite): %v", err)
	}
	if got != 42 {
		t.Errorf("GetVersion after overwrite: got %d, want 42", got)
	}
}

func TestFileShareStore_MetadataNotFound(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileShareStore(dir)
	if err != nil {
		t.Fatalf("NewFileShareStore: %v", err)
	}

	_, err = store.GetMetadata(context.Background(), 999)
	if err == nil {
		t.Fatal("GetMetadata: expected error for nonexistent index")
	}

	var storeErr *ShareStoreError
	if !errors.As(err, &storeErr) {
		t.Fatalf("GetMetadata error type: got %T, want *ShareStoreError", err)
	}
	if !errors.Is(storeErr.Err, ErrShareStoreNotFound) {
		t.Errorf("GetMetadata underlying error: got %v, want ErrShareStoreNotFound", storeErr.Err)
	}
}

func TestFileShareStore_SealedShareNotFound(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileShareStore(dir)
	if err != nil {
		t.Fatalf("NewFileShareStore: %v", err)
	}

	_, err = store.GetSealedShare(context.Background(), 999)
	if err == nil {
		t.Fatal("GetSealedShare: expected error for nonexistent node")
	}

	var storeErr *ShareStoreError
	if !errors.As(err, &storeErr) {
		t.Fatalf("GetSealedShare error type: got %T, want *ShareStoreError", err)
	}
	if !errors.Is(storeErr.Err, ErrShareStoreNotFound) {
		t.Errorf("GetSealedShare underlying error: got %v, want ErrShareStoreNotFound", storeErr.Err)
	}
}

func TestFileShareStore_VersionNotFound(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileShareStore(dir)
	if err != nil {
		t.Fatalf("NewFileShareStore: %v", err)
	}

	_, err = store.GetVersion(context.Background())
	if err == nil {
		t.Fatal("GetVersion: expected error for nonexistent version file")
	}

	var storeErr *ShareStoreError
	if !errors.As(err, &storeErr) {
		t.Fatalf("GetVersion error type: got %T, want *ShareStoreError", err)
	}
	if !errors.Is(storeErr.Err, ErrShareStoreNotFound) {
		t.Errorf("GetVersion underlying error: got %v, want ErrShareStoreNotFound", storeErr.Err)
	}
}

func TestFileShareStore_SaveMetadataError(t *testing.T) {

	dir := t.TempDir()
	store, err := NewFileShareStore(dir)
	if err != nil {
		t.Fatalf("NewFileShareStore: %v", err)
	}

	// Point sealDir at a path nested under a regular file. Creating files
	// inside a non-directory fails for all users, including root, which
	// makes this test reliable inside Docker containers.
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte{}, 0o600); err != nil {
		t.Fatalf("create blocker file: %v", err)
	}
	store.sealDir = filepath.Join(blocker, "seal")

	meta := &ShareMetadata{Index: 1, NodeID: 100, Version: 1}
	err = store.SaveMetadata(context.Background(), meta)
	if err == nil {
		t.Fatal("SaveMetadata: expected error on unwritable path")
	}

	var storeErr *ShareStoreError
	if !errors.As(err, &storeErr) {
		t.Fatalf("SaveMetadata error type: got %T, want *ShareStoreError", err)
	}
	if storeErr.Op != "save_metadata" {
		t.Errorf("SaveMetadata error op: got %q, want %q", storeErr.Op, "save_metadata")
	}
}

func TestFileShareStore_SaveSealedShareError(t *testing.T) {

	dir := t.TempDir()
	store, err := NewFileShareStore(dir)
	if err != nil {
		t.Fatalf("NewFileShareStore: %v", err)
	}

	// Point sealDir at a path nested under a regular file. Creating files
	// inside a non-directory fails for all users, including root, which
	// makes this test reliable inside Docker containers.
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte{}, 0o600); err != nil {
		t.Fatalf("create blocker file: %v", err)
	}
	store.sealDir = filepath.Join(blocker, "seal")

	err = store.SaveSealedShare(context.Background(), 1, []byte("data"))
	if err == nil {
		t.Fatal("SaveSealedShare: expected error on unwritable path")
	}

	var storeErr *ShareStoreError
	if !errors.As(err, &storeErr) {
		t.Fatalf("SaveSealedShare error type: got %T, want *ShareStoreError", err)
	}
	if storeErr.Op != "save_sealed_share" {
		t.Errorf("SaveSealedShare error op: got %q, want %q", storeErr.Op, "save_sealed_share")
	}
}

func TestFileShareStore_SaveVersionError(t *testing.T) {

	dir := t.TempDir()
	store, err := NewFileShareStore(dir)
	if err != nil {
		t.Fatalf("NewFileShareStore: %v", err)
	}

	// Point sealDir at a path nested under a regular file. Creating files
	// inside a non-directory fails for all users, including root, which
	// makes this test reliable inside Docker containers.
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte{}, 0o600); err != nil {
		t.Fatalf("create blocker file: %v", err)
	}
	store.sealDir = filepath.Join(blocker, "seal")

	err = store.SaveVersion(context.Background(), 1)
	if err == nil {
		t.Fatal("SaveVersion: expected error on unwritable path")
	}

	var storeErr *ShareStoreError
	if !errors.As(err, &storeErr) {
		t.Fatalf("SaveVersion error type: got %T, want *ShareStoreError", err)
	}
	if storeErr.Op != "save_version" {
		t.Errorf("SaveVersion error op: got %q, want %q", storeErr.Op, "save_version")
	}
}

func TestNewFileShareStore_EmptyDir(t *testing.T) {
	_, err := NewFileShareStore("")
	if err == nil {
		t.Fatal("NewFileShareStore: expected error for empty dir")
	}

	var storeErr *ShareStoreError
	if !errors.As(err, &storeErr) {
		t.Fatalf("error type: got %T, want *ShareStoreError", err)
	}
	if !errors.Is(storeErr.Err, ErrShareStoreInvalidDir) {
		t.Errorf("underlying error: got %v, want ErrShareStoreInvalidDir", storeErr.Err)
	}
}

func TestFileShareStore_SaveMetadata_NilMetadata(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileShareStore(dir)
	if err != nil {
		t.Fatalf("NewFileShareStore: %v", err)
	}

	err = store.SaveMetadata(context.Background(), nil)
	if err == nil {
		t.Fatal("SaveMetadata(nil): expected error")
	}

	var storeErr *ShareStoreError
	if !errors.As(err, &storeErr) {
		t.Fatalf("error type: got %T, want *ShareStoreError", err)
	}
	if !errors.Is(storeErr.Err, ErrShareStoreNilMetadata) {
		t.Errorf("underlying error: got %v, want ErrShareStoreNilMetadata", storeErr.Err)
	}
}

func TestFileShareStore_SaveMetadata_InvalidIndex(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileShareStore(dir)
	if err != nil {
		t.Fatalf("NewFileShareStore: %v", err)
	}

	meta := &ShareMetadata{Index: 0, NodeID: 100}
	err = store.SaveMetadata(context.Background(), meta)
	if err == nil {
		t.Fatal("SaveMetadata(index=0): expected error")
	}

	var storeErr *ShareStoreError
	if !errors.As(err, &storeErr) {
		t.Fatalf("error type: got %T, want *ShareStoreError", err)
	}
	if !errors.Is(storeErr.Err, ErrShareStoreInvalidIndex) {
		t.Errorf("underlying error: got %v, want ErrShareStoreInvalidIndex", storeErr.Err)
	}
}

func TestFileShareStore_GetMetadata_InvalidIndex(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileShareStore(dir)
	if err != nil {
		t.Fatalf("NewFileShareStore: %v", err)
	}

	_, err = store.GetMetadata(context.Background(), 0)
	if err == nil {
		t.Fatal("GetMetadata(index=0): expected error")
	}

	var storeErr *ShareStoreError
	if !errors.As(err, &storeErr) {
		t.Fatalf("error type: got %T, want *ShareStoreError", err)
	}
	if !errors.Is(storeErr.Err, ErrShareStoreInvalidIndex) {
		t.Errorf("underlying error: got %v, want ErrShareStoreInvalidIndex", storeErr.Err)
	}
}

func TestFileShareStore_SaveSealedShare_NilData(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileShareStore(dir)
	if err != nil {
		t.Fatalf("NewFileShareStore: %v", err)
	}

	err = store.SaveSealedShare(context.Background(), 1, nil)
	if err == nil {
		t.Fatal("SaveSealedShare(nil): expected error")
	}

	var storeErr *ShareStoreError
	if !errors.As(err, &storeErr) {
		t.Fatalf("error type: got %T, want *ShareStoreError", err)
	}
	if !errors.Is(storeErr.Err, ErrShareStoreNilShare) {
		t.Errorf("underlying error: got %v, want ErrShareStoreNilShare", storeErr.Err)
	}
}

func TestFileShareStore_ListMetadata_CorruptJSON(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := NewFileShareStore(dir)
	if err != nil {
		t.Fatalf("NewFileShareStore: %v", err)
	}

	// Write a corrupt metadata file directly to the seal directory.
	corruptPath := filepath.Join(dir, "seal", "metadata_1.json")
	if err := os.WriteFile(corruptPath, []byte("not-valid-json{{{"), 0o600); err != nil {
		t.Fatalf("write corrupt file: %v", err)
	}

	_, err = store.ListMetadata(context.Background())
	if err == nil {
		t.Fatal("ListMetadata: expected error for corrupt JSON file")
	}

	var storeErr *ShareStoreError
	if !errors.As(err, &storeErr) {
		t.Fatalf("error type: got %T, want *ShareStoreError", err)
	}
	if storeErr.Op != "list_metadata" {
		t.Errorf("error op: got %q, want %q", storeErr.Op, "list_metadata")
	}
}

func TestFileShareStore_GetVersion_InvalidContent(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := NewFileShareStore(dir)
	if err != nil {
		t.Fatalf("NewFileShareStore: %v", err)
	}

	// Write non-numeric content to the version file.
	versionPath := filepath.Join(dir, "seal", "version")
	if err := os.WriteFile(versionPath, []byte("not-a-number"), 0o600); err != nil {
		t.Fatalf("write version file: %v", err)
	}

	_, err = store.GetVersion(context.Background())
	if err == nil {
		t.Fatal("GetVersion: expected error for non-numeric version file")
	}

	var storeErr *ShareStoreError
	if !errors.As(err, &storeErr) {
		t.Fatalf("error type: got %T, want *ShareStoreError", err)
	}
	if storeErr.Op != "get_version" {
		t.Errorf("error op: got %q, want %q", storeErr.Op, "get_version")
	}
}

func TestFileShareStore_GetMetadata_NegativeIndex(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := NewFileShareStore(dir)
	if err != nil {
		t.Fatalf("NewFileShareStore: %v", err)
	}

	_, err = store.GetMetadata(context.Background(), -1)
	if err == nil {
		t.Fatal("GetMetadata(index=-1): expected error")
	}

	var storeErr *ShareStoreError
	if !errors.As(err, &storeErr) {
		t.Fatalf("error type: got %T, want *ShareStoreError", err)
	}
	if !errors.Is(storeErr.Err, ErrShareStoreInvalidIndex) {
		t.Errorf("underlying error: got %v, want ErrShareStoreInvalidIndex", storeErr.Err)
	}
}

func TestFileShareStore_SaveMetadata_NegativeIndex(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := NewFileShareStore(dir)
	if err != nil {
		t.Fatalf("NewFileShareStore: %v", err)
	}

	meta := &ShareMetadata{Index: -1, NodeID: 100}
	err = store.SaveMetadata(context.Background(), meta)
	if err == nil {
		t.Fatal("SaveMetadata(index=-1): expected error")
	}

	var storeErr *ShareStoreError
	if !errors.As(err, &storeErr) {
		t.Fatalf("error type: got %T, want *ShareStoreError", err)
	}
	if !errors.Is(storeErr.Err, ErrShareStoreInvalidIndex) {
		t.Errorf("underlying error: got %v, want ErrShareStoreInvalidIndex", storeErr.Err)
	}
}

func TestNewFileShareStore_UncreatableDir(t *testing.T) {
	t.Parallel()

	// Use a path nested under a regular file so os.MkdirAll fails.
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte{}, 0o600); err != nil {
		t.Fatalf("create blocker file: %v", err)
	}

	_, err := NewFileShareStore(filepath.Join(blocker, "nested"))
	if err == nil {
		t.Fatal("NewFileShareStore: expected error for uncreatable dir")
	}

	var storeErr *ShareStoreError
	if !errors.As(err, &storeErr) {
		t.Fatalf("error type: got %T, want *ShareStoreError", err)
	}
	if storeErr.Op != "new" {
		t.Errorf("error op: got %q, want %q", storeErr.Op, "new")
	}
}

func TestFileShareStore_ListMetadata_NonexistentDir(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := NewFileShareStore(dir)
	if err != nil {
		t.Fatalf("NewFileShareStore: %v", err)
	}

	// Remove the seal directory after creation.
	if err := os.RemoveAll(filepath.Join(dir, "seal")); err != nil {
		t.Fatalf("remove seal dir: %v", err)
	}

	_, err = store.ListMetadata(context.Background())
	if err == nil {
		t.Fatal("ListMetadata: expected error when seal dir is removed")
	}

	var storeErr *ShareStoreError
	if !errors.As(err, &storeErr) {
		t.Fatalf("error type: got %T, want *ShareStoreError", err)
	}
	if storeErr.Op != "list_metadata" {
		t.Errorf("error op: got %q, want %q", storeErr.Op, "list_metadata")
	}
}

func TestFileShareStore_InterfaceCompliance(t *testing.T) {
	var _ ShareStore = (*FileShareStore)(nil)
}
