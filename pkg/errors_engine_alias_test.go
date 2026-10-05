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

package quicraft

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// ---------------------------------------------------------------------------
// Errors that reach an embedder from the engine, raft and transport packages
//
// Three channels hand an embedder an error built inside an internal package:
// the result of a proposal whose batch failed to apply or persist, the Err of
// config.ShardFailedInfo, and the cause inside a *HostInitError. Those
// packages are unimportable from outside this module, so pkg/errors.go
// re-exports each such error as an alias of the SAME value or type. A
// re-export that decays into errors.New("<same text>") or a look-alike struct
// keeps compiling and silently stops matching, so every identity assertion is
// paired with a look-alike that must NOT match.
// ---------------------------------------------------------------------------

// internalSentinelAliases is the complete set of engine, raft and transport
// sentinels pkg/errors.go re-exports for the three channels above. A
// re-export missing from this table is unguarded.
var internalSentinelAliases = []struct {
	name     string
	exported error
	internal error
}{
	{"ErrShardFailed", ErrShardFailed, engine.ErrShardFailed},
	{"ErrSnapshotCorrupt", ErrSnapshotCorrupt, engine.ErrSnapshotCorrupt},
	{"ErrSnapshotSizeMismatch", ErrSnapshotSizeMismatch, engine.ErrSnapshotSizeMismatch},
	{"ErrDecompressionBomb", ErrDecompressionBomb, raft.ErrDecompressionBomb},
	{"ErrApplyGap", ErrApplyGap, engine.ErrApplyGap},
	{"ErrSnapshotMetaCorrupt", ErrSnapshotMetaCorrupt, engine.ErrSnapshotMetaCorrupt},
	{"ErrScratchNotUnlinked", ErrScratchNotUnlinked, engine.ErrScratchNotUnlinked},
	{"ErrUnsupportedSnapshotCompression", ErrUnsupportedSnapshotCompression, engine.ErrUnsupportedSnapshotCompression},
	{"ErrCorruptConfigEntry", ErrCorruptConfigEntry, raft.ErrCorruptConfigEntry},
	{"ErrEntropyUnavailable", ErrEntropyUnavailable, raft.ErrEntropyUnavailable},
	{"ErrCACertificateParse", ErrCACertificateParse, transport.ErrCACertificateParse},
	{"ErrInvalidCertKeyPair", ErrInvalidCertKeyPair, transport.ErrInvalidCertKeyPair},
}

// TestInternalSentinelAliasesAreTheInternalValues pins the identity of every
// re-export and proves it matches through a wrapping layer, which is how each
// one actually reaches a caller.
func TestInternalSentinelAliasesAreTheInternalValues(t *testing.T) {
	for _, a := range internalSentinelAliases {
		t.Run(a.name, func(t *testing.T) {
			if a.exported == nil || a.internal == nil {
				t.Fatalf("%s: nil sentinel (exported=%v internal=%v)", a.name, a.exported, a.internal)
			}
			if a.exported != a.internal {
				t.Fatalf("quicraft.%s is not the same error VALUE the internal package "+
					"returns; an embedder matching the public name no longer matches "+
					"the error the library hands it", a.name)
			}
			wrapped := &HostInitError{Field: "test", Err: fmt.Errorf("layer: %w", a.internal)}
			if !errors.Is(wrapped, a.exported) {
				t.Fatalf("errors.Is(%v, quicraft.%s) = false through wrapping", wrapped, a.name)
			}
			lookAlike := errors.New(a.exported.Error())
			if errors.Is(lookAlike, a.exported) {
				t.Fatalf("errors.Is matched a look-alike of %s with identical text; "+
					"identity is not being tested, message equality is", a.name)
			}
		})
	}
}

// TestInternalSentinelAliasesAreDistinct keeps two re-exports from collapsing
// onto one value, which would make errors.Is answer true for the wrong one.
func TestInternalSentinelAliasesAreDistinct(t *testing.T) {
	seen := make(map[error]string, len(internalSentinelAliases))
	for _, a := range internalSentinelAliases {
		if prev, dup := seen[a.exported]; dup {
			t.Errorf("quicraft.%s and quicraft.%s are the same value", prev, a.name)
		}
		seen[a.exported] = a.name
	}
}

// TestInternalErrorTypeAliasesMatchThroughWrapping proves every public error
// type re-exported from an internal package IS the internal type: errors.As
// into the public type recovers the fields of the value the internal package
// builds, through the wrapping each channel applies, and the typed error still
// matches the sentinel its Is method names.
func TestInternalErrorTypeAliasesMatchThroughWrapping(t *testing.T) {
	cause := errors.New("quicraft_test: cause")
	tests := []struct {
		name string
		// built is the error exactly as the internal package constructs it.
		built error
		// check recovers built through the public type and verifies it.
		check func(t *testing.T, err error)
	}{
		{
			name:  "ApplyError",
			built: &engine.ApplyError{ShardID: 7, ReplicaID: 3, Err: cause},
			check: func(t *testing.T, err error) {
				var typed *ApplyError
				if !errors.As(err, &typed) || typed.ShardID != 7 || typed.ReplicaID != 3 {
					t.Fatalf("errors.As(*ApplyError) on %v = %+v", err, typed)
				}
				if !errors.Is(err, cause) {
					t.Fatal("*ApplyError does not unwrap to the state machine's error")
				}
			},
		},
		{
			name:  "ApplyError wrapping a decompression bomb",
			built: &engine.ApplyError{ShardID: 7, ReplicaID: 3, Err: raft.ErrDecompressionBomb},
			check: func(t *testing.T, err error) {
				if !errors.Is(err, ErrDecompressionBomb) {
					t.Fatalf("errors.Is(%v, ErrDecompressionBomb) = false", err)
				}
			},
		},
		{
			name:  "ApplyError wrapping a DecompressError",
			built: &engine.ApplyError{ShardID: 7, ReplicaID: 3, Err: &raft.DecompressError{Err: cause}},
			check: func(t *testing.T, err error) {
				var typed *DecompressError
				if !errors.As(err, &typed) || !errors.Is(typed.Err, cause) {
					t.Fatalf("errors.As(*DecompressError) on %v = %+v", err, typed)
				}
			},
		},
		{
			name:  "ApplyError wrapping an apply gap",
			built: &engine.ApplyError{ShardID: 7, ReplicaID: 3, Err: engine.ErrApplyGap},
			check: func(t *testing.T, err error) {
				if !errors.Is(err, ErrApplyGap) {
					t.Fatalf("errors.Is(%v, ErrApplyGap) = false", err)
				}
			},
		},
		{
			name:  "SMPanicError",
			built: &engine.SMPanicError{ShardID: 7, ReplicaID: 3, Value: "boom"},
			check: func(t *testing.T, err error) {
				var typed *SMPanicError
				if !errors.As(err, &typed) || typed.Value != "boom" {
					t.Fatalf("errors.As(*SMPanicError) on %v = %+v", err, typed)
				}
				if !errors.Is(err, ErrShardFailed) {
					t.Fatalf("errors.Is(%v, ErrShardFailed) = false: a panicked state "+
						"machine is a failed shard and must say so by public identity", err)
				}
			},
		},
		{
			name:  "SaveStateError",
			built: &engine.SaveStateError{ShardID: 7, ReplicaID: 3, Err: logdb.ErrClosed},
			check: func(t *testing.T, err error) {
				var typed *SaveStateError
				if !errors.As(err, &typed) || typed.ShardID != 7 {
					t.Fatalf("errors.As(*SaveStateError) on %v = %+v", err, typed)
				}
				if !errors.Is(err, logdb.ErrClosed) {
					t.Fatal("*SaveStateError does not unwrap to the LogDB error")
				}
			},
		},
		{
			name:  "CommitError",
			built: &engine.CommitError{ShardID: 7, ReplicaID: 3, Err: raft.ErrCorruptConfigEntry},
			check: func(t *testing.T, err error) {
				var typed *CommitError
				if !errors.As(err, &typed) || typed.ReplicaID != 3 {
					t.Fatalf("errors.As(*CommitError) on %v = %+v", err, typed)
				}
				if !errors.Is(err, ErrCorruptConfigEntry) {
					t.Fatalf("errors.Is(%v, ErrCorruptConfigEntry) = false", err)
				}
			},
		},
		{
			name:  "WorkerPanicError",
			built: &engine.WorkerPanicError{WorkerID: 2, WorkerType: "step", Value: "boom"},
			check: func(t *testing.T, err error) {
				var typed *WorkerPanicError
				if !errors.As(err, &typed) || typed.WorkerID != 2 || typed.WorkerType != "step" {
					t.Fatalf("errors.As(*WorkerPanicError) on %v = %+v", err, typed)
				}
			},
		},
		{
			name: "SnapshotError wrapping an integrity failure",
			built: &engine.SnapshotError{ShardID: 7, ReplicaID: 3,
				Err: &engine.SnapshotMetaInvalidError{Dir: "/snap"}},
			check: func(t *testing.T, err error) {
				var typed *SnapshotError
				if !errors.As(err, &typed) || typed.Save {
					t.Fatalf("errors.As(*SnapshotError) on %v = %+v", err, typed)
				}
				var meta *SnapshotMetaInvalidError
				if !errors.As(err, &meta) || meta.Dir != "/snap" {
					t.Fatalf("errors.As(*SnapshotMetaInvalidError) on %v = %+v", err, meta)
				}
				if !errors.Is(err, ErrSnapshotCorrupt) || !errors.Is(err, ErrSnapshotMetaCorrupt) {
					t.Fatalf("%v does not match ErrSnapshotCorrupt and ErrSnapshotMetaCorrupt", err)
				}
			},
		},
		{
			name:  "SnapshotRecoveryBehindError",
			built: &engine.SnapshotRecoveryBehindError{ShardID: 7, ReplicaID: 3, SnapshotIndex: 9},
			check: func(t *testing.T, err error) {
				var typed *SnapshotRecoveryBehindError
				if !errors.As(err, &typed) || typed.SnapshotIndex != 9 {
					t.Fatalf("errors.As(*SnapshotRecoveryBehindError) on %v = %+v", err, typed)
				}
			},
		},
		{
			name: "SnapshotRecoveryExhaustedError wrapping a pre-restore panic",
			built: &engine.SnapshotRecoveryExhaustedError{
				ShardID: 7, ReplicaID: 3, SnapshotIndex: 9, Attempts: 5,
				Err: &engine.SnapshotRecoveryPanicError{ShardID: 7, ReplicaID: 3, Value: "boom"},
			},
			check: func(t *testing.T, err error) {
				var typed *SnapshotRecoveryExhaustedError
				if !errors.As(err, &typed) || typed.Attempts != 5 || typed.SnapshotIndex != 9 {
					t.Fatalf("errors.As(*SnapshotRecoveryExhaustedError) on %v = %+v", err, typed)
				}
				var cause *SnapshotRecoveryPanicError
				if !errors.As(err, &cause) || cause.Value != "boom" {
					t.Fatalf("errors.As(*SnapshotRecoveryPanicError) on %v = %+v", err, cause)
				}
			},
		},
		{
			name:  "SnapshotEncryptError wrapping a corrupt frame",
			built: &engine.SnapshotEncryptError{Op: "decrypt", Err: engine.ErrSnapshotCorrupt},
			check: func(t *testing.T, err error) {
				var typed *SnapshotEncryptError
				if !errors.As(err, &typed) || typed.Op != "decrypt" {
					t.Fatalf("errors.As(*SnapshotEncryptError) on %v = %+v", err, typed)
				}
				if !errors.Is(err, ErrSnapshotCorrupt) {
					t.Fatalf("errors.Is(%v, ErrSnapshotCorrupt) = false", err)
				}
			},
		},
		{
			name:  "ScratchUnlinkError",
			built: &engine.ScratchUnlinkError{Path: "/scratch", Err: cause},
			check: func(t *testing.T, err error) {
				var typed *ScratchUnlinkError
				if !errors.As(err, &typed) || typed.Path != "/scratch" {
					t.Fatalf("errors.As(*ScratchUnlinkError) on %v = %+v", err, typed)
				}
				if !errors.Is(err, ErrScratchNotUnlinked) {
					t.Fatalf("errors.Is(%v, ErrScratchNotUnlinked) = false", err)
				}
			},
		},
		{
			name:  "engine snapshot size mismatch",
			built: &engine.SnapshotSizeMismatchError{Path: "/snap", Recorded: 10, OnDisk: 4},
			check: func(t *testing.T, err error) {
				if !errors.Is(err, ErrSnapshotSizeMismatch) {
					t.Fatalf("errors.Is(%v, ErrSnapshotSizeMismatch) = false", err)
				}
			},
		},
		{
			name:  "CACertificateParseError",
			built: &transport.CACertificateParseError{Reason: "no issuer"},
			check: func(t *testing.T, err error) {
				var typed *CACertificateParseError
				if !errors.As(err, &typed) || typed.Reason != "no issuer" {
					t.Fatalf("errors.As(*CACertificateParseError) on %v = %+v", err, typed)
				}
				if !errors.Is(err, ErrCACertificateParse) {
					t.Fatalf("errors.Is(%v, ErrCACertificateParse) = false", err)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Both shapes an embedder receives: a cause inside *HostInitError,
			// and a value wrapped once more by an application layer.
			tt.check(t, &HostInitError{Field: "test", Err: tt.built})
			tt.check(t, fmt.Errorf("application: %w", tt.built))
		})
	}
}

// TestApplyFailureReachesEmbedderByPublicIdentity drives a real apply failure
// and matches what the two channels deliver — the proposal's error and
// ShardFailedInfo.Err — using only names an embedder can import.
func TestApplyFailureReachesEmbedderByPublicIdentity(t *testing.T) {
	tests := []struct {
		name          string
		panicOnPoison bool
		// checkProposal and checkFailed match the two deliveries.
		checkProposal func(t *testing.T, err error)
		checkFailed   func(t *testing.T, err error)
	}{
		{
			name: "Update returns an error",
			checkProposal: func(t *testing.T, err error) {
				var applyErr *ApplyError
				if !errors.As(err, &applyErr) || !errors.Is(applyErr.Err, errPoisonedApply) {
					t.Fatalf("SyncPropose = %v (%T), want a *ApplyError wrapping the "+
						"state machine's error", err, err)
				}
			},
			checkFailed: func(t *testing.T, err error) {
				var applyErr *ApplyError
				if !errors.As(err, &applyErr) || applyErr.ShardID != 1 {
					t.Fatalf("ShardFailedInfo.Err = %v (%T), want a *ApplyError", err, err)
				}
			},
		},
		{
			name:          "Update panics",
			panicOnPoison: true,
			checkProposal: func(t *testing.T, err error) {
				if !errors.Is(err, ErrShardFailed) {
					t.Fatalf("SyncPropose = %v, want ErrShardFailed", err)
				}
			},
			checkFailed: func(t *testing.T, err error) {
				var panicErr *SMPanicError
				if !errors.As(err, &panicErr) || panicErr.ShardID != 1 {
					t.Fatalf("ShardFailedInfo.Err = %v (%T), want a *SMPanicError", err, err)
				}
				if !errors.Is(err, ErrShardFailed) {
					t.Fatalf("ShardFailedInfo.Err = %v does not match ErrShardFailed", err)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testHostConfig(t)
			failed := make(chan config.ShardFailedInfo, 4)
			cfg.EventListener = &config.EventListener{
				OnShardFailed: func(info config.ShardFailedInfo) {
					select {
					case failed <- info:
					default:
					}
				},
			}
			h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
			if err != nil {
				t.Fatalf("NewHost: %v", err)
			}
			defer h.Close()

			machine := &poisonApplySM{panicOnPoison: tt.panicOnPoison}
			create := func(uint64, uint64) interface{} { return machine }
			if startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
				create, testShardConfig(1, 1)); startErr != nil {
				t.Fatalf("StartShard: %v", startErr)
			}
			waitForLeader(t, h, 1)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, proposeErr := h.SyncPropose(ctx, 1, poisonCmd)
			tt.checkProposal(t, proposeErr)

			select {
			case info := <-failed:
				tt.checkFailed(t, info.Err)
			case <-time.After(5 * time.Second):
				t.Fatal("OnShardFailed never fired")
			}
		})
	}
}

// TestStartShardSnapshotIntegrityReachesEmbedderByPublicIdentity drives the
// startup recovery checks and matches the cause inside *HostInitError by the
// public names.
func TestStartShardSnapshotIntegrityReachesEmbedderByPublicIdentity(t *testing.T) {
	tests := []struct {
		name      string
		wantField string
		// record writes the artifact and returns the LogDB record for it.
		record func(t *testing.T, dir string) logdb.Snapshot
		check  func(t *testing.T, err error)
	}{
		{
			name:      "snapshot.meta missing",
			wantField: "snapshot_integrity",
			record: func(t *testing.T, dir string) logdb.Snapshot {
				writeTestArtifact(t, dir)
				return logdb.Snapshot{Index: 42, Term: 1, Filepath: dir}
			},
			check: func(t *testing.T, err error) {
				var meta *SnapshotMetaInvalidError
				if !errors.As(err, &meta) {
					t.Fatalf("StartShard = %v, want a *SnapshotMetaInvalidError cause", err)
				}
				if !errors.Is(err, ErrSnapshotCorrupt) || !errors.Is(err, ErrSnapshotMetaCorrupt) {
					t.Fatalf("StartShard = %v, want ErrSnapshotCorrupt and ErrSnapshotMetaCorrupt", err)
				}
			},
		},
		{
			name:      "recorded FileSize disagrees",
			wantField: "snapshot_integrity",
			record: func(t *testing.T, dir string) logdb.Snapshot {
				writeTestArtifact(t, dir)
				if err := writeReceivedSnapshotMetadata(osSnapshotFS{}, dir, 42, 1, 1, 1, 0,
					config.NoCompression); err != nil {
					t.Fatalf("writeReceivedSnapshotMetadata: %v", err)
				}
				return logdb.Snapshot{Index: 42, Term: 1, Filepath: dir, FileSize: 1}
			},
			check: func(t *testing.T, err error) {
				if !errors.Is(err, ErrSnapshotSizeMismatch) {
					t.Fatalf("StartShard = %v, want ErrSnapshotSizeMismatch", err)
				}
			},
		},
		{
			name:      "unsupported compression",
			wantField: "snapshot_compression",
			record: func(t *testing.T, dir string) logdb.Snapshot {
				writeTestArtifact(t, dir)
				const bogus = config.CompressionType(250)
				if err := writeReceivedSnapshotMetadata(osSnapshotFS{}, dir, 42, 1, 1, 1, 0,
					bogus); err != nil {
					t.Fatalf("writeReceivedSnapshotMetadata: %v", err)
				}
				info, err := os.Stat(filepath.Join(dir, "snapshot.dat"))
				if err != nil {
					t.Fatalf("stat artifact: %v", err)
				}
				return logdb.Snapshot{Index: 42, Term: 1, Filepath: dir,
					FileSize: uint64(info.Size()), Compression: bogus}
			},
			check: func(t *testing.T, err error) {
				if !errors.Is(err, ErrUnsupportedSnapshotCompression) {
					t.Fatalf("StartShard = %v, want ErrUnsupportedSnapshotCompression", err)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newMockLogDB()
			h, err := NewHost(testHostConfig(t), WithLogDB(db), WithoutTransport(), WithoutEncryption())
			if err != nil {
				t.Fatalf("NewHost: %v", err)
			}
			defer h.Close()

			if err := db.SaveSnapshot(1, 1, tt.record(t, t.TempDir())); err != nil {
				t.Fatalf("SaveSnapshot: %v", err)
			}
			startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
				testCreateFunc, testShardConfig(1, 1))
			var hie *HostInitError
			if !errors.As(startErr, &hie) || hie.Field != tt.wantField {
				t.Fatalf("StartShard = %v, want HostInitError{Field: %s}", startErr, tt.wantField)
			}
			tt.check(t, startErr)
		})
	}
}

// TestNewHostTransportErrorsReachEmbedderByPublicIdentity drives the two
// transport-constructor refusals host configuration validation does not
// already catch, and matches the cause inside *HostInitError by public name.
func TestNewHostTransportErrorsReachEmbedderByPublicIdentity(t *testing.T) {
	t.Run("CA bundle with no issuer under revocation", func(t *testing.T) {
		cfg := testTransportHostConfig(t)
		leafPEM, keyPEM := nonCACertificatePEM(t)
		cfg.TransportConfig.MTLSConfig = &config.MTLSConfig{CACert: leafPEM, Cert: leafPEM, Key: keyPEM}
		cfg.TransportConfig.RevocationConfig = &config.RevocationConfig{AllowSoftFail: true}

		h, err := NewHost(cfg, WithMemoryLogDB(), WithoutEncryption())
		if err == nil {
			h.Close()
			t.Fatal("NewHost accepted a CA bundle with no issuer under revocation checking")
		}
		var hie *HostInitError
		if !errors.As(err, &hie) || hie.Field != "transport" {
			t.Fatalf("NewHost = %v, want HostInitError{Field: transport}", err)
		}
		var parseErr *CACertificateParseError
		if !errors.As(err, &parseErr) || !errors.Is(err, ErrCACertificateParse) {
			t.Fatalf("NewHost = %v, want a *CACertificateParseError matching ErrCACertificateParse", err)
		}
	})

	t.Run("pre-built certificate whose leaf does not parse", func(t *testing.T) {
		cfg := testTransportHostConfig(t)
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatalf("generate key: %v", err)
		}
		cfg.TransportConfig.MTLSConfig = &config.MTLSConfig{
			CACert:      cfg.TransportConfig.MTLSConfig.CACert,
			Certificate: &tls.Certificate{Certificate: [][]byte{{0x30, 0x00}}, PrivateKey: key},
		}

		h, err := NewHost(cfg, WithMemoryLogDB(), WithoutEncryption())
		if err == nil {
			h.Close()
			t.Fatal("NewHost accepted a pre-built certificate whose leaf does not parse")
		}
		var hie *HostInitError
		if !errors.As(err, &hie) || hie.Field != "transport" {
			t.Fatalf("NewHost = %v, want HostInitError{Field: transport}", err)
		}
		if !errors.Is(err, ErrInvalidCertKeyPair) {
			t.Fatalf("NewHost = %v, want ErrInvalidCertKeyPair", err)
		}
	})
}

// nonCACertificatePEM returns a self-signed leaf that cannot act as an issuer:
// neither IsCA nor KeyUsageCertSign. Host validation accepts it as a CA bundle
// (any parseable certificate does), but revocation checking needs an issuer.
func nonCACertificatePEM(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "quicraft-test-leaf"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}
