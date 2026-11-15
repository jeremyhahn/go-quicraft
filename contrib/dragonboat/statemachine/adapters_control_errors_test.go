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

package statemachine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
)

// ctlRegularSM is a regular dragonboat state machine whose SaveSnapshot and
// RecoverFromSnapshot return the configured errors.
type ctlRegularSM struct {
	counterSM
	saveErr    error
	recoverErr error
}

func (c *ctlRegularSM) SaveSnapshot(_ io.Writer, _ ISnapshotFileCollection, _ <-chan struct{}) error {
	return c.saveErr
}

func (c *ctlRegularSM) RecoverFromSnapshot(_ io.Reader, _ []SnapshotFile, _ <-chan struct{}) error {
	return c.recoverErr
}

// ctlConcurrentSM is a concurrent dragonboat state machine whose
// SaveSnapshot and RecoverFromSnapshot return the configured errors.
type ctlConcurrentSM struct {
	concurrentCounterSM
	saveErr    error
	recoverErr error
}

func (c *ctlConcurrentSM) SaveSnapshot(_ interface{}, _ io.Writer, _ ISnapshotFileCollection, _ <-chan struct{}) error {
	return c.saveErr
}

func (c *ctlConcurrentSM) RecoverFromSnapshot(_ io.Reader, _ []SnapshotFile, _ <-chan struct{}) error {
	return c.recoverErr
}

// ctlDiskSM is an on-disk dragonboat state machine whose SaveSnapshot and
// RecoverFromSnapshot return the configured errors.
type ctlDiskSM struct {
	diskCounterSM
	saveErr    error
	recoverErr error
}

func (d *ctlDiskSM) SaveSnapshot(_ interface{}, _ io.Writer, _ <-chan struct{}) error {
	return d.saveErr
}

func (d *ctlDiskSM) RecoverFromSnapshot(_ io.Reader, _ <-chan struct{}) error {
	return d.recoverErr
}

// ctlAdapter is one adapter kind under test: snapshot calls on an adapter
// built through the package's create-function wrapper around an inner state
// machine that returns saveErr and recoverErr.
type ctlAdapter struct {
	name string
	// build returns save and recover functions of an adapter whose inner
	// state machine fails with saveErr and recoverErr.
	build func(t *testing.T, saveErr, recoverErr error) (
		save func(ctx context.Context) error,
		recoverFn func(ctx context.Context) error,
	)
}

type ctlRegularSnapshotter interface {
	SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error
	RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error
}

type ctlCtxSnapshotter interface {
	SaveSnapshot(ctx context.Context, snapshotCtx interface{}, w io.Writer, stopper <-chan struct{}) error
	RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error
}

func ctlAdapters() []ctlAdapter {
	return []ctlAdapter{
		{
			name: "regular",
			build: func(t *testing.T, saveErr, recoverErr error) (func(context.Context) error, func(context.Context) error) {
				t.Helper()
				create := WrapCreateFunc(func(_, _ uint64) IStateMachine {
					return &ctlRegularSM{saveErr: saveErr, recoverErr: recoverErr}
				})
				a, ok := create(1, 1).(ctlRegularSnapshotter)
				if !ok {
					t.Fatal("WrapCreateFunc result does not implement the regular snapshot methods")
				}
				return func(ctx context.Context) error {
						return a.SaveSnapshot(ctx, &bytes.Buffer{}, make(chan struct{}))
					}, func(ctx context.Context) error {
						return a.RecoverFromSnapshot(ctx, &bytes.Buffer{}, make(chan struct{}))
					}
			},
		},
		{
			name: "concurrent",
			build: func(t *testing.T, saveErr, recoverErr error) (func(context.Context) error, func(context.Context) error) {
				t.Helper()
				create := WrapConcurrentCreateFunc(func(_, _ uint64) IConcurrentStateMachine {
					return &ctlConcurrentSM{saveErr: saveErr, recoverErr: recoverErr}
				})
				a, ok := create(1, 1).(ctlCtxSnapshotter)
				if !ok {
					t.Fatal("WrapConcurrentCreateFunc result does not implement the concurrent snapshot methods")
				}
				return func(ctx context.Context) error {
						return a.SaveSnapshot(ctx, nil, &bytes.Buffer{}, make(chan struct{}))
					}, func(ctx context.Context) error {
						return a.RecoverFromSnapshot(ctx, &bytes.Buffer{}, make(chan struct{}))
					}
			},
		},
		{
			name: "ondisk",
			build: func(t *testing.T, saveErr, recoverErr error) (func(context.Context) error, func(context.Context) error) {
				t.Helper()
				create := WrapOnDiskCreateFunc(func(_, _ uint64) IOnDiskStateMachine {
					return &ctlDiskSM{saveErr: saveErr, recoverErr: recoverErr}
				})
				a, ok := create(1, 1).(ctlCtxSnapshotter)
				if !ok {
					t.Fatal("WrapOnDiskCreateFunc result does not implement the on-disk snapshot methods")
				}
				return func(ctx context.Context) error {
						return a.SaveSnapshot(ctx, nil, &bytes.Buffer{}, make(chan struct{}))
					}, func(ctx context.Context) error {
						return a.RecoverFromSnapshot(ctx, &bytes.Buffer{}, make(chan struct{}))
					}
			},
		},
	}
}

var errCtlOther = errors.New("ctl: some other snapshot failure")

// assertCtlPassThrough checks that got is the sentinel passed through without
// an adapter error wrapper.
func assertCtlPassThrough(_ context.Context, t *testing.T, got, in, sentinel error) {
	t.Helper()
	if !errors.Is(got, sentinel) {
		t.Fatalf("got %v (%T), want an error that is %v", got, got, sentinel)
	}
	if got.Error() != in.Error() {
		t.Fatalf("got %v (%T), want the inner error %v returned unchanged", got, got, in)
	}
	var saveErr *AdapterSnapshotSaveError
	if errors.As(got, &saveErr) {
		t.Fatalf("got *AdapterSnapshotSaveError %v, want %v unwrapped", got, sentinel)
	}
	var recoverErr *AdapterSnapshotRecoverError
	if errors.As(got, &recoverErr) {
		t.Fatalf("got *AdapterSnapshotRecoverError %v, want %v unwrapped", got, sentinel)
	}
}

func TestAdapters_SaveSnapshot_ControlErrorPassThrough(t *testing.T) {
	ctx := context.Background()
	for _, kind := range ctlAdapters() {
		for _, sentinel := range []error{ErrSnapshotStopped, ErrSnapshotAborted, ErrSnapshotStreaming} {
			inputs := map[string]error{
				"bare":    sentinel,
				"wrapped": fmt.Errorf("inner save: %w", sentinel),
			}
			for form, in := range inputs {
				t.Run(kind.name+"/"+sentinel.Error()+"/"+form, func(t *testing.T) {
					save, _ := kind.build(t, in, nil)
					assertCtlPassThrough(ctx, t, save(ctx), in, sentinel)
				})
			}
		}
	}
}

func TestAdapters_SaveSnapshot_ControlErrorWrapsOther(t *testing.T) {
	ctx := context.Background()
	for _, kind := range ctlAdapters() {
		t.Run(kind.name, func(t *testing.T) {
			save, _ := kind.build(t, errCtlOther, nil)
			got := save(ctx)
			var saveErr *AdapterSnapshotSaveError
			if !errors.As(got, &saveErr) {
				t.Fatalf("got %v (%T), want *AdapterSnapshotSaveError", got, got)
			}
			if !errors.Is(saveErr.Cause, errCtlOther) {
				t.Fatalf("Cause = %v, want %v", saveErr.Cause, errCtlOther)
			}
			if !errors.Is(got, errCtlOther) {
				t.Fatalf("got %v, want an error that unwraps to %v", got, errCtlOther)
			}
		})
	}
}

func TestAdapters_RecoverFromSnapshot_ControlErrorStoppedPassThrough(t *testing.T) {
	ctx := context.Background()
	for _, kind := range ctlAdapters() {
		inputs := map[string]error{
			"bare":    ErrSnapshotStopped,
			"wrapped": fmt.Errorf("inner recover: %w", ErrSnapshotStopped),
		}
		for form, in := range inputs {
			t.Run(kind.name+"/"+form, func(t *testing.T) {
				_, recoverFn := kind.build(t, nil, in)
				assertCtlPassThrough(ctx, t, recoverFn(ctx), in, ErrSnapshotStopped)
			})
		}
	}
}

func TestAdapters_RecoverFromSnapshot_ControlErrorWrapsOthers(t *testing.T) {
	ctx := context.Background()
	inputs := map[string]error{
		"aborted":   ErrSnapshotAborted,
		"streaming": ErrSnapshotStreaming,
		"other":     errCtlOther,
	}
	for _, kind := range ctlAdapters() {
		for name, in := range inputs {
			t.Run(kind.name+"/"+name, func(t *testing.T) {
				_, recoverFn := kind.build(t, nil, in)
				got := recoverFn(ctx)
				var recoverErr *AdapterSnapshotRecoverError
				if !errors.As(got, &recoverErr) {
					t.Fatalf("got %v (%T), want *AdapterSnapshotRecoverError", got, got)
				}
				if !errors.Is(recoverErr.Cause, in) {
					t.Fatalf("Cause = %v, want %v", recoverErr.Cause, in)
				}
				if !errors.Is(got, in) {
					t.Fatalf("got %v, want an error that unwraps to %v", got, in)
				}
			})
		}
	}
}
