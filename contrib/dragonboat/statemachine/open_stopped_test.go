package statemachine

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// openStoppedSM is an on-disk state machine whose Open returns a chosen
// error and records the stopper channel it was given.
type openStoppedSM struct {
	*diskCounterSM
	openErr error
	gotStop <-chan struct{}
}

func (s *openStoppedSM) Open(stopper <-chan struct{}) (uint64, error) {
	s.gotStop = stopper
	if s.openErr != nil {
		return 42, s.openErr
	}
	return s.diskCounterSM.Open(stopper)
}

// adapterOpener is the quicraft-side Open of an adapter.
type adapterOpener interface {
	Open(ctx context.Context, initialDir string, stopper <-chan struct{}) (uint64, error)
}

// openStoppedCase builds an adapter, directly or through
// WrapOnDiskCreateFunc, over an inner state machine whose Open returns
// openErr, and calls the adapter's Open.
func openStoppedCase(ctx context.Context, t *testing.T, viaFactory bool, openErr error) (*openStoppedSM, uint64, error) {
	t.Helper()
	inner := &openStoppedSM{diskCounterSM: &diskCounterSM{count: 7}, openErr: openErr}
	var adapter adapterOpener
	if viaFactory {
		create := WrapOnDiskCreateFunc(func(_, _ uint64) IOnDiskStateMachine { return inner })
		a, ok := create(1, 1).(adapterOpener)
		if !ok {
			t.Fatalf("WrapOnDiskCreateFunc result %T has no quicraft Open", create(1, 1))
		}
		adapter = a
	} else {
		adapter = &diskAdapter{inner: inner}
	}
	index, err := adapter.Open(ctx, "/unused", make(chan struct{}))
	return inner, index, err
}

// assertOpenStoppedPassThrough checks that err is want, matches
// ErrOpenStopped and is not an *AdapterOpenError, with index 0.
func assertOpenStoppedPassThrough(ctx context.Context, t *testing.T, index uint64, err, want error) {
	t.Helper()
	if ctx.Err() != nil {
		t.Fatalf("context done: %v", ctx.Err())
	}
	if !errors.Is(err, ErrOpenStopped) {
		t.Fatalf("errors.Is(%v, ErrOpenStopped) = false", err)
	}
	var aoe *AdapterOpenError
	if errors.As(err, &aoe) {
		t.Fatalf("ErrOpenStopped was wrapped in *AdapterOpenError: %v", err)
	}
	if err != want {
		t.Fatalf("error = %#v, want the inner error value %#v", err, want)
	}
	if index != 0 {
		t.Fatalf("index = %d, want 0", index)
	}
}

func TestDiskAdapter_OpenStopped(t *testing.T) {
	ctx := context.Background()
	for _, viaFactory := range []bool{false, true} {
		name := "direct"
		if viaFactory {
			name = "WrapOnDiskCreateFunc"
		}
		t.Run(name, func(t *testing.T) {
			t.Run("sentinel", func(t *testing.T) {
				_, index, err := openStoppedCase(ctx, t, viaFactory, ErrOpenStopped)
				assertOpenStoppedPassThrough(ctx, t, index, err, ErrOpenStopped)
			})
			t.Run("wrapped sentinel", func(t *testing.T) {
				wrapped := fmt.Errorf("open: %w", ErrOpenStopped)
				_, index, err := openStoppedCase(ctx, t, viaFactory, wrapped)
				assertOpenStoppedPassThrough(ctx, t, index, err, wrapped)
			})
			t.Run("other error", func(t *testing.T) {
				other := &testOpenError{msg: "disk gone"}
				_, index, err := openStoppedCase(ctx, t, viaFactory, other)
				var aoe *AdapterOpenError
				if !errors.As(err, &aoe) {
					t.Fatalf("error %v is not an *AdapterOpenError", err)
				}
				if aoe.Cause != other {
					t.Fatalf("Cause = %v, want %v", aoe.Cause, other)
				}
				if errors.Is(err, ErrOpenStopped) {
					t.Fatalf("other error matched ErrOpenStopped: %v", err)
				}
				if index != 0 {
					t.Fatalf("index = %d, want 0", index)
				}
			})
		})
	}
}

func TestDiskAdapter_OpenStopped_ForwardsStopper(t *testing.T) {
	inner := &openStoppedSM{diskCounterSM: &diskCounterSM{count: 7}}
	adapter := &diskAdapter{inner: inner}
	stopper := make(chan struct{})
	index, err := adapter.Open(context.Background(), "/unused", stopper)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if index != 7 {
		t.Fatalf("index = %d, want 7", index)
	}
	if inner.gotStop != (<-chan struct{})(stopper) {
		t.Fatal("inner Open did not receive the adapter's stopper channel")
	}
}
