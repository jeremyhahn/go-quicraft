package dragonboat

import (
	"errors"
	"fmt"
	"io/fs"
	"testing"
)

// TestAdapterError_ErrorContract checks AdapterError's documented message
// and that it unwraps to the original quicraft error.
func TestAdapterError_ErrorContract(t *testing.T) {
	cause := errors.New("boom")
	ae := &AdapterError{Err: cause}

	if got, want := ae.Error(), "dragonboat: untranslated quicraft error: boom"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	if got := errors.Unwrap(ae); got != cause {
		t.Fatalf("errors.Unwrap = %v, want the original cause %v", got, cause)
	}
}

// TestShardSettingsError_ErrorContract checks ShardSettingsError's message,
// that it matches ErrInvalidShardSettings through a wrap, and that it does
// not match any other adapter sentinel.
func TestShardSettingsError_ErrorContract(t *testing.T) {
	cause := errors.New("bad rtt")
	sse := &ShardSettingsError{Err: cause}

	if got, want := sse.Error(), "dragonboat: invalid shard settings: bad rtt"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	if got := errors.Unwrap(sse); got != cause {
		t.Fatalf("errors.Unwrap = %v, want the original cause %v", got, cause)
	}
	if !errors.Is(fmt.Errorf("x: %w", sse), ErrInvalidShardSettings) {
		t.Fatal("wrapped ShardSettingsError does not match ErrInvalidShardSettings")
	}
	if errors.Is(sse, ErrInvalidOption) {
		t.Fatal("ShardSettingsError matches ErrInvalidOption")
	}
	if errors.Is(sse, ErrInvalidTarget) {
		t.Fatal("ShardSettingsError matches ErrInvalidTarget")
	}
}

// TestRemovalMarkerError_ErrorContract checks RemovalMarkerError's message
// and that it unwraps to its filesystem cause.
func TestRemovalMarkerError_ErrorContract(t *testing.T) {
	rme := &RemovalMarkerError{Op: "create", Path: "/d/1-1", Err: fs.ErrPermission}

	want := "dragonboat: removed-replica marker create failed: /d/1-1: permission denied"
	if got := rme.Error(); got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	if !errors.Is(rme, fs.ErrPermission) {
		t.Fatal("RemovalMarkerError does not match fs.ErrPermission")
	}
}

// TestTranslateErrorTypedSentinels_ErrorContract checks that translateError
// returns the typed errors carrying an adapter sentinel unchanged, without
// an AdapterError wrapper.
func TestTranslateErrorTypedSentinels_ErrorContract(t *testing.T) {
	sse := &ShardSettingsError{Err: errors.New("bad rtt")}
	got := translateError(sse)
	if gotSSE, ok := got.(*ShardSettingsError); !ok || gotSSE != sse {
		t.Fatalf("translateError(ShardSettingsError) = %T %v, want the same pointer", got, got)
	}

	got = translateError(&CompactionIndexNotSupportedError{})
	var cine *CompactionIndexNotSupportedError
	if !errors.As(got, &cine) {
		t.Fatalf("translateError(CompactionIndexNotSupportedError) = %T, not reachable via errors.As", got)
	}
	if _, isAdapter := got.(*AdapterError); isAdapter {
		t.Fatal("translateError wrapped CompactionIndexNotSupportedError in an AdapterError")
	}
}

// TestTranslateErrorRemovalMarker_ErrorContract checks that a
// RemovalMarkerError, which carries no adapter sentinel, is wrapped in an
// AdapterError that still exposes the marker error and its cause.
func TestTranslateErrorRemovalMarker_ErrorContract(t *testing.T) {
	rme := &RemovalMarkerError{Op: "create", Path: "/d/1-1", Err: fs.ErrPermission}
	got := translateError(rme)

	ae, ok := got.(*AdapterError)
	if !ok {
		t.Fatalf("translateError(RemovalMarkerError) = %T, want *AdapterError", got)
	}
	var gotRME *RemovalMarkerError
	if !errors.As(ae, &gotRME) || gotRME != rme {
		t.Fatalf("errors.As gave %v, want the original RemovalMarkerError", gotRME)
	}
	if !errors.Is(got, fs.ErrPermission) {
		t.Fatal("translated RemovalMarkerError does not match fs.ErrPermission")
	}
}

// TestIsTempErrorTypedErrors_ErrorContract checks that none of the typed
// configuration or filesystem errors is reported as temporary.
func TestIsTempErrorTypedErrors_ErrorContract(t *testing.T) {
	cases := map[string]error{
		"ShardSettingsError":               &ShardSettingsError{Err: errors.New("bad rtt")},
		"RemovalMarkerError":               &RemovalMarkerError{Op: "create", Path: "/d/1-1", Err: fs.ErrPermission},
		"CompactionIndexNotSupportedError": &CompactionIndexNotSupportedError{},
	}
	for name, err := range cases {
		if IsTempError(err) {
			t.Errorf("IsTempError(%s) = true, want false", name)
		}
	}
}
