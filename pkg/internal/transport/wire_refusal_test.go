// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// farWindow shares no version with DefaultWireWindow.
func farWindow() WireWindow {
	return WireWindow{
		Proto:         proto.WireWindow{Current: proto.WireVersion + 2, Previous: proto.WireVersion + 1},
		FrameCurrent:  WireVersion + 2,
		FramePrevious: WireVersion + 1,
	}
}

func TestValidateFrameHeaderWindow_OutsideWindowIsTyped(t *testing.T) {
	far := farWindow()
	header := make([]byte, FrameHeaderSize)
	header[0], header[1], header[2] = MagicByte0, MagicByte1, WireVersion
	binary.BigEndian.PutUint32(header[4:8], 1)
	err := ValidateFrameHeaderWindow(header, far)
	var fe *FrameVersionError
	if !errors.As(err, &fe) {
		t.Fatalf("ValidateFrameHeaderWindow = %v, want *FrameVersionError", err)
	}
	if !errors.Is(err, ErrUnsupportedVersion) || !isProtocolViolation(err) {
		t.Fatalf("%v: errors.Is ErrUnsupportedVersion / isProtocolViolation false", err)
	}
	if fe.Found != WireVersion || fe.Current != far.FrameCurrent || fe.Previous != far.FramePrevious {
		t.Fatalf("FrameVersionError = %+v, want found %d against %+v", *fe, WireVersion, far)
	}
	for _, v := range []byte{WireVersion, far.FrameCurrent, far.FramePrevious} {
		if !strings.Contains(err.Error(), strconv.Itoa(int(v))) {
			t.Fatalf("%q does not name version %d", err, v)
		}
	}
}

func TestUnmarshalFromWindow_OutsideWindowIsTyped(t *testing.T) {
	far := farWindow()
	batch := proto.MessageBatch{BinVer: proto.WireVersion, MaxBinVer: proto.WireVersion, DeploymentID: 1}
	buf := make([]byte, batch.Size())
	if _, err := batch.MarshalTo(buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	var got proto.MessageBatch
	_, err := got.UnmarshalFromWindow(buf, far.Proto)
	var be *proto.BatchVersionError
	if !errors.As(err, &be) {
		t.Fatalf("UnmarshalFromWindow = %v, want *proto.BatchVersionError", err)
	}
	if !errors.Is(err, proto.ErrUnsupportedVersion) || !isProtocolViolation(err) {
		t.Fatalf("%v: errors.Is proto.ErrUnsupportedVersion / isProtocolViolation false", err)
	}
	if be.BinVer != proto.WireVersion || be.MaxBinVer != proto.WireVersion || be.Local != far.Proto {
		t.Fatalf("BatchVersionError = %+v", *be)
	}
}

func TestReportWireVersionRefused_InvokesCallback(t *testing.T) {
	type call struct {
		remote string
		err    error
	}
	calls := make(chan call, 4)
	tr := &QUICTransport{cfg: Config{OnWireVersionRefused: func(remote string, err error) {
		calls <- call{remote, err}
	}}}

	// Not a version refusal: ignored.
	tr.reportWireVersionRefused(nil, ErrInvalidMagic)
	frameErr := &FrameVersionError{Found: 9, Current: 1, Previous: 1}
	tr.reportWireVersionRefused(nil, frameErr)
	batchErr := &proto.BatchVersionError{BinVer: 9, MaxBinVer: 9, Local: proto.DefaultWireWindow()}
	tr.reportWireVersionRefused(nil, batchErr)

	for _, want := range []error{frameErr, batchErr} {
		select {
		case c := <-calls:
			if c.err != want || c.remote != "<unknown>" {
				t.Fatalf("callback(%q, %v), want (<unknown>, %v)", c.remote, c.err, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("callback not invoked for %v", want)
		}
	}
	select {
	case c := <-calls:
		t.Fatalf("unexpected callback(%q, %v)", c.remote, c.err)
	case <-time.After(50 * time.Millisecond):
	}

	// A nil callback and a panicking one are both harmless.
	(&QUICTransport{}).reportWireVersionRefused(nil, frameErr)
	done := make(chan struct{})
	(&QUICTransport{cfg: Config{OnWireVersionRefused: func(string, error) {
		close(done)
		panic("callback panic")
	}}}).reportWireVersionRefused(nil, frameErr)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("panicking callback not invoked")
	}
}

// TestHandleMessageFrame_OutsideWindowInvokesCallback drives a batch outside
// the window through the real receive path, so removing the callback from
// handleMessageFrame fails it.
func TestHandleMessageFrame_OutsideWindowInvokesCallback(t *testing.T) {
	far := farWindow()
	calls := make(chan error, 4)
	cfg := Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
		Wire:          far,
		OnWireVersionRefused: func(_ string, err error) {
			calls <- err
		},
	}
	tr, err := NewQUICTransport(cfg, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport: %v", err)
	}
	batch := proto.MessageBatch{BinVer: proto.WireVersion, MaxBinVer: proto.WireVersion, DeploymentID: 42}
	payload := make([]byte, batch.Size())
	if _, err := batch.MarshalTo(payload); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	ferr := tr.handleMessageFrame(nil, bytes.NewReader(payload), uint32(len(payload)), FlagNone)
	if !errors.Is(ferr, proto.ErrUnsupportedVersion) {
		t.Fatalf("handleMessageFrame = %v, want proto.ErrUnsupportedVersion", ferr)
	}
	select {
	case got := <-calls:
		var be *proto.BatchVersionError
		if !errors.As(got, &be) {
			t.Fatalf("callback error %v, want *proto.BatchVersionError", got)
		}
		if be.BinVer != proto.WireVersion || be.MaxBinVer != proto.WireVersion || be.Local != far.Proto {
			t.Fatalf("callback BatchVersionError = %+v, want %d/%d against %+v",
				*be, proto.WireVersion, proto.WireVersion, far.Proto)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnWireVersionRefused not invoked for a batch outside the window")
	}
}
