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

package client

import (
	"testing"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// TestSessionValidity_NeverPreparedRefusesProposal checks that a session
// fresh from NewSession, whose series ID is below SeriesIDFirstProposal,
// is not valid for a normal proposal.
func TestSessionValidity_NeverPreparedRefusesProposal(t *testing.T) {
	s := NewSession(10, &testRNG{val: 500})
	if s.SeriesID() >= quicraft.SeriesIDFirstProposal {
		t.Fatalf("fresh SeriesID = %d, want below %d",
			s.SeriesID(), quicraft.SeriesIDFirstProposal)
	}
	if s.ValidForProposal(10) {
		t.Error("ValidForProposal must refuse a session that was never prepared")
	}
}

// TestSessionValidity_RespondedToBound checks that RespondedTo equal to
// SeriesID keeps the session valid and RespondedTo past SeriesID makes it
// invalid.
func TestSessionValidity_RespondedToBound(t *testing.T) {
	s := NewSession(10, &testRNG{val: 500})
	s.PrepareForPropose()

	s.Inner().SetRespondedTo(s.SeriesID())
	if got := s.Inner().RespondedTo(); got != s.SeriesID() {
		t.Fatalf("RespondedTo = %d, want %d", got, s.SeriesID())
	}
	if !s.ValidForProposal(10) {
		t.Error("ValidForProposal must accept RespondedTo == SeriesID")
	}

	s.Inner().SetRespondedTo(s.SeriesID() + 5)
	if got := s.Inner().RespondedTo(); got != s.SeriesID()+5 {
		t.Fatalf("RespondedTo = %d, want %d", got, s.SeriesID()+5)
	}
	if s.ValidForProposal(10) {
		t.Error("ValidForProposal must refuse RespondedTo > SeriesID")
	}
}

// TestSessionValidity_PrepareForRegisterClearsRespondedTo checks the
// documented effect of PrepareForRegister: the register series ID and a
// cleared RespondedTo.
func TestSessionValidity_PrepareForRegisterClearsRespondedTo(t *testing.T) {
	s := NewSession(10, &testRNG{val: 500})
	s.PrepareForPropose()
	s.Inner().SetRespondedTo(1)
	if got := s.Inner().RespondedTo(); got != 1 {
		t.Fatalf("RespondedTo before PrepareForRegister = %d, want 1", got)
	}

	s.PrepareForRegister()
	if got := s.Inner().RespondedTo(); got != 0 {
		t.Errorf("RespondedTo after PrepareForRegister = %d, want 0", got)
	}
	if s.SeriesID() != quicraft.SeriesIDForRegister {
		t.Errorf("SeriesID after PrepareForRegister = %d, want %d",
			s.SeriesID(), quicraft.SeriesIDForRegister)
	}
}

// TestSessionValidity_OtherShardRefusesProposal checks that a prepared
// shard-10 session is not valid for a proposal on shard 11.
func TestSessionValidity_OtherShardRefusesProposal(t *testing.T) {
	s := NewSession(10, &testRNG{val: 500})
	s.PrepareForPropose()
	if !s.ValidForProposal(10) {
		t.Fatal("prepared session must be valid on its own shard")
	}
	if s.ValidForProposal(11) {
		t.Error("ValidForProposal must refuse a shard other than the session's")
	}
}
