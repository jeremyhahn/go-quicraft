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

package quicraft_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/sm/kv"
)

// runSessionExample starts the single-node example shard in a temporary
// directory and runs fn against it, closing the host afterwards.
func runSessionExample(ctx context.Context, fn func(context.Context, *quicraft.Host) error) error {
	dir, err := os.MkdirTemp("", "quicraft-example-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	h, err := startExampleShard(ctx, dir)
	if err != nil {
		return err
	}
	defer h.Close()
	return fn(ctx, h)
}

// proposeAsync is the guide's "Use the async form" block: propose, wait on
// ResultC, and advance the session with ProposalCompleted on success.
func proposeAsync(ctx context.Context, h *quicraft.Host, session *quicraft.Session, cmd []byte) error {
	rs, err := h.ProposeWithSession(ctx, session, cmd)
	if err != nil {
		return err
	}
	defer rs.Release()

	r := <-rs.ResultC()
	if r.Err != nil {
		return r.Err // the session is unchanged; retry with it
	}
	session.ProposalCompleted() // advance to the next series
	return nil
}

// printKey reads key through the log and prints it as "key = value".
func printKey(ctx context.Context, h *quicraft.Host, key string) error {
	val, err := h.SyncRead(ctx, 1, []byte(key))
	if err != nil {
		return err
	}
	fmt.Printf("%s = %s\n", key, val.([]byte))
	return nil
}

// putCmd encodes a kv put of key=value.
func putCmd(key, value string) []byte {
	cmd, err := kv.EncodePut([]byte(key), []byte(value))
	if err != nil {
		log.Fatal(err)
	}
	return cmd
}

// ExampleHost_ProposeWithSession proposes two commands on one session with
// the async form, calling ProposalCompleted after each result, and shows
// both were applied: the second did not reuse the first's series.
func ExampleHost_ProposeWithSession() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := runSessionExample(ctx, func(ctx context.Context, h *quicraft.Host) error {
		session, err := h.NewSession(ctx, 1)
		if err != nil {
			return err
		}
		first := session.SeriesID()
		if err := proposeAsync(ctx, h, session, putCmd("a", "1")); err != nil {
			return err
		}
		if err := proposeAsync(ctx, h, session, putCmd("b", "2")); err != nil {
			return err
		}
		fmt.Println("series advanced:", session.SeriesID() == first+2)
		if err := printKey(ctx, h, "a"); err != nil {
			return err
		}
		return printKey(ctx, h, "b")
	})
	if err != nil {
		log.Fatal(err)
	}
	// Output:
	// series advanced: true
	// a = 1
	// b = 2
}

// ExampleHost_NewSessionWithOwner registers a session bound to an owner
// (here a tenant ID) and proposes on it.
func ExampleHost_NewSessionWithOwner() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const tenantID = 42
	err := runSessionExample(ctx, func(ctx context.Context, h *quicraft.Host) error {
		session, err := h.NewSessionWithOwner(ctx, 1, tenantID)
		if err != nil {
			return err
		}
		fmt.Println("owner:", session.OwnerID())
		if _, err := h.SyncProposeWithSession(ctx, session, putCmd("t", "42")); err != nil {
			return err
		}
		return printKey(ctx, h, "t")
	})
	if err != nil {
		log.Fatal(err)
	}
	// Output:
	// owner: 42
	// t = 42
}

// ExampleHost_RegisterSession registers a session under a client ID the
// caller saved, proposes on it, shows a second registration of the same ID
// is refused with a *SessionError while the shard holds it, and registers
// the saved ID again once the session is closed.
func ExampleHost_RegisterSession() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const savedClientID = 0x5eed
	err := runSessionExample(ctx, func(ctx context.Context, h *quicraft.Host) error {
		session, err := h.RegisterSession(ctx, quicraft.NewSession(1, savedClientID))
		if err != nil {
			return err
		}
		fmt.Println("client ID kept:", session.ClientID() == savedClientID)
		if _, err := h.SyncProposeWithSession(ctx, session, putCmd("s", "1")); err != nil {
			return err
		}

		_, err = h.RegisterSession(ctx, quicraft.NewSession(1, savedClientID))
		var serr *quicraft.SessionError
		fmt.Println("duplicate refused:", errors.As(err, &serr))

		rs, err := h.CloseSession(ctx, 1, session)
		if err != nil {
			return err
		}
		_, err = rs.Result()
		rs.Release()
		if err != nil {
			return err
		}

		again, err := h.RegisterSession(ctx, quicraft.NewSession(1, savedClientID))
		if err != nil {
			return err
		}
		if _, err := h.SyncProposeWithSession(ctx, again, putCmd("s", "2")); err != nil {
			return err
		}
		return printKey(ctx, h, "s")
	})
	if err != nil {
		log.Fatal(err)
	}
	// Output:
	// client ID kept: true
	// duplicate refused: true
	// s = 2
}
