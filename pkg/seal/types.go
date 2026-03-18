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
	"time"
)

// Mode defines the unsealing mode.
type Mode string

const (
	// ModeManual requires operators to manually provide Shamir shares
	// to unseal the barrier after a restart.
	ModeManual Mode = "manual"

	// ModeAuto enables cluster-coordinated automatic unsealing where
	// nodes exchange sealed shares over authenticated channels.
	ModeAuto Mode = "auto"
)

// Config holds seal configuration for both manual and auto-unseal modes.
type Config struct {
	// Mode selects manual (operator-driven) or auto (cluster-coordinated) unsealing.
	Mode Mode `json:"mode"`

	// Threshold is the minimum number of Shamir shares required to
	// reconstruct the root key (M in M-of-N).
	Threshold int `json:"threshold"`

	// TotalShares is the total number of Shamir shares generated (N in M-of-N).
	TotalShares int `json:"total_shares"`

	// RecoveryKeysEnabled enables generation of recovery keys for
	// emergency root key reconstruction.
	RecoveryKeysEnabled bool `json:"recovery_keys_enabled"`

	// RecoveryThreshold is the minimum number of recovery keys required
	// for emergency reconstruction.
	RecoveryThreshold int `json:"recovery_threshold"`

	// RecoveryTotalKeys is the total number of recovery keys generated.
	RecoveryTotalKeys int `json:"recovery_total_keys"`

	// ShareExchangeTimeout is the maximum duration to wait for peer nodes
	// to exchange shares during auto-unseal coordination.
	ShareExchangeTimeout time.Duration `json:"share_exchange_timeout"`

	// DeploymentID uniquely identifies the deployment cluster. Used as
	// additional context in HKDF derivation to bind keys to a deployment.
	DeploymentID uint64 `json:"deployment_id"`
}

// DefaultConfig returns a Config with sensible defaults for a three-node
// cluster using manual unseal mode with 3-of-5 Shamir sharing.
func DefaultConfig() *Config {
	return &Config{
		Mode:                 ModeManual,
		Threshold:            3,
		TotalShares:          5,
		RecoveryKeysEnabled:  false,
		RecoveryThreshold:    3,
		RecoveryTotalKeys:    5,
		ShareExchangeTimeout: 30 * time.Second,
		DeploymentID:         0,
	}
}

// Status reports the current seal state of a node.
type Status struct {
	// Sealed indicates whether the barrier is currently sealed.
	Sealed bool `json:"sealed"`

	// Initialized indicates whether the barrier has been initialized
	// with a root key and sealing strategy.
	Initialized bool `json:"initialized"`

	// Mode is the active unsealing mode.
	Mode Mode `json:"mode"`

	// Threshold is the number of shares required to unseal.
	Threshold int `json:"threshold"`

	// TotalShares is the total number of shares generated.
	TotalShares int `json:"total_shares"`

	// SharesProvided is the number of shares submitted so far in the
	// current unseal attempt.
	SharesProvided int `json:"shares_provided"`

	// Version is the seal version, incremented on each rekey operation.
	Version uint64 `json:"version"`
}

// BootstrapResult contains the output of a seal bootstrap operation.
// The shares and recovery keys are only returned once during initialization
// and must be securely distributed to custodians.
type BootstrapResult struct {
	// Shares contains the Shamir shares in hex-encoded string format.
	Shares []string `json:"shares"`

	// RecoveryKeys contains emergency recovery keys for root key
	// reconstruction outside the normal Shamir quorum.
	RecoveryKeys []string `json:"recovery_keys"`

	// RootToken is the initial admin token generated during bootstrap.
	RootToken string `json:"root_token"`

	// Version is the seal version at the time of bootstrap.
	Version uint64 `json:"version"`
}

// UnsealRequest is an unseal share submission from a custodian or peer node.
type UnsealRequest struct {
	// Share is a single Shamir share in hex-encoded string format.
	Share string `json:"share"`

	// NodeID identifies the node submitting the share.
	NodeID uint64 `json:"node_id"`

	// Signature is an optional cryptographic signature over the share
	// for authentication in auto-unseal mode.
	Signature []byte `json:"signature"`
}

// UnsealResponse reports unseal progress after a share submission.
type UnsealResponse struct {
	// Sealed indicates whether the barrier remains sealed after this submission.
	Sealed bool `json:"sealed"`

	// SharesProvided is the total number of valid shares submitted so far.
	SharesProvided int `json:"shares_provided"`

	// Threshold is the number of shares required to complete unsealing.
	Threshold int `json:"threshold"`

	// Error contains an error message if the submission was rejected.
	Error string `json:"error,omitempty"`
}

// PeerShareCoordinator coordinates share exchange between cluster nodes
// during auto-unseal. Each node holds one sealed share and communicates
// with peers to collect enough shares to meet the threshold.
type PeerShareCoordinator interface {
	// Start begins the share exchange protocol. The coordinator listens
	// for incoming share requests and broadcasts its own readiness.
	Start(ctx context.Context) error

	// Stop terminates the coordinator and releases resources.
	Stop() error

	// RequestShares broadcasts a share request to all known peer nodes.
	RequestShares(ctx context.Context) error

	// ProvideShare sends this node's sealed share to the specified peer.
	ProvideShare(ctx context.Context, toNodeID uint64) error

	// OnShareReceived processes an incoming share exchange message from
	// a peer node. The coordinator verifies the signature and decrypts
	// the share before adding it to the local accumulator.
	OnShareReceived(ctx context.Context, msg *ShareExchangeMessage) error

	// GetCollectedShares returns all shares collected so far.
	GetCollectedShares() []string

	// IsQuorumReached reports whether enough shares have been collected
	// to meet the reconstruction threshold.
	IsQuorumReached() bool
}

// ShareExchangeType identifies the type of share exchange message.
type ShareExchangeType int

const (
	// ShareExchangeRequest is sent by a node requesting shares from peers.
	ShareExchangeRequest ShareExchangeType = iota

	// ShareExchangeResponse is sent by a node providing its share to a requester.
	ShareExchangeResponse

	// ShareExchangeAck acknowledges receipt of a share.
	ShareExchangeAck

	// ShareExchangeReady signals that a node is ready to participate in
	// the share exchange protocol.
	ShareExchangeReady
)

// ShareExchangeMessage is a message exchanged between cluster nodes during
// auto-unseal. Each message is encrypted with the recipient's public key
// and signed by the sender for mutual authentication.
type ShareExchangeMessage struct {
	// Type identifies the purpose of this message.
	Type ShareExchangeType `json:"type"`

	// FromNodeID is the sender's node identifier.
	FromNodeID uint64 `json:"from_node_id"`

	// ToNodeID is the intended recipient's node identifier.
	ToNodeID uint64 `json:"to_node_id"`

	// EncryptedShare is the Shamir share encrypted with the recipient's
	// public key. Only present in ShareExchangeResponse messages.
	EncryptedShare []byte `json:"encrypted_share,omitempty"`

	// PublicKey is the sender's ephemeral public key for encrypting
	// the response share. Only present in ShareExchangeRequest messages.
	PublicKey []byte `json:"public_key,omitempty"`

	// Signature is a cryptographic signature over the message fields
	// for sender authentication.
	Signature []byte `json:"signature,omitempty"`

	// Timestamp is the wall-clock time when the message was created.
	Timestamp time.Time `json:"timestamp"`

	// Version is the seal version this exchange pertains to.
	Version uint64 `json:"version"`
}
