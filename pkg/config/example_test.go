// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package config_test

import (
	"errors"
	"fmt"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// ExampleHostConfig_Validate builds a HostConfig that passes validation.
// SetDefaults fills every tunable left at its zero value; the fields set
// here have no default.
func ExampleHostConfig_Validate() {
	hc := config.HostConfig{
		WALDir:        "/var/lib/quicraft/wal",
		NodeHostDir:   "/var/lib/quicraft/node",
		ListenAddress: "127.0.0.1:26001",
		RaftAddress:   "127.0.0.1:26001",
		DeploymentID:  1,
	}
	hc.SetDefaults()
	// A real host sets TransportConfig.MTLSConfig; this example disables
	// the transport so it needs no certificate material.
	hc.TransportConfig.TransportDisabled = true

	fmt.Println(hc.Validate())
	// Output:
	// <nil>
}

// ExampleHostConfig_Validate_mTLSRequired shows the typed error returned
// when the transport is enabled without mTLS material: mTLS has no
// insecure fallback.
func ExampleHostConfig_Validate_mTLSRequired() {
	hc := config.HostConfig{
		WALDir:        "/var/lib/quicraft/wal",
		NodeHostDir:   "/var/lib/quicraft/node",
		ListenAddress: "127.0.0.1:26001",
		RaftAddress:   "127.0.0.1:26001",
		DeploymentID:  1,
	}
	hc.SetDefaults()

	err := hc.Validate()
	var mtlsErr *config.MTLSConfigRequiredError
	fmt.Println(errors.As(err, &mtlsErr))
	fmt.Println(errors.Is(err, config.ErrMTLSConfigRequired))
	// Output:
	// true
	// true
}

// ExampleConfig_Validate builds a shard Config that passes validation,
// then shows the *ValidationError a missing ShardID produces.
func ExampleConfig_Validate() {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	fmt.Println(cfg.Validate())

	bad := config.Config{ReplicaID: 1}
	bad.SetDefaults()
	err := bad.Validate()
	var verr *config.ValidationError
	if errors.As(err, &verr) {
		fmt.Println(verr.Field)
	}
	fmt.Println(err)
	// Output:
	// <nil>
	// ShardID
	// config: invalid ShardID: must be > 0
}
