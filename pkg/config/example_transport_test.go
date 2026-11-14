// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package config_test

import (
	"errors"
	"fmt"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// ExampleTransportConfig_SetDefaults shows the UDP socket buffer sizes the
// quic-transport guide calls the default (docs/quic-transport/user.md, "Task:
// size the limits"): SetDefaults fills a zero size with 7 MB and keeps a size
// set before the call.
func ExampleTransportConfig_SetDefaults() {
	var zero config.TransportConfig
	zero.SetDefaults()
	fmt.Printf("recv=%d MB send=%d MB\n", zero.UDPRecvBufSize>>20, zero.UDPSendBufSize>>20)

	set := config.TransportConfig{UDPRecvBufSize: 16 << 20, UDPSendBufSize: -1}
	set.SetDefaults()
	fmt.Printf("recv=%d MB send=%d\n", set.UDPRecvBufSize>>20, set.UDPSendBufSize)
	// Output:
	// recv=7 MB send=7 MB
	// recv=16 MB send=-1
}

// ExampleTransportConfig_Validate_revocation shows the guide's revocation
// rule (docs/quic-transport/user.md, "Task: turn on revocation"): a mode
// with a source validates, and a mode with no usable source is refused at
// startup.
func ExampleTransportConfig_Validate_revocation() {
	crl := config.TransportConfig{
		// A real host sets MTLSConfig; disabling the transport lets this
		// example run without certificate material.
		TransportDisabled: true,
		RevocationConfig: &config.RevocationConfig{
			CRLPaths:      []string{"/etc/quicraft/crl/issuing.crl"},
			Mode:          "crl",
			CheckInterval: 60 * time.Second,
		},
	}
	crl.SetDefaults()
	fmt.Println("crl:", crl.Validate())

	ocsp := config.TransportConfig{
		TransportDisabled: true,
		RevocationConfig:  &config.RevocationConfig{Mode: "ocsp"},
	}
	ocsp.SetDefaults()
	var verr *config.ValidationError
	if err := ocsp.Validate(); errors.As(err, &verr) {
		fmt.Println("ocsp:", verr.Field)
	}
	// Output:
	// crl: <nil>
	// ocsp: TransportConfig.RevocationConfig.OCSPResponderURL
}
