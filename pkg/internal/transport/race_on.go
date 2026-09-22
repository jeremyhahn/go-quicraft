//go:build race

package transport

// raceEnabled reports whether this binary was built with the race detector.
//
// It exists for one test: TestSnapshotOver0RTTRejected drives a data race
// inside quic-go that no production path can reach. See its skip for the
// detail.
const raceEnabled = true
