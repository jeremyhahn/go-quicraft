//go:build integration

package clusterproof

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"sync"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// kvSep separates a put's key from its value in a proposal. Keys and values
// the harness writes never contain it.
const kvSep = 0

// encodePut is the proposal for put(key, value).
func encodePut(key, value string) []byte {
	b := make([]byte, 0, len(key)+1+len(value))
	b = append(b, key...)
	b = append(b, kvSep)
	return append(b, value...)
}

// decodePut splits a proposal built by encodePut.
func decodePut(cmd []byte) (string, string, error) {
	i := bytes.IndexByte(cmd, kvSep)
	if i < 0 {
		return "", "", fmt.Errorf("clusterproof: put without a separator: %q", cmd)
	}
	return string(cmd[:i]), string(cmd[i+1:]), nil
}

// allKeys is the Lookup query that returns a copy of the whole state; any
// other string query returns that one key's value.
type allKeys struct{}

// kv is the replicas' state machine: a last-write-wins string map, with a
// JSON snapshot so a restarted replica can recover from a snapshot as well
// as from the log.
type kv struct {
	mu   sync.RWMutex
	data map[string]string
	// applied is the log index of the last put applied; a snapshot carries
	// it so a recovery can report which snapshot it restored.
	applied uint64
}

// kvSnapshot is the snapshot payload.
type kvSnapshot struct {
	Applied uint64
	Data    map[string]string
}

var _ sm.StateMachine = (*kv)(nil)

func newKV(uint64, uint64) sm.StateMachine { return &kv{data: map[string]string{}} }

// newKVCreate is the CreateFunc every replica starts its shard with.
func newKVCreate() sm.CreateFunc { return sm.NewCreateFunc(newKV) }

// Update applies each put and answers it with the value's length, which the
// node checks so a misrouted result cannot pass for an acknowledgement.
func (s *kv) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, e := range entries {
		k, v, err := decodePut(e.Cmd)
		if err != nil {
			return err
		}
		s.data[k] = v
		s.applied = e.Index
		results[i] = sm.Result{Value: uint64(len(v))}
	}
	return nil
}

// Lookup answers allKeys with a copy of the map and a string key with its
// value and whether it is present.
func (s *kv) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	switch q := query.(type) {
	case allKeys:
		return maps.Clone(s.data), nil
	case string:
		v, ok := s.data[q]
		return lookupResult{Value: v, Found: ok}, nil
	default:
		return nil, fmt.Errorf("clusterproof: unknown query %T", query)
	}
}

// lookupResult is a single-key read's answer.
type lookupResult struct {
	Value string
	Found bool
}

func (s *kv) SaveSnapshot(_ context.Context, w io.Writer, _ <-chan struct{}) error {
	s.mu.RLock()
	b, err := json.Marshal(kvSnapshot{Applied: s.applied, Data: s.data})
	s.mu.RUnlock()
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

func (s *kv) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	var snap kvSnapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return err
	}
	if snap.Data == nil {
		snap.Data = map[string]string{}
	}
	s.mu.Lock()
	s.data, s.applied = snap.Data, snap.Applied
	s.mu.Unlock()
	nodeSnap.recovered.Store(snap.Applied)
	nodeSnap.smRecoveries.Add(1)
	return nil
}

func (s *kv) Close(context.Context) error { return nil }
