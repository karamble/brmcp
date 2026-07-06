// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package directory

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// RegState is a registration's position in the verification pipeline.
type RegState string

const (
	RegAwaitingFunding RegState = RegState(StateAwaitingFunding)
	RegCrawling        RegState = RegState(StateCrawling)
	RegTesting         RegState = RegState(StateTesting)
	RegPendingReview   RegState = RegState(StatePendingReview)
)

// Registration is one in-flight pipeline run. Fee and budget are frozen at
// register time; CallKey is persisted before the paid test call so a
// restart re-issues the same logical call and the provider's journal
// charges it once.
type Registration struct {
	State         RegState        `json:"state"`
	Description   string          `json:"description"`
	Tags          []string        `json:"tags,omitempty"`
	Test          TestSpec        `json:"test"`
	FeeAtoms      int64           `json:"feeAtoms"`
	BudgetAtoms   int64           `json:"budgetAtoms"`
	CallKey       string          `json:"callKey,omitempty"`
	PaidAtoms     int64           `json:"paidAtoms"`
	Renewal       bool            `json:"renewal"`
	CreatedAt     int64           `json:"createdAt"`
	Catalog       json.RawMessage `json:"catalog,omitempty"`
	TestOutcome   string          `json:"testOutcome,omitempty"`
	TestErr       string          `json:"testErr,omitempty"`
	TestLatencyMs int64           `json:"testLatencyMs,omitempty"`
}

// Listing is a published, searchable provider record. Catalog is the
// crawled tools/list result verbatim, price metadata included.
type Listing struct {
	Description           string          `json:"description"`
	Tags                  []string        `json:"tags,omitempty"`
	Test                  TestSpec        `json:"test"`
	Catalog               json.RawMessage `json:"catalog"`
	CatalogCheckedAt      int64           `json:"catalogCheckedAt"`
	LastVerifiedExecution int64           `json:"lastVerifiedExecution"`
	TestLatencyMs         int64           `json:"testLatencyMs"`
	ApprovedAt            int64           `json:"approvedAt"`
	RenewedAt             int64           `json:"renewedAt,omitempty"`
	ExpiresAt             int64           `json:"expiresAt"`
}

// Entry is one provider uid's full state. A renewal pipeline runs in Reg
// while Live keeps serving searches; first-time providers have Reg only.
type Entry struct {
	Live *Listing      `json:"live,omitempty"`
	Reg  *Registration `json:"reg,omitempty"`
	Flag string        `json:"flag,omitempty"`
}

// Peer is a curated federation peer whose snapshots seed leads.
type Peer struct {
	PubKey         string `json:"pubKey"`
	AddedAt        int64  `json:"addedAt"`
	LastVerifiedAt int64  `json:"lastVerifiedAt,omitempty"`
}

// Lead lifecycle states.
const (
	LeadNew       = "new"
	LeadPursuing  = "pursuing"
	LeadInvited   = "invited"
	LeadConverted = "converted"
)

// Lead is a provider learned from a peer snapshot, awaiting first-party
// verification through the normal registration pipeline.
type Lead struct {
	ProviderUID string   `json:"providerUid"`
	PeerUID     string   `json:"peerUid"`
	Description string   `json:"description,omitempty"`
	Tags        []string `json:"tags,omitempty"`
	State       string   `json:"state"`
	Note        string   `json:"note,omitempty"`
	CreatedAt   int64    `json:"createdAt"`
	UpdatedAt   int64    `json:"updatedAt"`
}

// jsonStore persists a string-keyed map as one JSON file, every mutation
// written synchronously via tmp+rename (the ledger idiom). Values are
// stored by value, but pointer fields are shared with the copies get/all
// hand out: mutate callbacks must REPLACE pointer fields (copy-on-write),
// never write through them, so readers only ever see immutable snapshots.
type jsonStore[V any] struct {
	mu   sync.Mutex
	path string
	m    map[string]V
}

func openJSONStore[V any](path string) (*jsonStore[V], error) {
	s := &jsonStore[V]{path: path, m: make(map[string]V)}
	raw, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return s, nil
	case err != nil:
		return nil, err
	}
	if err := json.Unmarshal(raw, &s.m); err != nil {
		return nil, fmt.Errorf("store %s corrupt: %w", path, err)
	}
	if s.m == nil {
		s.m = make(map[string]V)
	}
	return s, nil
}

func (s *jsonStore[V]) persistLocked() error {
	raw, err := json.MarshalIndent(s.m, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *jsonStore[V]) get(k string) (V, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[k]
	return v, ok
}

func (s *jsonStore[V]) put(k string, v V) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[k] = v
	return s.persistLocked()
}

// mutate runs fn on the stored value (zero value when absent) under the
// store lock and persists the result unless fn errors. fn must not call
// back into the store.
func (s *jsonStore[V]) mutate(k string, fn func(*V) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.m[k]
	if err := fn(&v); err != nil {
		return err
	}
	s.m[k] = v
	return s.persistLocked()
}

func (s *jsonStore[V]) delete(k string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.m[k]; !ok {
		return nil
	}
	delete(s.m, k)
	return s.persistLocked()
}

// all returns a copy of the map; values sharing pointers stay read-only.
func (s *jsonStore[V]) all() map[string]V {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]V, len(s.m))
	for k, v := range s.m {
		out[k] = v
	}
	return out
}
