// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package directory

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestJSONStoreRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.json")
	s, err := openJSONStore[Entry](path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.get("a"); ok {
		t.Fatal("empty store has entries")
	}
	if err := s.put("a", Entry{Reg: &Registration{State: RegAwaitingFunding, FeeAtoms: 5}}); err != nil {
		t.Fatal(err)
	}
	if err := s.mutate("a", func(e *Entry) error {
		e.Reg.State = RegCrawling
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("no")
	if err := s.mutate("a", func(e *Entry) error {
		e.Reg.State = RegTesting
		return wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("mutate error not surfaced: %v", err)
	}

	// Reopen: the erroring mutation must not have persisted.
	s2, err := openJSONStore[Entry](path)
	if err != nil {
		t.Fatal(err)
	}
	e, ok := s2.get("a")
	if !ok || e.Reg == nil || e.Reg.State != RegCrawling || e.Reg.FeeAtoms != 5 {
		t.Fatalf("reopened entry wrong: %+v", e)
	}
	if err := s2.delete("a"); err != nil {
		t.Fatal(err)
	}
	s3, err := openJSONStore[Entry](path)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(s3.all()); got != 0 {
		t.Fatalf("delete did not persist: %d entries", got)
	}
}
