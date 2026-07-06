// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package directory

import (
	"path/filepath"
	"testing"
)

func TestFundHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "regfund.json")
	h, err := openFundHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.record(fundEntry{TS: 1000, DirectoryUID: "a", Atoms: 300}); err != nil {
		t.Fatal(err)
	}
	if err := h.record(fundEntry{TS: 2000, DirectoryUID: "b", Atoms: 200}); err != nil {
		t.Fatal(err)
	}
	if got := h.totalSince(0); got != 500 {
		t.Fatalf("totalSince(0) = %d", got)
	}
	if got := h.totalSince(1500); got != 200 {
		t.Fatalf("window slide: totalSince(1500) = %d", got)
	}
	if got := h.totalSince(2001); got != 0 {
		t.Fatalf("empty window: %d", got)
	}

	// The history is the cap's memory; it must survive a restart.
	h2, err := openFundHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := h2.totalSince(0); got != 500 {
		t.Fatalf("history lost on reopen: %d", got)
	}
}
