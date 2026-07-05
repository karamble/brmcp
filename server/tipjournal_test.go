// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package server

import (
	"path/filepath"
	"testing"
)

func TestTipJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tips.json")
	j, err := OpenTipJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if j.Seen(7) {
		t.Fatal("fresh journal saw a tip")
	}
	if err := j.Record(7); err != nil {
		t.Fatal(err)
	}
	if !j.Seen(7) {
		t.Fatal("recorded tip not seen")
	}

	// Persistence across reopen.
	j2, err := OpenTipJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if !j2.Seen(7) {
		t.Fatal("tip forgotten across reopen")
	}

	// The set stays bounded to the newest entries.
	for i := uint64(100); i < 100+tipJournalKeep+50; i++ {
		if err := j2.Record(i); err != nil {
			t.Fatal(err)
		}
	}
	if j2.Seen(7) {
		t.Fatal("oldest entry survived the bound")
	}
	if !j2.Seen(100 + tipJournalKeep + 49) {
		t.Fatal("newest entry missing")
	}
}
