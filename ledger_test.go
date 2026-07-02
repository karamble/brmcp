// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package brmcp

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestLedgerLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	l, err := OpenLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	const uid = "aa"

	if err := l.Credit(uid, 100); err != nil {
		t.Fatal(err)
	}
	if err := l.Debit(uid, 40); err != nil {
		t.Fatal(err)
	}
	if err := l.Debit(uid, 61); !errors.Is(err, ErrInsufficient) {
		t.Fatalf("want ErrInsufficient, got %v", err)
	}
	if got := l.Balance(uid); got != 60 {
		t.Fatalf("balance %d != 60", got)
	}

	// Invoice settlement credits its recorded target once.
	if err := l.AddPendingInvoice("beef", uid, 25, 0); err != nil {
		t.Fatal(err)
	}
	gotUID, atoms, ok := l.ResolvePendingInvoice("beef", 7)
	if !ok || gotUID != uid || atoms != 25 {
		t.Fatalf("resolve: %s %d %v", gotUID, atoms, ok)
	}
	if err := l.Credit(gotUID, atoms); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := l.ResolvePendingInvoice("beef", 8); ok {
		t.Fatal("double resolve")
	}
	if _, _, ok := l.ResolvePendingInvoice("unknown", 9); ok {
		t.Fatal("unknown hash resolved")
	}
	if got := l.SettleIndex(); got != 9 {
		t.Fatalf("settle index %d != 9", got)
	}

	// Everything survives a reopen.
	l2, err := OpenLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := l2.Balance(uid); got != 85 {
		t.Fatalf("reopened balance %d != 85", got)
	}
	if got := l2.SettleIndex(); got != 9 {
		t.Fatalf("reopened settle index %d != 9", got)
	}
}
