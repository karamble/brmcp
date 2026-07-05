// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bridge_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/karamble/brmcp/bridge"
	"github.com/karamble/brmcp/brmcptest"
)

func TestTipMatcher(t *testing.T) {
	m := bridge.NewTipMatcher()
	uid := brmcptest.UID(4)

	// Unmatched events are not ours.
	if m.Resolve(uid, 1000, nil) {
		t.Fatal("resolved with no waiter")
	}

	// FIFO per (uid, amount): the oldest waiter resolves first.
	w1 := m.Expect(uid, 1000)
	w2 := m.Expect(uid, 1000)
	if !m.Resolve(strings.ToUpper(uid), 1000, nil) {
		t.Fatal("case-insensitive uid did not match")
	}
	select {
	case err := <-w1.Done():
		if err != nil {
			t.Fatalf("first waiter got %v", err)
		}
	default:
		t.Fatal("first waiter not resolved")
	}
	select {
	case <-w2.Done():
		t.Fatal("second waiter resolved out of order")
	default:
	}
	terminal := errors.New("route not found")
	if !m.Resolve(uid, 1000, terminal) {
		t.Fatal("second event did not match")
	}
	if err := <-w2.Done(); !errors.Is(err, terminal) {
		t.Fatalf("second waiter got %v", err)
	}

	// Amount mismatches do not match; cancelled waiters are gone.
	w3 := m.Expect(uid, 2000)
	if m.Resolve(uid, 999, nil) {
		t.Fatal("amount mismatch matched")
	}
	w3.Cancel()
	if m.Resolve(uid, 2000, nil) {
		t.Fatal("cancelled waiter matched")
	}
}
