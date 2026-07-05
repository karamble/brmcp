// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bridge

import (
	"strings"
	"sync"
)

// TipMatcher correlates initiated payments with the host's terminal
// tip-progress events for Payer implementations built on Bison Relay's tip
// flow. The notification API carries no attempt id, so matching is FIFO per
// (payee uid, milliatoms), case-insensitive on the uid. Two concurrent
// identical-amount payments to the same payee may swap resolutions, which
// is benign: credits land on the payee's shared balance either way, and
// exactly one payment is recorded per settled tip.
type TipMatcher struct {
	mu      sync.Mutex
	waiters []*TipWait
}

func NewTipMatcher() *TipMatcher {
	return &TipMatcher{}
}

// TipWait is one outstanding payment.
type TipWait struct {
	m      *TipMatcher
	uid    string
	matoms int64
	done   chan error
}

// Expect registers a waiter for a payment of matoms milliatoms to payeeUID.
func (m *TipMatcher) Expect(payeeUID string, matoms int64) *TipWait {
	w := &TipWait{m: m, uid: payeeUID, matoms: matoms, done: make(chan error, 1)}
	m.mu.Lock()
	m.waiters = append(m.waiters, w)
	m.mu.Unlock()
	return w
}

// Resolve completes the oldest waiter matching a terminal tip event with
// res (nil means settled), reporting false when no waiter matches - the
// event was not ours (chat tips, dashboard tips). Non-terminal events must
// not be fed.
func (m *TipMatcher) Resolve(payeeUID string, matoms int64, res error) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, w := range m.waiters {
		if strings.EqualFold(w.uid, payeeUID) && w.matoms == matoms {
			m.waiters = append(m.waiters[:i], m.waiters[i+1:]...)
			w.done <- res
			return true
		}
	}
	return false
}

// Done delivers the terminal result exactly once.
func (w *TipWait) Done() <-chan error { return w.done }

// Cancel unregisters an abandoned waiter (timeout and context paths).
func (w *TipWait) Cancel() {
	w.m.mu.Lock()
	defer w.m.mu.Unlock()
	for i, cand := range w.m.waiters {
		if cand == w {
			w.m.waiters = append(w.m.waiters[:i], w.m.waiters[i+1:]...)
			return
		}
	}
}
