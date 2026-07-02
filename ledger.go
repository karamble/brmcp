// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package brmcp

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// ErrInsufficient reports a debit larger than the caller's balance.
var ErrInsufficient = errors.New("insufficient balance")

type pendingInvoice struct {
	UID    string `json:"uid"`
	Atoms  int64  `json:"atoms"`
	Expiry int64  `json:"expiry"`
}

type ledgerData struct {
	// Balances maps caller uid (64-hex) to spendable atoms.
	Balances map[string]int64 `json:"balances"`
	// PendingInvoices maps hex payment hashes to their crediting target,
	// persisted so an invoice paid across a restart still credits.
	PendingInvoices map[string]pendingInvoice `json:"pending_invoices"`
	// SettleIndex resumes the dcrlnd invoice subscription.
	SettleIndex uint64 `json:"settle_index"`
}

// Ledger is the server-authoritative payment state: per-caller balances
// credited by tips and settled invoices, debited by paid tool calls. Every
// mutation persists synchronously; balances are money.
type Ledger struct {
	mu   sync.Mutex
	path string
	data ledgerData
}

func OpenLedger(path string) (*Ledger, error) {
	l := &Ledger{path: path, data: ledgerData{
		Balances:        make(map[string]int64),
		PendingInvoices: make(map[string]pendingInvoice),
	}}
	raw, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return l, nil
	case err != nil:
		return nil, err
	}
	if err := json.Unmarshal(raw, &l.data); err != nil {
		return nil, fmt.Errorf("ledger %s corrupt: %w", path, err)
	}
	if l.data.Balances == nil {
		l.data.Balances = make(map[string]int64)
	}
	if l.data.PendingInvoices == nil {
		l.data.PendingInvoices = make(map[string]pendingInvoice)
	}
	return l, nil
}

func (l *Ledger) persistLocked() error {
	raw, err := json.MarshalIndent(&l.data, "", "  ")
	if err != nil {
		return err
	}
	tmp := l.path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(l.path), 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, l.path)
}

func (l *Ledger) Balance(uid string) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.data.Balances[uid]
}

func (l *Ledger) Credit(uid string, atoms int64) error {
	if atoms <= 0 {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.data.Balances[uid] += atoms
	return l.persistLocked()
}

func (l *Ledger) Debit(uid string, atoms int64) error {
	if atoms <= 0 {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.data.Balances[uid] < atoms {
		return ErrInsufficient
	}
	l.data.Balances[uid] -= atoms
	return l.persistLocked()
}

// AddPendingInvoice records an issued invoice so its settlement credits uid.
func (l *Ledger) AddPendingInvoice(rhashHex, uid string, atoms, expiry int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.data.PendingInvoices[rhashHex] = pendingInvoice{UID: uid, Atoms: atoms, Expiry: expiry}
	return l.persistLocked()
}

// ResolvePendingInvoice consumes the pending record for a settled invoice
// and advances the subscription resume point. It does NOT credit: the
// caller credits the configured Billing exactly once per true return.
// Unknown hashes only advance the index (an invoice issued by something
// other than this harness).
func (l *Ledger) ResolvePendingInvoice(rhashHex string, settleIndex uint64) (uid string, atoms int64, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if settleIndex > l.data.SettleIndex {
		l.data.SettleIndex = settleIndex
	}
	inv, found := l.data.PendingInvoices[rhashHex]
	if found {
		delete(l.data.PendingInvoices, rhashHex)
	}
	// Persist even for unknown hashes: the index moved.
	if err := l.persistLocked(); err != nil {
		// Keep the pending record rather than risk losing the credit if
		// a restart replays from an already-advanced index.
		if found {
			l.data.PendingInvoices[rhashHex] = inv
		}
		return "", 0, false
	}
	return inv.UID, inv.Atoms, found
}

func (l *Ledger) SettleIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.data.SettleIndex
}
