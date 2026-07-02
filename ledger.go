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

type ledgerData struct {
	// Balances maps caller uid (64-hex) to spendable atoms.
	Balances map[string]int64 `json:"balances"`
}

// Ledger is the server-authoritative payment state: per-caller balances
// credited by tips, debited by paid tool calls. Every mutation persists
// synchronously; balances are money.
type Ledger struct {
	mu   sync.Mutex
	path string
	data ledgerData
}

func OpenLedger(path string) (*Ledger, error) {
	l := &Ledger{path: path, data: ledgerData{
		Balances: make(map[string]int64),
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

