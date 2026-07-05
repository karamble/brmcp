// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// tipJournalKeep bounds the persisted set of seen tip sequence ids.
const tipJournalKeep = 1000

// TipJournal remembers recently credited tip sequence ids so a tip
// redelivered after a crash between the credit and its acknowledgement is
// not credited twice. The crediting order is: credit, Record, ack - a
// crash before Record can still double-credit one tip (the credit is owed
// to the tipper, so it must land before the id does), but the window
// shrinks from the whole ack round trip to one file write.
type TipJournal struct {
	mu   sync.Mutex
	path string
	seqs []uint64
	seen map[uint64]bool
}

func OpenTipJournal(path string) (*TipJournal, error) {
	j := &TipJournal{path: path, seen: make(map[uint64]bool)}
	raw, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return j, nil
	case err != nil:
		return nil, err
	}
	if err := json.Unmarshal(raw, &j.seqs); err != nil {
		return nil, fmt.Errorf("tip journal %s corrupt: %w", path, err)
	}
	for _, s := range j.seqs {
		j.seen[s] = true
	}
	return j, nil
}

// Seen reports whether seq was already credited.
func (j *TipJournal) Seen(seq uint64) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.seen[seq]
}

// Record persists seq as credited, keeping the newest entries only.
func (j *TipJournal) Record(seq uint64) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.seen[seq] {
		return nil
	}
	j.seen[seq] = true
	j.seqs = append(j.seqs, seq)
	if len(j.seqs) > tipJournalKeep {
		drop := j.seqs[:len(j.seqs)-tipJournalKeep]
		for _, s := range drop {
			delete(j.seen, s)
		}
		j.seqs = append([]uint64(nil), j.seqs[len(j.seqs)-tipJournalKeep:]...)
	}
	raw, err := json.MarshalIndent(j.seqs, "", "  ")
	if err != nil {
		return err
	}
	tmp := j.path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(j.path), 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, j.path)
}
