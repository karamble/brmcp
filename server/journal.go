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
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// callJournal persists paid-call claims so money stays right across a
// restart: a charge journaled before its handler ran is refunded on the
// next start, and completed outcomes keep answering duplicates instead of
// re-executing. Free calls are never journaled; their claims are
// process-lifetime.
type callJournal struct {
	mu   sync.Mutex
	path string
	data map[string]*journalEntry
}

type journalEntry struct {
	Peer    string          `json:"peer"`
	Atoms   int64           `json:"atoms"`
	Done    bool            `json:"done,omitempty"`
	Expires int64           `json:"expires,omitempty"`
	Kept    bool            `json:"kept,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Out     json.RawMessage `json:"out,omitempty"`
	Err     string          `json:"err,omitempty"`
}

// journalOutcomeCap bounds the marshaled outcome bytes one entry retains
// for post-restart replay; larger outcomes are refused to post-restart
// duplicates instead of replayed.
const journalOutcomeCap = 64 * 1024

func openCallJournal(path string) (*callJournal, error) {
	j := &callJournal{path: path, data: make(map[string]*journalEntry)}
	raw, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return j, nil
	case err != nil:
		return nil, err
	}
	if err := json.Unmarshal(raw, &j.data); err != nil {
		return nil, fmt.Errorf("call journal %s corrupt: %w", path, err)
	}
	return j, nil
}

func (j *callJournal) persistLocked() error {
	raw, err := json.MarshalIndent(j.data, "", "  ")
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

// charged records that key's caller was debited atoms before the handler
// ran. It is written to disk before execution so a crash refunds it.
func (j *callJournal) charged(key, peer string, atoms int64) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.data[key] = &journalEntry{Peer: peer, Atoms: atoms}
	return j.persistLocked()
}

// complete marks key done and retains its outcome for replay when it fits
// the cap. Unjournaled keys (free calls) are ignored.
func (j *callJournal) complete(key string, result *mcp.CallToolResult, out any, err error, expires time.Time) {
	j.mu.Lock()
	defer j.mu.Unlock()
	e := j.data[key]
	if e == nil {
		return
	}
	e.Done = true
	e.Expires = expires.Unix()
	var res, o []byte
	if result != nil {
		res, _ = json.Marshal(result)
	}
	if out != nil {
		o, _ = json.Marshal(out)
	}
	if len(res)+len(o) <= journalOutcomeCap {
		e.Kept = true
		e.Result = res
		e.Out = o
		if err != nil {
			e.Err = err.Error()
		}
	}
	_ = j.persistLocked()
}

func (j *callJournal) remove(key string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, ok := j.data[key]; !ok {
		return
	}
	delete(j.data, key)
	_ = j.persistLocked()
}

// interrupted returns the charged-but-never-completed entries: the crash
// happened between the debit and the handler's completion.
func (j *callJournal) interrupted() map[string]journalEntry {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make(map[string]journalEntry)
	for k, e := range j.data {
		if !e.Done {
			out[k] = *e
		}
	}
	return out
}

// completedRecords rebuilds replayable call records from the journaled
// outcomes so post-restart duplicates share the original execution's fate.
// Entries whose outcome was not retained refuse the duplicate instead.
func (j *callJournal) completedRecords(now time.Time) map[string]*callRecord {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make(map[string]*callRecord)
	for k, e := range j.data {
		if !e.Done || now.After(time.Unix(e.Expires, 0)) {
			continue
		}
		rec := &callRecord{done: make(chan struct{}), expires: time.Unix(e.Expires, 0)}
		close(rec.done)
		switch {
		case !e.Kept:
			rec.err = errors.New("duplicate call: outcome not retained across a restart")
		default:
			if len(e.Result) > 0 {
				var res mcp.CallToolResult
				if err := json.Unmarshal(e.Result, &res); err == nil {
					rec.result = &res
				}
			}
			if len(e.Out) > 0 {
				rec.out = json.RawMessage(e.Out)
			}
			if e.Err != "" {
				rec.err = errors.New(e.Err)
			}
		}
		out[k] = rec
	}
	return out
}

// gc drops completed entries whose replay window passed.
func (j *callJournal) gc(now time.Time) {
	j.mu.Lock()
	defer j.mu.Unlock()
	var dropped bool
	for k, e := range j.data {
		if e.Done && now.After(time.Unix(e.Expires, 0)) {
			delete(j.data, k)
			dropped = true
		}
	}
	if dropped {
		_ = j.persistLocked()
	}
}
