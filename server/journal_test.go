// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// TestInterruptedChargeRefundedOnRestart simulates a crash between the
// charge and the handler's completion: the journaled entry must refund the
// caller on the next start and vanish from the journal.
func TestInterruptedChargeRefundedOnRestart(t *testing.T) {
	dir := t.TempDir()
	impl := &mcp.Implementation{Name: "t", Version: "0"}
	peer := "aa11"

	h1, err := NewHarness(impl, HarnessConfig{DataDir: dir, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	if err := h1.Billing().Credit(peer, 800); err != nil {
		t.Fatal(err)
	}
	// The charge path: debit, then journal, then the handler would run.
	if err := h1.Billing().Debit(peer, 500); err != nil {
		t.Fatal(err)
	}
	if err := h1.journal.charged(peer+"|key-0001", peer, 500); err != nil {
		t.Fatal(err)
	}
	// Crash here: h1 is abandoned without completing the call.

	h2, err := NewHarness(impl, HarnessConfig{DataDir: dir, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	if got := h2.Billing().Balance(peer); got != 800 {
		t.Fatalf("interrupted charge not refunded: balance %d != 800", got)
	}
	if got := len(h2.journal.interrupted()); got != 0 {
		t.Fatalf("interrupted entries survived reconcile: %d", got)
	}
	// The refund happens exactly once: a third start changes nothing.
	h3, err := NewHarness(impl, HarnessConfig{DataDir: dir, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	if got := h3.Billing().Balance(peer); got != 800 {
		t.Fatalf("refund repeated: balance %d != 800", got)
	}
}

// TestJournalOutcomeCap keeps oversized outcomes out of the journal while
// still refusing post-restart duplicates without re-execution.
func TestJournalOutcomeCap(t *testing.T) {
	dir := t.TempDir()
	j, err := openCallJournal(filepath.Join(dir, "calls.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := j.charged("p|k", "p", 5); err != nil {
		t.Fatal(err)
	}
	big := make([]byte, journalOutcomeCap+1)
	j.complete("p|k", nil, string(big), nil, time.Now().Add(time.Hour))

	j2, err := openCallJournal(filepath.Join(dir, "calls.json"))
	if err != nil {
		t.Fatal(err)
	}
	recs := j2.completedRecords(time.Now())
	rec := recs["p|k"]
	if rec == nil {
		t.Fatal("completed entry lost")
	}
	if rec.err == nil || rec.out != nil || rec.result != nil {
		t.Fatalf("oversized outcome replayed: %+v", rec)
	}
	if _, err := os.Stat(filepath.Join(dir, "calls.json")); err != nil {
		t.Fatal(err)
	}
}

// TestJournalDecodesV161Entries pins cross-version journal compatibility:
// a calls.json written by brmcp on go-sdk v1.6.1 must reconcile and replay
// after the operator upgrades in place. The fixture bytes are the exact
// v1.6.1 shape (no resultType, no inputRequests).
func TestJournalDecodesV161Entries(t *testing.T) {
	peer := strings.Repeat("1", 64)
	fixture := strings.ReplaceAll(`{
  "PEER|fixture-key-0001": {"peer":"PEER","atoms":500,"done":true,"expires":4102444800,"kept":true,"result":{"content":[{"type":"text","text":"{\"n\":1}"}]},"out":{"n":1}},
  "PEER|fixture-key-0002": {"peer":"PEER","atoms":700}
}`, "PEER", peer)
	path := filepath.Join(t.TempDir(), "calls.json")
	if err := os.WriteFile(path, []byte(fixture), 0o600); err != nil {
		t.Fatal(err)
	}
	j, err := openCallJournal(path)
	if err != nil {
		t.Fatal(err)
	}

	// The charged-but-uncompleted entry still reads as refundable.
	inter := j.interrupted()
	if len(inter) != 1 {
		t.Fatalf("interrupted entries: %d != 1", len(inter))
	}
	e, ok := inter[peer+"|fixture-key-0002"]
	if !ok || e.Peer != peer || e.Atoms != 700 {
		t.Fatalf("interrupted entry: %+v", inter)
	}

	// The completed outcome replays, text content decoded intact.
	recs := j.completedRecords(time.Now())
	if len(recs) != 1 {
		t.Fatalf("completed records: %d != 1", len(recs))
	}
	rec := recs[peer+"|fixture-key-0001"]
	if rec == nil || rec.err != nil || rec.result == nil {
		t.Fatalf("completed record: %+v", rec)
	}
	tc, ok := rec.result.Content[0].(*mcp.TextContent)
	if !ok || tc.Text != `{"n":1}` {
		t.Fatalf("replayed content: %+v", rec.result.Content)
	}

	// Replayed results must stay free of new-wire fields: a legacy caller
	// gets byte-compatible outcomes across the operator's upgrade.
	raw, err := json.Marshal(rec.result)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "resultType") {
		t.Fatalf("replayed result grew new-wire fields: %s", raw)
	}
}
