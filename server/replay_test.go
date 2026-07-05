// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package server_test

import (
	"context"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/karamble/brmcp"
	"github.com/karamble/brmcp/brmcptest"
	"github.com/karamble/brmcp/server"
)

// TestPaidCallReplayAcrossRestart proves the journal carries completed
// outcomes across a serving-process restart: the duplicate of a paid call
// is answered from the record, never re-executed, never re-charged.
func TestPaidCallReplayAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	impl := &mcp.Implementation{Name: "t", Version: "0"}
	var executions int

	newGen := func() (*server.Harness, *mcp.ClientSession) {
		t.Helper()
		h, err := server.NewHarness(impl, server.HarnessConfig{
			DataDir:        dir,
			AllowedPeers:   []string{clientUID},
			CallsPerMinute: 100,
			Logf:           t.Logf,
		})
		if err != nil {
			t.Fatal(err)
		}
		server.AddTool(h, &mcp.Tool{Name: "paid", Description: "paid tool"}, 500,
			func(context.Context, string, struct{}) (any, error) {
				executions++
				return map[string]int{"n": executions}, nil
			})
		return h, startHarnessFabric(t, h)
	}

	h1, s1 := newGen()
	if err := h1.Billing().Credit(clientUID, 600); err != nil {
		t.Fatal(err)
	}
	key := mcp.Meta{brmcp.CallKeyMetaKey: "restart-key-0001"}
	res1, err := s1.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "paid", Meta: key, Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res1.IsError {
		t.Fatalf("funded call failed: %s", brmcptest.Text(res1))
	}
	if executions != 1 {
		t.Fatalf("executions: %d != 1", executions)
	}

	// A fresh harness on the same data dir is the restarted process: same
	// ledger, same journal. The duplicate replays without running or
	// charging anything.
	h2, s2 := newGen()
	res2, err := s2.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "paid", Meta: key, Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res2.IsError {
		t.Fatalf("post-restart duplicate failed: %s", brmcptest.Text(res2))
	}
	if executions != 1 {
		t.Fatalf("post-restart duplicate executed: %d != 1", executions)
	}
	if got := h2.Billing().Balance(clientUID); got != 100 {
		t.Fatalf("post-restart duplicate charged: balance %d != 100", got)
	}
	if brmcptest.Text(res2) != brmcptest.Text(res1) {
		t.Fatalf("replayed outcome differs: %s != %s", brmcptest.Text(res2), brmcptest.Text(res1))
	}
}
