// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package server

import (
	"testing"
	"time"

	"github.com/karamble/brmcp"
	"github.com/karamble/brmcp/wire"
)

// TestDispatchPM verifies the plain-chat / MCP-envelope split: only
// non-envelope PMs reach the host's OnPM hook, and envelopes still reach
// the router when no hook is set.
func TestDispatchPM(t *testing.T) {
	router := brmcp.NewRouter(brmcp.RouterConfig{Logf: t.Logf})

	var gotUID, gotText string
	hooks := RunBotHooks{OnPM: func(uid, text string) {
		gotUID, gotText = uid, text
	}}

	dispatchPM(router, hooks, "peer1", "hello bot")
	if gotUID != "peer1" || gotText != "hello bot" {
		t.Fatalf("plain chat not delivered to OnPM: uid=%q text=%q", gotUID, gotText)
	}

	// An MCP wire envelope must bypass OnPM.
	parts, err := wire.Encode("00aabb", []byte(`{"jsonrpc":"2.0"}`), time.Now().Add(time.Minute), wire.DefaultChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	gotUID, gotText = "", ""
	dispatchPM(router, hooks, "peer1", parts[0])
	if gotUID != "" || gotText != "" {
		t.Fatalf("envelope leaked to OnPM: uid=%q text=%q", gotUID, gotText)
	}

	// Nil hook with plain chat must not panic and not hit the router.
	dispatchPM(router, RunBotHooks{}, "peer1", "more chat")
}
