// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package server

import (
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/karamble/brmcp/brmcptest"
)

func TestRateLimit(t *testing.T) {
	h, err := NewHarness(&mcp.Implementation{Name: "t", Version: "0"}, HarnessConfig{
		DataDir:        t.TempDir(),
		CallsPerMinute: 2,
		Log:            brmcptest.Logger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !h.takeToken("peer") || !h.takeToken("peer") {
		t.Fatal("bucket refused within budget")
	}
	if h.takeToken("peer") {
		t.Fatal("bucket allowed above budget")
	}
	if !h.takeToken("other") {
		t.Fatal("buckets are not per peer")
	}
}
