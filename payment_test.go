// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package brmcp_test

import (
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/karamble/brmcp"
)

func TestParsePaymentRequired(t *testing.T) {
	prJSON := `{"error":"payment_required","tool":"fortune","priceAtoms":10000,` +
		`"balanceAtoms":0,"shortfallAtoms":10000,"acceptedRails":["tip"]}`

	tests := []struct {
		name string
		res  *mcp.CallToolResult
		want bool
	}{
		{name: "nil result", res: nil, want: false},
		{name: "non-error result", res: &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: prJSON}},
		}, want: false},
		{name: "payment required", res: &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{&mcp.TextContent{Text: prJSON}},
		}, want: true},
		{name: "junk text", res: &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{&mcp.TextContent{Text: "tool exploded"}},
		}, want: false},
		{name: "other error json", res: &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{&mcp.TextContent{Text: `{"error":"rate_limited"}`}},
		}, want: false},
		{name: "payment after junk", res: &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{
				&mcp.TextContent{Text: "context"},
				&mcp.TextContent{Text: prJSON},
			},
		}, want: true},
	}
	for _, tc := range tests {
		pr := brmcp.ParsePaymentRequired(tc.res)
		if got := pr != nil; got != tc.want {
			t.Fatalf("%s: got %v, want %v", tc.name, got, tc.want)
		}
		if pr != nil {
			if pr.Tool != "fortune" || pr.ShortfallAtoms != 10000 {
				t.Fatalf("%s: parsed fields wrong: %+v", tc.name, pr)
			}
		}
	}
}
