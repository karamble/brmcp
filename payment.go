// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package brmcp

import (
	"encoding/json"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// PriceMetaKey is the tool _meta key advertising the per-call price in
// atoms, visible to clients in tools/list.
const PriceMetaKey = "brmcp/priceAtoms"

// PricingMetaKey marks tools whose price is computed per call from the
// arguments; the authoritative quote arrives in the payment_required error.
const PricingMetaKey = "brmcp/pricing"

// CallKeyMetaKey is the caller-supplied idempotency key in a tools/call
// _meta. A transport retry of the same logical call reuses the key; the
// harness executes and charges once and replays the recorded outcome to
// duplicates, so a lost reply can never double-bill.
const CallKeyMetaKey = "brmcp/callKey"

// PaymentRequired is the machine-readable body of the tool error returned
// when a paid call lacks balance. It is JSON in the result's text content so
// non-Go clients can parse it without extensions. Settlement is a Bison
// Relay tip: the payer's client requests an invoice from this bot's client
// over the relay (RMGetInvoice/RMInvoice) and pays it; the bot only sees the
// resulting tip credit.
type PaymentRequired struct {
	Error          string   `json:"error"` // always "payment_required"
	Tool           string   `json:"tool"`
	PriceAtoms     int64    `json:"priceAtoms"`
	BalanceAtoms   int64    `json:"balanceAtoms"`
	ShortfallAtoms int64    `json:"shortfallAtoms"`
	AcceptedRails  []string `json:"acceptedRails"`
}

// ParsePaymentRequired sniffs a tool error result for the payment_required
// JSON body, returning nil when the result is not a payment refusal.
func ParsePaymentRequired(res *mcp.CallToolResult) *PaymentRequired {
	if res == nil || !res.IsError {
		return nil
	}
	for _, c := range res.Content {
		tc, ok := c.(*mcp.TextContent)
		if !ok {
			continue
		}
		var pr PaymentRequired
		if err := json.Unmarshal([]byte(tc.Text), &pr); err == nil && pr.Error == "payment_required" {
			return &pr
		}
	}
	return nil
}

// SampleEnvelope is a representative v1 wire frame. Hosts that let users
// define content filters on private messages should refuse any rule that
// matches it: filtering envelope frames severs MCP sessions at the receive
// path.
const SampleEnvelope = `--mcp[v=1,sid=0123456789abcdef,mid=0123456789abcdef,seq=1/1,exp=1783000000]--eyJqc29ucnBjIjoiMi4wIn0=`
