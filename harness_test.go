// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package brmcp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// startHarnessFabric wires a harness (server role) and a plain router
// (client role) through an in-memory PM fabric and opens a client session.
func startHarnessFabric(t *testing.T, h *Harness) *mcp.ClientSession {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	var serverRouter *Router
	clientRouter := NewRouter(RouterConfig{
		Sender: senderFunc(func(_ context.Context, _, text string) error {
			go serverRouter.HandlePM(clientUID, text)
			return nil
		}),
		Logf: t.Logf,
	})
	serverRouter = h.Start(ctx, senderFunc(func(_ context.Context, _, text string) error {
		go clientRouter.HandlePM(serverUID, text)
		return nil
	}))
	t.Cleanup(clientRouter.Close)

	conn, err := clientRouter.Dial(serverUID)
	if err != nil {
		t.Fatal(err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "t", Version: "0"}, nil)
	session, err := client.Connect(ctx, conn.AsTransport(), nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { session.Close() })
	return session
}

func TestPaidToolGate(t *testing.T) {
	h, err := NewHarness(&mcp.Implementation{Name: "t", Version: "0"}, HarnessConfig{
		DataDir:        t.TempDir(),
		AllowedPeers:   []string{clientUID},
		CallsPerMinute: 100,
		Logf:           t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	AddTool(h, &mcp.Tool{Name: "paid", Description: "paid tool"}, 500,
		func(_ context.Context, peer string, _ struct{}) (any, error) {
			return map[string]string{"caller": peer[:4]}, nil
		})
	AddTool(h, &mcp.Tool{Name: "flaky", Description: "always fails"}, 500,
		func(context.Context, string, struct{}) (any, error) {
			return nil, errors.New("operator bug")
		})
	session := startHarnessFabric(t, h)
	ctx := context.Background()

	// The price is advertised in _meta.
	tl, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	var sawPrice bool
	for _, tool := range tl.Tools {
		if tool.Name == "paid" {
			if v, ok := tool.Meta[PriceMetaKey].(float64); ok && int64(v) == 500 {
				sawPrice = true
			}
		}
	}
	if !sawPrice {
		t.Fatalf("paid tool does not advertise its price: %+v", tl.Tools)
	}

	// No balance: the call must come back payment_required with the
	// tip rail.
	res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "paid", Arguments: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError {
		t.Fatal("unpaid call succeeded")
	}
	var pr PaymentRequired
	if err := json.Unmarshal([]byte(contentText(res)), &pr); err != nil {
		t.Fatalf("payment_required not parseable: %v: %s", err, contentText(res))
	}
	if pr.Error != "payment_required" || pr.PriceAtoms != 500 || pr.ShortfallAtoms != 500 {
		t.Fatalf("unexpected payment_required: %+v", pr)
	}
	if len(pr.AcceptedRails) != 1 || pr.AcceptedRails[0] != "tip" {
		t.Fatalf("unexpected rails: %v", pr.AcceptedRails)
	}

	// Simulate a received tip, then the call succeeds and debits.
	if err := h.Billing().Credit(clientUID, 600); err != nil {
		t.Fatal(err)
	}
	res, err = session.CallTool(ctx, &mcp.CallToolParams{Name: "paid", Arguments: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("funded call failed: %s", contentText(res))
	}
	if got := h.Billing().Balance(clientUID); got != 100 {
		t.Fatalf("balance after paid call: %d != 100", got)
	}

	// A handler error refunds the debit.
	if err := h.Billing().Credit(clientUID, 400); err != nil { // -> 500
		t.Fatal(err)
	}
	res, err = session.CallTool(ctx, &mcp.CallToolParams{Name: "flaky", Arguments: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError {
		t.Fatal("flaky tool reported success")
	}
	if got := h.Billing().Balance(clientUID); got != 500 {
		t.Fatalf("failed call was not refunded: balance %d != 500", got)
	}
}

func TestCallKeyIdempotency(t *testing.T) {
	h, err := NewHarness(&mcp.Implementation{Name: "t", Version: "0"}, HarnessConfig{
		DataDir:        t.TempDir(),
		AllowedPeers:   []string{clientUID},
		CallsPerMinute: 100,
		Logf:           t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	var executions int
	AddTool(h, &mcp.Tool{Name: "paid", Description: "paid tool"}, 500,
		func(context.Context, string, struct{}) (any, error) {
			executions++
			return map[string]int{"n": executions}, nil
		})
	session := startHarnessFabric(t, h)
	ctx := context.Background()

	// An unfunded keyed call refuses with payment_required and must NOT
	// pin that refusal to the key: after a top-up the same key runs for
	// real (the client retries the identical logical call post-payment).
	key := mcp.Meta{CallKeyMetaKey: "test-call-key-0001"}
	res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "paid", Meta: key, Arguments: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError {
		t.Fatal("unpaid keyed call succeeded")
	}
	if err := h.Billing().Credit(clientUID, 1200); err != nil {
		t.Fatal(err)
	}
	res, err = session.CallTool(ctx, &mcp.CallToolParams{Name: "paid", Meta: key, Arguments: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("funded keyed call failed: %s", contentText(res))
	}
	if executions != 1 {
		t.Fatalf("executions after first funded call: %d != 1", executions)
	}
	if got := h.Billing().Balance(clientUID); got != 700 {
		t.Fatalf("balance after first charge: %d != 700", got)
	}

	// A duplicate with the same key replays the outcome: no second
	// execution, no second debit, same payload.
	res2, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "paid", Meta: key, Arguments: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	if res2.IsError {
		t.Fatalf("duplicate keyed call failed: %s", contentText(res2))
	}
	if executions != 1 {
		t.Fatalf("duplicate executed the handler: executions %d != 1", executions)
	}
	if got := h.Billing().Balance(clientUID); got != 700 {
		t.Fatalf("duplicate was charged: balance %d != 700", got)
	}
	if contentText(res2) != contentText(res) {
		t.Fatalf("duplicate outcome differs: %s != %s", contentText(res2), contentText(res))
	}

	// A fresh key executes and charges again.
	res3, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "paid", Meta: mcp.Meta{CallKeyMetaKey: "test-call-key-0002"}, Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res3.IsError {
		t.Fatalf("fresh keyed call failed: %s", contentText(res3))
	}
	if executions != 2 {
		t.Fatalf("fresh key did not execute: executions %d != 2", executions)
	}
	if got := h.Billing().Balance(clientUID); got != 200 {
		t.Fatalf("balance after second charge: %d != 200", got)
	}
}

func TestRateLimit(t *testing.T) {
	h, err := NewHarness(&mcp.Implementation{Name: "t", Version: "0"}, HarnessConfig{
		DataDir:        t.TempDir(),
		CallsPerMinute: 2,
		Logf:           t.Logf,
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
