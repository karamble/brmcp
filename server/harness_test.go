// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/karamble/brmcp"
	"github.com/karamble/brmcp/brmcptest"
	"github.com/karamble/brmcp/server"
	"github.com/karamble/brmcp/wire"
)

var (
	clientUID = brmcptest.UID(1)
	serverUID = brmcptest.UID(2)
)

// startHarnessFabric wires a harness (server role) and a plain router
// (client role) through an in-memory PM fabric and opens a client session.
func startHarnessFabric(t *testing.T, h *server.Harness) *mcp.ClientSession {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	f := brmcptest.NewFabric()
	t.Cleanup(f.Close)
	clientRouter := f.NewRouter(clientUID, brmcp.RouterConfig{Log: brmcptest.Logger(t)})
	serverRouter := h.Start(ctx, f.Sender(serverUID))
	f.Attach(serverUID, serverRouter.HandlePM)

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
	h, err := server.NewHarness(&mcp.Implementation{Name: "t", Version: "0"}, server.HarnessConfig{
		DataDir:        t.TempDir(),
		AllowedPeers:   []string{clientUID},
		CallsPerMinute: 100,
		Log:            brmcptest.Logger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	server.AddTool(h, &mcp.Tool{Name: "paid", Description: "paid tool"}, 500,
		func(_ context.Context, peer string, _ struct{}) (any, error) {
			return map[string]string{"caller": peer[:4]}, nil
		})
	server.AddTool(h, &mcp.Tool{Name: "flaky", Description: "always fails"}, 500,
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
			if v, ok := tool.Meta[brmcp.PriceMetaKey].(float64); ok && int64(v) == 500 {
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
	var pr brmcp.PaymentRequired
	if err := json.Unmarshal([]byte(brmcptest.Text(res)), &pr); err != nil {
		t.Fatalf("payment_required not parseable: %v: %s", err, brmcptest.Text(res))
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
		t.Fatalf("funded call failed: %s", brmcptest.Text(res))
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
	h, err := server.NewHarness(&mcp.Implementation{Name: "t", Version: "0"}, server.HarnessConfig{
		DataDir:        t.TempDir(),
		AllowedPeers:   []string{clientUID},
		CallsPerMinute: 100,
		Log:            brmcptest.Logger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	var executions int
	server.AddTool(h, &mcp.Tool{Name: "paid", Description: "paid tool"}, 500,
		func(context.Context, string, struct{}) (any, error) {
			executions++
			return map[string]int{"n": executions}, nil
		})
	session := startHarnessFabric(t, h)
	ctx := context.Background()

	// An unfunded keyed call refuses with payment_required and must NOT
	// pin that refusal to the key: after a top-up the same key runs for
	// real (the client retries the identical logical call post-payment).
	key := mcp.Meta{brmcp.CallKeyMetaKey: "test-call-key-0001"}
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
		t.Fatalf("funded keyed call failed: %s", brmcptest.Text(res))
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
		t.Fatalf("duplicate keyed call failed: %s", brmcptest.Text(res2))
	}
	if executions != 1 {
		t.Fatalf("duplicate executed the handler: executions %d != 1", executions)
	}
	if got := h.Billing().Balance(clientUID); got != 700 {
		t.Fatalf("duplicate was charged: balance %d != 700", got)
	}
	if brmcptest.Text(res2) != brmcptest.Text(res) {
		t.Fatalf("duplicate outcome differs: %s != %s", brmcptest.Text(res2), brmcptest.Text(res))
	}

	// A fresh key executes and charges again.
	res3, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "paid", Meta: mcp.Meta{brmcp.CallKeyMetaKey: "test-call-key-0002"}, Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res3.IsError {
		t.Fatalf("fresh keyed call failed: %s", brmcptest.Text(res3))
	}
	if executions != 2 {
		t.Fatalf("fresh key did not execute: executions %d != 2", executions)
	}
	if got := h.Billing().Balance(clientUID); got != 200 {
		t.Fatalf("balance after second charge: %d != 200", got)
	}
}

// legacyCaller speaks the pre-2026-07-28 wire by hand - initialize first,
// no server/discover, no per-request _meta triple - so the server's legacy
// path stays covered now that the SDK client negotiates the new wire.
type legacyCaller struct {
	t     *testing.T
	sid   string
	peer  string
	snd   brmcp.PMSender
	inbox chan jsonrpc.Message
}

func newLegacyCaller(t *testing.T, f *brmcptest.Fabric, uid, peer string) *legacyCaller {
	t.Helper()
	lc := &legacyCaller{t: t, sid: wire.NewID(), peer: peer,
		snd: f.Sender(uid), inbox: make(chan jsonrpc.Message, 16)}
	f.Attach(uid, func(_, text string) {
		part, ok := wire.Parse(text)
		if !ok || part.SID != lc.sid || part.Total != 1 {
			return // replies here are single-part; other sids are not ours
		}
		msg, err := jsonrpc.DecodeMessage(part.Chunk)
		if err != nil {
			t.Errorf("bad reply frame: %v", err)
			return
		}
		lc.inbox <- msg
	})
	return lc
}

// send frames one JSON-RPC message onto the fabric. id 0 sends a
// notification.
func (lc *legacyCaller) send(ctx context.Context, id int64, method, params string) {
	lc.t.Helper()
	req := &jsonrpc.Request{Method: method, Params: json.RawMessage(params)}
	if id != 0 {
		rid, err := jsonrpc.MakeID(float64(id))
		if err != nil {
			lc.t.Fatal(err)
		}
		req.ID = rid
	}
	data, err := jsonrpc.EncodeMessage(req)
	if err != nil {
		lc.t.Fatal(err)
	}
	parts, err := wire.Encode(lc.sid, data, time.Now().Add(time.Minute), 0)
	if err != nil {
		lc.t.Fatal(err)
	}
	for _, pm := range parts {
		if err := lc.snd.SendPM(ctx, lc.peer, pm); err != nil {
			lc.t.Fatal(err)
		}
	}
}

func (lc *legacyCaller) await(ctx context.Context, id int64) *jsonrpc.Response {
	lc.t.Helper()
	for {
		select {
		case msg := <-lc.inbox:
			resp, ok := msg.(*jsonrpc.Response)
			// MakeID normalizes float64 to an int64-typed ID.
			if !ok || resp.ID.Raw() != int64(id) {
				continue
			}
			return resp
		case <-ctx.Done():
			lc.t.Fatalf("no response to request %d: %v", id, ctx.Err())
			return nil
		}
	}
}

// callResult decodes a tools/call response body.
func (lc *legacyCaller) callResult(resp *jsonrpc.Response) *mcp.CallToolResult {
	lc.t.Helper()
	if resp.Error != nil {
		lc.t.Fatalf("tools/call failed: %v", resp.Error)
	}
	res := &mcp.CallToolResult{}
	if err := json.Unmarshal(resp.Result, res); err != nil {
		lc.t.Fatalf("undecodable tools/call result: %v: %s", err, resp.Result)
	}
	return res
}

// TestLegacyWirePaidFlow drives the full paid-tool flow over hand-rolled
// pre-2026-07-28 frames: the grace-period contract is that agents on the
// old wire keep working against an upgraded harness, priced _meta, keyed
// idempotency and all.
func TestLegacyWirePaidFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	h, err := server.NewHarness(&mcp.Implementation{Name: "t", Version: "0"}, server.HarnessConfig{
		DataDir:        t.TempDir(),
		AllowedPeers:   []string{clientUID},
		CallsPerMinute: 100,
		Log:            brmcptest.Logger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	var executions atomic.Int64
	server.AddTool(h, &mcp.Tool{Name: "paid", Description: "paid tool"}, 500,
		func(context.Context, string, struct{}) (any, error) {
			return map[string]int64{"n": executions.Add(1)}, nil
		})

	f := brmcptest.NewFabric()
	t.Cleanup(f.Close)
	router := h.Start(ctx, f.Sender(serverUID))
	f.Attach(serverUID, router.HandlePM)
	lc := newLegacyCaller(t, f, clientUID, serverUID)

	// The legacy handshake echoes a known version verbatim.
	lc.send(ctx, 1, "initialize",
		`{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"legacy-agent","version":"0"}}`)
	resp := lc.await(ctx, 1)
	if resp.Error != nil {
		t.Fatalf("initialize failed: %v", resp.Error)
	}
	var init struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	if err := json.Unmarshal(resp.Result, &init); err != nil {
		t.Fatal(err)
	}
	if init.ProtocolVersion != "2025-11-25" {
		t.Fatalf("initialize answered %q; want the requested 2025-11-25", init.ProtocolVersion)
	}
	lc.send(ctx, 0, "notifications/initialized", `{}`)
	// The notification has no reply to await, and the fabric delivers each
	// PM on its own goroutine; give it a beat so tools/list cannot overtake
	// it.
	time.Sleep(50 * time.Millisecond)

	// Prices are advertised in _meta on the legacy wire too.
	lc.send(ctx, 2, "tools/list", `{}`)
	resp = lc.await(ctx, 2)
	if resp.Error != nil {
		t.Fatalf("tools/list failed: %v", resp.Error)
	}
	var tl struct {
		Tools []struct {
			Name string         `json:"name"`
			Meta map[string]any `json:"_meta"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(resp.Result, &tl); err != nil {
		t.Fatal(err)
	}
	var sawPrice bool
	for _, tool := range tl.Tools {
		if tool.Name == "paid" {
			if v, ok := tool.Meta[brmcp.PriceMetaKey].(float64); ok && int64(v) == 500 {
				sawPrice = true
			}
		}
	}
	if !sawPrice {
		t.Fatalf("paid tool does not advertise its price: %s", resp.Result)
	}

	// Unfunded keyed call refuses with the parseable payment_required body.
	const callParams = `{"name":"paid","arguments":{},"_meta":{"brmcp/callKey":"legacy-key-0000000001"}}`
	lc.send(ctx, 3, "tools/call", callParams)
	res := lc.callResult(lc.await(ctx, 3))
	pr := brmcp.ParsePaymentRequired(res)
	if pr == nil || pr.PriceAtoms != 500 || pr.ShortfallAtoms != 500 {
		t.Fatalf("expected payment_required with shortfall 500, got %+v: %s", pr, brmcptest.Text(res))
	}

	// Funded, the same key runs for real exactly once.
	if err := h.Billing().Credit(clientUID, 600); err != nil {
		t.Fatal(err)
	}
	lc.send(ctx, 4, "tools/call", callParams)
	res = lc.callResult(lc.await(ctx, 4))
	if res.IsError {
		t.Fatalf("funded call failed: %s", brmcptest.Text(res))
	}
	if got := executions.Load(); got != 1 {
		t.Fatalf("executions: %d != 1", got)
	}
	if got := h.Billing().Balance(clientUID); got != 100 {
		t.Fatalf("balance after paid call: %d != 100", got)
	}

	// A duplicate key replays the outcome without executing or charging.
	lc.send(ctx, 5, "tools/call", callParams)
	res2 := lc.callResult(lc.await(ctx, 5))
	if brmcptest.Text(res2) != brmcptest.Text(res) {
		t.Fatalf("replayed outcome differs: %s != %s", brmcptest.Text(res2), brmcptest.Text(res))
	}
	if got := executions.Load(); got != 1 {
		t.Fatalf("duplicate executed the handler: %d != 1", got)
	}
	if got := h.Billing().Balance(clientUID); got != 100 {
		t.Fatalf("duplicate was charged: balance %d != 100", got)
	}

	// An unknown legacy version negotiates DOWN to 2025-11-25 (the pre-1.7
	// SDK echoed its latest instead); pin the grace-period behavior
	// non-Go callers will see. Fresh caller: a sid takes one initialize.
	lc2 := newLegacyCaller(t, f, clientUID, serverUID)
	lc2.send(ctx, 1, "initialize",
		`{"protocolVersion":"2099-01-01","capabilities":{},"clientInfo":{"name":"x","version":"0"}}`)
	resp = lc2.await(ctx, 1)
	if resp.Error != nil {
		t.Fatalf("unknown-version initialize failed: %v", resp.Error)
	}
	if err := json.Unmarshal(resp.Result, &init); err != nil {
		t.Fatal(err)
	}
	if init.ProtocolVersion != "2025-11-25" {
		t.Fatalf("unknown version negotiated %q; want 2025-11-25", init.ProtocolVersion)
	}
}
