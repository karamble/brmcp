// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bridge_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/karamble/brmcp"
	"github.com/karamble/brmcp/bridge"
	"github.com/karamble/brmcp/brmcptest"
	"github.com/karamble/brmcp/server"
	"github.com/karamble/brmcp/wire"
)

var (
	agentUID = brmcptest.UID(1) // the bridge's own BR identity
	botUID   = brmcptest.UID(2) // the harness (tool bot)
)

const paidPrice = 500

// testClock drives the bridge's timers. With auto set, After fires
// immediately (fast paid-poll loops); otherwise Advance releases timers.
type testClock struct {
	mu      sync.Mutex
	now     time.Time
	auto    bool
	afters  int
	onAfter func(n int)
	timers  []testTimer
}

type testTimer struct {
	at time.Time
	ch chan time.Time
}

func newTestClock(auto bool) *testClock {
	return &testClock{now: time.Unix(1_800_000_000, 0), auto: auto}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	c.afters++
	n := c.afters
	hook := c.onAfter
	ch := make(chan time.Time, 1)
	if c.auto {
		ch <- c.now
	} else {
		c.timers = append(c.timers, testTimer{at: c.now.Add(d), ch: ch})
	}
	c.mu.Unlock()
	if hook != nil {
		hook(n)
	}
	return ch
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	var rest []testTimer
	for _, t := range c.timers {
		if !t.at.After(c.now) {
			t.ch <- c.now
		} else {
			rest = append(rest, t)
		}
	}
	c.timers = rest
}

// testPayer simulates the host settlement rail. By default a payment
// succeeds and credits the agent's balance on the harness, like a real tip.
type testPayer struct {
	h  *server.Harness
	mu sync.Mutex
	// credit controls whether success also funds the balance.
	credit bool
	fn     func(ctx context.Context, payeeUID string, atoms int64) error
	calls  int
}

func (p *testPayer) Pay(ctx context.Context, payeeUID string, atoms int64) error {
	p.mu.Lock()
	p.calls++
	fn := p.fn
	credit := p.credit
	p.mu.Unlock()
	if fn != nil {
		return fn(ctx, payeeUID, atoms)
	}
	if credit {
		return p.h.Billing().Credit(agentUID, atoms)
	}
	return nil
}

func (p *testPayer) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// flakySender injects transport failures into the bridge's outbound PMs.
type flakySender struct {
	inner brmcp.PMSender
	mu    sync.Mutex
	fail  int
}

func (s *flakySender) failNext(n int) {
	s.mu.Lock()
	s.fail = n
	s.mu.Unlock()
}

func (s *flakySender) SendPM(ctx context.Context, peer, text string) error {
	s.mu.Lock()
	if s.fail > 0 {
		s.fail--
		s.mu.Unlock()
		return errors.New("relay hiccup")
	}
	s.mu.Unlock()
	return s.inner.SendPM(ctx, peer, text)
}

type fixture struct {
	t       *testing.T
	ctx     context.Context
	fab     *brmcptest.Fabric
	harness *server.Harness
	bridge  *bridge.Bridge
	clk     *testClock
	payer   *testPayer
	sender  *flakySender
	execs   atomic.Int64
	dataDir string
	httpSrv *httptest.Server
	token   string
}

type fixtureOpts struct {
	clk      *testClock
	settings *bridge.Settings
	seed     func(dataDir string) // runs before bridge.New
}

// newFixture wires a harness bot and a bridge across an in-memory fabric
// and serves the bridge's handler over httptest.
func newFixture(t *testing.T, o fixtureOpts) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	fx := &fixture{t: t, ctx: ctx, clk: o.clk, dataDir: t.TempDir()}
	if fx.clk == nil {
		fx.clk = newTestClock(true)
	}

	h, err := server.NewHarness(&mcp.Implementation{Name: "bot", Version: "0"}, server.HarnessConfig{
		DataDir:        t.TempDir(),
		AllowedPeers:   []string{agentUID},
		CallsPerMinute: 10_000,
		Logf:           t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	fx.harness = h
	server.AddTool(h, &mcp.Tool{Name: "paid", Description: "paid tool"}, paidPrice,
		func(context.Context, string, struct{}) (any, error) {
			return map[string]int64{"n": fx.execs.Add(1)}, nil
		})
	server.AddTool(h, &mcp.Tool{Name: "free", Description: "free tool"}, 0,
		func(context.Context, string, struct{}) (any, error) {
			return map[string]string{"ok": "yes"}, nil
		})

	fab := brmcptest.NewFabric()
	t.Cleanup(fab.Close)
	fx.fab = fab
	botRouter := h.Start(ctx, fab.Sender(botUID))
	fab.Attach(botUID, botRouter.HandlePM)

	fx.payer = &testPayer{h: h, credit: true}
	fx.sender = &flakySender{inner: fab.Sender(agentUID)}
	if o.seed != nil {
		o.seed(fx.dataDir)
	}
	b, err := bridge.New(bridge.Config{
		DataDir: fx.dataDir,
		Sender:  fx.sender,
		Payer:   fx.payer,
		Name:    "brclientd",
		Logf:    t.Logf,
		Clock:   fx.clk,
	})
	if err != nil {
		t.Fatal(err)
	}
	fx.bridge = b
	fab.Attach(agentUID, b.HandlePM)
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })

	s := bridge.Settings{
		Enabled:         true,
		Mode:            "autopay",
		PerCallCapAtoms: 10_000,
		PerDayCapAtoms:  100_000,
		AllowedBots:     []string{botUID},
	}
	if o.settings != nil {
		s = *o.settings
	}
	// Enabling with nothing stored mints, and the returned plaintext is the
	// only time it is readable.
	minted, err := b.ApplySettings(s)
	if err != nil {
		t.Fatal(err)
	}
	fx.token = minted

	fx.httpSrv = httptest.NewServer(b.Handler())
	t.Cleanup(fx.httpSrv.Close)
	return fx
}

type bearerTransport struct {
	token string
}

func (bt bearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+bt.token)
	return http.DefaultTransport.RoundTrip(r)
}

func (fx *fixture) endpoint(uid string) string { return fx.httpSrv.URL + "/mcp/" + uid }

// session opens an MCP session to the bot's mirrored endpoint.
func (fx *fixture) session() *mcp.ClientSession {
	fx.t.Helper()
	tr := &mcp.StreamableClientTransport{
		Endpoint:   fx.endpoint(botUID),
		HTTPClient: &http.Client{Transport: bearerTransport{fx.token}},
	}
	cl := mcp.NewClient(&mcp.Implementation{Name: "agent", Version: "0"}, nil)
	session, err := cl.Connect(fx.ctx, tr, nil)
	if err != nil {
		fx.t.Fatalf("connect: %v", err)
	}
	fx.t.Cleanup(func() { session.Close() })
	return session
}

func (fx *fixture) call(session *mcp.ClientSession, tool string) (*mcp.CallToolResult, error) {
	return session.CallTool(fx.ctx, &mcp.CallToolParams{Name: tool, Arguments: map[string]any{}})
}

func requireNote(t *testing.T, res *mcp.CallToolResult, substr string) {
	t.Helper()
	text := brmcptest.Text(res)
	if !strings.Contains(text, substr) {
		t.Fatalf("result missing %q:\n%s", substr, text)
	}
}

func TestAutopayHappyPath(t *testing.T) {
	fx := newFixture(t, fixtureOpts{})
	session := fx.session()

	// The stateless endpoint must offer the new wire to a current agent.
	if v := session.InitializeResult().ProtocolVersion; v < "2026-07-28" {
		t.Fatalf("bridge endpoint negotiated %q; want the 2026-07-28 wire", v)
	}

	res, err := fx.call(session, "paid")
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("paid call failed: %s", brmcptest.Text(res))
	}
	if got := fx.execs.Load(); got != 1 {
		t.Fatalf("handler executions: %d != 1", got)
	}
	if got := fx.payer.callCount(); got != 1 {
		t.Fatalf("payer calls: %d != 1", got)
	}
	entries, today := fx.bridge.SpendLog()
	if len(entries) != 1 || entries[0].Atoms != paidPrice || entries[0].Rail != "tip" ||
		entries[0].Bot != botUID || entries[0].Tool != "paid" {
		t.Fatalf("spend log: %+v", entries)
	}
	if entries[0].Status != "paid" || entries[0].Err != "" {
		t.Fatalf("settled payment not marked paid: %+v", entries[0])
	}
	if today != paidPrice {
		t.Fatalf("todayAtoms: %d != %d", today, paidPrice)
	}
	// The spend log survives a reload, status included.
	b2, err := bridge.New(bridge.Config{
		DataDir: fx.dataDir, Sender: fx.sender, Payer: fx.payer, Clock: fx.clk,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	entries2, _ := b2.SpendLog()
	if len(entries2) != 1 || entries2[0].Status != "paid" {
		t.Fatalf("reloaded spend log: %+v", entries2)
	}
}

func TestFreeToolUnaffected(t *testing.T) {
	fx := newFixture(t, fixtureOpts{})
	session := fx.session()
	res, err := fx.call(session, "free")
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("free call failed: %s", brmcptest.Text(res))
	}
	if got := fx.payer.callCount(); got != 0 {
		t.Fatalf("free call reached the payer: %d calls", got)
	}
}

func TestPerCallCapRefusals(t *testing.T) {
	for name, cap := range map[string]int64{"zero": 0, "belowPrice": paidPrice - 1} {
		t.Run(name, func(t *testing.T) {
			fx := newFixture(t, fixtureOpts{settings: &bridge.Settings{
				Enabled:         true,
				Mode:            "autopay",
				PerCallCapAtoms: cap,
				PerDayCapAtoms:  100_000,
				AllowedBots:     []string{botUID},
			}})
			session := fx.session()
			res, err := fx.call(session, "paid")
			if err != nil {
				t.Fatal(err)
			}
			if !res.IsError {
				t.Fatal("capped call succeeded")
			}
			requireNote(t, res, "payment_required")
			requireNote(t, res, "brclientd: payment not made: ")
			requireNote(t, res, "exceeds the per-call cap")
			if got := fx.payer.callCount(); got != 0 {
				t.Fatalf("payer called despite cap: %d", got)
			}
			// The refusal is recorded so the operator can see what was
			// asked for, but no money moved, so it must not consume the
			// daily budget.
			entries, today := fx.bridge.SpendLog()
			if len(entries) != 1 {
				t.Fatalf("refusal not recorded exactly once: %+v", entries)
			}
			if entries[0].Status != "refused" || entries[0].Atoms != paidPrice {
				t.Fatalf("refusal entry wrong: %+v", entries[0])
			}
			if !strings.Contains(entries[0].Err, "per-call cap") {
				t.Fatalf("refusal reason not recorded: %q", entries[0].Err)
			}
			if today != 0 {
				t.Fatalf("a refusal consumed the daily budget: %d", today)
			}
		})
	}
}

func TestDailyCapWindow(t *testing.T) {
	clk := newTestClock(true)
	fx := newFixture(t, fixtureOpts{clk: clk, settings: &bridge.Settings{
		Enabled:         true,
		Mode:            "autopay",
		PerCallCapAtoms: paidPrice,
		PerDayCapAtoms:  2 * paidPrice,
		AllowedBots:     []string{botUID},
	}})
	session := fx.session()

	// Two calls fill the daily cap exactly (exact fill is allowed; the
	// per-call cap equals the price, also exact fill).
	for i := 0; i < 2; i++ {
		res, err := fx.call(session, "paid")
		if err != nil {
			t.Fatal(err)
		}
		if res.IsError {
			t.Fatalf("call %d failed: %s", i, brmcptest.Text(res))
		}
	}
	res, err := fx.call(session, "paid")
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError {
		t.Fatal("call over the daily cap succeeded")
	}
	requireNote(t, res, "would exceed the daily cap (1000 spent of 1000)")

	// The window is rolling: a day later the old spend no longer counts.
	clk.Advance(25 * time.Hour)
	res, err = fx.call(session, "paid")
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("call after window passed failed: %s", brmcptest.Text(res))
	}
	if _, today := fx.bridge.SpendLog(); today != paidPrice {
		t.Fatalf("todayAtoms after window: %d != %d", today, paidPrice)
	}
}

func TestApprovalFlow(t *testing.T) {
	clk := newTestClock(false)
	fx := newFixture(t, fixtureOpts{clk: clk, settings: &bridge.Settings{
		Enabled:         true,
		Mode:            "approval",
		PerCallCapAtoms: 10_000,
		PerDayCapAtoms:  100_000,
		AllowedBots:     []string{botUID},
	}})
	session := fx.session()

	type callOut struct {
		res *mcp.CallToolResult
		err error
	}
	start := func() chan callOut {
		out := make(chan callOut, 1)
		go func() {
			res, err := fx.call(session, "paid")
			out <- callOut{res, err}
		}()
		return out
	}
	waitPending := func() bridge.PendingPayment {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if ps := fx.bridge.PendingPayments(); len(ps) == 1 {
				return ps[0]
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("no pending payment appeared")
		return bridge.PendingPayment{}
	}

	// Approve: the payment settles and the call completes.
	out := start()
	p := waitPending()
	if p.Bot != botUID || p.Tool != "paid" || p.Atoms != paidPrice {
		t.Fatalf("pending payment: %+v", p)
	}
	if !fx.bridge.ResolvePayment(p.ID, true) {
		t.Fatal("resolve reported unknown id")
	}
	if fx.bridge.ResolvePayment(p.ID, true) {
		// The entry lives until the call unblocks; a repeat decision on
		// the same id must be a harmless no-op either way.
		t.Log("second resolve accepted (entry still parked); decision is single-shot")
	}
	o := <-out
	if o.err != nil {
		t.Fatal(o.err)
	}
	if o.res.IsError {
		t.Fatalf("approved call failed: %s", brmcptest.Text(o.res))
	}
	if len(fx.bridge.PendingPayments()) != 0 {
		t.Fatal("pending queue not drained after approval")
	}

	// Deny: the call comes back with the refusal note appended.
	out = start()
	p = waitPending()
	fx.bridge.ResolvePayment(p.ID, false)
	o = <-out
	if o.err != nil {
		t.Fatal(o.err)
	}
	requireNote(t, o.res, "brclientd: payment not made: payment denied by the user")

	// Timeout: no decision within the approval window fails the payment.
	out = start()
	waitPending()
	clk.Advance(120 * time.Second)
	o = <-out
	if o.err != nil {
		t.Fatal(o.err)
	}
	requireNote(t, o.res, "approval timed out after 2m0s")
	if len(fx.bridge.PendingPayments()) != 0 {
		t.Fatal("pending queue not drained after timeout")
	}

	// Unknown ids are reported.
	if fx.bridge.ResolvePayment("no-such-id", true) {
		t.Fatal("unknown id resolved")
	}
	// Only the approved call executed. The denial and the timeout are
	// recorded as refusals, and only the approved one counts as spend.
	if got := fx.execs.Load(); got != 1 {
		t.Fatalf("executions: %d != 1", got)
	}
	entries, today := fx.bridge.SpendLog()
	if len(entries) != 3 {
		t.Fatalf("spend entries: %+v", entries)
	}
	var paid, refused int
	for _, e := range entries {
		switch e.Status {
		case "paid":
			paid++
		case "refused":
			refused++
		}
	}
	if paid != 1 || refused != 2 {
		t.Fatalf("paid=%d refused=%d, want 1 and 2: %+v", paid, refused, entries)
	}
	if today != paidPrice {
		t.Fatalf("today = %d, want only the approved call to count (%d)", today, paidPrice)
	}
}

func TestPaidPollLateCredit(t *testing.T) {
	clk := newTestClock(true)
	fx := newFixture(t, fixtureOpts{clk: clk})
	// The payment settles but the credit lands only later (tips are
	// asynchronous): the second poll finds it.
	fx.payer.mu.Lock()
	fx.payer.fn = func(context.Context, string, int64) error { return nil }
	fx.payer.mu.Unlock()
	clk.mu.Lock()
	clk.onAfter = func(n int) {
		if n == 2 {
			if err := fx.harness.Billing().Credit(agentUID, paidPrice); err != nil {
				t.Error(err)
			}
		}
	}
	clk.mu.Unlock()

	session := fx.session()
	res, err := fx.call(session, "paid")
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("late-credit call failed: %s", brmcptest.Text(res))
	}
	if got := fx.execs.Load(); got != 1 {
		t.Fatalf("executions: %d != 1", got)
	}
}

func TestPaidPollExhaustion(t *testing.T) {
	fx := newFixture(t, fixtureOpts{})
	// Settlement reports success but the credit never lands: the bridge
	// polls briefly, then hands the bot's payment_required back as is.
	fx.payer.mu.Lock()
	fx.payer.credit = false
	fx.payer.mu.Unlock()

	session := fx.session()
	res, err := fx.call(session, "paid")
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError {
		t.Fatal("unfunded call succeeded")
	}
	requireNote(t, res, "payment_required")
	if text := brmcptest.Text(res); strings.Contains(text, "payment not made") {
		t.Fatalf("exhausted poll appended a refusal note: %s", text)
	}
	if got := fx.payer.callCount(); got != 1 {
		t.Fatalf("payer calls: %d != 1 (re-settled during poll?)", got)
	}
	if got := fx.execs.Load(); got != 0 {
		t.Fatalf("executions: %d != 0", got)
	}
}

func TestTransportRetryOnce(t *testing.T) {
	fx := newFixture(t, fixtureOpts{})
	session := fx.session()

	// Build the session and proxy on a healthy transport first.
	if res, err := fx.call(session, "free"); err != nil || res.IsError {
		t.Fatalf("warmup: %v %v", err, res)
	}

	// One send failure: the bridge resets the link and retries once.
	fx.sender.failNext(1)
	res, err := fx.call(session, "free")
	if err != nil {
		t.Fatalf("single failure not retried: %v", err)
	}
	if res.IsError {
		t.Fatalf("retried call failed: %s", brmcptest.Text(res))
	}

	// Two consecutive failures exhaust the retry budget and surface.
	fx.sender.failNext(2)
	res, err = fx.call(session, "free")
	if err == nil && !res.IsError {
		t.Fatal("double failure did not surface")
	}
	fx.sender.failNext(0)
}

func TestMirrorFidelity(t *testing.T) {
	fx := newFixture(t, fixtureOpts{})

	type shapeIn struct {
		Text  string `json:"text" jsonschema:"the text"`
		Count int    `json:"count,omitempty"`
	}
	server.AddTool(fx.harness, &mcp.Tool{Name: "shape", Description: "schema-rich"}, paidPrice,
		func(_ context.Context, _ string, in shapeIn) (any, error) { return in, nil })
	server.AddToolPriced(fx.harness, &mcp.Tool{Name: "metered", Description: "dynamic price"},
		func(context.Context, string, struct{}) (int64, error) { return 7, nil },
		func(context.Context, string, struct{}) (any, error) { return "ok", nil })

	session := fx.session()
	tl, err := session.ListTools(fx.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	tools := map[string]*mcp.Tool{}
	for _, tool := range tl.Tools {
		tools[tool.Name] = tool
	}
	shape := tools["shape"]
	if shape == nil {
		t.Fatalf("shape tool not mirrored: %v", tl.Tools)
	}
	if v, ok := shape.Meta[brmcp.PriceMetaKey].(float64); !ok || int64(v) != paidPrice {
		t.Fatalf("mirrored price meta: %+v", shape.Meta)
	}
	raw, err := json.Marshal(shape.InputSchema)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"text"`, `"count"`, `"the text"`} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("mirrored schema missing %s: %s", want, raw)
		}
	}
	metered := tools["metered"]
	if metered == nil {
		t.Fatalf("metered tool not mirrored: %v", tl.Tools)
	}
	if v, ok := metered.Meta[brmcp.PricingMetaKey].(string); !ok || v != "dynamic" {
		t.Fatalf("mirrored pricing meta: %+v", metered.Meta)
	}
}

func TestSpendPrunePreservesWindow(t *testing.T) {
	clk := newTestClock(true)
	const seeded = 1500
	fx := newFixture(t, fixtureOpts{
		clk: clk,
		seed: func(dataDir string) {
			entries := make([]bridge.SpendEntry, seeded)
			base := clk.Now().Unix()
			for i := range entries {
				entries[i] = bridge.SpendEntry{TS: base, Bot: botUID, Tool: "seed", Rail: "tip", Atoms: 1}
			}
			raw, err := json.Marshal(entries)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dataDir, "mcpspend.json"), raw, 0o600); err != nil {
				t.Fatal(err)
			}
		},
		settings: &bridge.Settings{
			Enabled:         true,
			Mode:            "autopay",
			PerCallCapAtoms: paidPrice,
			PerDayCapAtoms:  seeded + 2*paidPrice,
			AllowedBots:     []string{botUID},
		},
	})
	session := fx.session()

	// A paid call persists the log; every seeded entry is inside the
	// rolling window, so none may be pruned despite exceeding the bound.
	if res, err := fx.call(session, "paid"); err != nil || res.IsError {
		t.Fatalf("paid call: %v %v", err, res)
	}
	entries, today := fx.bridge.SpendLog()
	if len(entries) != seeded+1 {
		t.Fatalf("in-window entries pruned: %d != %d", len(entries), seeded+1)
	}
	if today != seeded+paidPrice {
		t.Fatalf("todayAtoms undercounts: %d != %d", today, seeded+paidPrice)
	}

	// Once the window passes, the log shrinks back to the bound.
	clk.Advance(25 * time.Hour)
	if res, err := fx.call(session, "paid"); err != nil || res.IsError {
		t.Fatalf("paid call after window: %v %v", err, res)
	}
	entries, today = fx.bridge.SpendLog()
	if len(entries) != 1000 {
		t.Fatalf("stale entries kept: %d != 1000", len(entries))
	}
	if today != paidPrice {
		t.Fatalf("todayAtoms after window: %d != %d", today, paidPrice)
	}
}

func TestPayFailureNotCounted(t *testing.T) {
	fx := newFixture(t, fixtureOpts{})
	fx.payer.mu.Lock()
	fx.payer.fn = func(context.Context, string, int64) error {
		return errors.New("no route to bot")
	}
	fx.payer.mu.Unlock()

	session := fx.session()
	res, err := fx.call(session, "paid")
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError {
		t.Fatal("unpaid call succeeded")
	}
	requireNote(t, res, "brclientd: payment not made: no route to bot")
	entries, today := fx.bridge.SpendLog()
	if len(entries) != 1 || today != 0 {
		t.Fatalf("failed payment miscounted: %+v today=%d", entries, today)
	}
	if entries[0].Status != "failed" || entries[0].Err != "no route to bot" {
		t.Fatalf("failure not recorded on the entry: %+v", entries[0])
	}
}

func TestPayTimeoutStaysCounted(t *testing.T) {
	fx := newFixture(t, fixtureOpts{settings: &bridge.Settings{
		Enabled:         true,
		Mode:            "autopay",
		PerCallCapAtoms: 10_000,
		PerDayCapAtoms:  100_000,
		AllowedBots:     []string{botUID},
		TipWaitSecs:     1,
	}})
	// The rail never confirms within the wait budget; the attempt may
	// still settle later, so the spend stays counted.
	fx.payer.mu.Lock()
	fx.payer.fn = func(ctx context.Context, _ string, _ int64) error {
		<-ctx.Done()
		return errors.New("tip not confirmed within 1s; the attempt keeps running")
	}
	fx.payer.mu.Unlock()

	session := fx.session()
	res, err := fx.call(session, "paid")
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError {
		t.Fatal("unconfirmed call succeeded")
	}
	requireNote(t, res, "payment not made: tip not confirmed within 1s")
	entries, today := fx.bridge.SpendLog()
	if len(entries) != 1 || today != paidPrice {
		t.Fatalf("unconfirmed payment not counted: %+v today=%d", entries, today)
	}
	if entries[0].Status != "pending" {
		t.Fatalf("unconfirmed payment not pending: %+v", entries[0])
	}
}

// lateOutcomeFixture runs one paid call whose rail never confirms within
// the wait budget, leaving the spend entry pending for ResolveSpend.
func lateOutcomeFixture(t *testing.T) *fixture {
	t.Helper()
	fx := newFixture(t, fixtureOpts{settings: &bridge.Settings{
		Enabled:         true,
		Mode:            "autopay",
		PerCallCapAtoms: 10_000,
		PerDayCapAtoms:  paidPrice, // exactly one launch fits the day
		AllowedBots:     []string{botUID},
		TipWaitSecs:     1,
	}})
	fx.payer.mu.Lock()
	fx.payer.fn = func(ctx context.Context, _ string, _ int64) error {
		<-ctx.Done()
		return errors.New("tip not confirmed within 1s; the attempt keeps running")
	}
	fx.payer.mu.Unlock()

	session := fx.session()
	res, err := fx.call(session, "paid")
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError {
		t.Fatal("unconfirmed call succeeded")
	}
	return fx
}

func TestResolveSpendLateFailure(t *testing.T) {
	fx := lateOutcomeFixture(t)

	// While pending the payment consumes the daily budget: a second call
	// must be refused.
	session := fx.session()
	res, err := fx.call(session, "paid")
	if err != nil {
		t.Fatal(err)
	}
	requireNote(t, res, "daily cap")

	// Unrelated terminal events (other payee, other amount, non-pending
	// entries) must not resolve anything.
	if fx.bridge.ResolveSpend(agentUID, paidPrice, errors.New("no route")) {
		t.Fatal("resolved a spend for the wrong payee")
	}
	if fx.bridge.ResolveSpend(botUID, paidPrice+1, errors.New("no route")) {
		t.Fatal("resolved a spend for the wrong amount")
	}

	// The late terminal failure marks the entry failed and releases the
	// budget.
	if !fx.bridge.ResolveSpend(botUID, paidPrice, errors.New("no route")) {
		t.Fatal("late failure did not resolve the pending spend")
	}
	// Two entries: the pending payment that failed late, and the refusal
	// its held budget caused above. Neither counts toward the day.
	entries, today := fx.bridge.SpendLog()
	if len(entries) != 2 || today != 0 {
		t.Fatalf("late failure still counted: %+v today=%d", entries, today)
	}
	if entries[0].Status != "failed" || entries[0].Err != "no route" {
		t.Fatalf("late failure not recorded: %+v", entries[0])
	}
	if fx.bridge.ResolveSpend(botUID, paidPrice, errors.New("no route")) {
		t.Fatal("resolved the same spend twice")
	}

	// With the failed payment uncounted, the day budget fits a new call.
	fx.payer.mu.Lock()
	fx.payer.fn = nil
	fx.payer.mu.Unlock()
	if res, err := fx.call(session, "paid"); err != nil || res.IsError {
		t.Fatalf("call after released budget: %v %v", err, res)
	}

	// The outcome survives a restart: a fresh bridge on the same data dir
	// reloads the marked entry and the released budget.
	b2, err := bridge.New(bridge.Config{
		DataDir: fx.dataDir, Sender: fx.sender, Payer: fx.payer, Clock: fx.clk,
	})
	if err != nil {
		t.Fatal(err)
	}
	// All three survive in order - the late failure, the refusal its held
	// budget caused, and the call that went through once it was released -
	// and only the paid one counts toward the day.
	entries, today = b2.SpendLog()
	if len(entries) != 3 || today != paidPrice {
		t.Fatalf("reloaded log wrong: %+v today=%d", entries, today)
	}
	if entries[0].Status != "failed" || entries[1].Status != "refused" || entries[2].Status != "paid" {
		t.Fatalf("reloaded statuses wrong: %+v", entries)
	}
}

func TestResolveSpendLatePaid(t *testing.T) {
	fx := lateOutcomeFixture(t)

	// The late terminal success confirms the pending entry; the spend
	// stays counted.
	if !fx.bridge.ResolveSpend(botUID, paidPrice, nil) {
		t.Fatal("late success did not resolve the pending spend")
	}
	entries, today := fx.bridge.SpendLog()
	if len(entries) != 1 || today != paidPrice {
		t.Fatalf("late success miscounted: %+v today=%d", entries, today)
	}
	if entries[0].Status != "paid" || entries[0].Err != "" {
		t.Fatalf("late success not recorded: %+v", entries[0])
	}

	// A pending entry reloaded from disk (no live seq) resolves by key the
	// same way after a restart.
	fx2 := lateOutcomeFixture(t)
	b2, err := bridge.New(bridge.Config{
		DataDir: fx2.dataDir, Sender: fx2.sender, Payer: fx2.payer, Clock: fx2.clk,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !b2.ResolveSpend(botUID, paidPrice, nil) {
		t.Fatal("reloaded pending spend did not resolve")
	}
	entries, _ = b2.SpendLog()
	if len(entries) != 1 || entries[0].Status != "paid" {
		t.Fatalf("reloaded resolve wrong: %+v", entries)
	}
}

func TestResolveSpendOldestFirst(t *testing.T) {
	fx := newFixture(t, fixtureOpts{settings: &bridge.Settings{
		Enabled:         true,
		Mode:            "autopay",
		PerCallCapAtoms: 10_000,
		PerDayCapAtoms:  2 * paidPrice,
		AllowedBots:     []string{botUID},
		TipWaitSecs:     1,
	}})
	fx.payer.mu.Lock()
	fx.payer.fn = func(ctx context.Context, _ string, _ int64) error {
		<-ctx.Done()
		return errors.New("tip not confirmed within 1s")
	}
	fx.payer.mu.Unlock()

	session := fx.session()
	for i := 0; i < 2; i++ {
		if _, err := fx.call(session, "paid"); err != nil {
			t.Fatal(err)
		}
	}
	if entries, _ := fx.bridge.SpendLog(); len(entries) != 2 {
		t.Fatalf("pending entries: %+v", entries)
	}

	// Identical-key outcomes claim the oldest pending entry first.
	if !fx.bridge.ResolveSpend(botUID, paidPrice, nil) {
		t.Fatal("first outcome unresolved")
	}
	entries, _ := fx.bridge.SpendLog()
	if entries[0].Status != "paid" || entries[1].Status != "pending" {
		t.Fatalf("oldest entry not claimed first: %+v", entries)
	}
	if !fx.bridge.ResolveSpend(botUID, paidPrice, errors.New("no route")) {
		t.Fatal("second outcome unresolved")
	}
	entries, today := fx.bridge.SpendLog()
	if entries[1].Status != "failed" {
		t.Fatalf("second entry not claimed: %+v", entries)
	}
	if today != paidPrice {
		t.Fatalf("todayAtoms: %d != %d", today, paidPrice)
	}
}

func TestPruneProtectsPending(t *testing.T) {
	clk := newTestClock(true)
	const seeded = 1500
	const pendingAt = 300
	fx := newFixture(t, fixtureOpts{
		clk: clk,
		seed: func(dataDir string) {
			// Everything is outside the daily-cap window, so only the
			// pending entry may stop the prune.
			entries := make([]bridge.SpendEntry, seeded)
			old := clk.Now().Add(-48 * time.Hour).Unix()
			for i := range entries {
				entries[i] = bridge.SpendEntry{TS: old, Bot: botUID, Tool: "seed", Rail: "tip", Atoms: 1}
			}
			entries[pendingAt].Status = "pending"
			entries[pendingAt].Atoms = 2
			raw, err := json.Marshal(entries)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dataDir, "mcpspend.json"), raw, 0o600); err != nil {
				t.Fatal(err)
			}
		},
	})
	session := fx.session()

	// The persist after a paid call prunes the stale legacy prefix but
	// stops at the pending entry awaiting its late outcome.
	if res, err := fx.call(session, "paid"); err != nil || res.IsError {
		t.Fatalf("paid call: %v %v", err, res)
	}
	entries, _ := fx.bridge.SpendLog()
	if len(entries) != seeded-pendingAt+1 {
		t.Fatalf("prune kept %d entries != %d", len(entries), seeded-pendingAt+1)
	}
	if entries[0].Status != "pending" {
		t.Fatalf("pending entry pruned: %+v", entries[0])
	}

	// Legacy entries (no status) are never claimed by late outcomes; the
	// surviving pending one still is.
	if fx.bridge.ResolveSpend(botUID, 1, errors.New("late")) {
		t.Fatal("legacy entry claimed by a late outcome")
	}
	if !fx.bridge.ResolveSpend(botUID, 2, errors.New("late")) {
		t.Fatal("pending entry not claimed by its late outcome")
	}
}

func TestUnreachableBotIs503(t *testing.T) {
	fx := newFixture(t, fixtureOpts{})
	// Every outbound PM fails: the proxy build cannot reach the bot and
	// the endpoint must say so instead of serving an empty tool list.
	fx.sender.failNext(1 << 30)
	if got := httpStatus(t, http.MethodPost, fx.endpoint(botUID), fx.token); got != http.StatusServiceUnavailable {
		t.Fatalf("unreachable bot answered %d != 503", got)
	}
	fx.sender.failNext(0)

	// With the transport healthy again the same endpoint serves tools.
	session := fx.session()
	if res, err := fx.call(session, "free"); err != nil || res.IsError {
		t.Fatalf("recovery call: %v %v", err, res)
	}
}

// legacyBot is a hand-rolled pre-2026-07-28 MCP server on the fabric: it
// rejects server/discover with method-not-found the way a v1.6.1 bot does,
// then serves the legacy handshake and one paid tool. It pins the bridge's
// fallback dial path against bots that have not upgraded yet.
type legacyBot struct {
	t   *testing.T
	snd brmcp.PMSender

	mu       sync.Mutex
	funded   bool
	methods  []string
	callKeys []string
	newMeta  bool
}

func (lb *legacyBot) setFunded(v bool) {
	lb.mu.Lock()
	lb.funded = v
	lb.mu.Unlock()
}

func (lb *legacyBot) handlePM(from, text string) {
	part, ok := wire.Parse(text)
	if !ok || part.Total != 1 {
		return
	}
	msg, err := jsonrpc.DecodeMessage(part.Chunk)
	if err != nil {
		lb.t.Errorf("legacy bot: bad frame: %v", err)
		return
	}
	req, ok := msg.(*jsonrpc.Request)
	if !ok {
		return
	}
	var params struct {
		Meta map[string]any `json:"_meta"`
		Name string         `json:"name"`
	}
	if len(req.Params) > 0 {
		_ = json.Unmarshal(req.Params, &params)
	}

	lb.mu.Lock()
	lb.methods = append(lb.methods, req.Method)
	if req.Method != "server/discover" {
		if _, ok := params.Meta[mcp.MetaKeyProtocolVersion]; ok {
			lb.newMeta = true
		}
	}
	if req.Method == "tools/call" {
		if k, _ := params.Meta[brmcp.CallKeyMetaKey].(string); k != "" {
			lb.callKeys = append(lb.callKeys, k)
		}
	}
	funded := lb.funded
	lb.mu.Unlock()

	var result any
	var respErr error
	switch req.Method {
	case "server/discover":
		// What a pre-1.7 bot answers; any error triggers the client's
		// legacy fallback, so the exact text is not load-bearing.
		respErr = &jsonrpc.Error{Code: jsonrpc.CodeMethodNotFound,
			Message: `method not found: "server/discover"`}
	case "initialize":
		result = map[string]any{
			"protocolVersion": "2025-11-25",
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": "legacy-bot", "version": "0"},
		}
	case "tools/list":
		result = map[string]any{"tools": []map[string]any{{
			"name":        "paid",
			"description": "paid tool",
			"inputSchema": map[string]any{"type": "object"},
			"_meta":       map[string]any{brmcp.PriceMetaKey: paidPrice},
		}}}
	case "tools/call":
		if funded {
			result = map[string]any{"content": []map[string]any{
				{"type": "text", "text": `{"ok":true}`}}}
		} else {
			pr, _ := json.Marshal(brmcp.PaymentRequired{
				Error: "payment_required", Tool: params.Name,
				PriceAtoms: paidPrice, ShortfallAtoms: paidPrice,
				AcceptedRails: []string{"tip"},
			})
			result = map[string]any{"isError": true, "content": []map[string]any{
				{"type": "text", "text": string(pr)}}}
		}
	default:
		if !req.ID.IsValid() {
			return // notification (notifications/initialized)
		}
		respErr = &jsonrpc.Error{Code: jsonrpc.CodeMethodNotFound,
			Message: "method not found"}
	}

	resp := &jsonrpc.Response{ID: req.ID, Error: respErr}
	if respErr == nil {
		raw, err := json.Marshal(result)
		if err != nil {
			lb.t.Errorf("legacy bot: marshal result: %v", err)
			return
		}
		resp.Result = raw
	}
	data, err := jsonrpc.EncodeMessage(resp)
	if err != nil {
		lb.t.Errorf("legacy bot: encode: %v", err)
		return
	}
	parts, err := wire.Encode(part.SID, data, time.Now().Add(time.Minute), 0)
	if err != nil {
		lb.t.Errorf("legacy bot: frame: %v", err)
		return
	}
	for _, pm := range parts {
		if err := lb.snd.SendPM(context.Background(), from, pm); err != nil {
			lb.t.Errorf("legacy bot: send: %v", err)
		}
	}
}

// TestLegacyBotFallback proves the bridge still completes a paid call
// against a bot that has not upgraded: the SDK client probes
// server/discover, eats the error, falls back to the legacy initialize (the
// one extra relay round trip the grace period costs), and the payment
// reissue reuses the idempotency key.
func TestLegacyBotFallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	fab := brmcptest.NewFabric()
	t.Cleanup(fab.Close)
	bot := &legacyBot{t: t, snd: fab.Sender(botUID)}
	fab.Attach(botUID, bot.handlePM)

	b, err := bridge.New(bridge.Config{
		DataDir: t.TempDir(),
		Sender:  fab.Sender(agentUID),
		Payer: bridge.PayerFunc(func(_ context.Context, payee string, atoms int64) error {
			if payee != botUID || atoms != paidPrice {
				t.Errorf("payment to %s of %d; want %s of %d", payee, atoms, botUID, paidPrice)
			}
			bot.setFunded(true)
			return nil
		}),
		Name:  "brclientd",
		Logf:  t.Logf,
		Clock: newTestClock(true),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	fab.Attach(agentUID, b.HandlePM)
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	token, err := b.ApplySettings(bridge.Settings{
		Enabled:         true,
		Mode:            "autopay",
		PerCallCapAtoms: 10_000,
		PerDayCapAtoms:  100_000,
		AllowedBots:     []string{botUID},
	})
	if err != nil {
		t.Fatal(err)
	}
	httpSrv := httptest.NewServer(b.Handler())
	t.Cleanup(httpSrv.Close)

	tr := &mcp.StreamableClientTransport{
		Endpoint:   httpSrv.URL + "/mcp/" + botUID,
		HTTPClient: &http.Client{Transport: bearerTransport{token}},
	}
	cl := mcp.NewClient(&mcp.Implementation{Name: "agent", Version: "0"}, nil)
	session, err := cl.Connect(ctx, tr, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { session.Close() })

	res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "paid", Arguments: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("paid call failed: %s", brmcptest.Text(res))
	}
	if text := brmcptest.Text(res); !strings.Contains(text, `"ok":true`) {
		t.Fatalf("unexpected result: %s", text)
	}

	bot.mu.Lock()
	defer bot.mu.Unlock()
	if len(bot.methods) < 2 || bot.methods[0] != "server/discover" || bot.methods[1] != "initialize" {
		t.Fatalf("dial sequence %v; want server/discover then the legacy initialize fallback", bot.methods)
	}
	if bot.newMeta {
		t.Fatal("post-fallback requests carried 2026-07-28 _meta; the session is not on the legacy wire")
	}
	if len(bot.callKeys) != 2 || bot.callKeys[0] != bot.callKeys[1] {
		t.Fatalf("callKeys %v; want the payment reissue to reuse one idempotency key", bot.callKeys)
	}
}
