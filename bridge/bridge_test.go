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

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/karamble/brmcp"
	"github.com/karamble/brmcp/bridge"
	"github.com/karamble/brmcp/brmcptest"
	"github.com/karamble/brmcp/server"
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
		Token:           "test-token-0123456789abcdef",
		Mode:            "autopay",
		PerCallCapAtoms: 10_000,
		PerDayCapAtoms:  100_000,
		AllowedBots:     []string{botUID},
	}
	if o.settings != nil {
		s = *o.settings
	}
	if err := b.ApplySettings(s); err != nil {
		t.Fatal(err)
	}
	fx.token = b.Settings().Token

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
	if today != paidPrice {
		t.Fatalf("todayAtoms: %d != %d", today, paidPrice)
	}
	// The spend log survives a reload.
	b2, err := bridge.New(bridge.Config{
		DataDir: fx.dataDir, Sender: fx.sender, Payer: fx.payer, Clock: fx.clk,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	entries2, _ := b2.SpendLog()
	if len(entries2) != 1 {
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
				Token:           "test-token-0123456789abcdef",
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
			if entries, _ := fx.bridge.SpendLog(); len(entries) != 0 {
				t.Fatalf("refused call recorded spend: %+v", entries)
			}
		})
	}
}

func TestDailyCapWindow(t *testing.T) {
	clk := newTestClock(true)
	fx := newFixture(t, fixtureOpts{clk: clk, settings: &bridge.Settings{
		Enabled:         true,
		Token:           "test-token-0123456789abcdef",
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
		Token:           "test-token-0123456789abcdef",
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
	// Only the approved call executed and only it recorded spend.
	if got := fx.execs.Load(); got != 1 {
		t.Fatalf("executions: %d != 1", got)
	}
	if entries, _ := fx.bridge.SpendLog(); len(entries) != 1 {
		t.Fatalf("spend entries: %+v", entries)
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
			Token:           "test-token-0123456789abcdef",
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
