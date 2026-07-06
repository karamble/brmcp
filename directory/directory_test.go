// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package directory_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/karamble/brmcp"
	"github.com/karamble/brmcp/brmcptest"
	"github.com/karamble/brmcp/directory"
	"github.com/karamble/brmcp/server"
)

var (
	providerUID = brmcptest.UID(1)
	dirUID      = brmcptest.UID(2)
	adminUID    = brmcptest.UID(3)
	consumerUID = brmcptest.UID(4)
	stingyUID   = brmcptest.UID(5)
)

// testClock is a manual clock: After timers fire only on Advance, Now is
// frozen between advances.
type testClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []testTimer
}

type testTimer struct {
	at time.Time
	ch chan time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Unix(1_800_000_000, 0)}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	c.timers = append(c.timers, testTimer{at: c.now.Add(d), ch: ch})
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

// testPayer routes directory payments to the payee's harness billing,
// crediting the directory's uid there like a real tip would. fn, when
// set, replaces that behavior.
type testPayer struct {
	mu    sync.Mutex
	rails map[string]*server.Harness // payee uid -> its harness
	payer string                     // uid credited on the payee side
	fn    func(ctx context.Context, payeeUID string, atoms int64) error
	calls int
}

func (p *testPayer) Pay(ctx context.Context, payeeUID string, atoms int64) error {
	p.mu.Lock()
	p.calls++
	fn := p.fn
	h, ok := p.rails[payeeUID]
	p.mu.Unlock()
	if fn != nil {
		return fn(ctx, payeeUID, atoms)
	}
	if !ok {
		return errors.New("no rail to payee")
	}
	return h.Billing().Credit(p.payer, atoms)
}

func (p *testPayer) setFn(fn func(ctx context.Context, payeeUID string, atoms int64) error) {
	p.mu.Lock()
	p.fn = fn
	p.mu.Unlock()
}

func (p *testPayer) setRail(uid string, h *server.Harness) {
	p.mu.Lock()
	p.rails[uid] = h
	p.mu.Unlock()
}

func (p *testPayer) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// suggestLog records KX suggestions the directory pushed.
type suggestLog struct {
	mu    sync.Mutex
	pairs [][2]string // {invitee, target}
}

func (l *suggestLog) record(invitee, target string) {
	l.mu.Lock()
	l.pairs = append(l.pairs, [2]string{invitee, target})
	l.mu.Unlock()
}

func (l *suggestLog) all() [][2]string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([][2]string(nil), l.pairs...)
}

// fixture wires one directory, one healthy provider, and the fabric.
type fixture struct {
	t          *testing.T
	ctx        context.Context
	fab        *brmcptest.Fabric
	clk        *testClock
	svc        *directory.Service
	payer      *testPayer
	provider   *server.Harness
	provRouter *brmcp.Router
	execs      *atomic.Int64
	introCalls *atomic.Int64
	suggests   *suggestLog
	dataDir    string
}

const (
	feeAtoms   = 1000
	paidPrice  = 500
	testBudget = 2000
)

// startDirectory builds and starts a directory Service over the fabric.
// Calling it again with the same dataDir models a bot restart.
func startDirectory(t *testing.T, ctx context.Context, f *brmcptest.Fabric,
	clk *testClock, payer *testPayer, dataDir string) *directory.Service {
	t.Helper()
	intro := directory.IntroducerFunc(func(context.Context, string, string) error {
		return errors.New("no introducer in this test")
	})
	return startDirectoryAt(t, ctx, f, clk, payer, dataDir, dirUID, intro, nil)
}

// startDirectoryAt starts a directory Service on an arbitrary uid, e.g. a
// federation peer.
func startDirectoryAt(t *testing.T, ctx context.Context, f *brmcptest.Fabric,
	clk *testClock, payer *testPayer, dataDir, selfUID string,
	intro directory.Introducer, sugg directory.Suggester) *directory.Service {
	t.Helper()
	svc, err := directory.New(directory.Config{
		DataDir: dataDir,
		Policy: directory.Policy{
			AdminUIDs:          []string{adminUID},
			ListingFeeAtoms:    feeAtoms,
			SnapshotPriceAtoms: 50,
			ExpiryDays:         30,
			CallsPerMinute:     1000,
			TestBudgetMaxAtoms: 100_000,
			Name:               "dir-test",
		},
		Payer:      payer,
		Introducer: intro,
		Suggester:  sugg,
		SelfUID:    selfUID,
		Clock:      clk,
		Logf:       t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	dirRouter := svc.Start(ctx, f.Sender(selfUID))
	f.Attach(selfUID, dirRouter.HandlePM)
	return svc
}

// newProviderHarness builds a provider bot with a free echo and a paid
// tool that counts executions. The returned router is the provider's own
// dual-role router: it serves the harness AND dials the directory, exactly
// like a production bot.
func newProviderHarness(t *testing.T, f *brmcptest.Fabric, ctx context.Context,
	uid string, execs *atomic.Int64) (*server.Harness, *brmcp.Router) {
	t.Helper()
	h, err := server.NewHarness(&mcp.Implementation{Name: "prov-" + uid[:4], Version: "0"},
		server.HarnessConfig{
			DataDir:        t.TempDir(),
			AllowFunc:      func(string) bool { return true },
			CallsPerMinute: 1000,
			Logf:           t.Logf,
		})
	if err != nil {
		t.Fatal(err)
	}
	server.AddTool(h, &mcp.Tool{Name: "echo", Description: "echoes text back"}, 0,
		func(_ context.Context, _ string, in struct {
			Text string `json:"text"`
		}) (any, error) {
			return map[string]string{"echo": in.Text}, nil
		})
	server.AddTool(h, &mcp.Tool{Name: "paid_echo", Description: "echoes, for money"}, paidPrice,
		func(_ context.Context, _ string, in struct {
			Text string `json:"text"`
		}) (any, error) {
			execs.Add(1)
			return map[string]string{"echo": in.Text}, nil
		})
	router := h.Start(ctx, f.Sender(uid))
	f.Attach(uid, router.HandlePM)
	return h, router
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)

	f := brmcptest.NewFabric()
	clk := newTestClock()
	execs := &atomic.Int64{}
	provider, provRouter := newProviderHarness(t, f, ctx, providerUID, execs)

	payer := &testPayer{rails: map[string]*server.Harness{providerUID: provider}, payer: dirUID}
	dataDir := t.TempDir()
	introCalls := &atomic.Int64{}
	intro := directory.IntroducerFunc(func(context.Context, string, string) error {
		introCalls.Add(1)
		return nil
	})
	suggests := &suggestLog{}
	sugg := directory.SuggesterFunc(func(_ context.Context, invitee, target string) error {
		suggests.record(invitee, target)
		return nil
	})
	svc := startDirectoryAt(t, ctx, f, clk, payer, dataDir, dirUID, intro, sugg)

	t.Cleanup(func() {
		cancel()
		svc.Close()
		f.Close()
	})
	return &fixture{
		t: t, ctx: ctx, fab: f, clk: clk, svc: svc, payer: payer,
		provider: provider, provRouter: provRouter, execs: execs,
		introCalls: introCalls, suggests: suggests, dataDir: dataDir,
	}
}

// dialTo opens an MCP session to target over an existing router, e.g. a
// provider bot's own dual-role router.
func (fx *fixture) dialTo(r *brmcp.Router, target, name string) *mcp.ClientSession {
	fx.t.Helper()
	conn, err := r.Dial(target)
	if err != nil {
		fx.t.Fatal(err)
	}
	cl := mcp.NewClient(&mcp.Implementation{Name: "t-" + name, Version: "0"}, nil)
	session, err := cl.Connect(fx.ctx, conn.AsTransport(), nil)
	if err != nil {
		fx.t.Fatalf("connect %s: %v", name, err)
	}
	fx.t.Cleanup(func() { session.Close() })
	return session
}

// dialFrom opens an MCP session to the fixture directory over an existing
// router.
func (fx *fixture) dialFrom(r *brmcp.Router, name string) *mcp.ClientSession {
	fx.t.Helper()
	return fx.dialTo(r, dirUID, name)
}

// dial opens an MCP session from a fresh identity (consumer, admin) to the
// directory. Identities with an attached harness must use dialFrom with
// their harness router instead: one uid, one router on the fabric.
func (fx *fixture) dial(uid string) *mcp.ClientSession {
	fx.t.Helper()
	r := fx.fab.NewRouter(uid, brmcp.RouterConfig{Logf: fx.t.Logf})
	return fx.dialFrom(r, uid[:4])
}

// call invokes a directory tool and decodes the JSON result into out.
func (fx *fixture) call(session *mcp.ClientSession, tool string, args map[string]any, out any) error {
	fx.t.Helper()
	if args == nil {
		args = map[string]any{}
	}
	res, err := session.CallTool(fx.ctx, &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		return err
	}
	if res.IsError {
		return fmt.Errorf("tool error: %s", brmcptest.Text(res))
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal([]byte(brmcptest.Text(res)), out)
}

// callDecodeResult decodes a tool result's JSON text.
func (fx *fixture) callDecodeResult(res *mcp.CallToolResult, out any) error {
	return json.Unmarshal([]byte(brmcptest.Text(res)), out)
}

// waitStatus polls my_status until the wanted state or a deadline.
func (fx *fixture) waitStatus(session *mcp.ClientSession, want string) directory.StatusOut {
	fx.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var st directory.StatusOut
	for time.Now().Before(deadline) {
		if err := fx.call(session, "my_status", nil, &st); err != nil {
			fx.t.Fatalf("my_status: %v", err)
		}
		if st.State == want {
			return st
		}
		time.Sleep(20 * time.Millisecond)
	}
	fx.t.Fatalf("state never became %s; last: %+v", want, st)
	return st
}

func TestRegistrationE2E(t *testing.T) {
	fx := newFixture(t)
	prov := fx.dialFrom(fx.provRouter, "prov")

	// Nothing on file yet.
	var st directory.StatusOut
	if err := fx.call(prov, "my_status", nil, &st); err != nil || st.State != directory.StateNone {
		t.Fatalf("initial status: %+v err=%v", st, err)
	}

	// Register: the reply is the funding invoice.
	err := fx.call(prov, "register", map[string]any{
		"description": "echo services for tests",
		"tags":        []string{"Echo", "test", "echo"},
		"test": map[string]any{
			"tool":     "paid_echo",
			"args":     map[string]any{"text": "proof"},
			"maxAtoms": testBudget,
		},
	}, &st)
	if err != nil {
		t.Fatal(err)
	}
	if st.State != directory.StateAwaitingFunding ||
		st.RequiredAtoms != feeAtoms+testBudget || st.ShortfallAtoms != feeAtoms+testBudget {
		t.Fatalf("invoice wrong: %+v", st)
	}

	// Partial funding is not enough.
	fx.svc.CreditTip(providerUID, 500)
	if err := fx.call(prov, "my_status", nil, &st); err != nil {
		t.Fatal(err)
	}
	if st.State != directory.StateAwaitingFunding || st.ShortfallAtoms != feeAtoms+testBudget-500 {
		t.Fatalf("partial funding status wrong: %+v", st)
	}

	// Full funding: crawl + live test run, then park for review.
	fx.svc.CreditTip(providerUID, feeAtoms+testBudget-500)
	st = fx.waitStatus(prov, directory.StatePendingReview)
	if st.Note != "" {
		t.Fatalf("clean test parked with note: %+v", st)
	}
	if got := fx.execs.Load(); got != 1 {
		t.Fatalf("test executed %d times, want 1", got)
	}
	if got := fx.payer.callCount(); got != 1 {
		t.Fatalf("payer invoked %d times, want 1", got)
	}
	// Fee and test spend left the provider's escrow: 3000 - 1000 - 500.
	if bal := fx.provider.Billing().Balance(dirUID); bal != 0 {
		t.Fatalf("provider-side directory balance not consumed: %d", bal)
	}

	// Not searchable before approval.
	consumer := fx.dial(consumerUID)
	var sr directory.SearchOut
	if err := fx.call(consumer, "search", map[string]any{"query": "echo"}, &sr); err != nil {
		t.Fatal(err)
	}
	if sr.Total != 0 {
		t.Fatalf("unapproved listing searchable: %+v", sr)
	}

	// The admin surface is invisible to the consumer.
	tl, err := consumer.ListTools(fx.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range tl.Tools {
		if tool.Name == "approve" || tool.Name == "pending_registrations" {
			t.Fatalf("admin tool %s visible to consumer", tool.Name)
		}
	}

	// Approve as admin.
	admin := fx.dial(adminUID)
	var pend directory.PendingListOut
	if err := fx.call(admin, "pending_registrations", nil, &pend); err != nil {
		t.Fatal(err)
	}
	if len(pend.Registrations) != 1 || pend.Registrations[0].UID != providerUID ||
		pend.Registrations[0].TestOutcome != "ok" {
		t.Fatalf("pending queue wrong: %+v", pend)
	}
	if err := fx.call(admin, "approve", map[string]any{"uid": providerUID}, &st); err != nil {
		t.Fatal(err)
	}
	if st.State != directory.StateListed {
		t.Fatalf("approve did not list: %+v", st)
	}

	// Searchable now, tool-level, with the advertised price.
	if err := fx.call(consumer, "search", map[string]any{"query": "paid"}, &sr); err != nil {
		t.Fatal(err)
	}
	if sr.Total != 1 || len(sr.Hits) != 1 {
		t.Fatalf("search after approve: %+v", sr)
	}
	hit := sr.Hits[0]
	if hit.ProviderUID != providerUID || hit.Tool != "paid_echo" || hit.PriceAtoms != paidPrice {
		t.Fatalf("hit wrong: %+v", hit)
	}
	// Tag normalization: "Echo", "test", "echo" collapse to two tags.
	if len(hit.Tags) != 2 {
		t.Fatalf("tags not normalized: %+v", hit.Tags)
	}

	// Price ceiling filters: the free echo passes, the 500-atom tool and
	// dynamic pricing do not. Decode into a fresh value - omitempty zeros
	// merged over a reused struct would fake stale prices.
	var ceil directory.SearchOut
	if err := fx.call(consumer, "search", map[string]any{"maxPriceAtoms": 100}, &ceil); err != nil {
		t.Fatal(err)
	}
	if len(ceil.Hits) != 1 || ceil.Hits[0].Tool != "echo" || ceil.Hits[0].PriceAtoms != 0 {
		t.Fatalf("ceiling result wrong: %+v", ceil)
	}

	// get_provider returns the crawled catalog verbatim.
	var po directory.ProviderOut
	if err := fx.call(consumer, "get_provider", map[string]any{"uid": providerUID}, &po); err != nil {
		t.Fatal(err)
	}
	if len(po.Catalog) == 0 || po.ExpiresAt == 0 {
		t.Fatalf("provider record incomplete: %+v", po)
	}

	// Categories histogram the normalized tags.
	var cats directory.CategoriesOut
	if err := fx.call(consumer, "list_categories", nil, &cats); err != nil {
		t.Fatal(err)
	}
	if cats.Tags["echo"] != 1 || cats.Tags["test"] != 1 {
		t.Fatalf("categories wrong: %+v", cats)
	}
}

func TestBudgetViolationParks(t *testing.T) {
	fx := newFixture(t)

	// A provider whose nominated budget cannot cover its own tool price.
	execs := &atomic.Int64{}
	stingy, stingyRouter := newProviderHarness(t, fx.fab, fx.ctx, stingyUID, execs)
	fx.payer.mu.Lock()
	fx.payer.rails[stingyUID] = stingy
	fx.payer.mu.Unlock()

	prov := fx.dialFrom(stingyRouter, "stingy")
	var st directory.StatusOut
	err := fx.call(prov, "register", map[string]any{
		"description": "underfunded",
		"test":        map[string]any{"tool": "paid_echo", "maxAtoms": 100},
	}, &st)
	if err != nil {
		t.Fatal(err)
	}
	fx.svc.CreditTip(stingyUID, feeAtoms+100)
	st = fx.waitStatus(prov, directory.StatePendingReview)
	if st.Note == "" {
		t.Fatalf("budget violation parked without a note: %+v", st)
	}
	if execs.Load() != 0 {
		t.Fatal("tool executed despite budget refusal")
	}
	if fx.payer.callCount() != 0 {
		t.Fatal("payer invoked despite budget refusal")
	}

	// Reject with a reason the provider can read.
	admin := fx.dial(adminUID)
	if err := fx.call(admin, "reject", map[string]any{"uid": stingyUID, "reason": "budget cannot cover the nominated tool"}, &st); err != nil {
		t.Fatal(err)
	}
	if err := fx.call(prov, "my_status", nil, &st); err != nil {
		t.Fatal(err)
	}
	if st.State != directory.StateRejected || st.Note == "" {
		t.Fatalf("reject not surfaced: %+v", st)
	}
}

func TestValidationRefusals(t *testing.T) {
	fx := newFixture(t)
	prov := fx.dialFrom(fx.provRouter, "prov")

	bad := []map[string]any{
		{"description": "", "test": map[string]any{"tool": "x", "maxAtoms": 10}},
		{"description": "d", "test": map[string]any{"tool": "", "maxAtoms": 10}},
		{"description": "d", "test": map[string]any{"tool": "x", "maxAtoms": 0}},
		{"description": "d", "test": map[string]any{"tool": "x", "maxAtoms": 200_000}},
	}
	for i, args := range bad {
		if err := fx.call(prov, "register", args, nil); err == nil {
			t.Fatalf("bad register %d accepted", i)
		}
	}
}
