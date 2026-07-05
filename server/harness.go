// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/karamble/brmcp"
)

// Billing is the balance store paid tools debit against. The built-in
// Ledger implements it; services with their own accounting (tip-funded
// balances etc.) plug that in via HarnessConfig.Billing instead.
type Billing interface {
	Balance(uid string) int64
	// Debit returns ErrInsufficient when the balance cannot cover atoms.
	Debit(uid string, atoms int64) error
	Credit(uid string, atoms int64) error
}

// PriceFunc computes a tool's price per call, before the handler runs. It
// sees the caller and the decoded arguments, so prices may vary by
// parameters (e.g. video seconds). Returning an error refuses the call.
type PriceFunc[In any] func(ctx context.Context, peer string, in In) (atoms int64, err error)

// HarnessConfig configures a serving harness.
type HarnessConfig struct {
	// DataDir holds the ledger and any harness state.
	DataDir string
	// AllowedPeers is the default-deny caller allowlist (64-hex uids).
	AllowedPeers []string
	// AllowFunc, when non-nil, replaces the allowlist entirely (e.g. an
	// open service that admits every KX'd caller and lets billing and
	// rate limits do the gating).
	AllowFunc func(uid string) bool
	// Billing, when non-nil, replaces the built-in ledger for balance
	// reads, debits, and credits.
	Billing Billing
	// CallsPerMinute rate-limits each caller. Zero selects 30.
	CallsPerMinute int
	// TTL/ChunkSize/Assembler tune the transport (zero = defaults).
	TTL       time.Duration
	ChunkSize int
	Logf      func(format string, args ...any)
}

// Harness carries an MCP tool server over Bison Relay PMs with default-deny
// authorization, rate limiting, and paid tools settled by Bison Relay tips.
// Operators register tools and connect a PM backend; everything else is
// plumbing they never see.
type Harness struct {
	cfg     HarnessConfig
	impl    *mcp.Implementation
	ledger  *Ledger
	billing Billing
	router  *brmcp.Router
	logf    func(format string, args ...any)

	mu      sync.Mutex
	allowed map[string]bool
	servers map[string]*mcp.Server // per peer: tool closures need the caller
	buckets map[string]*bucket
	calls   map[string]*callRecord
	tools   []toolReg
}

type toolReg struct {
	name     string
	register func(s *mcp.Server, h *Harness, peer string)
}

func NewHarness(impl *mcp.Implementation, cfg HarnessConfig) (*Harness, error) {
	if cfg.CallsPerMinute <= 0 {
		cfg.CallsPerMinute = 30
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	ledger, err := OpenLedger(filepath.Join(cfg.DataDir, "ledger.json"))
	if err != nil {
		return nil, err
	}
	h := &Harness{
		cfg:     cfg,
		impl:    impl,
		ledger:  ledger,
		billing: cfg.Billing,
		logf:    cfg.Logf,
		allowed: make(map[string]bool),
		servers: make(map[string]*mcp.Server),
		buckets: make(map[string]*bucket),
		calls:   make(map[string]*callRecord),
	}
	if h.billing == nil {
		h.billing = ledger
	}
	for _, uid := range cfg.AllowedPeers {
		h.allowed[uid] = true
	}
	return h, nil
}

// Billing exposes the balance store paid tools debit against (the bot glue
// credits tips into it).
func (h *Harness) Billing() Billing { return h.billing }

// Allowed reports whether peer may open sessions.
func (h *Harness) Allowed(peer string) bool {
	if h.cfg.AllowFunc != nil {
		return h.cfg.AllowFunc(peer)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.allowed[peer]
}

// Start wires the harness to a PM sender and returns the router whose
// HandlePM the backend must feed with every inbound private message.
func (h *Harness) Start(ctx context.Context, sender brmcp.PMSender) *brmcp.Router {
	h.router = brmcp.NewRouter(brmcp.RouterConfig{
		Sender:    sender,
		Allow:     h.Allowed,
		TTL:       h.cfg.TTL,
		ChunkSize: h.cfg.ChunkSize,
		InboxSize: 0,
		Logf:      h.logf,
		Accept: func(conn *brmcp.Conn) {
			srv := h.serverFor(conn.Peer())
			if _, err := srv.Connect(ctx, conn.AsTransport(), nil); err != nil {
				h.logf("brmcp: session %s: %v", conn.SessionID(), err)
				conn.Close()
			}
		},
	})
	go func() {
		<-ctx.Done()
		h.router.Close()
	}()
	return h.router
}

// serverFor lazily builds the per-caller MCP server. Tool handlers close
// over the caller uid, which is how paid calls debit the right balance.
func (h *Harness) serverFor(peer string) *mcp.Server {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s := h.servers[peer]; s != nil {
		return s
	}
	s := mcp.NewServer(h.impl, nil)
	for _, reg := range h.tools {
		reg.register(s, h, peer)
	}
	h.servers[peer] = s
	return s
}

// AddTool registers a tool with a fixed price. priceAtoms of zero makes it
// free; a positive price is advertised in _meta and enforced against the
// caller's balance before the handler runs.
func AddTool[In any](h *Harness, tool *mcp.Tool, priceAtoms int64,
	fn func(ctx context.Context, peer string, in In) (any, error)) {

	if priceAtoms > 0 {
		if tool.Meta == nil {
			tool.Meta = mcp.Meta{}
		}
		tool.Meta[brmcp.PriceMetaKey] = priceAtoms
	}
	AddToolPriced(h, tool, func(context.Context, string, In) (int64, error) {
		return priceAtoms, nil
	}, fn)
}

// callRecord tracks one keyed call from claim to completion so duplicates
// wait for and share the original's outcome.
type callRecord struct {
	done    chan struct{}
	result  *mcp.CallToolResult
	out     any
	err     error
	expires time.Time
}

// callKeyTTL bounds how long a completed outcome stays replayable; a
// duplicate arriving later re-executes (and re-charges) like a fresh call.
const callKeyTTL = 30 * time.Minute

// claimCall registers key and reports whether it was already claimed. The
// caller that gets dup=false owns execution and must finish the record via
// completeCall or abandon it via releaseCall.
func (h *Harness) claimCall(key string) (*callRecord, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now()
	for k, c := range h.calls {
		if !c.expires.IsZero() && now.After(c.expires) {
			delete(h.calls, k)
		}
	}
	if c, ok := h.calls[key]; ok {
		return c, true
	}
	c := &callRecord{done: make(chan struct{})}
	h.calls[key] = c
	return c, false
}

// releaseCall abandons a claim without recording an outcome (pre-execution
// refusals like payment_required, which a later retry must re-run).
func (h *Harness) releaseCall(key string, c *callRecord) {
	h.mu.Lock()
	delete(h.calls, key)
	h.mu.Unlock()
	close(c.done)
}

func (h *Harness) completeCall(c *callRecord, result *mcp.CallToolResult, out any, err error) {
	h.mu.Lock()
	c.result, c.out, c.err = result, out, err
	c.expires = time.Now().Add(callKeyTTL)
	h.mu.Unlock()
	close(c.done)
}

// AddToolPriced registers a tool whose price is computed per call by price
// (e.g. per-second video). Unless a fixed price is already advertised, the
// tool is marked dynamic in _meta.
func AddToolPriced[In any](h *Harness, tool *mcp.Tool, price PriceFunc[In],
	fn func(ctx context.Context, peer string, in In) (any, error)) {

	if tool.Meta == nil {
		tool.Meta = mcp.Meta{}
	}
	if _, fixed := tool.Meta[brmcp.PriceMetaKey]; !fixed {
		if _, marked := tool.Meta[brmcp.PricingMetaKey]; !marked {
			tool.Meta[brmcp.PricingMetaKey] = "dynamic"
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tools = append(h.tools, toolReg{
		name: tool.Name,
		register: func(s *mcp.Server, h *Harness, peer string) {
			mcp.AddTool(s, tool, func(ctx context.Context, req *mcp.CallToolRequest, in In) (*mcp.CallToolResult, any, error) {
				// Idempotency: a retried call carrying the same caller key
				// waits for and shares the original execution's outcome
				// instead of executing (and charging) again. Keys are
				// scoped per peer so callers cannot touch each other's.
				var key string
				var rec *callRecord
				if req != nil && req.Params != nil {
					if v, ok := req.Params.Meta[brmcp.CallKeyMetaKey].(string); ok && len(v) >= 8 && len(v) <= 128 {
						key = peer + "|" + v
					}
				}
				if key != "" {
					var dup bool
					rec, dup = h.claimCall(key)
					if dup {
						select {
						case <-rec.done:
							return rec.result, rec.out, rec.err
						case <-ctx.Done():
							return nil, nil, ctx.Err()
						}
					}
				}
				finish := func(result *mcp.CallToolResult, out any, err error) (*mcp.CallToolResult, any, error) {
					if rec != nil {
						h.completeCall(rec, result, out, err)
					}
					return result, out, err
				}
				refuse := func(result *mcp.CallToolResult, err error) (*mcp.CallToolResult, any, error) {
					// Pre-execution refusals are not recorded: after a
					// top-up (or a rate-limit pause) the same key must run
					// the call for real.
					if rec != nil {
						h.releaseCall(key, rec)
					}
					return result, nil, err
				}
				if !h.takeToken(peer) {
					return refuse(nil, fmt.Errorf("rate limited; retry later"))
				}
				priceAtoms, err := price(ctx, peer, in)
				if err != nil {
					return refuse(nil, err)
				}
				if priceAtoms > 0 {
					if pr := h.charge(ctx, tool.Name, peer, priceAtoms); pr != nil {
						return refuse(paymentRequiredResult(pr), nil)
					}
					ctx = context.WithValue(ctx, chargedAtomsKey{}, priceAtoms)
				}
				out, err := fn(ctx, peer, in)
				if err != nil {
					// The caller should not pay for the operator's failure.
					if priceAtoms > 0 {
						if cerr := h.billing.Credit(peer, priceAtoms); cerr != nil {
							h.logf("brmcp: refund %d to %s failed: %v", priceAtoms, peer, cerr)
						}
					}
					return finish(nil, nil, err)
				}
				return finish(nil, out, nil)
			})
		},
	})
}

type chargedAtomsKey struct{}

// ChargedAtoms reports the amount debited from the caller for the current
// tool call, so handlers can echo the actual charge in their own delivery
// channel. Zero for free tools.
func ChargedAtoms(ctx context.Context) int64 {
	atoms, _ := ctx.Value(chargedAtomsKey{}).(int64)
	return atoms
}

// charge debits the call price, or reports what payment is missing.
func (h *Harness) charge(_ context.Context, tool, peer string, price int64) *brmcp.PaymentRequired {
	err := h.billing.Debit(peer, price)
	if err == nil {
		return nil
	}
	balance := h.billing.Balance(peer)
	return &brmcp.PaymentRequired{
		Error:          "payment_required",
		Tool:           tool,
		PriceAtoms:     price,
		BalanceAtoms:   balance,
		ShortfallAtoms: price - balance,
		AcceptedRails:  []string{"tip"},
	}
}

func paymentRequiredResult(pr *brmcp.PaymentRequired) *mcp.CallToolResult {
	raw, err := json.Marshal(pr)
	if err != nil {
		raw = []byte(`{"error":"payment_required"}`)
	}
	return &mcp.CallToolResult{
		IsError: true,
		Content: []mcp.Content{&mcp.TextContent{Text: string(raw)}},
	}
}

// bucket is a minimal token bucket: CallsPerMinute tokens, refilled
// continuously, without external dependencies.
type bucket struct {
	tokens float64
	last   time.Time
}

func (h *Harness) takeToken(peer string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now()
	b := h.buckets[peer]
	if b == nil {
		b = &bucket{tokens: float64(h.cfg.CallsPerMinute), last: now}
		h.buckets[peer] = b
	}
	rate := float64(h.cfg.CallsPerMinute) / 60.0
	b.tokens += now.Sub(b.last).Seconds() * rate
	if max := float64(h.cfg.CallsPerMinute); b.tokens > max {
		b.tokens = max
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
