// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package brmcp

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// PriceMetaKey is the tool _meta key advertising the per-call price in
// atoms, visible to clients in tools/list.
const PriceMetaKey = "brmcp/priceAtoms"

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
	router  *Router
	logf    func(format string, args ...any)

	mu      sync.Mutex
	allowed map[string]bool
	servers map[string]*mcp.Server // per peer: tool closures need the caller
	buckets map[string]*bucket
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
func (h *Harness) Start(ctx context.Context, sender PMSender) *Router {
	h.router = NewRouter(RouterConfig{
		Sender:    sender,
		Allow:     h.Allowed,
		TTL:       h.cfg.TTL,
		ChunkSize: h.cfg.ChunkSize,
		InboxSize: 0,
		Logf:      h.logf,
		Accept: func(conn *Conn) {
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
		tool.Meta[PriceMetaKey] = priceAtoms
	}
	AddToolPriced(h, tool, func(context.Context, string, In) (int64, error) {
		return priceAtoms, nil
	}, fn)
}

// PricingMetaKey marks tools whose price is computed per call from the
// arguments; the authoritative quote arrives in the payment_required error.
const PricingMetaKey = "brmcp/pricing"

// AddToolPriced registers a tool whose price is computed per call by price
// (e.g. per-second video). Unless a fixed price is already advertised, the
// tool is marked dynamic in _meta.
func AddToolPriced[In any](h *Harness, tool *mcp.Tool, price PriceFunc[In],
	fn func(ctx context.Context, peer string, in In) (any, error)) {

	if tool.Meta == nil {
		tool.Meta = mcp.Meta{}
	}
	if _, fixed := tool.Meta[PriceMetaKey]; !fixed {
		if _, marked := tool.Meta[PricingMetaKey]; !marked {
			tool.Meta[PricingMetaKey] = "dynamic"
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tools = append(h.tools, toolReg{
		name: tool.Name,
		register: func(s *mcp.Server, h *Harness, peer string) {
			mcp.AddTool(s, tool, func(ctx context.Context, _ *mcp.CallToolRequest, in In) (*mcp.CallToolResult, any, error) {
				if !h.takeToken(peer) {
					return nil, nil, fmt.Errorf("rate limited; retry later")
				}
				priceAtoms, err := price(ctx, peer, in)
				if err != nil {
					return nil, nil, err
				}
				if priceAtoms > 0 {
					if pr := h.charge(ctx, tool.Name, peer, priceAtoms); pr != nil {
						return paymentRequiredResult(pr), nil, nil
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
					return nil, nil, err
				}
				return nil, out, nil
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
func (h *Harness) charge(_ context.Context, tool, peer string, price int64) *PaymentRequired {
	err := h.billing.Debit(peer, price)
	if err == nil {
		return nil
	}
	balance := h.billing.Balance(peer)
	return &PaymentRequired{
		Error:          "payment_required",
		Tool:           tool,
		PriceAtoms:     price,
		BalanceAtoms:   balance,
		ShortfallAtoms: price - balance,
		AcceptedRails:  []string{"tip"},
	}
}

func paymentRequiredResult(pr *PaymentRequired) *mcp.CallToolResult {
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
