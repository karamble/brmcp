// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bridge

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/karamble/brmcp"
	"github.com/karamble/brmcp/wire"
)

// Payer settles one payment of atoms to a Bison Relay peer (64-hex uid),
// blocking until the payment reaches a terminal state or ctx ends. The
// bridge derives ctx with the settings' payment-wait deadline. Errors are
// appended verbatim to the tool result as the reason payment was not made,
// so implementations author the rail-specific wording (e.g. that a Bison
// Relay tip attempt keeps running in the background and still credits the
// payee after a deadline). Return nil only when settlement is confirmed.
type Payer interface {
	Pay(ctx context.Context, payeeUID string, atoms int64) error
}

// PayerFunc adapts a function to Payer.
type PayerFunc func(ctx context.Context, payeeUID string, atoms int64) error

// Pay implements Payer.
func (f PayerFunc) Pay(ctx context.Context, payeeUID string, atoms int64) error {
	return f(ctx, payeeUID, atoms)
}

// Clock abstracts time so cap windows, approval timeouts, and the
// post-payment poll are testable. A nil Config.Clock selects the system
// clock.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time                         { return time.Now() }
func (systemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Config wires a Bridge to its host. DataDir, Sender, and Payer are
// required.
type Config struct {
	// DataDir holds mcpclient.json (settings) and mcpspend.json (the spend
	// log).
	DataDir string
	// Sender delivers outgoing envelope parts; the host resolves the peer
	// uid to a private message.
	Sender brmcp.PMSender
	// Payer settles payment_required refusals.
	Payer Payer
	// ListenAddr, when non-empty, makes the bridge own a TCP listener that
	// binds only while the settings enable the bridge. Empty means the host
	// mounts Handler() itself and owns the listener lifecycle. The bridge
	// never chooses a bind address on its own.
	ListenAddr string
	// Name brands the MCP client identity and the refusal-note prefix.
	// Empty selects "brmcp-bridge".
	Name string
	// Logf, when non-nil, receives diagnostic lines.
	Logf func(format string, args ...any)
	// Clock overrides the system clock (tests).
	Clock Clock
	// TTL, ChunkSize, Assembler tune the underlying router (zero selects
	// the brmcp defaults).
	TTL       time.Duration
	ChunkSize int
	Assembler wire.AssemblerConfig
}

// Bridge is the client-side brmcp engine: it mirrors allowed bots' tools on
// local streamable-HTTP MCP endpoints (/mcp/<bot-uid>, bearer gated,
// disabled by default), relays calls over Bison Relay, and settles
// payment_required refusals under the user's spending policy.
type Bridge struct {
	cfg    Config
	clk    Clock
	logf   func(format string, args ...any)
	router *brmcp.Router

	mu       sync.Mutex
	ctx      context.Context // base context for bot sessions, set by Start
	settings Settings
	bots     map[string]*botLink
	pending  map[string]*pendingPayment
	spend    []SpendEntry
	spendSeq int64
	httpSrv  *http.Server
	lnAddr   net.Addr
	closed   bool
}

// New validates cfg, loads the persisted settings and spend state, and
// builds the router. It opens no sockets and sends nothing; call Start.
func New(cfg Config) (*Bridge, error) {
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("brmcp bridge: Config.DataDir is required")
	}
	if cfg.Sender == nil {
		return nil, fmt.Errorf("brmcp bridge: Config.Sender is required")
	}
	if cfg.Payer == nil {
		return nil, fmt.Errorf("brmcp bridge: Config.Payer is required")
	}
	if cfg.Name == "" {
		cfg.Name = "brmcp-bridge"
	}
	b := &Bridge{
		cfg:     cfg,
		clk:     cfg.Clock,
		logf:    cfg.Logf,
		ctx:     context.Background(),
		bots:    make(map[string]*botLink),
		pending: make(map[string]*pendingPayment),
	}
	if b.clk == nil {
		b.clk = systemClock{}
	}
	if b.logf == nil {
		b.logf = func(string, ...any) {}
	}
	if err := b.loadState(); err != nil {
		return nil, err
	}
	b.router = brmcp.NewRouter(brmcp.RouterConfig{
		Sender:    cfg.Sender,
		Allow:     b.botAllowed,
		TTL:       cfg.TTL,
		ChunkSize: cfg.ChunkSize,
		Assembler: cfg.Assembler,
		Logf:      b.logf,
	})
	return b, nil
}

// Start activates the bridge: ctx becomes the base context for bot
// sessions, and the owned listener binds when ListenAddr is set and the
// settings enable the bridge. A bind failure is returned and the bridge
// stays usable; a later ApplySettings retries the bind. Cancelling ctx
// closes the bridge.
func (b *Bridge) Start(ctx context.Context) error {
	b.mu.Lock()
	b.ctx = ctx
	var err error
	if b.settings.Enabled && b.cfg.ListenAddr != "" && !b.closed {
		err = b.startListenerLocked()
	}
	b.mu.Unlock()
	go func() {
		<-ctx.Done()
		b.Close()
	}()
	return err
}

// Close stops the listener, closes every bot session and the router.
// Idempotent.
func (b *Bridge) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.stopListenerLocked()
	links := make([]*botLink, 0, len(b.bots))
	for _, l := range b.bots {
		links = append(links, l)
	}
	b.bots = make(map[string]*botLink)
	b.mu.Unlock()
	for _, l := range links {
		l.reset()
	}
	b.router.Close()
	return nil
}

// HandlePM feeds one inbound private message from peer (64-hex uid).
// Non-envelope text is ignored, so the host can feed every PM it receives.
func (b *Bridge) HandlePM(peerUID, text string) {
	b.router.HandlePM(peerUID, text)
}

// baseCtx returns the context bot sessions live on.
func (b *Bridge) baseCtx() context.Context {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ctx
}
