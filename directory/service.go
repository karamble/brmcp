// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package directory

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/karamble/brmcp"
	"github.com/karamble/brmcp/server"
)

// Payer settles one payment of atoms to a Bison Relay peer (64-hex uid),
// blocking until the payment reaches a terminal state or ctx ends. Return
// nil only when settlement is confirmed.
type Payer interface {
	Pay(ctx context.Context, payeeUID string, atoms int64) error
}

// PayerFunc adapts a function to Payer.
type PayerFunc func(ctx context.Context, payeeUID string, atoms int64) error

// Pay implements Payer.
func (f PayerFunc) Pay(ctx context.Context, payeeUID string, atoms int64) error {
	return f(ctx, payeeUID, atoms)
}

// Introducer requests a transitive KX with target through mediator, so the
// directory can reach federation leads it is not yet KX'd with. Completion
// arrives asynchronously via Service.NotifyKX.
type Introducer interface {
	Introduce(ctx context.Context, mediatorUID, targetUID string) error
}

// IntroducerFunc adapts a function to Introducer.
type IntroducerFunc func(ctx context.Context, mediatorUID, targetUID string) error

// Introduce implements Introducer.
func (f IntroducerFunc) Introduce(ctx context.Context, mediatorUID, targetUID string) error {
	return f(ctx, mediatorUID, targetUID)
}

// Clock abstracts time so expiry, renewal, and payment polls are testable.
// A nil Config.Clock selects the system clock.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time                         { return time.Now() }
func (systemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Policy is the directory's advertised terms, loaded from the operator's
// policy file. Zero values select the documented defaults.
type Policy struct {
	AdminUIDs          []string `json:"admin_uids"`
	ListingFeeAtoms    int64    `json:"listing_fee_atoms"`
	SnapshotPriceAtoms int64    `json:"snapshot_price_atoms"`
	SearchPriceAtoms   int64    `json:"search_price_atoms"`
	ExpiryDays         int      `json:"expiry_days"`
	CallsPerMinute     int      `json:"calls_per_minute"`
	TestBudgetMaxAtoms int64    `json:"test_budget_max_atoms"`
	RecrawlHours       int      `json:"recrawl_hours"`
	Name               string   `json:"name"`
}

func (p Policy) withDefaults() Policy {
	if p.ListingFeeAtoms <= 0 {
		p.ListingFeeAtoms = 100_000
	}
	if p.SnapshotPriceAtoms <= 0 {
		p.SnapshotPriceAtoms = 10_000
	}
	if p.ExpiryDays <= 0 {
		p.ExpiryDays = 30
	}
	if p.TestBudgetMaxAtoms <= 0 {
		p.TestBudgetMaxAtoms = 10_000_000
	}
	if p.RecrawlHours <= 0 {
		p.RecrawlHours = 24
	}
	if p.Name == "" {
		p.Name = "brmcpdir"
	}
	return p
}

// Config wires a Service to its host bot. DataDir and Payer are required;
// Introducer is required only for federation lead pursuit.
type Config struct {
	// DataDir holds the index, peers, leads, journals, the snapshot
	// signing key, and the harness's ledger and call journal.
	DataDir string
	// Policy carries the operator's terms.
	Policy Policy
	// Payer settles the directory's outbound payments: provider test
	// calls and peer snapshot purchases.
	Payer Payer
	// Introducer requests transitive KX toward federation leads. May be
	// nil; pursuing a lead that needs an introduction then fails.
	Introducer Introducer
	// SelfUID is the directory's own Bison Relay uid, excluded from
	// federation leads.
	SelfUID string
	// Clock overrides the system clock (tests).
	Clock Clock
	// Logf, when non-nil, receives diagnostic lines.
	Logf func(format string, args ...any)
	// TTL and ChunkSize tune the underlying router (zero = defaults).
	TTL       time.Duration
	ChunkSize int
}

// Service is the directory engine: a brmcp harness serving the public and
// admin tools, plus the client side that crawls and live-tests providers.
type Service struct {
	cfg     Config
	policy  Policy
	clk     Clock
	logf    func(format string, args ...any)
	harness *server.Harness
	signer  *snapshotSigner
	index   *jsonStore[Entry]
	peers   *jsonStore[Peer]
	leads   *jsonStore[Lead]
	spend   *spendJournal
	alog    *adminLog

	mu         sync.Mutex
	ctx        context.Context
	cancel     context.CancelFunc
	router     *brmcp.Router
	selfUID    string
	inflight   map[string]bool
	adminTools map[string]bool
	admins     map[string]bool
	closed     bool
	wg         sync.WaitGroup
}

// New validates cfg, loads the persisted state, and builds the harness and
// its tools. It opens no sessions and sends nothing; call Start.
func New(cfg Config) (*Service, error) {
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("brmcpdir: Config.DataDir is required")
	}
	if cfg.Payer == nil {
		return nil, fmt.Errorf("brmcpdir: Config.Payer is required")
	}
	s := &Service{
		cfg:        cfg,
		policy:     cfg.Policy.withDefaults(),
		clk:        cfg.Clock,
		logf:       cfg.Logf,
		ctx:        context.Background(),
		cancel:     func() {},
		selfUID:    cfg.SelfUID,
		inflight:   make(map[string]bool),
		adminTools: make(map[string]bool),
		admins:     make(map[string]bool),
	}
	if s.clk == nil {
		s.clk = systemClock{}
	}
	if s.logf == nil {
		s.logf = func(string, ...any) {}
	}
	for _, uid := range s.policy.AdminUIDs {
		s.admins[uid] = true
	}
	var err error
	if s.signer, err = loadOrCreateSigner(filepath.Join(cfg.DataDir, "snapshotkey.json")); err != nil {
		return nil, err
	}
	if s.index, err = openJSONStore[Entry](filepath.Join(cfg.DataDir, "index.json")); err != nil {
		return nil, err
	}
	if s.peers, err = openJSONStore[Peer](filepath.Join(cfg.DataDir, "peers.json")); err != nil {
		return nil, err
	}
	if s.leads, err = openJSONStore[Lead](filepath.Join(cfg.DataDir, "leads.json")); err != nil {
		return nil, err
	}
	if s.spend, err = openSpendJournal(filepath.Join(cfg.DataDir, "spend.json")); err != nil {
		return nil, err
	}
	if s.alog, err = openAdminLog(filepath.Join(cfg.DataDir, "adminlog.json")); err != nil {
		return nil, err
	}
	s.harness, err = server.NewHarness(&mcp.Implementation{Name: s.policy.Name, Version: "0.1.0"},
		server.HarnessConfig{
			DataDir:        cfg.DataDir,
			AllowFunc:      func(string) bool { return true },
			CallsPerMinute: s.policy.CallsPerMinute,
			ToolVisible:    s.toolVisible,
			TTL:            cfg.TTL,
			ChunkSize:      cfg.ChunkSize,
			Logf:           s.logf,
		})
	if err != nil {
		return nil, err
	}
	s.registerPublicTools()
	s.registerAdminTools()
	return s, nil
}

// Start wires the service to a PM sender and returns the router whose
// HandlePM the host must feed with every inbound envelope. In-flight
// registrations resume and the maintenance sweeper starts. Cancelling ctx
// closes the service.
func (s *Service) Start(ctx context.Context, sender brmcp.PMSender) *brmcp.Router {
	sctx, cancel := context.WithCancel(ctx)
	router := s.harness.Start(sctx, sender)
	s.mu.Lock()
	s.ctx = sctx
	s.cancel = cancel
	s.router = router
	s.mu.Unlock()

	for uid, e := range s.index.all() {
		if e.Reg == nil {
			continue
		}
		switch e.Reg.State {
		case RegAwaitingFunding:
			s.pokeFunding(uid)
		case RegCrawling, RegTesting:
			s.spawnPipeline(uid)
		}
	}
	s.wg.Add(1)
	go s.runMaintenance(sctx)
	go func() {
		<-sctx.Done()
		s.Close()
	}()
	return router
}

// Close stops the maintenance sweeper and waits for in-flight pipelines.
// Idempotent.
func (s *Service) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	cancel := s.cancel
	s.mu.Unlock()
	cancel()
	s.wg.Wait()
	return nil
}

// CreditTip credits a received tip (atoms) to the sender's balance and, if
// a registration awaits funding, moves it forward. The host must dedup
// redelivered tips before calling (server.TipJournal).
func (s *Service) CreditTip(fromUID string, atoms int64) {
	if atoms <= 0 {
		return
	}
	if err := s.harness.Billing().Credit(fromUID, atoms); err != nil {
		s.logf("brmcpdir: credit %d to %s: %v", atoms, fromUID, err)
		return
	}
	s.pokeFunding(fromUID)
}

// NotifyKX reports a completed key exchange; a lead pursuit waiting on the
// introduction proceeds to the invite.
func (s *Service) NotifyKX(uid string) {
	s.resumeLeadAfterKX(uid)
}

// PublicKey returns the hex ed25519 key snapshots are signed with.
func (s *Service) PublicKey() string {
	return s.signer.sign(nil).Pub
}

// SetSelfUID records the directory's own Bison Relay uid once the host
// learns it (the identity is only readable from a connected client).
// Config.SelfUID seeds the initial value.
func (s *Service) SetSelfUID(uid string) {
	s.mu.Lock()
	s.selfUID = uid
	s.mu.Unlock()
}

func (s *Service) getSelfUID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.selfUID
}

// Harness exposes the underlying serving harness (host introspection).
func (s *Service) Harness() *server.Harness { return s.harness }

func (s *Service) isAdmin(uid string) bool { return s.admins[uid] }

// toolVisible hides admin tools from non-admin callers entirely.
func (s *Service) toolVisible(peer, tool string) bool {
	return !s.adminTools[tool] || s.isAdmin(peer)
}

func (s *Service) baseCtx() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ctx
}

func (s *Service) routerHandle() *brmcp.Router {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.router
}

// spawnPipeline runs the registration pipeline for uid unless one is
// already in flight or the service is closed.
func (s *Service) spawnPipeline(uid string) {
	s.mu.Lock()
	if s.closed || s.inflight[uid] || s.router == nil {
		s.mu.Unlock()
		return
	}
	s.inflight[uid] = true
	s.wg.Add(1)
	s.mu.Unlock()
	go func() {
		defer func() {
			s.mu.Lock()
			delete(s.inflight, uid)
			s.mu.Unlock()
			s.wg.Done()
		}()
		s.runPipeline(uid)
	}()
}

// runMaintenance sweeps periodically: expired listings are removed, stale
// catalogs re-crawled for free, and stalled funding re-checked.
func (s *Service) runMaintenance(ctx context.Context) {
	defer s.wg.Done()
	const sweepInterval = time.Minute
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.clk.After(sweepInterval):
		}
		s.sweep(ctx)
	}
}
