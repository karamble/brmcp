// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package directory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/karamble/brmcp"
	"github.com/karamble/brmcp/server"
)

// AutoFund is a provider's policy for paying directory listing costs
// without an operator in the loop. Disabled by default: credit requests
// then only reach the Notify callback.
type AutoFund struct {
	Enabled bool `json:"enabled"`
	// MaxAtomsPerRequest caps one registration's fee plus test budget.
	MaxAtomsPerRequest int64 `json:"max_atoms_per_request"`
	// MaxAtomsPerMonth caps the rolling 30-day total across directories.
	MaxAtomsPerMonth int64 `json:"max_atoms_per_month"`
	// AllowedDirectoryUIDs, when non-empty, restricts auto-funding to
	// these directories; invites from others are declined.
	AllowedDirectoryUIDs []string `json:"allowed_directory_uids"`
}

// RegistrantConfig wires a provider-side Registrant to its host bot.
// Router, Payer, DataDir, Description, and Test are required.
type RegistrantConfig struct {
	// Description, Tags, and Test form the registration this provider
	// submits to directories.
	Description string
	Tags        []string
	Test        TestSpec
	// AutoFund gates unattended funding of listings.
	AutoFund AutoFund
	// DataDir persists the fund history the monthly cap counts.
	DataDir string
	// Router is the host's brmcp router; registrations dial out over it.
	Router *brmcp.Router
	// Payer settles funding tips toward directories.
	Payer Payer
	// Clock overrides the system clock (tests).
	Clock Clock
	// Notify, when non-nil, receives every registration outcome or
	// refusal; nil falls back to Logf.
	Notify func(directoryUID string, st StatusOut, err error)
	// Name brands the MCP client identity. Empty selects "registrant".
	Name string
	Logf func(format string, args ...any)
}

// Registrant is the provider-side directory client: it registers the host
// bot at directories, funds listings under the AutoFund policy, and serves
// the listing_invite tool federation credit requests arrive on.
type Registrant struct {
	cfg  RegistrantConfig
	clk  Clock
	logf func(format string, args ...any)
	fund *fundHistory

	mu  sync.Mutex
	ctx context.Context
}

// NewRegistrant validates cfg and loads the fund history.
func NewRegistrant(cfg RegistrantConfig) (*Registrant, error) {
	if cfg.Router == nil {
		return nil, errors.New("registrant: Config.Router is required")
	}
	if cfg.Payer == nil {
		return nil, errors.New("registrant: Config.Payer is required")
	}
	if cfg.DataDir == "" {
		return nil, errors.New("registrant: Config.DataDir is required")
	}
	if err := validateRegister(RegisterIn{
		Description: cfg.Description, Tags: cfg.Tags, Test: cfg.Test,
	}, cfg.Test.MaxAtoms); err != nil {
		return nil, fmt.Errorf("registrant: %w", err)
	}
	if cfg.Name == "" {
		cfg.Name = "registrant"
	}
	r := &Registrant{cfg: cfg, clk: cfg.Clock, logf: cfg.Logf, ctx: context.Background()}
	if r.clk == nil {
		r.clk = systemClock{}
	}
	if r.logf == nil {
		r.logf = func(string, ...any) {}
	}
	var err error
	if r.fund, err = openFundHistory(filepath.Join(cfg.DataDir, "regfund.json")); err != nil {
		return nil, err
	}
	return r, nil
}

// Start sets the base context asynchronous registrations (accepted
// invites) run on. Optional; the zero value is context.Background.
func (r *Registrant) Start(ctx context.Context) {
	r.mu.Lock()
	r.ctx = ctx
	r.mu.Unlock()
}

func (r *Registrant) baseCtx() context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ctx
}

func (r *Registrant) notify(directoryUID string, st StatusOut, err error) {
	if r.cfg.Notify != nil {
		r.cfg.Notify(directoryUID, st, err)
		return
	}
	r.logf("registrant: directory %s: state=%s err=%v", directoryUID[:8], st.State, err)
}

// RegisterTools contributes the listing_invite tool to the host's harness
// so directories can send federation credit requests over plain MCP.
func (r *Registrant) RegisterTools(h *server.Harness) {
	server.AddTool(h, &mcp.Tool{
		Name: "listing_invite",
		Description: "Invite this provider to register at the calling " +
			"directory. Acceptance is decided by the operator's auto-fund " +
			"policy; the registration itself happens asynchronously.",
	}, 0, func(_ context.Context, peer string, in InviteIn) (any, error) {
		estimate := in.ListingFeeAtoms + r.cfg.Test.MaxAtoms
		if ok, reason := r.allowFund(peer, estimate); !ok {
			r.notify(peer, StatusOut{Note: "invite declined: " + reason}, nil)
			return InviteOut{Accepted: false, Note: reason}, nil
		}
		go func() {
			if err := r.Register(r.baseCtx(), peer); err != nil {
				r.logf("registrant: invited registration at %s: %v", peer[:8], err)
			}
		}()
		return InviteOut{Accepted: true}, nil
	})
}

// Register submits this provider's registration to a directory, funds the
// invoice under the AutoFund policy, and reports the follow-up status via
// Notify. It does not wait for verification to finish; poll my_status (or
// call Register again) for progress.
func (r *Registrant) Register(ctx context.Context, directoryUID string) error {
	session, cleanup, err := dialPeer(ctx, r.cfg.Router, r.cfg.Name, directoryUID)
	if err != nil {
		r.notify(directoryUID, StatusOut{}, err)
		return err
	}
	defer cleanup()

	var st StatusOut
	err = callDecode(ctx, session, "register", RegisterIn{
		Description: r.cfg.Description,
		Tags:        r.cfg.Tags,
		Test:        r.cfg.Test,
	}, &st)
	if err != nil {
		r.notify(directoryUID, StatusOut{}, err)
		return err
	}
	if st.ShortfallAtoms > 0 {
		if ok, reason := r.allowFund(directoryUID, st.ShortfallAtoms); !ok {
			err := fmt.Errorf("funding declined: %s", reason)
			r.notify(directoryUID, st, err)
			return err
		}
		// The history entry lands before the money moves, so a crash
		// can only over-count against the cap, never under-count.
		if err := r.fund.record(fundEntry{
			TS: r.clk.Now().Unix(), DirectoryUID: directoryUID, Atoms: st.ShortfallAtoms,
		}); err != nil {
			r.notify(directoryUID, st, err)
			return err
		}
		payCtx, cancel := context.WithTimeout(ctx, payWait)
		err = r.cfg.Payer.Pay(payCtx, directoryUID, st.ShortfallAtoms)
		cancel()
		if err != nil {
			err = fmt.Errorf("funding payment: %w", err)
			r.notify(directoryUID, st, err)
			return err
		}
		// One follow-up read; verification continues directory-side.
		if err := callDecode(ctx, session, "my_status", struct{}{}, &st); err != nil {
			r.notify(directoryUID, StatusOut{}, err)
			return err
		}
	}
	r.notify(directoryUID, st, nil)
	return nil
}

// allowFund applies the AutoFund policy to one prospective spend.
func (r *Registrant) allowFund(directoryUID string, atoms int64) (bool, string) {
	p := r.cfg.AutoFund
	if !p.Enabled {
		return false, "auto-funding is disabled"
	}
	if len(p.AllowedDirectoryUIDs) > 0 {
		allowed := false
		for _, uid := range p.AllowedDirectoryUIDs {
			if uid == directoryUID {
				allowed = true
				break
			}
		}
		if !allowed {
			return false, "directory not in the allowlist"
		}
	}
	if p.MaxAtomsPerRequest > 0 && atoms > p.MaxAtomsPerRequest {
		return false, fmt.Sprintf("%d atoms exceeds the %d per-request cap", atoms, p.MaxAtomsPerRequest)
	}
	if p.MaxAtomsPerMonth > 0 {
		window := r.clk.Now().Add(-30 * 24 * time.Hour).Unix()
		if spent := r.fund.totalSince(window); spent+atoms > p.MaxAtomsPerMonth {
			return false, fmt.Sprintf("%d atoms would exceed the %d monthly cap (%d spent)", atoms, p.MaxAtomsPerMonth, spent)
		}
	}
	return true, ""
}

// callDecode invokes a free tool and decodes its JSON result into out.
func callDecode(ctx context.Context, session *mcp.ClientSession, tool string, args, out any) error {
	res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		return err
	}
	if res.IsError {
		return fmt.Errorf("%s: %s", tool, resultText(res))
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(resultJSON(res), out)
}

// fundEntry is one auto-funded listing payment.
type fundEntry struct {
	TS           int64  `json:"ts"`
	DirectoryUID string `json:"directoryUid"`
	Atoms        int64  `json:"atoms"`
}

// fundHistory persists the payments the rolling monthly cap counts.
type fundHistory struct {
	mu      sync.Mutex
	path    string
	entries []fundEntry
}

func openFundHistory(path string) (*fundHistory, error) {
	h := &fundHistory{path: path}
	raw, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return h, nil
	case err != nil:
		return nil, err
	}
	if err := json.Unmarshal(raw, &h.entries); err != nil {
		return nil, fmt.Errorf("fund history %s corrupt: %w", path, err)
	}
	return h, nil
}

func (h *fundHistory) record(e fundEntry) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.entries = append(h.entries, e)
	raw, err := json.MarshalIndent(h.entries, "", "  ")
	if err != nil {
		return err
	}
	tmp := h.path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(h.path), 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, h.path)
}

func (h *fundHistory) totalSince(unix int64) int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	var total int64
	for _, e := range h.entries {
		if e.TS >= unix {
			total += e.Atoms
		}
	}
	return total
}
