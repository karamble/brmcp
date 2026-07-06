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
	"sort"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/karamble/brmcp/server"
)

// adminLog is the append-only JSONL audit trail of every mutating admin
// call. Append-only deviates from the package's whole-file persistence on
// purpose: an audit log must never be rewritten.
type adminLog struct {
	mu   sync.Mutex
	path string
}

type adminLogEntry struct {
	TS     int64  `json:"ts"`
	Actor  string `json:"actor"`
	Action string `json:"action"`
	Args   any    `json:"args,omitempty"`
}

func openAdminLog(path string) (*adminLog, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	return &adminLog{path: path}, nil
}

func (a *adminLog) append(clk Clock, actor, action string, args any) {
	raw, err := json.Marshal(adminLogEntry{
		TS: clk.Now().Unix(), Actor: actor, Action: action, Args: args,
	})
	if err != nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	f, err := os.OpenFile(a.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(raw, '\n'))
}

// adminTool registers a tool that only admin uids can see or call. The
// visibility gate hides it from everyone else; the handler check is the
// belt to that suspender.
func adminTool[In any](s *Service, name, desc string,
	fn func(ctx context.Context, actor string, in In) (any, error)) {

	s.adminTools[name] = true
	server.AddTool(s.harness, &mcp.Tool{Name: name, Description: desc}, 0,
		func(ctx context.Context, peer string, in In) (any, error) {
			if !s.isAdmin(peer) {
				return nil, errors.New("admin only")
			}
			return fn(ctx, peer, in)
		})
}

// PendingOut is one registration awaiting or under verification/review.
type PendingOut struct {
	UID         string   `json:"uid"`
	State       string   `json:"state"`
	Description string   `json:"description"`
	Tags        []string `json:"tags,omitempty"`
	Test        TestSpec `json:"test"`
	Renewal     bool     `json:"renewal"`
	TestOutcome string   `json:"testOutcome,omitempty"`
	TestErr     string   `json:"testErr,omitempty"`
	FeeAtoms    int64    `json:"feeAtoms"`
	BudgetAtoms int64    `json:"budgetAtoms"`
	PaidAtoms   int64    `json:"paidAtoms"`
	CreatedAt   int64    `json:"createdAt"`
}

// RejectIn names a registration and the reason surfaced to the provider.
type RejectIn struct {
	UID    string `json:"uid"`
	Reason string `json:"reason,omitempty"`
}

// PeerIn adds a federation peer with its snapshot signing key.
type PeerIn struct {
	UID    string `json:"uid"`
	PubKey string `json:"pubKey"`
}

// FlagIn attaches a moderation note to a provider entry.
type FlagIn struct {
	UID  string `json:"uid"`
	Note string `json:"note"`
}

// StatsOut summarizes the index for the operator.
type StatsOut struct {
	Live          int   `json:"live"`
	InPipeline    int   `json:"inPipeline"`
	PendingReview int   `json:"pendingReview"`
	Leads         int   `json:"leads"`
	Peers         int   `json:"peers"`
	SpendEntries  int   `json:"spendEntries"`
	SpendAtoms    int64 `json:"spendAtoms"`
}

func (s *Service) registerAdminTools() {
	adminTool(s, "pending_registrations",
		"List every registration in the pipeline or awaiting review.",
		func(_ context.Context, _ string, _ struct{}) (any, error) {
			out := []PendingOut{}
			entries := s.index.all()
			uids := make([]string, 0, len(entries))
			for uid, e := range entries {
				if e.Reg != nil {
					uids = append(uids, uid)
				}
			}
			sort.Strings(uids)
			for _, uid := range uids {
				r := entries[uid].Reg
				out = append(out, PendingOut{
					UID: uid, State: string(r.State), Description: r.Description,
					Tags: r.Tags, Test: r.Test, Renewal: r.Renewal,
					TestOutcome: r.TestOutcome, TestErr: r.TestErr,
					FeeAtoms: r.FeeAtoms, BudgetAtoms: r.BudgetAtoms,
					PaidAtoms: r.PaidAtoms, CreatedAt: r.CreatedAt,
				})
			}
			return out, nil
		})

	adminTool(s, "approve",
		"Publish a reviewed registration as a live listing.",
		func(_ context.Context, actor string, in ProviderIn) (any, error) {
			if err := s.approveRegistration(in.UID); err != nil {
				return nil, err
			}
			s.alog.append(s.clk, actor, "approve", in)
			return s.statusFor(in.UID), nil
		})

	adminTool(s, "reject",
		"Discard a registration; the reason reaches the provider via my_status.",
		func(_ context.Context, actor string, in RejectIn) (any, error) {
			if err := s.rejectRegistration(in.UID, in.Reason); err != nil {
				return nil, err
			}
			s.alog.append(s.clk, actor, "reject", in)
			return s.statusFor(in.UID), nil
		})

	adminTool(s, "leads",
		"List federation leads and their pursuit state.",
		func(_ context.Context, _ string, _ struct{}) (any, error) {
			out := []Lead{}
			for _, l := range s.leads.all() {
				out = append(out, l)
			}
			sort.Slice(out, func(i, j int) bool { return out[i].ProviderUID < out[j].ProviderUID })
			return out, nil
		})

	adminTool(s, "pursue_lead",
		"Invite a lead to register: direct listing_invite call, transitive KX via the peer when not yet reachable.",
		func(ctx context.Context, actor string, in ProviderIn) (any, error) {
			res, err := s.pursueLead(ctx, in.UID)
			if err != nil {
				return nil, err
			}
			s.alog.append(s.clk, actor, "pursue_lead", in)
			return res, nil
		})

	adminTool(s, "add_peer",
		"Trust a peer directory's snapshot signing key; its snapshots then seed leads.",
		func(_ context.Context, actor string, in PeerIn) (any, error) {
			if len(in.UID) != 64 || in.PubKey == "" {
				return nil, errors.New("uid (64-hex) and pubKey are required")
			}
			err := s.peers.put(in.UID, Peer{PubKey: in.PubKey, AddedAt: s.clk.Now().Unix()})
			if err != nil {
				return nil, err
			}
			s.alog.append(s.clk, actor, "add_peer", in)
			return map[string]bool{"added": true}, nil
		})

	adminTool(s, "remove_peer",
		"Forget a peer directory.",
		func(_ context.Context, actor string, in ProviderIn) (any, error) {
			if err := s.peers.delete(in.UID); err != nil {
				return nil, err
			}
			s.alog.append(s.clk, actor, "remove_peer", in)
			return map[string]bool{"removed": true}, nil
		})

	adminTool(s, "verify_peer",
		"Buy and verify a peer's signed snapshot; unknown providers become leads.",
		func(ctx context.Context, actor string, in ProviderIn) (any, error) {
			n, err := s.runPeerVerification(ctx, in.UID)
			if err != nil {
				return nil, err
			}
			s.alog.append(s.clk, actor, "verify_peer", in)
			return map[string]int{"newLeads": n}, nil
		})

	adminTool(s, "run_verification",
		"Spot-check a live listing with a fresh paid test, funded by the provider's residual balance.",
		func(_ context.Context, actor string, in ProviderIn) (any, error) {
			if err := s.spotCheck(in.UID); err != nil {
				return nil, err
			}
			s.alog.append(s.clk, actor, "run_verification", in)
			return s.statusFor(in.UID), nil
		})

	adminTool(s, "recrawl",
		"Refresh a live listing's catalog now (free tools/list).",
		func(ctx context.Context, actor string, in ProviderIn) (any, error) {
			e, ok := s.index.get(in.UID)
			if !ok || e.Live == nil {
				return nil, errors.New("no listing for that uid")
			}
			s.recrawlListing(ctx, in.UID)
			s.alog.append(s.clk, actor, "recrawl", in)
			e, _ = s.index.get(in.UID)
			if e.Live == nil {
				return nil, errors.New("listing vanished during recrawl")
			}
			return providerOut(in.UID, e.Live), nil
		})

	adminTool(s, "unlist",
		"Take a listing down immediately.",
		func(_ context.Context, actor string, in ProviderIn) (any, error) {
			err := s.index.mutate(in.UID, func(e *Entry) error {
				if e.Live == nil {
					return errors.New("no listing for that uid")
				}
				e.Live = nil
				return nil
			})
			if err != nil {
				return nil, err
			}
			e, _ := s.index.get(in.UID)
			if e.Reg == nil && e.Flag == "" {
				_ = s.index.delete(in.UID)
			}
			s.alog.append(s.clk, actor, "unlist", in)
			return map[string]bool{"unlisted": true}, nil
		})

	adminTool(s, "flag",
		"Attach a moderation note to a provider entry.",
		func(_ context.Context, actor string, in FlagIn) (any, error) {
			err := s.index.mutate(in.UID, func(e *Entry) error {
				e.Flag = in.Note
				return nil
			})
			if err != nil {
				return nil, err
			}
			s.alog.append(s.clk, actor, "flag", in)
			return map[string]bool{"flagged": true}, nil
		})

	adminTool(s, "stats",
		"Index, lead, peer, and outbound spend summary.",
		func(_ context.Context, _ string, _ struct{}) (any, error) {
			var out StatsOut
			for _, e := range s.index.all() {
				if e.Live != nil {
					out.Live++
				}
				if e.Reg != nil {
					if e.Reg.State == RegPendingReview {
						out.PendingReview++
					} else {
						out.InPipeline++
					}
				}
			}
			out.Leads = len(s.leads.all())
			out.Peers = len(s.peers.all())
			for _, sp := range s.spend.all() {
				out.SpendEntries++
				out.SpendAtoms += sp.Atoms
			}
			return out, nil
		})
}

// spotCheck re-verifies a live listing on demand. The paid test spends the
// provider's residual balance, so the balance must cover the listing's
// test budget up front.
func (s *Service) spotCheck(uid string) error {
	start := false
	err := s.index.mutate(uid, func(e *Entry) error {
		if e.Live == nil {
			return errors.New("no listing for that uid")
		}
		if e.Reg != nil {
			return errors.New("a registration is already in flight")
		}
		if bal := s.harness.Billing().Balance(uid); bal < e.Live.Test.MaxAtoms {
			return fmt.Errorf("provider balance %d cannot fund the %d atom test", bal, e.Live.Test.MaxAtoms)
		}
		e.Reg = &Registration{
			State:       RegTesting,
			Description: e.Live.Description,
			Tags:        e.Live.Tags,
			Test:        e.Live.Test,
			BudgetAtoms: e.Live.Test.MaxAtoms,
			CallKey:     newCallKey(),
			Renewal:     true,
			CreatedAt:   s.clk.Now().Unix(),
			Catalog:     e.Live.Catalog,
		}
		start = true
		return nil
	})
	if err != nil {
		return err
	}
	if start {
		s.spawnPipeline(uid)
	}
	return nil
}
