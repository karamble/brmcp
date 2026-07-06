// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package directory

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// errNoChange aborts a store mutation without persisting or reporting.
var errNoChange = errors.New("no change")

func newCallKey() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

// handleRegister is the register tool: it validates the application,
// freezes the terms, and answers with the funding invoice. The session
// peer uid is the registered identity.
func (s *Service) handleRegister(_ context.Context, peer string, in RegisterIn) (any, error) {
	if self := s.getSelfUID(); self != "" && peer == self {
		return nil, errors.New("the directory cannot list itself")
	}
	if err := validateRegister(in, s.policy.TestBudgetMaxAtoms); err != nil {
		return nil, err
	}
	now := s.clk.Now().Unix()
	busy := false
	err := s.index.mutate(peer, func(e *Entry) error {
		if e.Reg != nil && (e.Reg.State == RegCrawling || e.Reg.State == RegTesting) {
			busy = true
			return nil
		}
		e.Reg = &Registration{
			State:       RegAwaitingFunding,
			Description: in.Description,
			Tags:        normalizeTags(in.Tags),
			Test:        in.Test,
			FeeAtoms:    s.policy.ListingFeeAtoms,
			BudgetAtoms: in.Test.MaxAtoms,
			Renewal:     e.Live != nil,
			CreatedAt:   now,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !busy {
		s.convertLead(peer)
		s.pokeFunding(peer)
	}
	st := s.statusFor(peer)
	if busy {
		st.Note = "verification in progress; the previous registration continues"
	}
	return st, nil
}

func validateRegister(in RegisterIn, budgetMax int64) error {
	if strings.TrimSpace(in.Description) == "" {
		return errors.New("description is required")
	}
	if len(in.Description) > MaxDescriptionLen {
		return fmt.Errorf("description exceeds %d characters", MaxDescriptionLen)
	}
	if len(in.Tags) > MaxTags {
		return fmt.Errorf("at most %d tags", MaxTags)
	}
	for _, tag := range in.Tags {
		if tag == "" || len(tag) > MaxTagLen {
			return fmt.Errorf("tags must be 1-%d characters", MaxTagLen)
		}
	}
	if strings.TrimSpace(in.Test.Tool) == "" {
		return errors.New("test.tool is required")
	}
	if in.Test.MaxAtoms <= 0 {
		return errors.New("test.maxAtoms must be positive")
	}
	if in.Test.MaxAtoms > budgetMax {
		return fmt.Errorf("test.maxAtoms exceeds the directory's %d atom ceiling", budgetMax)
	}
	return nil
}

func normalizeTags(tags []string) []string {
	out := make([]string, 0, len(tags))
	seen := make(map[string]bool, len(tags))
	for _, t := range tags {
		t = strings.ToLower(strings.TrimSpace(t))
		if t == "" || seen[t] {
			continue
		}
		seen[t] = true
		out = append(out, t)
	}
	return out
}

// statusFor builds a caller's pull-based status view.
func (s *Service) statusFor(peer string) StatusOut {
	e, ok := s.index.get(peer)
	switch {
	case ok && e.Reg != nil:
		st := StatusOut{
			State:           string(e.Reg.State),
			FeeAtoms:        e.Reg.FeeAtoms,
			TestBudgetAtoms: e.Reg.BudgetAtoms,
		}
		if e.Reg.State == RegAwaitingFunding {
			required := e.Reg.FeeAtoms + e.Reg.BudgetAtoms
			balance := s.harness.Billing().Balance(peer)
			st.RequiredAtoms = required
			st.BalanceAtoms = balance
			if balance < required {
				st.ShortfallAtoms = required - balance
			}
		}
		if e.Reg.State == RegPendingReview && e.Reg.TestErr != "" {
			st.Note = e.Reg.TestErr
		}
		return st
	case ok && e.Live != nil:
		return StatusOut{State: StateListed, ExpiresAt: e.Live.ExpiresAt, Note: e.Flag}
	case ok && e.Flag != "":
		return StatusOut{State: StateRejected, Note: e.Flag}
	default:
		return StatusOut{State: StateNone}
	}
}

// pokeFunding moves an awaiting registration into the pipeline once the
// caller's balance covers fee plus test budget; the fee is debited here.
func (s *Service) pokeFunding(uid string) {
	start := false
	err := s.index.mutate(uid, func(e *Entry) error {
		if e.Reg == nil || e.Reg.State != RegAwaitingFunding {
			return errNoChange
		}
		required := e.Reg.FeeAtoms + e.Reg.BudgetAtoms
		if s.harness.Billing().Balance(uid) < required {
			return errNoChange
		}
		if err := s.harness.Billing().Debit(uid, e.Reg.FeeAtoms); err != nil {
			return errNoChange
		}
		r := *e.Reg
		r.State = RegCrawling
		e.Reg = &r
		start = true
		return nil
	})
	if err != nil && !errors.Is(err, errNoChange) {
		s.logf("brmcpdir: funding check %s: %v", uid[:8], err)
	}
	if start {
		s.spawnPipeline(uid)
	}
}

// runPipeline advances one registration from its persisted state through
// crawl and test. Any stage failure parks it for admin review; a passing
// renewal promotes without review.
func (s *Service) runPipeline(uid string) {
	ctx := s.baseCtx()
	e, ok := s.index.get(uid)
	if !ok || e.Reg == nil {
		return
	}
	if e.Reg.State == RegCrawling {
		catalog, err := s.crawl(ctx, uid)
		if err != nil {
			// A shutdown is not a verification failure: leave the
			// persisted state alone so the next start resumes it.
			if ctx.Err() == nil {
				s.park(uid, "crawl: "+err.Error())
			}
			return
		}
		callKey := e.Reg.CallKey
		if callKey == "" {
			callKey = newCallKey()
		}
		if err := s.index.mutate(uid, func(e *Entry) error {
			if e.Reg == nil {
				return errNoChange
			}
			r := *e.Reg
			r.Catalog = catalog
			// The key is persisted before any payment so a restart
			// re-issues the same logical call and the provider's
			// journal charges it once.
			r.CallKey = callKey
			r.State = RegTesting
			e.Reg = &r
			return nil
		}); err != nil {
			if !errors.Is(err, errNoChange) {
				s.logf("brmcpdir: persist crawl %s: %v", uid[:8], err)
			}
			return
		}
	}

	e, ok = s.index.get(uid)
	if !ok || e.Reg == nil || e.Reg.State != RegTesting {
		return
	}
	tested, passed := s.runTest(ctx, uid, *e.Reg)
	if !passed && ctx.Err() != nil {
		// Interrupted by shutdown, not judged: stay in RegTesting and
		// resume on the next start with the same call key.
		return
	}
	promote := false
	err := s.index.mutate(uid, func(e *Entry) error {
		if e.Reg == nil {
			return errNoChange
		}
		r := *e.Reg
		r.PaidAtoms = tested.PaidAtoms
		r.TestOutcome = tested.TestOutcome
		r.TestErr = tested.TestErr
		r.TestLatencyMs = tested.TestLatencyMs
		if passed && r.Renewal {
			promote = true
		} else {
			r.State = RegPendingReview
		}
		e.Reg = &r
		return nil
	})
	if err != nil && !errors.Is(err, errNoChange) {
		s.logf("brmcpdir: persist test %s: %v", uid[:8], err)
		return
	}
	if promote {
		if err := s.promote(uid); err != nil {
			s.logf("brmcpdir: renewal promote %s: %v", uid[:8], err)
		} else {
			s.logf("brmcpdir: renewed %s", uid[:8])
		}
	}
}

// park moves a failed registration to admin review with the failure noted.
func (s *Service) park(uid, msg string) {
	err := s.index.mutate(uid, func(e *Entry) error {
		if e.Reg == nil {
			return errNoChange
		}
		r := *e.Reg
		r.State = RegPendingReview
		r.TestOutcome = "failed"
		r.TestErr = msg
		e.Reg = &r
		return nil
	})
	if err != nil && !errors.Is(err, errNoChange) {
		s.logf("brmcpdir: park %s: %v", uid[:8], err)
	}
	s.logf("brmcpdir: %s parked for review: %s", uid[:8], msg)
}

// promote publishes a verified registration as the live listing. Renewals
// keep their original approval date.
func (s *Service) promote(uid string) error {
	now := s.clk.Now().Unix()
	return s.index.mutate(uid, func(e *Entry) error {
		if e.Reg == nil {
			return errors.New("no registration to promote")
		}
		if e.Reg.TestOutcome != "ok" {
			return errors.New("verification has not passed")
		}
		l := &Listing{
			Description:           e.Reg.Description,
			Tags:                  e.Reg.Tags,
			Test:                  e.Reg.Test,
			Catalog:               e.Reg.Catalog,
			CatalogCheckedAt:      now,
			LastVerifiedExecution: now,
			TestLatencyMs:         e.Reg.TestLatencyMs,
			ApprovedAt:            now,
			ExpiresAt:             now + int64(s.policy.ExpiryDays)*86400,
		}
		if e.Live != nil {
			l.ApprovedAt = e.Live.ApprovedAt
			l.RenewedAt = now
		}
		e.Live = l
		e.Reg = nil
		e.Flag = ""
		return nil
	})
}

// approveRegistration is the admin approve path: only parked registrations
// that passed their test may go live.
func (s *Service) approveRegistration(uid string) error {
	e, ok := s.index.get(uid)
	if !ok || e.Reg == nil || e.Reg.State != RegPendingReview {
		return errors.New("nothing pending review for that uid")
	}
	return s.promote(uid)
}

// rejectRegistration discards a registration; the reason is surfaced to
// the provider through my_status. Fees are not refunded.
func (s *Service) rejectRegistration(uid, reason string) error {
	return s.index.mutate(uid, func(e *Entry) error {
		if e.Reg == nil {
			return errors.New("no registration for that uid")
		}
		e.Reg = nil
		if reason != "" {
			e.Flag = reason
		}
		return nil
	})
}

// convertLead marks a pursued lead converted once its provider registers.
func (s *Service) convertLead(uid string) {
	err := s.leads.mutate(uid, func(l *Lead) error {
		if l.ProviderUID == "" || l.State == LeadConverted {
			return errNoChange
		}
		l.State = LeadConverted
		l.UpdatedAt = s.clk.Now().Unix()
		return nil
	})
	if err != nil && !errors.Is(err, errNoChange) {
		s.logf("brmcpdir: convert lead %s: %v", uid[:8], err)
	}
}

// sweep is one maintenance pass: expiry GC, free catalog refreshes, and
// funding re-checks.
func (s *Service) sweep(ctx context.Context) {
	now := s.clk.Now().Unix()
	for uid, e := range s.index.all() {
		if ctx.Err() != nil {
			return
		}
		if e.Live != nil && e.Live.ExpiresAt <= now {
			s.expire(uid, now)
			continue
		}
		if e.Live != nil && now-e.Live.CatalogCheckedAt >= int64(s.policy.RecrawlHours)*3600 {
			s.recrawlListing(ctx, uid)
		}
		if e.Reg != nil && e.Reg.State == RegAwaitingFunding {
			s.pokeFunding(uid)
		}
	}
}

// expire removes a lapsed listing; the whole entry goes unless a renewal
// is still in flight.
func (s *Service) expire(uid string, now int64) {
	e, ok := s.index.get(uid)
	if !ok || e.Live == nil || e.Live.ExpiresAt > now {
		return
	}
	if e.Reg == nil {
		if err := s.index.delete(uid); err != nil {
			s.logf("brmcpdir: expire %s: %v", uid[:8], err)
			return
		}
	} else {
		err := s.index.mutate(uid, func(e *Entry) error {
			if e.Live == nil || e.Live.ExpiresAt > now {
				return errNoChange
			}
			e.Live = nil
			return nil
		})
		if err != nil && !errors.Is(err, errNoChange) {
			s.logf("brmcpdir: expire %s: %v", uid[:8], err)
			return
		}
	}
	s.logf("brmcpdir: listing %s expired", uid[:8])
}

// recrawlListing refreshes a live catalog for free; execution verification
// stamps move only on paid tests.
func (s *Service) recrawlListing(ctx context.Context, uid string) {
	catalog, err := s.crawl(ctx, uid)
	if err != nil {
		s.logf("brmcpdir: recrawl %s: %v", uid[:8], err)
		return
	}
	now := s.clk.Now().Unix()
	err = s.index.mutate(uid, func(e *Entry) error {
		if e.Live == nil {
			return errNoChange
		}
		l := *e.Live
		l.Catalog = catalog
		l.CatalogCheckedAt = now
		e.Live = &l
		return nil
	})
	if err != nil && !errors.Is(err, errNoChange) {
		s.logf("brmcpdir: recrawl persist %s: %v", uid[:8], err)
	}
}
