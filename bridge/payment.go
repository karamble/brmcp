// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bridge

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/karamble/brmcp"
)

// PendingPayment is one payment parked for a human decision (approval
// mode). The queue is in-memory by design: a parked payment is coupled to
// the live tool call that wants to pay, and a restart fails that call
// anyway, so restart means fail-safe denial.
type PendingPayment struct {
	ID      string `json:"id"`
	Bot     string `json:"bot"`
	Tool    string `json:"tool"`
	Atoms   int64  `json:"atoms"`
	Created int64  `json:"created"`
}

type pendingPayment struct {
	PendingPayment

	decision chan bool
	once     sync.Once
}

func (p *pendingPayment) decide(approve bool) {
	p.once.Do(func() { p.decision <- approve })
}

// SpendEntry is one settled payment, persisted in mcpspend.json.
type SpendEntry struct {
	TS    int64  `json:"ts"`
	Bot   string `json:"bot"`
	Tool  string `json:"tool"`
	Rail  string `json:"rail"`
	Atoms int64  `json:"atoms"`
}

// spendKeep bounds the persisted spend log; entries still inside the
// rolling daily-cap window are never pruned regardless.
const spendKeep = 1000

// settle pays one payment_required under the configured caps and mode,
// blocking until the payment reaches a terminal state or the wait budget
// runs out.
func (b *Bridge) settle(ctx context.Context, bot, tool string, pr *brmcp.PaymentRequired) error {
	atoms := pr.ShortfallAtoms
	if atoms <= 0 {
		atoms = pr.PriceAtoms
	}
	if atoms <= 0 {
		return fmt.Errorf("bot requested a nonpositive amount")
	}

	b.mu.Lock()
	s := b.settings
	spentToday := b.spentSinceLocked(b.clk.Now().Add(-24 * time.Hour))
	b.mu.Unlock()

	// Caps bound BOTH modes; zero means zero, approval cannot override.
	if s.PerCallCapAtoms <= 0 || atoms > s.PerCallCapAtoms {
		return fmt.Errorf("%d atoms exceeds the per-call cap (%d)", atoms, s.PerCallCapAtoms)
	}
	if s.PerDayCapAtoms <= 0 || spentToday+atoms > s.PerDayCapAtoms {
		return fmt.Errorf("%d atoms would exceed the daily cap (%d spent of %d)",
			atoms, spentToday, s.PerDayCapAtoms)
	}
	if s.Mode == "approval" {
		if err := b.awaitApproval(ctx, bot, tool, atoms,
			time.Duration(s.ApprovalTimeoutSecs)*time.Second); err != nil {
			return err
		}
	}

	payCtx, cancel := context.WithTimeout(ctx, time.Duration(s.TipWaitSecs)*time.Second)
	defer cancel()
	if err := b.cfg.Payer.Pay(payCtx, bot, atoms); err != nil {
		return err
	}
	b.mu.Lock()
	b.recordSpendLocked(bot, tool, "tip", atoms)
	b.mu.Unlock()
	b.logf("brmcp bridge: paid %d atoms for %s/%s", atoms, bot[:8], tool)
	return nil
}

// awaitApproval parks the payment in the pending queue until the user
// decides, the timeout passes, or the call context ends.
func (b *Bridge) awaitApproval(ctx context.Context, bot, tool string, atoms int64,
	timeout time.Duration) error {

	var idb [8]byte
	if _, err := rand.Read(idb[:]); err != nil {
		return err
	}
	p := &pendingPayment{
		PendingPayment: PendingPayment{
			ID:      hex.EncodeToString(idb[:]),
			Bot:     bot,
			Tool:    tool,
			Atoms:   atoms,
			Created: b.clk.Now().Unix(),
		},
		decision: make(chan bool, 1),
	}
	b.mu.Lock()
	b.pending[p.ID] = p
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.pending, p.ID)
		b.mu.Unlock()
	}()

	b.logf("brmcp bridge: payment awaiting approval: %s atoms=%d bot=%s tool=%s",
		p.ID, atoms, bot[:8], tool)
	select {
	case ok := <-p.decision:
		if !ok {
			return fmt.Errorf("payment denied by the user")
		}
		return nil
	case <-b.clk.After(timeout):
		return fmt.Errorf("approval timed out after %s", timeout)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PendingPayments lists payments parked for approval, oldest first.
func (b *Bridge) PendingPayments() []PendingPayment {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]PendingPayment, 0, len(b.pending))
	for _, p := range b.pending {
		out = append(out, p.PendingPayment)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Created != out[j].Created {
			return out[i].Created < out[j].Created
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// ResolvePayment delivers the user's decision for a parked payment,
// reporting false when no such payment is pending. Repeat decisions on the
// same id are ignored.
func (b *Bridge) ResolvePayment(id string, approve bool) bool {
	b.mu.Lock()
	p := b.pending[id]
	b.mu.Unlock()
	if p == nil {
		return false
	}
	p.decide(approve)
	return true
}

// SpendLog returns a copy of the recorded payments and the rolling
// twenty-four-hour total the daily cap is enforced against.
func (b *Bridge) SpendLog() (entries []SpendEntry, todayAtoms int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	entries = append(entries, b.spend...)
	return entries, b.spentSinceLocked(b.clk.Now().Add(-24 * time.Hour))
}

func (b *Bridge) recordSpendLocked(bot, tool, rail string, atoms int64) {
	b.spend = append(b.spend, SpendEntry{
		TS: b.clk.Now().Unix(), Bot: bot, Tool: tool, Rail: rail, Atoms: atoms,
	})
	if err := b.persistSpendLocked(); err != nil {
		b.logf("brmcp bridge: persist spend log: %v", err)
	}
}

// spentSinceLocked sums spends after the cutoff (the rolling per-day cap).
func (b *Bridge) spentSinceLocked(cutoff time.Time) int64 {
	var total int64
	cut := cutoff.Unix()
	for _, s := range b.spend {
		if s.TS >= cut {
			total += s.Atoms
		}
	}
	return total
}

func (b *Bridge) persistSpendLocked() error {
	// Bound the log, but never drop entries still inside the daily-cap
	// window: pruning those would undercount the rolling total and let
	// spending exceed the cap.
	if len(b.spend) > spendKeep {
		first := len(b.spend) - spendKeep
		cut := b.clk.Now().Add(-24 * time.Hour).Unix()
		for i := 0; i < first; i++ {
			if b.spend[i].TS >= cut {
				first = i
				break
			}
		}
		if first > 0 {
			b.spend = append([]SpendEntry(nil), b.spend[first:]...)
		}
	}
	raw, err := json.MarshalIndent(b.spend, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(b.spendPath(), raw, 0o600)
}
