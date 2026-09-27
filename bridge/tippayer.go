// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bridge

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

// TipFunc starts one Bison Relay tip of atoms to a peer uid. It returns once
// the attempt is dispatched; the outcome arrives later through
// TipPayer.Progress.
type TipFunc func(ctx context.Context, payeeUID string, atoms int64) error

// TipPayer is a Payer over Bison Relay's asynchronous tip flow. Pay starts a
// tip and blocks until the host feeds the matching terminal progress event
// or ctx ends. Outcomes that no live Pay consumed (the wait budget had
// passed, or the attempt outlived a restart) go to the late handler, so a
// Bridge host can record them with ResolveSpend.
type TipPayer struct {
	tip     TipFunc
	matcher *TipMatcher

	mu   sync.Mutex
	late func(payeeUID string, atoms int64, err error)
}

// NewTipPayer returns a TipPayer that starts tips with tip.
func NewTipPayer(tip TipFunc) *TipPayer {
	return &TipPayer{tip: tip, matcher: NewTipMatcher()}
}

// SetLate installs the handler for outcomes no live Pay consumed. It may be
// set after construction; nil drops them.
func (p *TipPayer) SetLate(f func(payeeUID string, atoms int64, err error)) {
	p.mu.Lock()
	p.late = f
	p.mu.Unlock()
}

func (p *TipPayer) resolveLate(payeeUID string, atoms int64, err error) {
	p.mu.Lock()
	f := p.late
	p.mu.Unlock()
	if f != nil {
		f(payeeUID, atoms, err)
	}
}

// Pay implements Payer.
func (p *TipPayer) Pay(ctx context.Context, payeeUID string, atoms int64) error {
	waitSecs := 0
	if dl, ok := ctx.Deadline(); ok {
		waitSecs = int(math.Round(time.Until(dl).Seconds()))
	}
	w := p.matcher.Expect(payeeUID, atoms*1000)
	if err := p.tip(ctx, payeeUID, atoms); err != nil {
		w.Cancel()
		return fmt.Errorf("tip: %w", err)
	}
	select {
	case err := <-w.Done():
		if err != nil {
			return fmt.Errorf("tip failed: %w", err)
		}
		return nil
	case <-ctx.Done():
		w.Cancel()
		// The terminal event may have resolved the wait in the same
		// instant the deadline fired; it was consumed, so forward it or
		// the outcome would be lost.
		select {
		case res := <-w.Done():
			p.resolveLate(payeeUID, atoms, res)
		default:
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("tip not confirmed within %ds; the attempt keeps "+
				"running in the background and still credits the payee", waitSecs)
		}
		return ctx.Err()
	}
}

// Progress feeds one tip-progress event from the host's Bison Relay client.
// Events that will be retried are ignored; any other event ends the attempt,
// settled when completed and failed with attemptErr otherwise.
func (p *TipPayer) Progress(payeeUID string, amtMAtoms int64, completed bool, attemptErr error, willRetry bool) {
	if willRetry && !completed {
		return
	}
	var res error
	if !completed {
		res = attemptErr
		if res == nil {
			res = errors.New("tip attempt ended without completing")
		}
	}
	if p.matcher.Resolve(payeeUID, amtMAtoms, res) {
		return
	}
	// Tips from other flows can carry sub-atom amounts; those were never
	// a payment made here.
	if amtMAtoms%1000 == 0 {
		p.resolveLate(payeeUID, amtMAtoms/1000, res)
	}
}
