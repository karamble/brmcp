// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bridge_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/karamble/brmcp/bridge"
	"github.com/karamble/brmcp/brmcptest"
)

type lateCall struct {
	uid   string
	atoms int64
	err   error
}

type lateLog struct {
	mu    sync.Mutex
	calls []lateCall
}

func (l *lateLog) record(uid string, atoms int64, err error) {
	l.mu.Lock()
	l.calls = append(l.calls, lateCall{uid, atoms, err})
	l.mu.Unlock()
}

func (l *lateLog) get() []lateCall {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]lateCall(nil), l.calls...)
}

// payWith runs Pay with a tip func that feeds events to the payer once the
// tip is dispatched.
func payWith(t *testing.T, events func(p *bridge.TipPayer, uid string)) error {
	t.Helper()
	var p *bridge.TipPayer
	p = bridge.NewTipPayer(func(ctx context.Context, uid string, atoms int64) error {
		go events(p, uid)
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return p.Pay(ctx, brmcptest.UID(7), 250)
}

func TestTipPayerSettled(t *testing.T) {
	err := payWith(t, func(p *bridge.TipPayer, uid string) {
		p.Progress(strings.ToUpper(uid), 250_000, true, nil, false)
	})
	if err != nil {
		t.Fatalf("settled tip: %v", err)
	}
}

func TestTipPayerFailed(t *testing.T) {
	err := payWith(t, func(p *bridge.TipPayer, uid string) {
		p.Progress(uid, 250_000, false, errors.New("no route"), false)
	})
	if err == nil || !strings.Contains(err.Error(), "tip failed: no route") {
		t.Fatalf("failed tip: got %v", err)
	}

	err = payWith(t, func(p *bridge.TipPayer, uid string) {
		p.Progress(uid, 250_000, false, nil, false)
	})
	if err == nil || !strings.Contains(err.Error(), "ended without completing") {
		t.Fatalf("failed tip without error: got %v", err)
	}
}

func TestTipPayerIgnoresRetries(t *testing.T) {
	err := payWith(t, func(p *bridge.TipPayer, uid string) {
		p.Progress(uid, 250_000, false, errors.New("retrying"), true)
		p.Progress(uid, 250_000, true, nil, false)
	})
	if err != nil {
		t.Fatalf("a retried attempt ended the payment: %v", err)
	}
}

func TestTipPayerDispatchError(t *testing.T) {
	var late lateLog
	p := bridge.NewTipPayer(func(context.Context, string, int64) error {
		return errors.New("client is quitting")
	})
	p.SetLate(late.record)
	uid := brmcptest.UID(7)
	err := p.Pay(context.Background(), uid, 250)
	if err == nil || !strings.Contains(err.Error(), "tip: client is quitting") {
		t.Fatalf("dispatch error: got %v", err)
	}
	// The abandoned wait must not swallow a later event.
	p.Progress(uid, 250_000, true, nil, false)
	if got := late.get(); len(got) != 1 {
		t.Fatalf("event after a failed dispatch: late calls %v", got)
	}
}

func TestTipPayerLateOutcome(t *testing.T) {
	var late lateLog
	p := bridge.NewTipPayer(func(context.Context, string, int64) error { return nil })
	p.SetLate(late.record)
	uid := brmcptest.UID(7)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := p.Pay(ctx, uid, 250)
	if err == nil || !strings.Contains(err.Error(), "not confirmed within") {
		t.Fatalf("deadline: got %v", err)
	}
	if got := late.get(); len(got) != 0 {
		t.Fatalf("late handler called before any event: %v", got)
	}

	p.Progress(uid, 250_000, true, nil, false)
	got := late.get()
	if len(got) != 1 || got[0].uid != uid || got[0].atoms != 250 || got[0].err != nil {
		t.Fatalf("late outcome: got %v", got)
	}

	// A sub-atom tip from another flow was never a payment made here.
	p.Progress(uid, 250_500, true, nil, false)
	if got := late.get(); len(got) != 1 {
		t.Fatalf("sub-atom tip reached the late handler: %v", got)
	}
}

// When the outcome and the deadline land together, the outcome is reported
// exactly once: to Pay's caller, or to the late handler.
func TestTipPayerOutcomeAtDeadline(t *testing.T) {
	uid := brmcptest.UID(7)
	for i := 0; i < 200; i++ {
		var late lateLog
		ctx, cancel := context.WithCancel(context.Background())
		var p *bridge.TipPayer
		p = bridge.NewTipPayer(func(context.Context, string, int64) error {
			p.Progress(uid, 250_000, true, nil, false)
			cancel()
			return nil
		})
		p.SetLate(late.record)
		err := p.Pay(ctx, uid, 250)
		n := len(late.get())
		switch {
		case err == nil && n == 0:
		case err != nil && n == 1:
		default:
			t.Fatalf("run %d: pay err %v with %d late calls", i, err, n)
		}
	}
}
