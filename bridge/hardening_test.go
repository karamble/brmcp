// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bridge_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/karamble/brmcp/bridge"
)

func TestPayFailureNotCounted(t *testing.T) {
	fx := newFixture(t, fixtureOpts{})
	fx.payer.mu.Lock()
	fx.payer.fn = func(context.Context, string, int64) error {
		return errors.New("no route to bot")
	}
	fx.payer.mu.Unlock()

	session := fx.session()
	res, err := fx.call(session, "paid")
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError {
		t.Fatal("unpaid call succeeded")
	}
	requireNote(t, res, "brclientd: payment not made: no route to bot")
	entries, today := fx.bridge.SpendLog()
	if len(entries) != 0 || today != 0 {
		t.Fatalf("failed payment stayed counted: %+v today=%d", entries, today)
	}
}

func TestPayTimeoutStaysCounted(t *testing.T) {
	fx := newFixture(t, fixtureOpts{settings: &bridge.Settings{
		Enabled:         true,
		Token:           "test-token-0123456789abcdef",
		Mode:            "autopay",
		PerCallCapAtoms: 10_000,
		PerDayCapAtoms:  100_000,
		AllowedBots:     []string{botUID},
		TipWaitSecs:     1,
	}})
	// The rail never confirms within the wait budget; the attempt may
	// still settle later, so the spend stays counted.
	fx.payer.mu.Lock()
	fx.payer.fn = func(ctx context.Context, _ string, _ int64) error {
		<-ctx.Done()
		return errors.New("tip not confirmed within 1s; the attempt keeps running")
	}
	fx.payer.mu.Unlock()

	session := fx.session()
	res, err := fx.call(session, "paid")
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError {
		t.Fatal("unconfirmed call succeeded")
	}
	requireNote(t, res, "payment not made: tip not confirmed within 1s")
	entries, today := fx.bridge.SpendLog()
	if len(entries) != 1 || today != paidPrice {
		t.Fatalf("unconfirmed payment not counted: %+v today=%d", entries, today)
	}
}

func TestUnreachableBotIs503(t *testing.T) {
	fx := newFixture(t, fixtureOpts{})
	// Every outbound PM fails: the proxy build cannot reach the bot and
	// the endpoint must say so instead of serving an empty tool list.
	fx.sender.failNext(1 << 30)
	if got := httpStatus(t, http.MethodPost, fx.endpoint(botUID), fx.token); got != http.StatusServiceUnavailable {
		t.Fatalf("unreachable bot answered %d != 503", got)
	}
	fx.sender.failNext(0)

	// With the transport healthy again the same endpoint serves tools.
	session := fx.session()
	if res, err := fx.call(session, "free"); err != nil || res.IsError {
		t.Fatalf("recovery call: %v %v", err, res)
	}
}
