// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package botkit_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/companyzero/bisonrelay/clientrpc/types"
	"github.com/companyzero/bisonrelay/zkidentity"
	"github.com/decred/dcrd/dcrutil/v4"

	"github.com/karamble/brmcp/botkit"
)

const testUID = "0707070707070707070707070707070707070707070707070707070707070707"

type fakeBot struct {
	user, msg string
	tips      chan fakeTip
}

type fakeTip struct {
	uid      zkidentity.ShortID
	amt      dcrutil.Amount
	attempts int32
}

func (f *fakeBot) SendPM(_ context.Context, user, msg string) error {
	f.user, f.msg = user, msg
	return nil
}

func (f *fakeBot) PayTip(_ context.Context, uid zkidentity.ShortID, amt dcrutil.Amount, attempts int32) error {
	f.tips <- fakeTip{uid, amt, attempts}
	return nil
}

func TestSender(t *testing.T) {
	f := &fakeBot{}
	s := botkit.Sender{Bot: f}
	if err := s.SendPM(context.Background(), strings.ToUpper(testUID), "part"); err != nil {
		t.Fatal(err)
	}
	if f.user != testUID || f.msg != "part" {
		t.Fatalf("sent %q to %q", f.msg, f.user)
	}
	if err := s.SendPM(context.Background(), "alice", "part"); err == nil {
		t.Fatal("a nick was accepted as a peer")
	}
}

func TestTipPayer(t *testing.T) {
	f := &fakeBot{tips: make(chan fakeTip, 1)}
	p := botkit.NewTipPayer(f)
	uid := bytes.Repeat([]byte{7}, 32)

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done <- p.Pay(ctx, testUID, 250)
	}()
	tip := <-f.tips
	if tip.uid.String() != testUID || tip.amt != 250 || tip.attempts != 3 {
		t.Fatalf("tipped %v to %s in %d attempts", tip.amt, tip.uid, tip.attempts)
	}

	botkit.HandleTipProgress(p, nil)
	botkit.HandleTipProgress(p, &types.TipProgressEvent{Uid: uid, AmountMatoms: 250_000,
		AttemptErr: "no route", WillRetry: true})
	botkit.HandleTipProgress(p, &types.TipProgressEvent{Uid: uid, AmountMatoms: 250_000,
		AttemptErr: "invoice expired"})
	if err := <-done; err == nil || !strings.Contains(err.Error(), "tip failed: invoice expired") {
		t.Fatalf("pay: got %v", err)
	}
}

func TestLateBotDisconnected(t *testing.T) {
	var l botkit.LateBot
	l.Set(nil)
	if err := l.SendPM(context.Background(), testUID, "part"); err == nil {
		t.Fatal("sent without a bot")
	}
	if err := l.PayTip(context.Background(), zkidentity.ShortID{}, 1, 1); err == nil {
		t.Fatal("tipped without a bot")
	}
}
