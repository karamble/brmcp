// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package botkit connects brmcp to a bot that reaches its Bison Relay client
// over clientrpc, such as a bisonbotkit *kit.Bot: a PM sender, and a tip
// payer driven by the client's tip-progress stream.
//
// Peers are addressed by uid only. clientrpc also accepts nicks, and a nick
// resolved here could pay the wrong user.
package botkit

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/companyzero/bisonrelay/clientrpc/types"
	"github.com/companyzero/bisonrelay/zkidentity"
	"github.com/decred/dcrd/dcrutil/v4"
	kit "github.com/vctt94/bisonbotkit"

	"github.com/karamble/brmcp/bridge"
)

// Bot is the part of a bisonbotkit *kit.Bot that brmcp uses.
type Bot interface {
	SendPM(ctx context.Context, user, msg string) error
	PayTip(ctx context.Context, uid zkidentity.ShortID, tipAmt dcrutil.Amount, maxAttempts int32) error
}

var errNotConnected = errors.New("bot not connected")

// LateBot is a Bot for hosts that get their bot after start and lose it on
// disconnect, such as server.RunBotHooked through its OnBot hook.
type LateBot struct {
	mu  sync.Mutex
	bot *kit.Bot
}

// Set installs the current bot; nil means disconnected.
func (l *LateBot) Set(bot *kit.Bot) {
	l.mu.Lock()
	l.bot = bot
	l.mu.Unlock()
}

func (l *LateBot) current() *kit.Bot {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.bot
}

// SendPM implements Bot.
func (l *LateBot) SendPM(ctx context.Context, user, msg string) error {
	bot := l.current()
	if bot == nil {
		return errNotConnected
	}
	return bot.SendPM(ctx, user, msg)
}

// PayTip implements Bot.
func (l *LateBot) PayTip(ctx context.Context, uid zkidentity.ShortID, tipAmt dcrutil.Amount, maxAttempts int32) error {
	bot := l.current()
	if bot == nil {
		return errNotConnected
	}
	return bot.PayTip(ctx, uid, tipAmt, maxAttempts)
}

// tipAttempts is how many invoice requests one payment may make.
const tipAttempts = 3

func parseUID(s string) (zkidentity.ShortID, error) {
	var uid zkidentity.ShortID
	if err := uid.FromString(s); err != nil {
		return uid, fmt.Errorf("peer uid: %w", err)
	}
	return uid, nil
}

// Sender implements brmcp.PMSender over a Bot.
type Sender struct {
	Bot Bot
}

// SendPM implements brmcp.PMSender.
func (s Sender) SendPM(ctx context.Context, peer, text string) error {
	uid, err := parseUID(peer)
	if err != nil {
		return err
	}
	return s.Bot.SendPM(ctx, uid.String(), text)
}

// NewTipPayer returns a payer that tips through bot. The host feeds it the
// bot's tip-progress events with HandleTipProgress.
func NewTipPayer(bot Bot) *bridge.TipPayer {
	return bridge.NewTipPayer(func(ctx context.Context, payeeUID string, atoms int64) error {
		uid, err := parseUID(payeeUID)
		if err != nil {
			return err
		}
		return bot.PayTip(ctx, uid, dcrutil.Amount(atoms), tipAttempts)
	})
}

// HandleTipProgress feeds one clientrpc tip-progress event to p.
func HandleTipProgress(p *bridge.TipPayer, ev *types.TipProgressEvent) {
	if ev == nil {
		return
	}
	var attemptErr error
	if ev.AttemptErr != "" {
		attemptErr = errors.New(ev.AttemptErr)
	}
	p.Progress(fmt.Sprintf("%x", ev.Uid), ev.AmountMatoms, ev.Completed, attemptErr, ev.WillRetry)
}
