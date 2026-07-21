// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package server

import (
	"context"
	"encoding/hex"
	"errors"
	"path/filepath"
	"sync"
	"time"

	"github.com/companyzero/bisonrelay/clientrpc/types"
	kit "github.com/vctt94/bisonbotkit"
	kitconfig "github.com/vctt94/bisonbotkit/config"

	"github.com/karamble/brmcp"
	"github.com/karamble/brmcp/wire"
)

// matomsPerAtom converts Bison Relay's milliatom tip amounts to atoms.
const matomsPerAtom = 1000

// botBackend adapts the current bisonbotkit bot to PMSender. The bot is
// swapped on reconnect, so the pointer is guarded.
type botBackend struct {
	mu  sync.Mutex
	bot *kit.Bot
}

func (bb *botBackend) current() *kit.Bot {
	bb.mu.Lock()
	defer bb.mu.Unlock()
	return bb.bot
}

func (bb *botBackend) set(b *kit.Bot) {
	bb.mu.Lock()
	bb.bot = b
	bb.mu.Unlock()
}

func (bb *botBackend) SendPM(ctx context.Context, peer, text string) error {
	bot := bb.current()
	if bot == nil {
		return errors.New("brmcp: bot not connected")
	}
	return bot.SendPM(ctx, peer, text)
}

// RunBotHooks lets a host observe what RunBot otherwise consumes alone,
// e.g. to drive a directory.Registrant beside the harness. Every hook may
// be nil.
type RunBotHooks struct {
	// OnRouter delivers the started router once, before any traffic
	// flows, so the host can dial out over the bot's own identity.
	OnRouter func(router *brmcp.Router)
	// OnBot delivers the current bot on each (re)connect and nil when
	// the connection is lost.
	OnBot func(bot *kit.Bot)
	// OnTipProgress sees every tip progress event before it is acked.
	OnTipProgress func(ev *types.TipProgressEvent)
	// OnPM sees every inbound PM that is NOT an MCP wire envelope - the
	// plain chat the router deliberately ignores so the DM thread stays
	// usable for humans. Hosts build chat commands on it.
	OnPM func(uid, text string)
	// OnTipReceived fires after an inbound tip has been processed
	// (credited to the ledger when the peer is allowed, journaled, acked)
	// so hosts can react to payments without polling the ledger.
	OnTipReceived func(uid string, atoms int64)
}

// dispatchPM routes one inbound PM: MCP wire envelopes go to the router,
// everything else is plain human chat and goes to the host's OnPM hook.
func dispatchPM(router *brmcp.Router, hooks RunBotHooks, uid, text string) {
	if _, isEnvelope := wire.Parse(text); !isEnvelope {
		if hooks.OnPM != nil {
			hooks.OnPM(uid, text)
		}
		return
	}
	router.HandlePM(uid, text)
}

// RunBot serves the harness over a bisonbotkit bot until ctx ends. It owns
// the notification channels, credits tips from allowed peers into the
// ledger, and recreates the bot whenever its clientrpc websocket dies (the
// kit's Run returns with it).
func RunBot(ctx context.Context, h *Harness, cfg *kitconfig.BotConfig) error {
	return RunBotHooked(ctx, h, cfg, RunBotHooks{})
}

// RunBotHooked is RunBot with host observation hooks.
func RunBotHooked(ctx context.Context, h *Harness, cfg *kitconfig.BotConfig, hooks RunBotHooks) error {
	pmChan := make(chan *types.ReceivedPM, 64)
	tipChan := make(chan *types.ReceivedTip, 16)
	tipProgressChan := make(chan *types.TipProgressEvent, 16)
	cfg.PMChan = pmChan
	cfg.TipReceivedChan = tipChan
	cfg.TipProgressChan = tipProgressChan

	// Tips redelivered after a crash between credit and ack must not
	// credit twice; the journal needs a DataDir to live in.
	var tips *TipJournal
	if h.cfg.DataDir != "" {
		var err error
		tips, err = OpenTipJournal(filepath.Join(h.cfg.DataDir, "tips.json"))
		if err != nil {
			return err
		}
	}

	backend := &botBackend{}
	router := h.Start(ctx, backend)
	if hooks.OnRouter != nil {
		hooks.OnRouter(router)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case pm := <-pmChan:
				if pm == nil || pm.Msg == nil {
					continue
				}
				dispatchPM(router, hooks, hex.EncodeToString(pm.Uid), pm.Msg.Message)
			}
		}
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case tip := <-tipChan:
				if tip == nil {
					continue
				}
				uid := hex.EncodeToString(tip.Uid)
				atoms := tip.AmountMatoms / matomsPerAtom
				already := tips != nil && tips.Seen(tip.SequenceId)
				// Only allowed peers earn tool credit; anyone else's tip is
				// still received by the wallet but must not grow the ledger.
				// A redelivered tip (crash between credit and ack) only
				// needs its acknowledgement.
				if h.Allowed(uid) && !already {
					if err := h.Billing().Credit(uid, atoms); err != nil {
						h.logf("brmcp: credit tip from %s: %v", uid[:8], err)
						continue
					}
					h.logf("brmcp: tip from %s credited %d atoms", uid[:8], atoms)
				}
				if tips != nil && !already {
					if err := tips.Record(tip.SequenceId); err != nil {
						h.logf("brmcp: record tip %d: %v", tip.SequenceId, err)
					}
				}
				if bot := backend.current(); bot != nil {
					if err := bot.AckTipReceived(ctx, tip.SequenceId); err != nil {
						h.logf("brmcp: ack tip %d: %v", tip.SequenceId, err)
					}
				}
				if hooks.OnTipReceived != nil && !already {
					hooks.OnTipReceived(uid, atoms)
				}
			}
		}
	}()
	// The kit blocks its tip stream until progress events are consumed.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-tipProgressChan:
				if ev == nil {
					continue
				}
				if hooks.OnTipProgress != nil {
					hooks.OnTipProgress(ev)
				}
				if bot := backend.current(); bot != nil {
					if err := bot.AckTipProgress(ctx, ev.SequenceId); err != nil {
						h.logf("brmcp: ack tip progress %d: %v", ev.SequenceId, err)
					}
				}
			}
		}
	}()

	for {
		bot, err := kit.NewBot(cfg)
		if err != nil {
			h.logf("brmcp: bot init: %v (retrying)", err)
		} else {
			backend.set(bot)
			if hooks.OnBot != nil {
				hooks.OnBot(bot)
			}
			err = bot.Run(ctx)
			backend.set(nil)
			if hooks.OnBot != nil {
				hooks.OnBot(nil)
			}
			bot.Close()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			h.logf("brmcp: bot run ended: %v (reconnecting)", err)
		}
		select {
		case <-time.After(3 * time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
