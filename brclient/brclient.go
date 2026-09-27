// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package brclient hosts the brmcp client bridge on an embedded Bison Relay
// client (github.com/companyzero/bisonrelay/client). Attach supplies the
// bridge's PM sender, its tip payer and its inbound PM feed from the client.
//
// Peers are addressed by uid only. The bridge never knows a peer by nick,
// and resolving one here could pay the wrong user.
package brclient

import (
	"context"
	"time"

	"github.com/companyzero/bisonrelay/client"
	"github.com/companyzero/bisonrelay/client/clientintf"
	"github.com/companyzero/bisonrelay/rpc"
	"github.com/decred/dcrd/dcrutil/v4"

	"github.com/karamble/brmcp/bridge"
)

type pmer interface {
	PM(uid clientintf.UserID, msg string) error
}

type tipper interface {
	TipUser(uid clientintf.UserID, dcrAmount float64, maxAttempts int32) error
}

// Sender implements brmcp.PMSender over a Bison Relay client.
type Sender struct {
	c pmer
}

// NewSender returns a Sender that sends through c.
func NewSender(c *client.Client) Sender {
	return Sender{c: c}
}

// SendPM implements brmcp.PMSender.
func (s Sender) SendPM(_ context.Context, peer, text string) error {
	var uid clientintf.UserID
	if err := uid.FromString(peer); err != nil {
		return err
	}
	return s.c.PM(uid, text)
}

func tipFunc(c tipper) bridge.TipFunc {
	return func(_ context.Context, payeeUID string, atoms int64) error {
		var uid clientintf.UserID
		if err := uid.FromString(payeeUID); err != nil {
			return err
		}
		// One attempt is one invoice request over the relay; the client
		// retries the LN payment within it.
		return c.TipUser(uid, dcrutil.Amount(atoms).ToCoin(), 1)
	}
}

// NewTipPayer returns a payer that tips through c and follows the client's
// tip-progress notifications for the outcome.
func NewTipPayer(c *client.Client) *bridge.TipPayer {
	p := bridge.NewTipPayer(tipFunc(c))
	c.NotificationManager().Register(client.OnTipAttemptProgressNtfn(func(ru *client.RemoteUser,
		amtMAtoms int64, completed bool, _ int, attemptErr error, willRetry bool) {

		p.Progress(ru.ID().String(), amtMAtoms, completed, attemptErr, willRetry)
	}))
	return p
}

// Attach builds a bridge on c. It fills cfg.Sender and cfg.Payer, records
// late tip outcomes in the bridge's spend log, and feeds it every inbound
// PM. The caller still calls Start, and still keeps brmcp envelopes
// (brmcp.IsEnvelope) out of its chat views.
func Attach(c *client.Client, cfg bridge.Config) (*bridge.Bridge, error) {
	payer := NewTipPayer(c)
	cfg.Sender = NewSender(c)
	cfg.Payer = payer
	b, err := bridge.New(cfg)
	if err != nil {
		return nil, err
	}
	payer.SetLate(func(uid string, atoms int64, err error) {
		b.ResolveSpend(uid, atoms, err)
	})
	c.NotificationManager().Register(client.OnPMNtfn(func(ru *client.RemoteUser, pm rpc.RMPrivateMessage, _ time.Time) {
		b.HandlePM(ru.ID().String(), pm.Message)
	}))
	return b, nil
}
