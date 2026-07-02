// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package brmcp

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/decred/dcrlnd/lnrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// DcrlndConfig locates the operator's dcrlnd for the invoice payment rail.
// Bison Relay clients already run beside dcrlnd, so this points at the same
// node the bot's brclient uses.
type DcrlndConfig struct {
	Addr         string `json:"addr"`
	TLSCertPath  string `json:"tls_cert"`
	MacaroonPath string `json:"macaroon"`
}

// invoiceExpiry is how long an issued top-up invoice stays payable. Long
// enough for a human to approve on the client side, short enough that the
// pending map cannot grow unbounded.
const invoiceExpiry = time.Hour

// InvoiceIssuer creates BOLT11 invoices on dcrlnd and credits the billing
// store when they settle. The ledger persists the invoice book (payment
// hash correlation + resume index) even when billing is external.
type InvoiceIssuer struct {
	ln      lnrpc.LightningClient
	ledger  *Ledger
	billing Billing
	logf    func(format string, args ...any)
}

func NewInvoiceIssuer(cfg DcrlndConfig, ledger *Ledger, billing Billing, logf func(string, ...any)) (*InvoiceIssuer, error) {
	if cfg.Addr == "" || cfg.TLSCertPath == "" || cfg.MacaroonPath == "" {
		return nil, errors.New("brmcp: incomplete dcrlnd config")
	}
	tlsCreds, err := credentials.NewClientTLSFromFile(cfg.TLSCertPath, "")
	if err != nil {
		return nil, fmt.Errorf("brmcp: dcrlnd tls cert: %w", err)
	}
	conn, err := grpc.NewClient(cfg.Addr,
		grpc.WithTransportCredentials(tlsCreds),
		grpc.WithPerRPCCredentials(macaroonCreds{path: cfg.MacaroonPath}),
	)
	if err != nil {
		return nil, fmt.Errorf("brmcp: dial dcrlnd: %w", err)
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	if billing == nil {
		billing = ledger
	}
	return &InvoiceIssuer{ln: lnrpc.NewLightningClient(conn), ledger: ledger, billing: billing, logf: logf}, nil
}

// Issue creates an invoice whose settlement will credit uid with atoms.
func (ii *InvoiceIssuer) Issue(ctx context.Context, uid string, atoms int64) (payReq string, expiry int64, err error) {
	resp, err := ii.ln.AddInvoice(ctx, &lnrpc.Invoice{
		Memo:   "brmcp " + uid[:8],
		Value:  atoms,
		Expiry: int64(invoiceExpiry / time.Second),
	})
	if err != nil {
		return "", 0, err
	}
	exp := time.Now().Add(invoiceExpiry).Unix()
	if err := ii.ledger.AddPendingInvoice(hex.EncodeToString(resp.RHash), uid, atoms, exp); err != nil {
		return "", 0, err
	}
	return resp.PaymentRequest, exp, nil
}

// Watch consumes invoice settlements until ctx ends, reconnecting with
// backoff. Resumes from the persisted settle index so a payment landing
// while the harness was down still credits.
func (ii *InvoiceIssuer) Watch(ctx context.Context) {
	for ctx.Err() == nil {
		err := ii.watchOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		ii.logf("brmcp: invoice subscription lost: %v (retrying)", err)
		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
			return
		}
	}
}

func (ii *InvoiceIssuer) watchOnce(ctx context.Context) error {
	stream, err := ii.ln.SubscribeInvoices(ctx, &lnrpc.InvoiceSubscription{
		SettleIndex: ii.ledger.SettleIndex(),
	})
	if err != nil {
		return err
	}
	for {
		inv, err := stream.Recv()
		if err != nil {
			return err
		}
		if inv.State != lnrpc.Invoice_SETTLED {
			continue
		}
		rhash := hex.EncodeToString(inv.RHash)
		uid, atoms, ok := ii.ledger.ResolvePendingInvoice(rhash, inv.SettleIndex)
		if !ok {
			continue
		}
		if err := ii.billing.Credit(uid, atoms); err != nil {
			ii.logf("brmcp: credit settled invoice %s (%d atoms to %s): %v",
				rhash[:8], atoms, uid[:8], err)
			continue
		}
		ii.logf("brmcp: invoice %s settled, credited %d atoms to %s", rhash[:8], atoms, uid[:8])
	}
}

// macaroonCreds attaches dcrlnd's macaroon as hex metadata, read fresh per
// call so a rotated macaroon is picked up without a restart.
type macaroonCreds struct {
	path string
}

func (m macaroonCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	raw, err := os.ReadFile(m.path)
	if err != nil {
		return nil, fmt.Errorf("read dcrlnd macaroon: %w", err)
	}
	return map[string]string{"macaroon": hex.EncodeToString(raw)}, nil
}

func (macaroonCreds) RequireTransportSecurity() bool { return true }
