// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package directory_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/karamble/brmcp"
	"github.com/karamble/brmcp/directory"
)

const fakeDirUID = "6666666666666666666666666666666666666666666666666666666666666666"

// notifyLog collects Registrant outcome callbacks.
type notifyLog struct {
	mu sync.Mutex
	ch chan struct{}
	st []directory.StatusOut
	er []error
}

func newNotifyLog() *notifyLog { return &notifyLog{ch: make(chan struct{}, 16)} }

func (n *notifyLog) cb(_ string, st directory.StatusOut, err error) {
	n.mu.Lock()
	n.st = append(n.st, st)
	n.er = append(n.er, err)
	n.mu.Unlock()
	n.ch <- struct{}{}
}

func (n *notifyLog) last(t *testing.T) (directory.StatusOut, error) {
	t.Helper()
	select {
	case <-n.ch:
	case <-time.After(30 * time.Second):
		t.Fatal("no notify")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.st[len(n.st)-1], n.er[len(n.er)-1]
}

func (fx *fixture) newRegistrant(t *testing.T, fund directory.AutoFund, notify *notifyLog) *directory.Registrant {
	t.Helper()
	r, err := directory.NewRegistrant(directory.RegistrantConfig{
		Description: "echo services",
		Tags:        []string{"echo"},
		Test: directory.TestSpec{
			Tool:     "paid_echo",
			Args:     map[string]any{"text": "proof"},
			MaxAtoms: testBudget,
		},
		AutoFund: fund,
		DataDir:  t.TempDir(),
		Router:   fx.provRouter,
		Payer: directory.PayerFunc(func(_ context.Context, payeeUID string, atoms int64) error {
			if payeeUID != dirUID {
				t.Errorf("registrant paid %s", payeeUID[:4])
			}
			fx.svc.CreditTip(providerUID, atoms)
			return nil
		}),
		Clock:  fx.clk,
		Notify: notify.cb,
		Logf:   t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRegistrantProactiveRegister(t *testing.T) {
	fx := newFixture(t)
	notify := newNotifyLog()
	reg := fx.newRegistrant(t, directory.AutoFund{
		Enabled:            true,
		MaxAtomsPerRequest: feeAtoms + testBudget,
		MaxAtomsPerMonth:   10 * (feeAtoms + testBudget),
	}, notify)

	if err := reg.Register(fx.ctx, dirUID); err != nil {
		t.Fatal(err)
	}
	st, err := notify.last(t)
	if err != nil {
		t.Fatalf("notify carried error: %v", err)
	}
	if st.State == directory.StateAwaitingFunding && st.ShortfallAtoms > 0 {
		t.Fatalf("funding did not land: %+v", st)
	}

	// Verification completes directory-side; approve and confirm.
	prov := fx.dialFrom(fx.provRouter, "prov")
	fx.waitStatus(prov, directory.StatePendingReview)
	admin := fx.dial(adminUID)
	var out directory.StatusOut
	if err := fx.call(admin, "approve", map[string]any{"uid": providerUID}, &out); err != nil {
		t.Fatal(err)
	}
	if out.State != directory.StateListed {
		t.Fatalf("not listed after approve: %+v", out)
	}
	if got := fx.execs.Load(); got != 1 {
		t.Fatalf("test executions %d", got)
	}
}

func TestRegistrantSkipsFreshListing(t *testing.T) {
	fx := newFixture(t)
	prov := fx.dialFrom(fx.provRouter, "prov")
	admin := fx.dial(adminUID)
	fx.listProvider(prov, admin)

	// A restart-style Register on a fresh listing must not renew: no new
	// registration, no money moved, the listed status reported as-is.
	notify := newNotifyLog()
	reg := fx.newRegistrant(t, directory.AutoFund{
		Enabled:            true,
		MaxAtomsPerRequest: feeAtoms + testBudget,
	}, notify)
	if err := reg.Register(fx.ctx, dirUID); err != nil {
		t.Fatal(err)
	}
	st, err := notify.last(t)
	if err != nil || st.State != directory.StateListed {
		t.Fatalf("fresh listing not skipped: %+v err=%v", st, err)
	}
	var pend directory.PendingListOut
	if err := fx.call(admin, "pending_registrations", nil, &pend); err != nil {
		t.Fatal(err)
	}
	if len(pend.Registrations) != 0 {
		t.Fatalf("skip still registered: %+v", pend)
	}
	if got := fx.execs.Load(); got != 1 {
		t.Fatalf("skip re-verified: %d executions", got)
	}
}

func TestRegistrantRenewsNearExpiry(t *testing.T) {
	fx := newFixture(t)
	prov := fx.dialFrom(fx.provRouter, "prov")
	admin := fx.dial(adminUID)
	fx.listProvider(prov, admin)

	// Six days before expiry the same Register call renews for real.
	fx.clk.Advance(24 * 24 * time.Hour)
	notify := newNotifyLog()
	reg := fx.newRegistrant(t, directory.AutoFund{
		Enabled:            true,
		MaxAtomsPerRequest: feeAtoms + testBudget,
		MaxAtomsPerMonth:   10 * (feeAtoms + testBudget),
	}, notify)
	if err := reg.Register(fx.ctx, dirUID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		var po directory.ProviderOut
		if err := fx.call(prov, "get_provider", map[string]any{"uid": providerUID}, &po); err != nil {
			t.Fatal(err)
		}
		if po.RenewedAt != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("near-expiry register never renewed: %+v", po)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := fx.execs.Load(); got != 2 {
		t.Fatalf("renewal executions %d, want 2", got)
	}
}

func TestRegistrantFundingCaps(t *testing.T) {
	fx := newFixture(t)
	notify := newNotifyLog()

	// Per-request cap below the invoice: register succeeds, funding is
	// declined, nothing is paid.
	reg := fx.newRegistrant(t, directory.AutoFund{
		Enabled:            true,
		MaxAtomsPerRequest: feeAtoms + testBudget - 1,
	}, notify)
	if err := reg.Register(fx.ctx, dirUID); err == nil {
		t.Fatal("over-cap funding not declined")
	}
	if _, err := notify.last(t); err == nil {
		t.Fatal("notify missing the refusal")
	}
	prov := fx.dialFrom(fx.provRouter, "prov")
	var st directory.StatusOut
	if err := fx.call(prov, "my_status", nil, &st); err != nil {
		t.Fatal(err)
	}
	if st.State != directory.StateAwaitingFunding || st.BalanceAtoms != 0 {
		t.Fatalf("money moved despite the cap: %+v", st)
	}

	// Disabled auto-funding declines too.
	reg = fx.newRegistrant(t, directory.AutoFund{}, notify)
	if err := reg.Register(fx.ctx, dirUID); err == nil {
		t.Fatal("disabled auto-fund still paid")
	}
	notify.last(t)
}

// dialProvider opens a session to the provider harness from a fresh uid.
func (fx *fixture) dialProvider(uid, name string) *mcp.ClientSession {
	fx.t.Helper()
	r := fx.fab.NewRouter(uid, brmcp.RouterConfig{Logf: fx.t.Logf})
	return fx.dialTo(r, providerUID, name)
}

func TestListingInvitePolicy(t *testing.T) {
	fx := newFixture(t)
	notify := newNotifyLog()

	// Allowlist excludes the caller: polite decline, no registration.
	reg := fx.newRegistrant(t, directory.AutoFund{
		Enabled:              true,
		MaxAtomsPerRequest:   feeAtoms + testBudget,
		AllowedDirectoryUIDs: []string{dirUID},
	}, notify)
	reg.RegisterTools(fx.provider)
	reg.Start(fx.ctx)

	caller := fx.dialProvider(fakeDirUID, "fakedir")
	res, err := caller.CallTool(fx.ctx, &mcp.CallToolParams{
		Name: "listing_invite",
		Arguments: map[string]any{
			"directory":       "evil-dir",
			"listingFeeAtoms": 10,
		},
	})
	if err != nil || res.IsError {
		t.Fatalf("invite call failed: err=%v res=%+v", err, res)
	}
	var out directory.InviteOut
	if err := fx.callDecodeResult(res, &out); err != nil {
		t.Fatal(err)
	}
	if out.Accepted || out.Note == "" {
		t.Fatalf("unlisted directory accepted: %+v", out)
	}
	notify.last(t)
}
