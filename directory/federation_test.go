// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package directory_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/karamble/brmcp"
	"github.com/karamble/brmcp/directory"
	"github.com/karamble/brmcp/server"
)

var (
	dirBUID  = "7777777777777777777777777777777777777777777777777777777777777777"
	lonerUID = "8888888888888888888888888888888888888888888888888888888888888888"
)

// listOnDirectory drives register -> fund -> approve against one
// directory service.
func listOnDirectory(t *testing.T, fx *fixture, svc *directory.Service,
	prov, admin *mcp.ClientSession, uid string) {
	t.Helper()
	var st directory.StatusOut
	err := fx.call(prov, "register", map[string]any{
		"description": "echo services",
		"tags":        []string{"echo"},
		"test": map[string]any{
			"tool":     "paid_echo",
			"args":     map[string]any{"text": "proof"},
			"maxAtoms": testBudget,
		},
	}, &st)
	if err != nil {
		t.Fatal(err)
	}
	svc.CreditTip(uid, feeAtoms+testBudget)
	fx.waitStatus(prov, directory.StatePendingReview)
	if err := fx.call(admin, "approve", map[string]any{"uid": uid}, &st); err != nil {
		t.Fatal(err)
	}
	if st.State != directory.StateListed {
		t.Fatalf("listing failed: %+v", st)
	}
}

func TestFederationE2E(t *testing.T) {
	fx := newFixture(t)

	// Directory B: the federation peer, with its own payment rail to the
	// provider for its own verification test.
	payerB := &testPayer{rails: map[string]*server.Harness{providerUID: fx.provider}, payer: dirBUID}
	svcB := startDirectoryAt(t, fx.ctx, fx.fab, fx.clk, payerB, t.TempDir(), dirBUID,
		directory.IntroducerFunc(func(context.Context, string, string) error {
			t.Error("peer directory introduced unexpectedly")
			return nil
		}))
	t.Cleanup(func() { svcB.Close() })
	// A pays B for snapshots over this rail.
	fx.payer.setRail(dirBUID, svcB.Harness())

	// The provider gets listed on B first.
	adminRouter := fx.fab.NewRouter(adminUID, brmcp.RouterConfig{Logf: t.Logf})
	adminB := fx.dialTo(adminRouter, dirBUID, "adminB")
	provB := fx.dialTo(fx.provRouter, dirBUID, "provB")
	listOnDirectory(t, fx, svcB, provB, adminB, providerUID)

	adminA := fx.dialTo(adminRouter, dirUID, "adminA")

	// verify_peer refuses a peer that is not curated yet.
	if err := fx.call(adminA, "verify_peer", map[string]any{"uid": dirBUID}, nil); err == nil {
		t.Fatal("uncurated peer verified")
	}

	// A tampered trust root: the snapshot is bought but must not verify,
	// and no leads may appear.
	if err := fx.call(adminA, "add_peer", map[string]any{
		"uid": dirBUID, "pubKey": strings.Repeat("00", 32),
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := fx.call(adminA, "verify_peer", map[string]any{"uid": dirBUID}, nil); err == nil {
		t.Fatal("snapshot verified against the wrong key")
	}
	var leads []directory.Lead
	if err := fx.call(adminA, "leads", nil, &leads); err != nil {
		t.Fatal(err)
	}
	if len(leads) != 0 {
		t.Fatalf("leads from an unverified snapshot: %+v", leads)
	}

	// The real key: verification yields exactly one lead (the provider;
	// B itself and A are excluded).
	if err := fx.call(adminA, "remove_peer", map[string]any{"uid": dirBUID}, nil); err != nil {
		t.Fatal(err)
	}
	if err := fx.call(adminA, "add_peer", map[string]any{
		"uid": dirBUID, "pubKey": svcB.PublicKey(),
	}, nil); err != nil {
		t.Fatal(err)
	}
	var vr map[string]int
	if err := fx.call(adminA, "verify_peer", map[string]any{"uid": dirBUID}, &vr); err != nil {
		t.Fatal(err)
	}
	if vr["newLeads"] != 1 {
		t.Fatalf("verify_peer leads: %+v", vr)
	}
	if err := fx.call(adminA, "leads", nil, &leads); err != nil {
		t.Fatal(err)
	}
	if len(leads) != 1 || leads[0].ProviderUID != providerUID ||
		leads[0].PeerUID != dirBUID || leads[0].State != directory.LeadNew {
		t.Fatalf("lead wrong: %+v", leads)
	}

	// The provider runs a Registrant that accepts invites from A and
	// auto-funds; pursuing the lead completes the whole loop.
	notify := newNotifyLog()
	reg := fx.newRegistrant(t, directory.AutoFund{
		Enabled:              true,
		MaxAtomsPerRequest:   feeAtoms + testBudget,
		MaxAtomsPerMonth:     10 * (feeAtoms + testBudget),
		AllowedDirectoryUIDs: []string{dirUID},
	}, notify)
	reg.RegisterTools(fx.provider)
	reg.Start(fx.ctx)

	var lead directory.Lead
	if err := fx.call(adminA, "pursue_lead", map[string]any{"uid": providerUID}, &lead); err != nil {
		t.Fatal(err)
	}
	if lead.State != directory.LeadInvited {
		t.Fatalf("direct invite did not land: %+v", lead)
	}
	if fx.introCalls.Load() != 0 {
		t.Fatal("introducer used although the provider was reachable")
	}

	// The accepted invite registers back at A asynchronously.
	provA := fx.dialFrom(fx.provRouter, "provA")
	fx.waitStatus(provA, directory.StatePendingReview)
	var st directory.StatusOut
	if err := fx.call(adminA, "approve", map[string]any{"uid": providerUID}, &st); err != nil {
		t.Fatal(err)
	}
	if st.State != directory.StateListed {
		t.Fatalf("approve on A failed: %+v", st)
	}
	if err := fx.call(adminA, "leads", nil, &leads); err != nil {
		t.Fatal(err)
	}
	if len(leads) != 1 || leads[0].State != directory.LeadConverted {
		t.Fatalf("lead not converted: %+v", leads)
	}

	// Verified independently on both directories: two paid executions.
	if got := fx.execs.Load(); got != 2 {
		t.Fatalf("provider verified %d times, want 2 (B + A)", got)
	}

	consumer := fx.dial(consumerUID)
	var sr directory.SearchOut
	if err := fx.call(consumer, "search", map[string]any{"query": "paid"}, &sr); err != nil {
		t.Fatal(err)
	}
	if sr.Total != 1 || sr.Hits[0].ProviderUID != providerUID {
		t.Fatalf("provider not searchable on A: %+v", sr)
	}
}

func TestFederationIntroducerFallback(t *testing.T) {
	fx := newFixture(t)

	// Peer B lists a provider that runs no Registrant yet.
	lonerExecs := &atomic.Int64{}
	lonerH, lonerRouter := newProviderHarness(t, fx.fab, fx.ctx, lonerUID, lonerExecs)
	payerB := &testPayer{rails: map[string]*server.Harness{lonerUID: lonerH}, payer: dirBUID}
	svcB := startDirectoryAt(t, fx.ctx, fx.fab, fx.clk, payerB, t.TempDir(), dirBUID,
		directory.IntroducerFunc(func(context.Context, string, string) error { return nil }))
	t.Cleanup(func() { svcB.Close() })
	fx.payer.setRail(dirBUID, svcB.Harness())
	fx.payer.setRail(lonerUID, lonerH)

	adminRouter := fx.fab.NewRouter(adminUID, brmcp.RouterConfig{Logf: t.Logf})
	adminB := fx.dialTo(adminRouter, dirBUID, "adminB")
	provLB := fx.dialTo(lonerRouter, dirBUID, "lonerB")
	listOnDirectory(t, fx, svcB, provLB, adminB, lonerUID)

	adminA := fx.dialTo(adminRouter, dirUID, "adminA")
	if err := fx.call(adminA, "add_peer", map[string]any{
		"uid": dirBUID, "pubKey": svcB.PublicKey(),
	}, nil); err != nil {
		t.Fatal(err)
	}
	var vr map[string]int
	if err := fx.call(adminA, "verify_peer", map[string]any{"uid": dirBUID}, &vr); err != nil {
		t.Fatal(err)
	}
	if vr["newLeads"] != 1 {
		t.Fatalf("leads: %+v", vr)
	}

	// No listing_invite tool answers, so the pursuit falls back to a
	// transitive introduction through B and parks until KX completes.
	var lead directory.Lead
	if err := fx.call(adminA, "pursue_lead", map[string]any{"uid": lonerUID}, &lead); err != nil {
		t.Fatal(err)
	}
	if lead.State != directory.LeadPursuing {
		t.Fatalf("fallback did not park the lead: %+v", lead)
	}
	if fx.introCalls.Load() != 1 {
		t.Fatalf("introducer calls: %d", fx.introCalls.Load())
	}

	// The loner restarts with a Registrant wired in (per-peer tool
	// servers are built at first contact, so gaining a tool means a
	// restart); the completed KX then resumes the pursuit, the invite is
	// accepted, and registration reaches A.
	lonerH2, lonerRouter2 := newProviderHarness(t, fx.fab, fx.ctx, lonerUID, lonerExecs)
	fx.payer.setRail(lonerUID, lonerH2)
	notify := newNotifyLog()
	reg2, err := directory.NewRegistrant(directory.RegistrantConfig{
		Description: "loner echo",
		Test: directory.TestSpec{
			Tool:     "paid_echo",
			Args:     map[string]any{"text": "proof"},
			MaxAtoms: testBudget,
		},
		AutoFund: directory.AutoFund{
			Enabled:              true,
			MaxAtomsPerRequest:   feeAtoms + testBudget,
			AllowedDirectoryUIDs: []string{dirUID},
		},
		DataDir: t.TempDir(),
		Router:  lonerRouter2,
		Payer: directory.PayerFunc(func(_ context.Context, payee string, atoms int64) error {
			fx.svc.CreditTip(lonerUID, atoms)
			return nil
		}),
		Clock:  fx.clk,
		Notify: notify.cb,
		Logf:   t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	reg2.RegisterTools(lonerH2)
	reg2.Start(fx.ctx)
	fx.svc.NotifyKX(lonerUID)

	lonerA := fx.dialTo(lonerRouter2, dirUID, "lonerA")
	fx.waitStatus(lonerA, directory.StatePendingReview)

	deadline := time.Now().Add(10 * time.Second)
	for {
		var leads []directory.Lead
		if err := fx.call(adminA, "leads", nil, &leads); err != nil {
			t.Fatal(err)
		}
		if len(leads) == 1 && leads[0].State == directory.LeadConverted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lead never converted: %+v", leads)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if lonerExecs.Load() != 2 {
		t.Fatalf("loner verified %d times, want 2 (B + A)", lonerExecs.Load())
	}
}
