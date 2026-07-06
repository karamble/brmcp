// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package directory_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/karamble/brmcp/directory"
)

// listProvider drives the happy registration path to a live listing.
func (fx *fixture) listProvider(prov, admin *mcp.ClientSession) {
	fx.t.Helper()
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
		fx.t.Fatal(err)
	}
	fx.svc.CreditTip(providerUID, feeAtoms+testBudget)
	fx.waitStatus(prov, directory.StatePendingReview)
	if err := fx.call(admin, "approve", map[string]any{"uid": providerUID}, &st); err != nil {
		fx.t.Fatal(err)
	}
	if st.State != directory.StateListed {
		fx.t.Fatalf("listing failed: %+v", st)
	}
}

func TestRenewalSkipsReview(t *testing.T) {
	fx := newFixture(t)
	prov := fx.dialFrom(fx.provRouter, "prov")
	admin := fx.dial(adminUID)
	fx.listProvider(prov, admin)

	var before directory.ProviderOut
	if err := fx.call(prov, "get_provider", map[string]any{"uid": providerUID}, &before); err != nil {
		t.Fatal(err)
	}

	// Renew: same registration flow, but a passing test promotes with no
	// admin involvement while the live listing keeps serving.
	var st directory.StatusOut
	err := fx.call(prov, "register", map[string]any{
		"description": "echo services, renewed",
		"tags":        []string{"echo"},
		"test": map[string]any{
			"tool":     "paid_echo",
			"args":     map[string]any{"text": "again"},
			"maxAtoms": testBudget,
		},
	}, &st)
	if err != nil {
		t.Fatal(err)
	}

	// The live listing answers searches while the renewal runs.
	consumer := fx.dial(consumerUID)
	var sr directory.SearchOut
	if err := fx.call(consumer, "search", map[string]any{"query": "echo"}, &sr); err != nil {
		t.Fatal(err)
	}
	if sr.Total == 0 {
		t.Fatal("live listing vanished during renewal")
	}

	fx.svc.CreditTip(providerUID, feeAtoms+testBudget)
	deadline := time.Now().Add(30 * time.Second)
	var after directory.ProviderOut
	for {
		if err := fx.call(prov, "get_provider", map[string]any{"uid": providerUID}, &after); err != nil {
			t.Fatal(err)
		}
		if after.RenewedAt != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("renewal never promoted: %+v", after)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if after.ApprovedAt != before.ApprovedAt {
		t.Fatalf("renewal changed the approval date: %d -> %d", before.ApprovedAt, after.ApprovedAt)
	}
	if after.Description != "echo services, renewed" {
		t.Fatalf("renewal did not refresh the listing: %+v", after)
	}
	if got := fx.execs.Load(); got != 2 {
		t.Fatalf("test executions %d, want 2 (initial + renewal)", got)
	}
	var pend directory.PendingListOut
	if err := fx.call(admin, "pending_registrations", nil, &pend); err != nil {
		t.Fatal(err)
	}
	if len(pend.Registrations) != 0 {
		t.Fatalf("renewal parked for review: %+v", pend)
	}
}

func TestExpiryGC(t *testing.T) {
	fx := newFixture(t)
	prov := fx.dialFrom(fx.provRouter, "prov")
	admin := fx.dial(adminUID)
	fx.listProvider(prov, admin)

	// A month passes; the armed sweep timer fires and collects the lapsed
	// listing.
	fx.clk.Advance(31 * 24 * time.Hour)
	consumer := fx.dial(consumerUID)
	deadline := time.Now().Add(10 * time.Second)
	for {
		var sr directory.SearchOut
		if err := fx.call(consumer, "search", map[string]any{"query": "echo"}, &sr); err != nil {
			t.Fatal(err)
		}
		if sr.Total == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expired listing still searchable")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := fx.call(consumer, "get_provider", map[string]any{"uid": providerUID}, nil); err == nil {
		t.Fatal("expired provider still served")
	}
}

func TestFreeRecrawlRefreshesCatalogOnly(t *testing.T) {
	fx := newFixture(t)
	prov := fx.dialFrom(fx.provRouter, "prov")
	admin := fx.dial(adminUID)
	fx.listProvider(prov, admin)

	var before directory.ProviderOut
	if err := fx.call(prov, "get_provider", map[string]any{"uid": providerUID}, &before); err != nil {
		t.Fatal(err)
	}

	// Past the recrawl age but far from expiry: the sweep refreshes the
	// catalog stamp for free; execution verification stays untouched.
	fx.clk.Advance(25 * time.Hour)
	deadline := time.Now().Add(10 * time.Second)
	var after directory.ProviderOut
	for {
		if err := fx.call(prov, "get_provider", map[string]any{"uid": providerUID}, &after); err != nil {
			t.Fatal(err)
		}
		if after.CatalogCheckedAt > before.CatalogCheckedAt {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("catalog never recrawled: %+v", after)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if after.LastVerifiedExecution != before.LastVerifiedExecution {
		t.Fatalf("free recrawl moved the execution stamp: %+v", after)
	}
	if len(after.Catalog) == 0 {
		t.Fatalf("recrawl lost the catalog: %+v", after)
	}
}

// readReg reads the persisted registration state straight from index.json.
func readReg(t *testing.T, dataDir, uid string) (state, callKey string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dataDir, "index.json"))
	if err != nil {
		t.Fatal(err)
	}
	var idx map[string]struct {
		Reg *struct {
			State   string `json:"state"`
			CallKey string `json:"callKey"`
		} `json:"reg"`
	}
	if err := json.Unmarshal(raw, &idx); err != nil {
		t.Fatal(err)
	}
	e, ok := idx[uid]
	if !ok || e.Reg == nil {
		t.Fatalf("no persisted registration for %s", uid[:4])
	}
	return e.Reg.State, e.Reg.CallKey
}

func TestRestartResumesAwaitingFunding(t *testing.T) {
	fx := newFixture(t)
	prov := fx.dialFrom(fx.provRouter, "prov")

	var st directory.StatusOut
	err := fx.call(prov, "register", map[string]any{
		"description": "echo services",
		"test": map[string]any{
			"tool":     "paid_echo",
			"args":     map[string]any{"text": "proof"},
			"maxAtoms": testBudget,
		},
	}, &st)
	if err != nil {
		t.Fatal(err)
	}
	if state, _ := readReg(t, fx.dataDir, providerUID); state != directory.StateAwaitingFunding {
		t.Fatalf("persisted state %s", state)
	}

	// Restart the directory on the same data dir.
	fx.svc.Close()
	ctx2, cancel2 := context.WithCancel(fx.ctx)
	t.Cleanup(cancel2)
	svc2 := startDirectory(t, ctx2, fx.fab, fx.clk, fx.payer, fx.dataDir)
	t.Cleanup(func() { svc2.Close() })
	fx.svc = svc2

	prov2 := fx.dialFrom(fx.provRouter, "prov2")
	if err := fx.call(prov2, "my_status", nil, &st); err != nil {
		t.Fatal(err)
	}
	if st.State != directory.StateAwaitingFunding {
		t.Fatalf("registration lost across restart: %+v", st)
	}
	svc2.CreditTip(providerUID, feeAtoms+testBudget)
	fx.waitStatus(prov2, directory.StatePendingReview)
	if got := fx.execs.Load(); got != 1 {
		t.Fatalf("test executed %d times, want 1", got)
	}
}

func TestRestartResumesTestingWithSameCallKey(t *testing.T) {
	fx := newFixture(t)
	prov := fx.dialFrom(fx.provRouter, "prov")

	// Block the rail inside Pay so the shutdown lands mid-payment.
	payStarted := make(chan struct{})
	started := false
	fx.payer.setFn(func(ctx context.Context, payeeUID string, atoms int64) error {
		if !started {
			started = true
			close(payStarted)
		}
		<-ctx.Done()
		return ctx.Err()
	})

	var st directory.StatusOut
	err := fx.call(prov, "register", map[string]any{
		"description": "echo services",
		"test": map[string]any{
			"tool":     "paid_echo",
			"args":     map[string]any{"text": "proof"},
			"maxAtoms": testBudget,
		},
	}, &st)
	if err != nil {
		t.Fatal(err)
	}
	fx.svc.CreditTip(providerUID, feeAtoms+testBudget)
	select {
	case <-payStarted:
	case <-time.After(30 * time.Second):
		t.Fatal("payment never launched")
	}

	// Crash mid-payment: the persisted state must stay resumable with the
	// same call key, not park as a failure.
	fx.svc.Close()
	state, key1 := readReg(t, fx.dataDir, providerUID)
	if state != directory.StateTesting || key1 == "" {
		t.Fatalf("mid-payment shutdown persisted state=%s key=%q", state, key1)
	}

	// Restart with a working rail; the pipeline resumes and completes.
	fx.payer.setFn(nil)
	ctx2, cancel2 := context.WithCancel(fx.ctx)
	t.Cleanup(cancel2)
	svc2 := startDirectory(t, ctx2, fx.fab, fx.clk, fx.payer, fx.dataDir)
	t.Cleanup(func() { svc2.Close() })
	fx.svc = svc2

	prov2 := fx.dialFrom(fx.provRouter, "prov2")
	st = fx.waitStatus(prov2, directory.StatePendingReview)
	if st.Note != "" {
		t.Fatalf("resumed pipeline parked with note: %+v", st)
	}
	if _, key2 := readReg(t, fx.dataDir, providerUID); key2 != key1 {
		t.Fatalf("restart minted a new call key: %q -> %q", key1, key2)
	}
	if got := fx.execs.Load(); got != 1 {
		t.Fatalf("test executed %d times, want exactly 1", got)
	}

	// Both payment launches are on the record: the interrupted one and
	// the successful one. Record-at-launch never hides ambiguous money.
	admin := fx.dial(adminUID)
	var stats struct {
		SpendEntries int   `json:"spendEntries"`
		SpendAtoms   int64 `json:"spendAtoms"`
	}
	if err := fx.call(admin, "stats", nil, &stats); err != nil {
		t.Fatal(err)
	}
	if stats.SpendEntries != 2 || stats.SpendAtoms != 2*paidPrice {
		t.Fatalf("spend journal wrong: %+v", stats)
	}
}
