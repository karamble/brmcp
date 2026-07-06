// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package directory_test

import (
	"context"
	"strings"
	"testing"

	"github.com/karamble/brmcp"
	"github.com/karamble/brmcp/directory"
	"github.com/karamble/brmcp/server"
)

func TestIntroduceTool(t *testing.T) {
	fx := newFixture(t)
	prov := fx.dialFrom(fx.provRouter, "prov")
	admin := fx.dial(adminUID)
	consumer := fx.dial(consumerUID)

	// The tool is public: every caller sees it.
	tl, err := consumer.ListTools(fx.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	seen := false
	for _, tool := range tl.Tools {
		if tool.Name == "introduce" {
			seen = true
		}
	}
	if !seen {
		t.Fatal("introduce missing from the public tool list")
	}

	// Unknown uid: refused.
	if err := fx.call(consumer, "introduce", map[string]any{"uid": providerUID}, nil); err == nil {
		t.Fatal("introduction to an unlisted uid accepted")
	}

	// Pending but not yet approved: still refused - the directory only
	// vouches for verified listings.
	var st directory.StatusOut
	err = fx.call(prov, "register", map[string]any{
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
	fx.waitStatus(prov, directory.StatePendingReview)
	if err := fx.call(consumer, "introduce", map[string]any{"uid": providerUID}, nil); err == nil {
		t.Fatal("introduction to a pending registration accepted")
	}
	if got := fx.suggests.all(); len(got) != 0 {
		t.Fatalf("suggestions pushed before listing: %v", got)
	}

	// Listed: the introduction goes through and names the right pair.
	if err := fx.call(admin, "approve", map[string]any{"uid": providerUID}, &st); err != nil {
		t.Fatal(err)
	}
	var out directory.IntroduceOut
	if err := fx.call(consumer, "introduce", map[string]any{"uid": providerUID}, &out); err != nil {
		t.Fatal(err)
	}
	if !out.Suggested || out.Note == "" {
		t.Fatalf("introduce result wrong: %+v", out)
	}
	got := fx.suggests.all()
	if len(got) != 1 || got[0][0] != consumerUID || got[0][1] != providerUID {
		t.Fatalf("suggestion pair wrong: %v", got)
	}

	// A provider cannot be introduced to itself.
	if err := fx.call(prov, "introduce", map[string]any{"uid": providerUID}, nil); err == nil {
		t.Fatal("self-introduction accepted")
	}
	if got := fx.suggests.all(); len(got) != 1 {
		t.Fatalf("refusals still suggested: %v", got)
	}
}

func TestIntroduceUnsupported(t *testing.T) {
	fx := newFixture(t)

	// A directory without a Suggester (stock brclient daemon) refuses
	// with a clear reason.
	ctx2, cancel2 := context.WithCancel(fx.ctx)
	t.Cleanup(cancel2)
	svc2 := startDirectoryAt(t, ctx2, fx.fab, fx.clk,
		&testPayer{rails: map[string]*server.Harness{}, payer: dirBUID},
		t.TempDir(), dirBUID,
		directory.IntroducerFunc(func(context.Context, string, string) error { return nil }),
		nil)
	t.Cleanup(func() { svc2.Close() })

	r := fx.fab.NewRouter(stingyUID, brmcp.RouterConfig{Logf: t.Logf})
	c2 := fx.dialTo(r, dirBUID, "c2")
	err := fx.call(c2, "introduce", map[string]any{"uid": providerUID}, nil)
	if err == nil || !strings.Contains(err.Error(), "does not support introductions") {
		t.Fatalf("unsupported introduce not surfaced: %v", err)
	}
}
