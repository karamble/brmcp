// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bridge_test

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/karamble/brmcp/bridge"
)

// The bearer token is a durable credential, so only its hash is kept: the
// plaintext is returned by the call that mints it and is unreadable after
// that. The delicate part is what an ordinary save does - nothing can echo a
// token back any more, so "no token in the request" has to mean keep, not
// mint, or every save would cut off every connected agent.

func TestTokenIsNeverPersisted(t *testing.T) {
	fx := newFixture(t, fixtureOpts{settings: &bridge.Settings{}})
	b := fx.bridge

	minted, err := b.ApplySettings(bridge.Settings{Enabled: true, AllowedBots: []string{botUID}})
	if err != nil {
		t.Fatal(err)
	}
	if minted == "" {
		t.Fatal("enabling with nothing stored did not mint")
	}

	raw, err := os.ReadFile(filepath.Join(fx.dataDir, "mcpclient.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), minted) {
		t.Fatal("the settings file contains the token in plaintext")
	}
	var onDisk struct {
		TokenHash string `json:"token_hash"`
		Token     string `json:"token"`
	}
	if err := json.Unmarshal(raw, &onDisk); err != nil {
		t.Fatal(err)
	}
	if len(onDisk.TokenHash) != 64 {
		t.Errorf("token_hash = %q, want a 64-hex sha256 digest", onDisk.TokenHash)
	}
	if onDisk.Token != "" {
		t.Errorf("a token field was written: %q", onDisk.Token)
	}
}

func TestMintedTokenIsReturnedOnceAndAuthorizes(t *testing.T) {
	fx := newFixture(t, fixtureOpts{settings: &bridge.Settings{}})
	b := fx.bridge

	minted, err := b.ApplySettings(bridge.Settings{Enabled: true, AllowedBots: []string{botUID}})
	if err != nil {
		t.Fatal(err)
	}
	if got := gateStatus(t, b, minted, "127.0.0.1:9"); got == http.StatusUnauthorized {
		t.Fatalf("the minted token was rejected: %d", got)
	}
	if !b.Settings().TokenSet {
		t.Error("TokenSet is false after minting")
	}
}

// The regression this whole shape exists for: a caps-only save must leave the
// token alone. Minting here would sever every agent on an unrelated edit.
func TestOrdinarySaveKeepsTheToken(t *testing.T) {
	fx := newFixture(t, fixtureOpts{settings: &bridge.Settings{}})
	b := fx.bridge

	minted, err := b.ApplySettings(bridge.Settings{Enabled: true, AllowedBots: []string{botUID}})
	if err != nil {
		t.Fatal(err)
	}

	next := b.Settings()
	next.PerCallCapAtoms = 4242
	again, err := b.ApplySettings(next)
	if err != nil {
		t.Fatal(err)
	}
	if again != "" {
		t.Errorf("an ordinary save minted %q, want no new token", again)
	}
	if got := gateStatus(t, b, minted, "127.0.0.1:9"); got == http.StatusUnauthorized {
		t.Fatal("an ordinary save invalidated the existing token")
	}
}

func TestRecycleSeversTheOldToken(t *testing.T) {
	fx := newFixture(t, fixtureOpts{settings: &bridge.Settings{}})
	b := fx.bridge

	first, err := b.ApplySettings(bridge.Settings{Enabled: true, AllowedBots: []string{botUID}})
	if err != nil {
		t.Fatal(err)
	}

	next := b.Settings()
	next.RecycleToken = true
	second, err := b.ApplySettings(next)
	if err != nil {
		t.Fatal(err)
	}
	if second == "" || second == first {
		t.Fatalf("recycle returned %q, want a fresh token", second)
	}
	if got := gateStatus(t, b, first, "127.0.0.1:9"); got != http.StatusUnauthorized {
		t.Errorf("the old token still authorizes: %d", got)
	}
	if got := gateStatus(t, b, second, "127.0.0.1:9"); got == http.StatusUnauthorized {
		t.Error("the recycled token does not authorize")
	}
}

// Enabling has always produced a usable listener; that must survive the token
// becoming unreadable, or a fresh install would come up permanently refusing.
func TestFirstEnableMints(t *testing.T) {
	fx := newFixture(t, fixtureOpts{settings: &bridge.Settings{}})
	b := fx.bridge

	if b.Settings().TokenSet {
		t.Fatal("a fresh bridge already reports a token")
	}
	minted, err := b.ApplySettings(bridge.Settings{Enabled: true, AllowedBots: []string{botUID}})
	if err != nil {
		t.Fatal(err)
	}
	if len(minted) != 32 {
		t.Fatalf("minted %q, want 32 hex characters", minted)
	}
}
