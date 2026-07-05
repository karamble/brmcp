// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bridge_test

import (
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/karamble/brmcp/bridge"
	"github.com/karamble/brmcp/brmcptest"
)

func httpStatus(t *testing.T, method, url, token string) int {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func TestBearerAndPathGate(t *testing.T) {
	fx := newFixture(t, fixtureOpts{})

	cases := []struct {
		name  string
		uid   string
		token string
		want  int
	}{
		{"no token", botUID, "", http.StatusUnauthorized},
		{"wrong token", botUID, "nope", http.StatusUnauthorized},
		{"malformed uid", "abc123", fx.token, http.StatusNotFound},
		{"non-allowlisted uid", brmcptest.UID(3), fx.token, http.StatusNotFound},
	}
	for _, c := range cases {
		if got := httpStatus(t, http.MethodPost, fx.endpoint(c.uid), c.token); got != c.want {
			t.Errorf("%s: status %d != %d", c.name, got, c.want)
		}
	}
	// A valid token and allowed uid reaches the MCP handler (the session
	// tests prove the full handshake; here it just must pass the gate).
	if got := httpStatus(t, http.MethodPost, fx.endpoint(botUID), fx.token); got == http.StatusUnauthorized || got == http.StatusNotFound {
		t.Errorf("valid request rejected at the gate: %d", got)
	}

	// Disabling the bridge blanks the whole surface.
	s := fx.bridge.Settings()
	s.Enabled = false
	if err := fx.bridge.ApplySettings(s); err != nil {
		t.Fatal(err)
	}
	if got := httpStatus(t, http.MethodPost, fx.endpoint(botUID), fx.token); got != http.StatusNotFound {
		t.Errorf("disabled bridge answered %d != 404", got)
	}
}

func TestEmptyTokenNeverAuthorizes(t *testing.T) {
	// A crafted settings file can carry enabled with an empty token
	// (ApplySettings would mint one); the gate must still refuse.
	fx := newFixture(t, fixtureOpts{})

	dataDir := t.TempDir()
	raw, _ := json.Marshal(bridge.Settings{
		Enabled:     true,
		AllowedBots: []string{botUID},
	})
	if err := os.WriteFile(filepath.Join(dataDir, "mcpclient.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	b, err := bridge.New(bridge.Config{
		DataDir: dataDir, Sender: fx.sender, Payer: fx.payer, Clock: fx.clk,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if !b.Settings().Enabled || b.Settings().Token != "" {
		t.Fatalf("seed not loaded: %+v", b.Settings())
	}
	srv := httpTestServer(t, b)
	if got := httpStatus(t, http.MethodPost, srv+"/mcp/"+botUID, ""); got != http.StatusUnauthorized {
		t.Errorf("empty token, empty bearer: %d != 401", got)
	}
	if got := httpStatus(t, http.MethodPost, srv+"/mcp/"+botUID, "anything"); got != http.StatusUnauthorized {
		t.Errorf("empty token, some bearer: %d != 401", got)
	}
}

func TestSettingsHotApply(t *testing.T) {
	fx := newFixture(t, fixtureOpts{settings: &bridge.Settings{}})
	b := fx.bridge

	// Fresh state: defaults applied, disabled.
	s := b.Settings()
	if s.Enabled || s.Mode != "approval" || s.ApprovalTimeoutSecs != 120 || s.TipWaitSecs != 180 {
		t.Fatalf("defaults not applied: %+v", s)
	}

	// Enabling with an empty token mints one.
	s.Enabled = true
	s.AllowedBots = []string{botUID}
	if err := b.ApplySettings(s); err != nil {
		t.Fatal(err)
	}
	if tok := b.Settings().Token; len(tok) != 32 {
		t.Fatalf("minted token: %q", tok)
	}

	// A non-hex bot uid is rejected and the prior settings survive.
	bad := b.Settings()
	bad.AllowedBots = []string{"not-a-uid"}
	if err := b.ApplySettings(bad); err == nil {
		t.Fatal("invalid bot uid accepted")
	}
	if got := b.Settings().AllowedBots; len(got) != 1 || !strings.EqualFold(got[0], botUID) {
		t.Fatalf("settings mutated by rejected apply: %v", got)
	}

	// Unknown modes coerce to approval (fail safe).
	odd := b.Settings()
	odd.Mode = "bogus"
	if err := b.ApplySettings(odd); err != nil {
		t.Fatal(err)
	}
	if got := b.Settings().Mode; got != "approval" {
		t.Fatalf("mode coercion: %q", got)
	}

	// A persistence failure rolls the settings back.
	settingsFile := filepath.Join(fx.dataDir, "mcpclient.json")
	if err := os.Chmod(settingsFile, 0o400); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(settingsFile, 0o600) })
	prev := b.Settings()
	next := prev
	next.PerCallCapAtoms = 42
	if err := b.ApplySettings(next); err == nil {
		t.Fatal("apply succeeded despite an unwritable settings file")
	}
	if got := b.Settings(); got.PerCallCapAtoms != prev.PerCallCapAtoms || got.Token != prev.Token {
		t.Fatalf("settings not rolled back: %+v", got)
	}
	os.Chmod(settingsFile, 0o600)
}

func TestOwnedListenerLifecycle(t *testing.T) {
	fx := newFixture(t, fixtureOpts{settings: &bridge.Settings{}})

	b, err := bridge.New(bridge.Config{
		DataDir:    t.TempDir(),
		Sender:     fx.sender,
		Payer:      fx.payer,
		ListenAddr: "127.0.0.1:0",
		Logf:       t.Logf,
		Clock:      fx.clk,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.Start(fx.ctx); err != nil {
		t.Fatal(err)
	}
	if b.ListenAddr() != nil {
		t.Fatal("listener bound while disabled")
	}

	s := bridge.Settings{Enabled: true, AllowedBots: []string{botUID}}
	if err := b.ApplySettings(s); err != nil {
		t.Fatal(err)
	}
	addr := b.ListenAddr()
	if addr == nil {
		t.Fatal("listener not bound on enable")
	}
	token := b.Settings().Token
	url := "http://" + addr.String() + "/mcp/" + botUID
	if got := httpStatus(t, http.MethodPost, url, token); got == http.StatusUnauthorized {
		t.Fatalf("minted token rejected: %d", got)
	}

	// A token change restarts the listener, severing old-token clients.
	s = b.Settings()
	s.Token = "rotated-token-0123456789abcdef"
	if err := b.ApplySettings(s); err != nil {
		t.Fatal(err)
	}
	addr2 := b.ListenAddr()
	if addr2 == nil {
		t.Fatal("listener gone after token change")
	}
	url2 := "http://" + addr2.String() + "/mcp/" + botUID
	if got := httpStatus(t, http.MethodPost, url2, token); got != http.StatusUnauthorized {
		t.Fatalf("old token still authorized: %d", got)
	}
	if got := httpStatus(t, http.MethodPost, url2, s.Token); got == http.StatusUnauthorized {
		t.Fatalf("new token rejected: %d", got)
	}

	// Disable stops the listener.
	s = b.Settings()
	s.Enabled = false
	if err := b.ApplySettings(s); err != nil {
		t.Fatal(err)
	}
	if b.ListenAddr() != nil {
		t.Fatal("listener survived disable")
	}
	if _, err := net.DialTimeout("tcp", addr2.String(), time.Second); err == nil {
		t.Fatal("disabled listener still accepting")
	}
}

func TestTeardown(t *testing.T) {
	fx := newFixture(t, fixtureOpts{})
	session := fx.session()
	if res, err := fx.call(session, "free"); err != nil || res.IsError {
		t.Fatalf("warmup: %v %v", err, res)
	}

	// De-listing the bot closes its session and gates new requests.
	s := fx.bridge.Settings()
	s.AllowedBots = nil
	if err := fx.bridge.ApplySettings(s); err != nil {
		t.Fatal(err)
	}
	if got := httpStatus(t, http.MethodPost, fx.endpoint(botUID), fx.token); got != http.StatusNotFound {
		t.Fatalf("de-listed bot answered %d != 404", got)
	}

	// Close is idempotent and final.
	if err := fx.bridge.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fx.bridge.Close(); err != nil {
		t.Fatal(err)
	}
}

// httpTestServer serves a bridge's handler for tests outside the fixture.
func httpTestServer(t *testing.T, b *bridge.Bridge) string {
	t.Helper()
	srv := http.Server{Handler: b.Handler()}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return "http://" + ln.Addr().String()
}
