// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bridge_test

import (
	"encoding/json"
	"io"
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
	// This bridge takes over the agent identity on the fabric so its
	// endpoint preflight can reach the bot.
	fx.fab.Attach(agentUID, b.HandlePM)
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

// postMCP sends one JSON-RPC frame to the bridge endpoint the way a raw
// (non-SDK) agent would, returning the response and the decoded body: SSE
// data lines concatenated, plain JSON as-is.
func postMCP(t *testing.T, url, token, body string, hdr ...string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		var sb strings.Builder
		for _, line := range strings.Split(text, "\n") {
			if data, ok := strings.CutPrefix(line, "data:"); ok {
				sb.WriteString(strings.TrimSpace(data))
			}
		}
		text = sb.String()
	}
	return resp, text
}

// rpcResult decodes one JSON-RPC response body and fails on an error reply.
func rpcResult(t *testing.T, body string) json.RawMessage {
	t.Helper()
	var frame struct {
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &frame); err != nil {
		t.Fatalf("undecodable JSON-RPC body: %v: %s", err, body)
	}
	if len(frame.Error) > 0 {
		t.Fatalf("JSON-RPC error: %s", frame.Error)
	}
	return frame.Result
}

// TestLegacyWireHTTP drives the bridge endpoint the way a pre-2026-07-28
// agent does - raw initialize, no session header discipline - and pins the
// two visible changes of the stateless handler: no Mcp-Session-Id, and no
// standalone GET stream.
func TestLegacyWireHTTP(t *testing.T) {
	fx := newFixture(t, fixtureOpts{})
	url := fx.endpoint(botUID)

	resp, body := postMCP(t, url, fx.token,
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"legacy","version":"0"}}}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("initialize status %d: %s", resp.StatusCode, body)
	}
	var init struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	if err := json.Unmarshal(rpcResult(t, body), &init); err != nil {
		t.Fatal(err)
	}
	if init.ProtocolVersion != "2025-11-25" {
		t.Fatalf("initialize answered %q; want 2025-11-25", init.ProtocolVersion)
	}
	if sid := resp.Header.Get("Mcp-Session-Id"); sid != "" {
		t.Fatalf("stateless endpoint issued a session id %q", sid)
	}

	// A bare tools/call with no prior initialize on this connection and no
	// session header: per-request synthesis is what keeps old agents
	// working against the stateless endpoint.
	resp, body = postMCP(t, url, fx.token,
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"free","arguments":{}}}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("tools/call status %d: %s", resp.StatusCode, body)
	}
	if res := rpcResult(t, body); !strings.Contains(string(res), "yes") {
		t.Fatalf("free tool result: %s", res)
	}

	// The standalone SSE stream and session DELETE are gone; both eras of
	// agents treat 405 as "not offered".
	get, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	get.Header.Set("Authorization", "Bearer "+fx.token)
	get.Header.Set("Accept", "text/event-stream")
	get.Header.Set("Mcp-Session-Id", "ignored")
	gresp, err := http.DefaultClient.Do(get)
	if err != nil {
		t.Fatal(err)
	}
	gresp.Body.Close()
	if gresp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET status %d != 405", gresp.StatusCode)
	}
	if allow := gresp.Header.Get("Allow"); allow != http.MethodPost {
		t.Fatalf("GET Allow %q != POST", allow)
	}
	if got := httpStatus(t, http.MethodDelete, url, fx.token); got != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE status %d != 405", got)
	}
}

// TestStatelessSurface pins the dual-wire advertisement: one discover
// probe, no session residue, both protocol generations offered.
func TestStatelessSurface(t *testing.T) {
	fx := newFixture(t, fixtureOpts{})
	url := fx.endpoint(botUID)

	// SEP-2243 header standardization: a new-wire request must mirror its
	// protocol version and method into the Mcp-Protocol-Version and
	// Mcp-Method headers (SDK clients do this on their own; raw callers
	// must too).
	resp, body := postMCP(t, url, fx.token,
		`{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}}`,
		"Mcp-Protocol-Version", "2026-07-28", "Mcp-Method", "server/discover")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("discover status %d: %s", resp.StatusCode, body)
	}
	var disc struct {
		SupportedVersions []string `json:"supportedVersions"`
	}
	if err := json.Unmarshal(rpcResult(t, body), &disc); err != nil {
		t.Fatal(err)
	}
	var new2026, legacy bool
	for _, v := range disc.SupportedVersions {
		new2026 = new2026 || v == "2026-07-28"
		legacy = legacy || v == "2025-11-25"
	}
	if !new2026 || !legacy {
		t.Fatalf("supportedVersions %v; want both 2026-07-28 and 2025-11-25", disc.SupportedVersions)
	}
	if sid := resp.Header.Get("Mcp-Session-Id"); sid != "" {
		t.Fatalf("discover response carries a session id %q", sid)
	}
}
