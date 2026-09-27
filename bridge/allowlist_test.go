// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bridge_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/karamble/brmcp/bridge"
	"github.com/karamble/brmcp/brmcptest"
)

// recordGate probes the bearer and IP gates without touching a bot: it posts
// to a malformed uid path, so a request that passes both gates answers 404
// (the uid gate) while a refused one answers 401. RemoteAddr is settable,
// which real sockets do not allow.
func recordGate(t *testing.T, b *bridge.Bridge, token, remote string) *http.Response {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/mcp/not-a-uid", strings.NewReader("{}"))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	if remote != "" {
		req.RemoteAddr = remote
	}
	rec := httptest.NewRecorder()
	b.Handler().ServeHTTP(rec, req)
	return rec.Result()
}

func gateStatus(t *testing.T, b *bridge.Bridge, token, remote string) int {
	t.Helper()
	resp := recordGate(t, b, token, remote)
	resp.Body.Close()
	return resp.StatusCode
}

func applyAllowedIPs(t *testing.T, b *bridge.Bridge, ips []string) {
	t.Helper()
	s := b.Settings()
	s.AllowedIPs = ips
	if _, err := b.ApplySettings(s); err != nil {
		t.Fatal(err)
	}
}

func TestAllowedIPCanonicalization(t *testing.T) {
	fx := newFixture(t, fixtureOpts{})
	valid := []struct {
		in   []string
		want []string
	}{
		{[]string{"10.1.2.3"}, []string{"10.1.2.3"}},
		{[]string{" 10.0.0.0/8 "}, []string{"10.0.0.0/8"}},
		{[]string{"10.1.2.3/8"}, []string{"10.0.0.0/8"}},
		{[]string{"::ffff:192.0.2.1"}, []string{"192.0.2.1"}},
		{[]string{"::ffff:10.0.0.0/104"}, []string{"10.0.0.0/8"}},
		{[]string{"fe80::1%eth0"}, []string{"fe80::1"}},
		{[]string{"2001:db8::/32"}, []string{"2001:db8::/32"}},
		{[]string{"", " ", "10.1.2.3"}, []string{"10.1.2.3"}},
		{nil, nil},
	}
	for _, tc := range valid {
		applyAllowedIPs(t, fx.bridge, tc.in)
		if got := fx.bridge.Settings().AllowedIPs; !reflect.DeepEqual(got, tc.want) {
			t.Errorf("AllowedIPs %v canonicalized to %v, want %v", tc.in, got, tc.want)
		}
	}
	for _, bad := range []string{"nonsense", "10.0.0.1/33", "::ffff:10.0.0.0/95"} {
		s := fx.bridge.Settings()
		s.AllowedIPs = []string{bad}
		_, err := fx.bridge.ApplySettings(s)
		if err == nil || !strings.Contains(err.Error(), bad) {
			t.Errorf("entry %q: want error naming it, got %v", bad, err)
		}
	}
}

func TestApplySettingsRejectsInvalidAllowedIP(t *testing.T) {
	fx := newFixture(t, fixtureOpts{})
	settingsFile := filepath.Join(fx.dataDir, "mcpclient.json")
	before, err := os.ReadFile(settingsFile)
	if err != nil {
		t.Fatal(err)
	}
	prev := fx.bridge.Settings()

	s := fx.bridge.Settings()
	s.AllowedIPs = []string{"10.0.0.1", "banana"}
	if _, err := fx.bridge.ApplySettings(s); err == nil {
		t.Fatal("invalid allowed_ips entry accepted")
	}
	if got := fx.bridge.Settings(); !reflect.DeepEqual(got, prev) {
		t.Fatalf("settings mutated by rejected apply: %+v", got)
	}
	after, err := os.ReadFile(settingsFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("rejected apply rewrote mcpclient.json")
	}
}

func TestAllowedIPsRoundTrip(t *testing.T) {
	fx := newFixture(t, fixtureOpts{})
	applyAllowedIPs(t, fx.bridge, []string{"10.0.0.0/8"})

	b, err := bridge.New(bridge.Config{
		DataDir: fx.dataDir, Sender: fx.sender, Payer: fx.payer,
		Log: brmcptest.Logger(t), Clock: fx.clk,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if got := b.Settings().AllowedIPs; !reflect.DeepEqual(got, []string{"10.0.0.0/8"}) {
		t.Fatalf("allowed_ips not restored: %v", got)
	}
	// Enforcement is live straight from disk: a denied remote is refused
	// (and recorded), an allowed one passes the IP gate.
	if got := gateStatus(t, b, fx.token, "192.0.2.1:9"); got != http.StatusUnauthorized {
		t.Fatalf("denied remote after restart: %d != 401", got)
	}
	if d := b.LastDenied(); d == nil || d.IP != "192.0.2.1" {
		t.Fatalf("denial not recorded after restart: %+v", d)
	}
	if got := gateStatus(t, b, fx.token, "10.1.2.3:9"); got != http.StatusNotFound {
		t.Fatalf("allowed remote after restart: %d != 404 (uid gate)", got)
	}
}

func TestLegacySettingsUnrestricted(t *testing.T) {
	fx := newFixture(t, fixtureOpts{})

	dataDir := t.TempDir()
	// "Legacy" here means written before allowed_ips existed, which is what
	// this test is about; the token is stored as its hash like any other.
	const legacyToken = "legacy-token-0123456789abcdef"
	sum := sha256.Sum256([]byte(legacyToken))
	legacy := `{"enabled":true,"token_hash":"` + hex.EncodeToString(sum[:]) +
		`","mode":"autopay","per_call_cap_atoms":1,"per_day_cap_atoms":1,"allowed_bots":["` +
		botUID + `"],"approval_timeout_secs":120,"tip_wait_secs":180}`
	if err := os.WriteFile(filepath.Join(dataDir, "mcpclient.json"), []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	b, err := bridge.New(bridge.Config{
		DataDir: dataDir, Sender: fx.sender, Payer: fx.payer, Log: brmcptest.Logger(t), Clock: fx.clk,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if got := b.Settings().AllowedIPs; len(got) != 0 {
		t.Fatalf("legacy file grew allowed_ips: %v", got)
	}
	if got := gateStatus(t, b, legacyToken, "203.0.113.9:7"); got != http.StatusNotFound {
		t.Fatalf("legacy settings must not restrict remotes: %d != 404", got)
	}
	// An unrestricted settings marshal keeps the legacy file shape.
	raw, err := json.Marshal(b.Settings())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "allowed_ips") {
		t.Fatalf("unrestricted settings marshal carries allowed_ips: %s", raw)
	}
}

func TestLastDenied(t *testing.T) {
	fx := newFixture(t, fixtureOpts{})
	b := fx.bridge
	if d := b.LastDenied(); d != nil {
		t.Fatalf("fresh bridge has a denial: %+v", d)
	}
	applyAllowedIPs(t, b, []string{"10.0.0.0/8"})

	// A denial records the bare canonical IP (port stripped, v4-mapped
	// unmapped) so the operator can allow exactly that string.
	if got := gateStatus(t, b, fx.token, "[::ffff:192.0.2.9]:5"); got != http.StatusUnauthorized {
		t.Fatalf("denied remote: %d != 401", got)
	}
	d := b.LastDenied()
	if d == nil || d.IP != "192.0.2.9" || d.At.IsZero() {
		t.Fatalf("denial not recorded as bare IP: %+v", d)
	}
	// The next successful auth clears it.
	if got := gateStatus(t, b, fx.token, "10.1.2.3:5"); got != http.StatusNotFound {
		t.Fatalf("allowed remote: %d != 404", got)
	}
	if d := b.LastDenied(); d != nil {
		t.Fatalf("denial survived a successful auth: %+v", d)
	}
	// A later denial re-records.
	gateStatus(t, b, fx.token, "192.0.2.10:5")
	if d := b.LastDenied(); d == nil || d.IP != "192.0.2.10" {
		t.Fatalf("denial not re-recorded: %+v", d)
	}
}

func TestIPGateDenialIsGeneric401(t *testing.T) {
	fx := newFixture(t, fixtureOpts{})
	applyAllowedIPs(t, fx.bridge, []string{"10.0.0.0/8"})

	// The bearer check runs first: a wrong token from a denied address is
	// refused before the IP gate and records nothing.
	if got := gateStatus(t, fx.bridge, "nope", "192.0.2.1:1"); got != http.StatusUnauthorized {
		t.Fatalf("wrong token, denied IP: %d != 401", got)
	}
	if d := fx.bridge.LastDenied(); d != nil {
		t.Fatalf("bearer refusal recorded an IP denial: %+v", d)
	}

	// A valid token from a denied address answers byte-identically to a
	// bad token from an allowed one.
	badTok := recordGate(t, fx.bridge, "nope", "10.0.0.7:1")
	wrongIP := recordGate(t, fx.bridge, fx.token, "192.0.2.1:1")
	badBody, _ := io.ReadAll(badTok.Body)
	wrongBody, _ := io.ReadAll(wrongIP.Body)
	badTok.Body.Close()
	wrongIP.Body.Close()
	if wrongIP.StatusCode != http.StatusUnauthorized {
		t.Fatalf("valid token, denied IP: %d != 401", wrongIP.StatusCode)
	}
	if wrongIP.StatusCode != badTok.StatusCode || string(wrongBody) != string(badBody) ||
		!reflect.DeepEqual(wrongIP.Header, badTok.Header) {
		t.Fatalf("IP denial differs from bad-token response:\n%d %q %v\n%d %q %v",
			wrongIP.StatusCode, wrongBody, wrongIP.Header,
			badTok.StatusCode, badBody, badTok.Header)
	}
}

func TestIPGateAllowDeny(t *testing.T) {
	fx := newFixture(t, fixtureOpts{})
	b := fx.bridge

	// Default fixture settings carry no restriction: any remote passes.
	if got := gateStatus(t, b, fx.token, "203.0.113.9:7"); got != http.StatusNotFound {
		t.Fatalf("unrestricted remote: %d != 404", got)
	}
	// Unrestricted bridges never parse the remote at all.
	if got := gateStatus(t, b, fx.token, "bogus"); got != http.StatusNotFound {
		t.Fatalf("unrestricted malformed remote: %d != 404", got)
	}

	applyAllowedIPs(t, b, []string{"10.0.0.0/8", "192.0.2.7"})
	cases := []struct {
		remote string
		want   int
	}{
		{"10.1.2.3:555", http.StatusNotFound},          // CIDR containment
		{"192.0.2.7:555", http.StatusNotFound},         // single IP
		{"[::ffff:10.1.2.3]:555", http.StatusNotFound}, // v4-mapped remote vs v4 CIDR
		{"192.0.2.8:555", http.StatusUnauthorized},     // outside both
		{"bogus", http.StatusUnauthorized},             // restricted lists fail closed
	}
	for _, c := range cases {
		if got := gateStatus(t, b, fx.token, c.remote); got != c.want {
			t.Errorf("remote %q: %d != %d", c.remote, got, c.want)
		}
	}
}

func TestAllowedIPsHotApplyOwnedListener(t *testing.T) {
	fx := newFixture(t, fixtureOpts{settings: &bridge.Settings{}})

	b, err := bridge.New(bridge.Config{
		DataDir:    t.TempDir(),
		Sender:     fx.sender,
		Payer:      fx.payer,
		ListenAddr: "127.0.0.1:0",
		Log:        brmcptest.Logger(t),
		Clock:      fx.clk,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.Start(fx.ctx); err != nil {
		t.Fatal(err)
	}
	token, err := b.ApplySettings(bridge.Settings{Enabled: true, AllowedBots: []string{botUID}})
	if err != nil {
		t.Fatal(err)
	}
	addr := b.ListenAddr()
	if addr == nil {
		t.Fatal("listener not bound on enable")
	}
	url := "http://" + addr.String() + "/mcp/not-a-uid"
	if got := httpStatus(t, http.MethodPost, url, token); got != http.StatusNotFound {
		t.Fatalf("pre-restriction request: %d != 404", got)
	}

	// Restricting the source addresses hot-applies: the listener stays
	// bound (no restart), yet the very next request is refused.
	s := b.Settings()
	s.AllowedIPs = []string{"10.0.0.0/8"}
	if _, err := b.ApplySettings(s); err != nil {
		t.Fatal(err)
	}
	if got := b.ListenAddr(); got == nil || got.String() != addr.String() {
		t.Fatalf("allowed_ips change moved the listener: %v != %v", got, addr)
	}
	if got := httpStatus(t, http.MethodPost, url, token); got != http.StatusUnauthorized {
		t.Fatalf("restricted loopback request: %d != 401", got)
	}

	// Allowing the loopback restores access, again without a restart.
	s = b.Settings()
	s.AllowedIPs = []string{"127.0.0.1"}
	if _, err := b.ApplySettings(s); err != nil {
		t.Fatal(err)
	}
	if got := httpStatus(t, http.MethodPost, url, token); got != http.StatusNotFound {
		t.Fatalf("re-allowed loopback request: %d != 404", got)
	}
}
