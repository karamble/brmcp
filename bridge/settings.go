// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bridge

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Settings is the user's bridge policy, persisted as mcpclient.json in the
// data dir. The listen address is deliberately not here: it is host startup
// configuration, not a runtime setting.
type Settings struct {
	Enabled bool `json:"enabled"`
	// TokenSet reports that a bearer token exists. The token is stored only as
	// a SHA-256 hash, so the plaintext is returned by the call that mints it
	// (ApplySettings) and is never readable afterwards. Reply-only.
	TokenSet bool `json:"token_set"`
	// RecycleToken asks for a fresh token, severing every session authorized
	// under the old one. Request-only, and the only way to change the token:
	// no caller supplies a token value, so none can set a weak one.
	RecycleToken bool `json:"recycle_token,omitempty"`
	// Mode is "approval" (every payment waits for a human decision) or
	// "autopay" (payments under the caps run unattended). Any other value
	// is coerced to "approval".
	Mode string `json:"mode"`
	// Caps are hard ceilings on BOTH modes; zero means never pay.
	PerCallCapAtoms int64 `json:"per_call_cap_atoms"`
	PerDayCapAtoms  int64 `json:"per_day_cap_atoms"`
	// AllowedBots is the default-deny list of callable bot uids (64-hex,
	// matched case-insensitively).
	AllowedBots []string `json:"allowed_bots"`
	// AllowedIPs restricts the source addresses the listener accepts:
	// single IPs or CIDR ranges (IPv4 or IPv6). Empty means any address.
	// Entries are canonicalized on apply; a request from any other address
	// is answered with the same generic 401 as a bad token.
	AllowedIPs []string `json:"allowed_ips,omitempty"`
	// ApprovalTimeoutSecs bounds how long a call waits for a decision.
	// Nonpositive selects 120.
	ApprovalTimeoutSecs int `json:"approval_timeout_secs"`
	// TipWaitSecs bounds how long a call waits for the payment to complete
	// before giving up. Nonpositive selects 180.
	TipWaitSecs int `json:"tip_wait_secs"`
}

// storedSettings is the on-disk shape: the policy plus the token's hash. The
// plaintext is never written. TokenSet and RecycleToken ride along in the
// embedded struct and are recomputed on load rather than trusted, so there is
// no second field list to keep in sync with Settings.
type storedSettings struct {
	Settings
	TokenHash string `json:"token_hash,omitempty"`
}

func (s Settings) withDefaults() Settings {
	if s.Mode != "autopay" {
		s.Mode = "approval"
	}
	if s.ApprovalTimeoutSecs <= 0 {
		s.ApprovalTimeoutSecs = 120
	}
	if s.TipWaitSecs <= 0 {
		s.TipWaitSecs = 180
	}
	return s
}

var uidRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

func (b *Bridge) botAllowed(uid string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, bot := range b.settings.AllowedBots {
		if strings.EqualFold(bot, uid) {
			return true
		}
	}
	return false
}

// Settings returns the current settings with defaults applied.
func (b *Bridge) Settings() Settings {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.settings
}

// ApplySettings validates, persists, and hot-applies s: enabling starts the
// owned listener, disabling stops it, and minting a token while enabled
// restarts it, severing streams authorized under the old one. Bots removed
// from the allowlist have their live sessions closed. Validation and
// persistence failures leave the prior settings active.
//
// A token is minted when s.RecycleToken is set, or when enabling with none
// stored. The returned plaintext is the only time it is ever readable: what is
// kept is its hash. Every other call returns "" and leaves the token untouched
// - an empty token is not a request to mint, or an ordinary save would cut off
// every agent using the current one.
func (b *Bridge) ApplySettings(s Settings) (string, error) {
	s = s.withDefaults()
	for _, bot := range s.AllowedBots {
		if !uidRe.MatchString(strings.ToLower(bot)) {
			return "", fmt.Errorf("allowed bot %q is not a 64-hex uid", bot)
		}
	}
	canonicalIPs, ipPrefixes, err := parseAllowedIPs(s.AllowedIPs)
	if err != nil {
		return "", err
	}
	s.AllowedIPs = canonicalIPs
	recycle := s.RecycleToken
	s.RecycleToken = false

	b.mu.Lock()
	minted := ""
	if recycle || (s.Enabled && !b.hasToken) {
		var tok [16]byte
		if _, err := rand.Read(tok[:]); err != nil {
			b.mu.Unlock()
			return "", err
		}
		minted = hex.EncodeToString(tok[:])
	}
	prev, prevHash, prevHas := b.settings, b.tokenHash, b.hasToken
	prevPrefixes := b.ipPrefixes
	if minted != "" {
		b.tokenHash = sha256.Sum256([]byte(minted))
		b.hasToken = true
	}
	s.TokenSet = b.hasToken
	b.settings = s
	b.ipPrefixes = ipPrefixes
	if err := b.persistSettingsLocked(); err != nil {
		b.settings, b.tokenHash, b.hasToken = prev, prevHash, prevHas
		b.ipPrefixes = prevPrefixes
		b.mu.Unlock()
		return "", err
	}
	// Sessions of bots no longer on the allowlist are torn down; the router
	// and the HTTP gate already refuse their traffic.
	var dropped []*botLink
	for uid, link := range b.bots {
		if !s.allowsBot(uid) {
			dropped = append(dropped, link)
			delete(b.bots, uid)
		}
	}
	var lerr error
	if b.cfg.ListenAddr != "" && !b.closed {
		switch {
		case s.Enabled && b.httpSrv == nil:
			lerr = b.startListenerLocked()
		case !s.Enabled && b.httpSrv != nil:
			b.stopListenerLocked()
		case s.Enabled && minted != "":
			b.stopListenerLocked()
			lerr = b.startListenerLocked()
		}
	}
	b.mu.Unlock()
	for _, l := range dropped {
		l.reset()
	}
	return minted, lerr
}

func (s Settings) allowsBot(uid string) bool {
	for _, bot := range s.AllowedBots {
		if strings.EqualFold(bot, uid) {
			return true
		}
	}
	return false
}

func (b *Bridge) settingsPath() string { return filepath.Join(b.cfg.DataDir, "mcpclient.json") }
func (b *Bridge) spendPath() string    { return filepath.Join(b.cfg.DataDir, "mcpspend.json") }

func (b *Bridge) loadState() error {
	b.settings = Settings{}.withDefaults()
	if raw, err := os.ReadFile(b.settingsPath()); err == nil {
		var st storedSettings
		if err := json.Unmarshal(raw, &st); err != nil {
			return fmt.Errorf("parse %s: %w", b.settingsPath(), err)
		}
		// The hash is the only token state there is: a file without one has no
		// token, and the gate refuses everything until one is minted. Nothing
		// is carried over from an older plaintext field.
		if st.TokenHash != "" {
			h, err := hex.DecodeString(st.TokenHash)
			if err != nil || len(h) != sha256.Size {
				return fmt.Errorf("parse %s: token_hash is not a sha256 hex digest", b.settingsPath())
			}
			copy(b.tokenHash[:], h)
			b.hasToken = true
		}
		b.settings = st.Settings.withDefaults()
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	b.settings.TokenSet = b.hasToken
	b.settings.RecycleToken = false
	// The API stores validated canonical entries, so an unparseable entry can
	// appear only through a hand-edited file; it is dropped with a warning so
	// the operator knows the effective list shrank.
	var ipEntries []string
	var ipPrefixes []netip.Prefix
	for _, e := range b.settings.AllowedIPs {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		c, p, err := parseIPEntry(e)
		if err != nil {
			b.log.Warnf("dropping invalid allowed_ips entry %q from %s", e, b.settingsPath())
			continue
		}
		ipEntries = append(ipEntries, c)
		ipPrefixes = append(ipPrefixes, p)
	}
	b.settings.AllowedIPs = ipEntries
	b.ipPrefixes = ipPrefixes
	if raw, err := os.ReadFile(b.spendPath()); err == nil {
		if err := json.Unmarshal(raw, &b.spend); err != nil {
			return fmt.Errorf("parse %s: %w", b.spendPath(), err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (b *Bridge) persistSettingsLocked() error {
	st := storedSettings{Settings: b.settings}
	if b.hasToken {
		st.TokenHash = hex.EncodeToString(b.tokenHash[:])
	}
	raw, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(b.settingsPath(), raw, 0o600)
}
