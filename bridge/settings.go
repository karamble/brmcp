// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bridge

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Settings is the user's bridge policy, persisted as mcpclient.json in the
// data dir. The listen address is deliberately not here: it is host startup
// configuration, not a runtime setting.
type Settings struct {
	Enabled bool   `json:"enabled"`
	Token   string `json:"token"`
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
	// ApprovalTimeoutSecs bounds how long a call waits for a decision.
	// Nonpositive selects 120.
	ApprovalTimeoutSecs int `json:"approval_timeout_secs"`
	// TipWaitSecs bounds how long a call waits for the payment to complete
	// before giving up. Nonpositive selects 180.
	TipWaitSecs int `json:"tip_wait_secs"`
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
// owned listener, disabling stops it, and a token change while enabled
// restarts it, severing streams authorized under the old token. Bots
// removed from the allowlist have their live sessions closed. Enabling with
// an empty token mints a random one, visible via Settings. Validation and
// persistence failures leave the prior settings active.
func (b *Bridge) ApplySettings(s Settings) error {
	s = s.withDefaults()
	for _, bot := range s.AllowedBots {
		if !uidRe.MatchString(strings.ToLower(bot)) {
			return fmt.Errorf("allowed bot %q is not a 64-hex uid", bot)
		}
	}
	if s.Enabled && s.Token == "" {
		var tok [16]byte
		if _, err := rand.Read(tok[:]); err != nil {
			return err
		}
		s.Token = hex.EncodeToString(tok[:])
	}

	b.mu.Lock()
	prev := b.settings
	b.settings = s
	if err := b.persistSettingsLocked(); err != nil {
		b.settings = prev
		b.mu.Unlock()
		return err
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
		case s.Enabled && s.Token != prev.Token:
			b.stopListenerLocked()
			lerr = b.startListenerLocked()
		}
	}
	b.mu.Unlock()
	for _, l := range dropped {
		l.reset()
	}
	return lerr
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
		var s Settings
		if err := json.Unmarshal(raw, &s); err != nil {
			return fmt.Errorf("parse %s: %w", b.settingsPath(), err)
		}
		b.settings = s.withDefaults()
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
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
	raw, err := json.MarshalIndent(b.settings, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(b.settingsPath(), raw, 0o600)
}
