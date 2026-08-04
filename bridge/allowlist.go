// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bridge

import (
	"fmt"
	"net"
	"net/netip"
	"strings"
	"time"
)

// parseIPEntry parses one allowed_ips entry: a single IPv4/IPv6 address or a
// CIDR prefix. v4-mapped forms are unmapped and zones stripped so entries
// match the normalized request address (netip.Prefix.Contains never crosses
// the mapped/unmapped boundary); prefixes are stored masked.
func parseIPEntry(s string) (string, netip.Prefix, error) {
	if strings.Contains(s, "/") {
		p, err := netip.ParsePrefix(s)
		if err != nil {
			return "", netip.Prefix{}, fmt.Errorf("invalid IP or CIDR entry %q", s)
		}
		if p.Addr().Is4In6() {
			// A mapped prefix shorter than /96 cannot be expressed as an
			// IPv4 range and would never match a normalized address.
			if p.Bits() < 96 {
				return "", netip.Prefix{}, fmt.Errorf("invalid IP or CIDR entry %q", s)
			}
			p = netip.PrefixFrom(p.Addr().Unmap(), p.Bits()-96)
		}
		p = p.Masked()
		return p.String(), p, nil
	}
	addr, err := netip.ParseAddr(s)
	if err != nil {
		return "", netip.Prefix{}, fmt.Errorf("invalid IP or CIDR entry %q", s)
	}
	addr = addr.Unmap().WithZone("")
	return addr.String(), netip.PrefixFrom(addr, addr.BitLen()), nil
}

// parseAllowedIPs validates and canonicalizes allowed_ips entries. Blank
// entries are skipped; an invalid entry rejects the whole set by name so a
// typo cannot silently widen access. An empty result means no restriction.
func parseAllowedIPs(entries []string) ([]string, []netip.Prefix, error) {
	var canonical []string
	var prefixes []netip.Prefix
	for _, e := range entries {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		c, p, err := parseIPEntry(e)
		if err != nil {
			return nil, nil, err
		}
		canonical = append(canonical, c)
		prefixes = append(prefixes, p)
	}
	return canonical, prefixes, nil
}

// remoteIPAllowed reports whether a request from remoteAddr ("host:port")
// passes the allowed-IP prefixes. An empty set means no restriction; a
// restricted set fails closed on any unparseable remote address.
func remoteIPAllowed(prefixes []netip.Prefix, remoteAddr string) bool {
	if len(prefixes) == 0 {
		return true
	}
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return false
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	addr = addr.Unmap().WithZone("")
	for _, p := range prefixes {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}

// deniedIP normalizes a denied remote address to the bare form an allowed_ips
// entry would need, falling back to the raw string when it does not parse.
func deniedIP(remoteAddr string) string {
	ip := remoteAddr
	if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
		ip = host
		if addr, err := netip.ParseAddr(host); err == nil {
			ip = addr.Unmap().WithZone("").String()
		}
	}
	return ip
}

// DeniedAttempt is the most recent allowed-IP denial of the listener,
// surfaced to the host so its dashboard can show (and allow) the address an
// agent actually connects from - behind Docker or a proxy that address is
// rarely the one the user expects. In-memory only; cleared by the next
// successful auth. The agent itself sees only the generic 401.
type DeniedAttempt struct {
	IP string    `json:"ip"`
	At time.Time `json:"at"`
}

// LastDenied returns the most recent allowed-IP denial, or nil when none has
// happened or a request has since authenticated successfully.
func (b *Bridge) LastDenied() *DeniedAttempt {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastDenied
}
