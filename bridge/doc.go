// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package bridge is the client bridge: it exposes a local streamable-HTTP
// MCP endpoint per allowed Bison Relay bot (/mcp/<bot-uid>, bearer gated,
// disabled by default) that mirrors the remote bot's tools verbatim and
// relays every call over the relay. To the agent this is an ordinary MCP
// server on localhost; it needs no Bison Relay awareness, no wallet, and no
// keys.
//
// The bridge is also where the user's spending policy lives: a default-deny
// bot allowlist, per-call and rolling twenty-four-hour caps that bind in
// both modes (zero means never pay), and approval or autopay settlement of
// payment_required refusals through a host-supplied Payer. Hosts embed the
// bridge by wiring a brmcp.PMSender, feeding inbound private messages to
// HandlePM, and implementing Payer over their payment rail (TipMatcher
// helps hosts built on Bison Relay's tip notifications). See docs/BRIDGE.md.
package bridge
