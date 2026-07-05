// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package brmcp carries Model Context Protocol sessions over Bison Relay
// private messages. It binds the MCP go-sdk's Transport contract to an
// abstract PM send/receive pair, so the same code serves both a
// bisonbotkit-backed bot process and an embedded Bison Relay client.
//
// This package holds the protocol surface both ends share: the session
// Router and Conn over the wire envelope codec, the envelope predicate
// IsEnvelope, and the payment metadata (PriceMetaKey, PricingMetaKey,
// CallKeyMetaKey, PaymentRequired). The roles build on it:
//
//   - wire holds the byte-level envelope codec (see WIRE.md).
//   - server is the serving harness: default-deny authorization, rate
//     limiting, priced tools, the prepaid ledger, and bisonbotkit glue.
//   - bridge is the client bridge: local MCP endpoints that mirror remote
//     bots' tools and settle payments under the user's spending policy.
//   - brmcptest is an in-memory PM fabric for tests and examples.
package brmcp
