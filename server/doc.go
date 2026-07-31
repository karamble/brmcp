// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package server is the serving harness: it publishes MCP tools over Bison
// Relay private messages with default-deny authorization, per-caller rate
// limiting, and paid tools settled by Bison Relay tips against a prepaid
// per-caller balance (the built-in Ledger, or any store implementing
// Billing). RunBot serves a harness over a bisonbotkit bot; embedded hosts
// wire Harness.Start to their own client instead.
//
// Tool handlers are plain request/response. On MCP revision 2026-07-28 and
// later the server cannot initiate requests mid-call (elicitation, sampling
// and roots all fail there), and the harness does not use the
// multi-round-trip tool-result protocol; handlers must complete from their
// arguments alone. The harness configures no MCP keepalive - it would be
// inert on the new revision anyway - and relies on the transport's idle
// expiry for session liveness.
package server
