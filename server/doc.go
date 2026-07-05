// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package server is the serving harness: it publishes MCP tools over Bison
// Relay private messages with default-deny authorization, per-caller rate
// limiting, and paid tools settled by Bison Relay tips against a prepaid
// per-caller balance (the built-in Ledger, or any store implementing
// Billing). RunBot serves a harness over a bisonbotkit bot; embedded hosts
// wire Harness.Start to their own client instead.
package server
