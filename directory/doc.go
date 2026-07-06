// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package directory implements a yellow-pages service for brmcp tool
// providers on Bison Relay. A directory is itself a brmcp bot: providers
// register and fund a live verification test over ordinary MCP tool calls,
// the directory crawls their catalogs first-hand, an operator (usually an
// AI agent driving the uid-gated admin tools) approves listings, and
// consumers search the verified, tool-level index. Federation is
// verify-don't-trust: peer snapshots only seed leads, every listing is a
// first-party observation. See docs/DIRECTORY.md for the full contract.
package directory
