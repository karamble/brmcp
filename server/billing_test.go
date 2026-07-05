// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package server_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/karamble/brmcp/server"
)

// nullBilling is a Billing stand-in for services with their own accounting.
type nullBilling struct{}

func (nullBilling) Balance(string) int64       { return 0 }
func (nullBilling) Debit(string, int64) error  { return nil }
func (nullBilling) Credit(string, int64) error { return nil }

func TestLedgerOpensOnlyWithoutBilling(t *testing.T) {
	impl := &mcp.Implementation{Name: "t", Version: "0"}

	// A custom Billing needs no DataDir and must create no ledger file.
	dir := t.TempDir()
	if _, err := server.NewHarness(impl, server.HarnessConfig{
		DataDir: dir,
		Billing: nullBilling{},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "ledger.json")); !os.IsNotExist(err) {
		t.Fatalf("custom-billing harness touched the ledger: %v", err)
	}
	if _, err := server.NewHarness(impl, server.HarnessConfig{Billing: nullBilling{}}); err != nil {
		t.Fatalf("custom billing without DataDir: %v", err)
	}

	// Without Billing the built-in ledger requires a DataDir.
	if _, err := server.NewHarness(impl, server.HarnessConfig{}); err == nil {
		t.Fatal("no Billing and no DataDir accepted")
	}
	h, err := server.NewHarness(impl, server.HarnessConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Billing().Credit("aa", 5); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "ledger.json")); err != nil {
		t.Fatalf("built-in ledger not persisted: %v", err)
	}
}
