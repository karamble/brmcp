// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// brmcp-serve is an example MCP service offered over Bison Relay DMs. It
// connects to a running brclient/brclientd via clientrpc (bisonbotkit) and
// serves two stub tools: a free echo and a paid fortune. Operators copy
// this binary's shape and register their own tools.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/decred/dcrd/dcrutil/v4"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	kitconfig "github.com/vctt94/bisonbotkit/config"

	"github.com/karamble/brmcp/server"
)

// serveConfig is the operator-edited harness config, created as a template
// on first run. The allowlist is empty by default: nobody can call tools
// until the operator adds caller uids.
type serveConfig struct {
	AllowedUIDs    []string `json:"allowed_uids"`
	CallsPerMinute int      `json:"calls_per_minute"`
}

func loadServeConfig(path string) (*serveConfig, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		cfg := &serveConfig{
			AllowedUIDs:    []string{},
			CallsPerMinute: 30,
		}
		out, _ := json.MarshalIndent(cfg, "", "  ")
		if err := os.WriteFile(path, out, 0o600); err != nil {
			return nil, err
		}
		log.Printf("created %s - add caller uids to allowed_uids", path)
		return cfg, nil
	}
	if err != nil {
		return nil, err
	}
	cfg := &serveConfig{}
	if err := json.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return cfg, nil
}

type echoIn struct {
	Text string `json:"text" jsonschema:"the text to echo back"`
}

var fortunes = []string{
	"The relay carries more than messages.",
	"A tool worth calling is worth paying for.",
	"Consensus is silence that agrees.",
	"Your keys, your tools.",
	"Latency is just the network thinking it over.",
}

func main() {
	defaultDir := dcrutil.AppDataDir("brmcp-serve", false)
	datadir := flag.String("datadir", defaultDir, "data directory (bot config, ledger, harness config)")
	flag.Parse()

	if err := os.MkdirAll(*datadir, 0o700); err != nil {
		log.Fatal(err)
	}
	cfg, err := loadServeConfig(filepath.Join(*datadir, "brmcp.json"))
	if err != nil {
		log.Fatal(err)
	}
	botCfg, err := kitconfig.LoadBotConfig(*datadir, "brmcp-serve.conf")
	if err != nil {
		log.Fatal(err)
	}

	h, err := server.NewHarness(
		&mcp.Implementation{Name: "brmcp-example", Version: "0.1.0"},
		server.HarnessConfig{
			DataDir:        *datadir,
			AllowedPeers:   cfg.AllowedUIDs,
			CallsPerMinute: cfg.CallsPerMinute,
			Logf:           log.Printf,
		})
	if err != nil {
		log.Fatal(err)
	}

	server.AddTool(h, &mcp.Tool{
		Name:        "echo",
		Description: "Echo the provided text back to the caller.",
	}, 0, func(_ context.Context, _ string, in echoIn) (any, error) {
		return map[string]string{"echo": in.Text}, nil
	})
	// 0.0001 DCR per call demonstrates the paid path end to end.
	server.AddTool(h, &mcp.Tool{
		Name:        "fortune",
		Description: "Return a fortune. Paid per call.",
	}, 10_000, func(context.Context, string, struct{}) (any, error) {
		return map[string]string{"fortune": fortunes[rand.Intn(len(fortunes))]}, nil
	})

	if len(cfg.AllowedUIDs) == 0 {
		log.Printf("allowed_uids is empty: all callers are refused until %s is edited",
			filepath.Join(*datadir, "brmcp.json"))
	}
	log.Printf("serving MCP over Bison Relay via %s", botCfg.RPCURL)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := server.RunBot(ctx, h, botCfg); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
