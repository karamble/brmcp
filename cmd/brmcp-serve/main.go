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
	"sync"
	"syscall"

	"github.com/companyzero/bisonrelay/clientrpc/types"
	"github.com/companyzero/bisonrelay/zkidentity"
	"github.com/decred/dcrd/dcrutil/v4"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	kit "github.com/vctt94/bisonbotkit"
	kitconfig "github.com/vctt94/bisonbotkit/config"

	"github.com/karamble/brmcp"
	"github.com/karamble/brmcp/bridge"
	"github.com/karamble/brmcp/directory"
	"github.com/karamble/brmcp/server"
)

// directoryEntry configures this bot's presence in brmcpdir directories:
// what it registers as, which invites it accepts, and what it auto-funds.
type directoryEntry struct {
	Description    string             `json:"description"`
	Tags           []string           `json:"tags"`
	Test           directory.TestSpec `json:"test"`
	AutoFund       directory.AutoFund `json:"auto_fund"`
	RegisterAtUIDs []string           `json:"register_at_uids"`
}

// serveConfig is the operator-edited harness config, created as a template
// on first run. The allowlist is empty by default: nobody can call tools
// until the operator adds caller uids.
type serveConfig struct {
	AllowedUIDs    []string        `json:"allowed_uids"`
	CallsPerMinute int             `json:"calls_per_minute"`
	Directory      *directoryEntry `json:"directory,omitempty"`
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

	// With a directory block configured, a Registrant lists this bot in
	// brmcpdir directories: it serves listing_invite beside the tools and
	// pays listing costs under the auto-fund policy.
	var hooks server.RunBotHooks
	if cfg.Directory != nil {
		payer := &tipPayer{matcher: bridge.NewTipMatcher()}
		hooks = server.RunBotHooks{
			OnBot:         payer.set,
			OnTipProgress: payer.progress,
			OnRouter: func(router *brmcp.Router) {
				reg, err := directory.NewRegistrant(directory.RegistrantConfig{
					Description: cfg.Directory.Description,
					Tags:        cfg.Directory.Tags,
					Test:        cfg.Directory.Test,
					AutoFund:    cfg.Directory.AutoFund,
					DataDir:     *datadir,
					Router:      router,
					Payer:       payer,
					Logf:        log.Printf,
				})
				if err != nil {
					log.Printf("directory registrant disabled: %v", err)
					return
				}
				reg.RegisterTools(h)
				reg.Start(ctx)
				for _, uid := range cfg.Directory.RegisterAtUIDs {
					go func(uid string) {
						if err := reg.Register(ctx, uid); err != nil {
							log.Printf("register at directory %s: %v", uid[:8], err)
						}
					}(uid)
				}
			},
		}
	}
	if err := server.RunBotHooked(ctx, h, botCfg, hooks); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

// tipPayer settles Registrant payments as Bison Relay tips, resolved by
// the matching terminal tip-progress events.
type tipPayer struct {
	matcher *bridge.TipMatcher

	mu  sync.Mutex
	bot *kit.Bot
}

func (p *tipPayer) set(bot *kit.Bot) {
	p.mu.Lock()
	p.bot = bot
	p.mu.Unlock()
}

func (p *tipPayer) progress(ev *types.TipProgressEvent) {
	if ev.Completed || !ev.WillRetry {
		var res error
		if !ev.Completed {
			res = errors.New(ev.AttemptErr)
		}
		p.matcher.Resolve(fmt.Sprintf("%x", ev.Uid), ev.AmountMatoms, res)
	}
}

func (p *tipPayer) Pay(ctx context.Context, payeeUID string, atoms int64) error {
	p.mu.Lock()
	bot := p.bot
	p.mu.Unlock()
	if bot == nil {
		return errors.New("bot not connected")
	}
	var sid zkidentity.ShortID
	if err := sid.FromString(payeeUID); err != nil {
		return fmt.Errorf("payee uid: %w", err)
	}
	w := p.matcher.Expect(payeeUID, atoms*1000)
	if err := bot.PayTip(ctx, sid, dcrutil.Amount(atoms), 3); err != nil {
		w.Cancel()
		return fmt.Errorf("tip: %w", err)
	}
	select {
	case err := <-w.Done():
		if err != nil {
			return fmt.Errorf("tip failed: %w", err)
		}
		return nil
	case <-ctx.Done():
		w.Cancel()
		return errors.New("tip not confirmed in time; the attempt keeps " +
			"running in the background and still credits the payee")
	}
}
