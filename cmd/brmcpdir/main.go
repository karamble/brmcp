// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// brmcpdir is a yellow-pages directory for brmcp tool providers on Bison
// Relay. It connects to a running brclient/brclientd via clientrpc
// (bisonbotkit), serves the public search/registration tools and the
// uid-gated admin tools over MCP envelopes, crawls and live-tests
// registering providers, and federates with curated peer directories.
// See docs/DIRECTORY.md for the full contract.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/companyzero/bisonrelay/clientrpc/types"
	"github.com/companyzero/bisonrelay/zkidentity"
	"github.com/decred/dcrd/dcrutil/v4"
	kit "github.com/vctt94/bisonbotkit"
	kitconfig "github.com/vctt94/bisonbotkit/config"

	"github.com/karamble/brmcp"
	"github.com/karamble/brmcp/bridge"
	"github.com/karamble/brmcp/directory"
	"github.com/karamble/brmcp/server"
)

const matomsPerAtom = 1000

// loadPolicy reads the operator's policy file, writing a template on the
// first run.
func loadPolicy(path string) (directory.Policy, error) {
	var p directory.Policy
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		p.AdminUIDs = []string{}
		out, _ := json.MarshalIndent(p, "", "  ")
		if err := os.WriteFile(path, out, 0o600); err != nil {
			return p, err
		}
		log.Printf("created %s - add operator uids to admin_uids", path)
		return p, nil
	}
	if err != nil {
		return p, err
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return p, fmt.Errorf("parse %s: %w", path, err)
	}
	return p, nil
}

// backend adapts the current bisonbotkit bot to the directory's PMSender,
// Payer, and Introducer. The bot is swapped on reconnect, so the pointer
// is guarded.
type backend struct {
	matcher *bridge.TipMatcher

	mu  sync.Mutex
	bot *kit.Bot
}

func (b *backend) current() *kit.Bot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.bot
}

func (b *backend) set(bot *kit.Bot) {
	b.mu.Lock()
	b.bot = bot
	b.mu.Unlock()
}

func (b *backend) SendPM(ctx context.Context, peer, text string) error {
	bot := b.current()
	if bot == nil {
		return errors.New("brmcpdir: bot not connected")
	}
	return bot.SendPM(ctx, peer, text)
}

// Pay settles one payment as a Bison Relay tip and blocks on the matching
// terminal tip-progress event.
func (b *backend) Pay(ctx context.Context, payeeUID string, atoms int64) error {
	bot := b.current()
	if bot == nil {
		return errors.New("brmcpdir: bot not connected")
	}
	var sid zkidentity.ShortID
	if err := sid.FromString(payeeUID); err != nil {
		return fmt.Errorf("payee uid: %w", err)
	}
	w := b.matcher.Expect(payeeUID, atoms*matomsPerAtom)
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

// Introduce requests a transitive KX with target through mediator.
func (b *backend) Introduce(ctx context.Context, mediatorUID, targetUID string) error {
	bot := b.current()
	if bot == nil {
		return errors.New("brmcpdir: bot not connected")
	}
	return bot.MediateKX(ctx, mediatorUID, targetUID)
}

// statusClient pushes KX suggestions through brclientd's status server,
// which exposes the client library's SuggestKX (clientrpc does not). The
// same mTLS chain the clientrpc connection uses authenticates here.
type statusClient struct {
	base string
	hc   *http.Client
}

func newStatusClient(baseURL, serverCertPath, clientCertPath, clientKeyPath string) (*statusClient, error) {
	serverPEM, err := os.ReadFile(serverCertPath)
	if err != nil {
		return nil, fmt.Errorf("read server cert: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(serverPEM) {
		return nil, fmt.Errorf("parse server cert at %s", serverCertPath)
	}
	clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load client cert pair: %w", err)
	}
	return &statusClient{
		base: strings.TrimRight(baseURL, "/"),
		hc: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{
				RootCAs:      pool,
				Certificates: []tls.Certificate{clientCert},
			}},
		},
	}, nil
}

// Suggest implements directory.Suggester via POST /contacts/suggest-kx.
func (s *statusClient) Suggest(ctx context.Context, inviteeUID, targetUID string) error {
	body, err := json.Marshal(struct {
		Invitee string `json:"invitee"`
		Target  string `json:"target"`
	}{Invitee: inviteeUID, Target: targetUID})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		s.base+"/contacts/suggest-kx", strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := s.hc.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNoContent && res.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(res.Body, 512))
		return fmt.Errorf("status %d: %s", res.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}

func main() {
	defaultDir := dcrutil.AppDataDir("brmcpdir", false)
	datadir := flag.String("datadir", defaultDir, "data directory (bot config, policy, index, journals)")
	flag.Parse()

	if err := os.MkdirAll(*datadir, 0o700); err != nil {
		log.Fatal(err)
	}
	policy, err := loadPolicy(filepath.Join(*datadir, "brmcpdir.json"))
	if err != nil {
		log.Fatal(err)
	}
	if len(policy.AdminUIDs) == 0 {
		log.Printf("admin_uids is empty: the admin tools are unreachable until %s is edited",
			filepath.Join(*datadir, "brmcpdir.json"))
	}
	botCfg, err := kitconfig.LoadBotConfig(*datadir, "brmcpdir.conf")
	if err != nil {
		log.Fatal(err)
	}

	be := &backend{matcher: bridge.NewTipMatcher()}

	// Introductions ride brclientd's status server; without it (stock
	// brclient) the introduce tool reports itself unsupported.
	var suggester directory.Suggester
	if statusURL := botCfg.ExtraConfig["brstatusurl"]; statusURL != "" {
		sc, err := newStatusClient(statusURL,
			botCfg.ServerCertPath, botCfg.ClientCertPath, botCfg.ClientKeyPath)
		if err != nil {
			log.Fatalf("brstatusurl configured but unusable: %v", err)
		}
		suggester = sc
		log.Printf("introductions enabled via %s", statusURL)
	} else {
		log.Printf("brstatusurl not set: the introduce tool is disabled")
	}

	svc, err := directory.New(directory.Config{
		DataDir:    *datadir,
		Policy:     policy,
		Payer:      be,
		Introducer: be,
		Suggester:  suggester,
		Logf:       log.Printf,
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pmChan := make(chan *types.ReceivedPM, 64)
	tipChan := make(chan *types.ReceivedTip, 16)
	tipProgressChan := make(chan *types.TipProgressEvent, 16)
	kxChan := make(chan *types.KXCompleted, 16)
	botCfg.PMChan = pmChan
	botCfg.TipReceivedChan = tipChan
	botCfg.TipProgressChan = tipProgressChan
	botCfg.KXChan = kxChan

	tips, err := server.OpenTipJournal(filepath.Join(*datadir, "tips.json"))
	if err != nil {
		log.Fatal(err)
	}

	router := svc.Start(ctx, be)
	log.Printf("directory %q serving over Bison Relay via %s (snapshot key %s)",
		svc.Name(), botCfg.RPCURL, svc.PublicKey()[:16])

	// Envelope frames feed the router; everything else is ignored - the
	// directory speaks nothing but MCP.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case pm := <-pmChan:
				if pm == nil || pm.Msg == nil {
					continue
				}
				if brmcp.IsEnvelope(pm.Msg.Message) {
					router.HandlePM(hex.EncodeToString(pm.Uid), pm.Msg.Message)
				}
			}
		}
	}()
	// Tips fund registrations; the journal keeps redeliveries from
	// crediting twice.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case tip := <-tipChan:
				if tip == nil {
					continue
				}
				uid := hex.EncodeToString(tip.Uid)
				if !tips.Seen(tip.SequenceId) {
					svc.CreditTip(uid, tip.AmountMatoms/matomsPerAtom)
					if err := tips.Record(tip.SequenceId); err != nil {
						log.Printf("record tip %d: %v", tip.SequenceId, err)
					}
				}
				if bot := be.current(); bot != nil {
					if err := bot.AckTipReceived(ctx, tip.SequenceId); err != nil {
						log.Printf("ack tip %d: %v", tip.SequenceId, err)
					}
				}
			}
		}
	}()
	// Terminal tip-progress events resolve outbound payments; the kit
	// blocks its stream until every event is acked.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-tipProgressChan:
				if ev == nil {
					continue
				}
				if ev.Completed || !ev.WillRetry {
					var res error
					if !ev.Completed {
						res = errors.New(ev.AttemptErr)
					}
					be.matcher.Resolve(hex.EncodeToString(ev.Uid), ev.AmountMatoms, res)
				}
				if bot := be.current(); bot != nil {
					if err := bot.AckTipProgress(ctx, ev.SequenceId); err != nil {
						log.Printf("ack tip progress %d: %v", ev.SequenceId, err)
					}
				}
			}
		}
	}()
	// Completed key exchanges resume parked federation pursuits.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case kx := <-kxChan:
				if kx == nil {
					continue
				}
				svc.NotifyKX(hex.EncodeToString(kx.Uid))
			}
		}
	}()

	// Recreate the bot whenever its clientrpc websocket dies.
	for {
		bot, err := kit.NewBot(botCfg)
		if err != nil {
			log.Printf("bot init: %v (retrying)", err)
		} else {
			be.set(bot)
			var ident types.PublicIdentity
			if err := bot.UserPublicIdentity(ctx, &types.PublicIdentityReq{}, &ident); err != nil {
				log.Printf("read own identity: %v", err)
			} else {
				svc.SetSelfUID(hex.EncodeToString(ident.Identity))
				log.Printf("connected as %q (%s)", ident.Nick, hex.EncodeToString(ident.Identity)[:16])
			}
			err = bot.Run(ctx)
			be.set(nil)
			bot.Close()
			if ctx.Err() != nil {
				return
			}
			log.Printf("bot run ended: %v (reconnecting)", err)
		}
		select {
		case <-time.After(3 * time.Second):
		case <-ctx.Done():
			return
		}
	}
}
