// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package directory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/karamble/brmcp"
)

const (
	dialTimeout     = 2 * time.Minute
	crawlTimeout    = 2 * time.Minute
	testCallTimeout = 10 * time.Minute
	payWait         = 180 * time.Second
	creditPollTries = 6
	creditPollDelay = 3 * time.Second
)

// SpendEntry is one outbound payment the directory launched. Entries are
// recorded before the rail is invoked and removed only on definitive rail
// failure, so a crash window never hides money that may have left.
type SpendEntry struct {
	TS    int64  `json:"ts"`
	UID   string `json:"uid"`
	Tool  string `json:"tool"`
	Atoms int64  `json:"atoms"`
}

type spendJournal struct {
	mu      sync.Mutex
	path    string
	entries []SpendEntry
}

func openSpendJournal(path string) (*spendJournal, error) {
	j := &spendJournal{path: path}
	raw, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return j, nil
	case err != nil:
		return nil, err
	}
	if err := json.Unmarshal(raw, &j.entries); err != nil {
		return nil, fmt.Errorf("spend journal %s corrupt: %w", path, err)
	}
	return j, nil
}

func (j *spendJournal) persistLocked() error {
	raw, err := json.MarshalIndent(j.entries, "", "  ")
	if err != nil {
		return err
	}
	tmp := j.path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(j.path), 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, j.path)
}

// record appends an entry and returns its position token for removal.
func (j *spendJournal) record(e SpendEntry) (int, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.entries = append(j.entries, e)
	return len(j.entries) - 1, j.persistLocked()
}

// remove zeroes an entry recorded in error (definitive rail failure).
func (j *spendJournal) remove(token int) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if token < 0 || token >= len(j.entries) {
		return
	}
	j.entries = append(j.entries[:token], j.entries[token+1:]...)
	if err := j.persistLocked(); err != nil {
		// The stray entry overstates spending, which is the safe side.
		_ = err
	}
}

func (j *spendJournal) all() []SpendEntry {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]SpendEntry(nil), j.entries...)
}

// dialPeer opens a fresh MCP client session to uid over router. The
// returned cleanup closes the session and its connection.
func dialPeer(ctx context.Context, router *brmcp.Router, name, uid string) (*mcp.ClientSession, func(), error) {
	if router == nil {
		return nil, nil, errors.New("no router; not started")
	}
	conn, err := router.Dial(uid)
	if err != nil {
		return nil, nil, err
	}
	cl := mcp.NewClient(&mcp.Implementation{Name: name, Version: "0.1.0"}, nil)
	dctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	session, err := cl.Connect(dctx, conn.AsTransport(), nil)
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("connect to %s: %w", uid[:8], err)
	}
	cleanup := func() {
		_ = session.Close()
		conn.Close()
	}
	return session, cleanup, nil
}

// dial opens a fresh MCP client session to uid over the shared router.
func (s *Service) dial(ctx context.Context, uid string) (*mcp.ClientSession, func(), error) {
	return dialPeer(ctx, s.routerHandle(), s.policy.Name, uid)
}

// crawl fetches uid's tool catalog verbatim, price metadata included.
func (s *Service) crawl(ctx context.Context, uid string) (json.RawMessage, error) {
	session, cleanup, err := s.dial(ctx, uid)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	lctx, cancel := context.WithTimeout(ctx, crawlTimeout)
	defer cancel()
	tl, err := session.ListTools(lctx, nil)
	if err != nil {
		return nil, fmt.Errorf("list tools: %w", err)
	}
	raw, err := json.Marshal(tl.Tools)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

// callResult is one relayed call's outcome.
type callResult struct {
	res       *mcp.CallToolResult
	paidAtoms int64
	latencyMs int64
}

// paidCall relays one tool call to uid, settling at most one
// payment_required within maxAtoms. debit, when non-nil, moves the amount
// out of the payer-of-record's escrow before the rail is invoked (test
// calls: the provider prefunded); nil means the directory pays out of
// pocket (snapshot purchases). The same callKey is reused across the
// payment retry so the remote journal executes and charges once.
func (s *Service) paidCall(ctx context.Context, uid, tool string, args json.RawMessage,
	callKey string, maxAtoms int64, debit func(atoms int64) error) (callResult, error) {

	session, cleanup, err := s.dial(ctx, uid)
	if err != nil {
		return callResult{}, err
	}
	defer func() { cleanup() }()

	meta := mcp.Meta{brmcp.CallKeyMetaKey: callKey}
	var out callResult
	paid := false
	polls := 0
	for attempt := 0; ; attempt++ {
		cctx, cancel := context.WithTimeout(ctx, testCallTimeout)
		t0 := s.clk.Now()
		res, err := session.CallTool(cctx, &mcp.CallToolParams{Name: tool, Arguments: args, Meta: meta})
		cancel()
		if err != nil {
			// One transport-level retry on a fresh session.
			if attempt == 0 && ctx.Err() == nil {
				cleanup()
				session, cleanup, err = s.dial(ctx, uid)
				if err != nil {
					return out, err
				}
				continue
			}
			return out, err
		}
		pr := brmcp.ParsePaymentRequired(res)
		if pr == nil {
			out.res = res
			out.latencyMs = s.clk.Now().Sub(t0).Milliseconds()
			return out, nil
		}
		if paid {
			// The tip settles asynchronously; poll briefly for the
			// credit to land before giving up.
			if polls < creditPollTries {
				polls++
				select {
				case <-s.clk.After(creditPollDelay):
					continue
				case <-ctx.Done():
					return out, ctx.Err()
				}
			}
			return out, errors.New("payment made but the call still refuses; credit did not land")
		}
		amount := pr.ShortfallAtoms
		if amount <= 0 {
			amount = pr.PriceAtoms
		}
		if amount <= 0 {
			return out, errors.New("payment_required without an amount")
		}
		if amount > maxAtoms {
			return out, fmt.Errorf("quote %d atoms exceeds the %d atom budget", amount, maxAtoms)
		}
		if debit != nil {
			if err := debit(amount); err != nil {
				return out, fmt.Errorf("escrow debit: %w", err)
			}
		}
		token, err := s.spend.record(SpendEntry{
			TS: s.clk.Now().Unix(), UID: uid, Tool: tool, Atoms: amount,
		})
		if err != nil {
			s.logf("brmcpdir: spend journal: %v", err)
		}
		payCtx, payCancel := context.WithTimeout(ctx, payWait)
		payErr := s.cfg.Payer.Pay(payCtx, uid, amount)
		timedOut := payCtx.Err() != nil
		payCancel()
		if payErr != nil {
			if !timedOut {
				// Definitive rail failure: no money left.
				s.spend.remove(token)
				if debit != nil {
					if cerr := s.harness.Billing().Credit(uid, amount); cerr != nil {
						s.logf("brmcpdir: escrow refund %d to %s: %v", amount, uid[:8], cerr)
					}
				}
			}
			return out, fmt.Errorf("payment: %w", payErr)
		}
		paid = true
		out.paidAtoms += amount
	}
}

// runTest executes the provider-nominated verification call against uid,
// funded by the registration's remaining budget, and records the outcome
// on the registration.
func (s *Service) runTest(ctx context.Context, uid string, reg Registration) (Registration, bool) {
	remaining := reg.BudgetAtoms - reg.PaidAtoms
	if remaining < 0 {
		remaining = 0
	}
	var args json.RawMessage
	if reg.Test.Args != nil {
		raw, err := json.Marshal(reg.Test.Args)
		if err == nil {
			args = raw
		}
	}
	debit := func(atoms int64) error {
		return s.harness.Billing().Debit(uid, atoms)
	}
	cr, err := s.paidCall(ctx, uid, reg.Test.Tool, args, reg.CallKey, remaining, debit)
	reg.PaidAtoms += cr.paidAtoms
	reg.TestLatencyMs = cr.latencyMs
	switch {
	case err != nil:
		reg.TestOutcome = "failed"
		reg.TestErr = err.Error()
	case cr.res != nil && cr.res.IsError:
		reg.TestOutcome = "failed"
		reg.TestErr = resultText(cr.res)
	default:
		reg.TestOutcome = "ok"
		reg.TestErr = ""
	}
	return reg, reg.TestOutcome == "ok"
}

// resultJSON concatenates a result's full text content for decoding.
func resultJSON(res *mcp.CallToolResult) []byte {
	if res == nil {
		return nil
	}
	var out []byte
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			if len(out) > 0 {
				out = append(out, '\n')
			}
			out = append(out, tc.Text...)
		}
	}
	return out
}

// resultText is resultJSON bounded for error diagnostics.
func resultText(res *mcp.CallToolResult) string {
	out := string(resultJSON(res))
	if len(out) > 512 {
		out = out[:512]
	}
	return out
}
