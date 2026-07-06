// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bridge

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/karamble/brmcp"
)

// botLink is one bot's session state: the relay connection, the MCP client
// session on it, and the local proxy server mirroring the bot's tools.
type botLink struct {
	mu      sync.Mutex
	uid     string
	conn    *brmcp.Conn
	session *mcp.ClientSession
	proxy   *mcp.Server
}

func (l *botLink) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.resetLocked()
}

func (l *botLink) resetLocked() {
	if l.session != nil {
		_ = l.session.Close()
	}
	if l.conn != nil {
		l.conn.Close()
	}
	l.session = nil
	l.conn = nil
	l.proxy = nil
}

// proxyServerFor returns (building if needed) the local MCP server whose
// tools mirror the remote bot's catalog 1:1, including the price metadata.
func (b *Bridge) proxyServerFor(uid string) (*mcp.Server, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, fmt.Errorf("bridge closed")
	}
	link := b.bots[uid]
	if link == nil {
		link = &botLink{uid: uid}
		b.bots[uid] = link
	}
	b.mu.Unlock()

	link.mu.Lock()
	defer link.mu.Unlock()
	if link.proxy != nil {
		return link.proxy, nil
	}
	session, err := b.dialLocked(link)
	if err != nil {
		return nil, err
	}
	lctx, cancel := context.WithTimeout(b.baseCtx(), 2*time.Minute)
	defer cancel()
	tl, err := session.ListTools(lctx, nil)
	if err != nil {
		link.resetLocked()
		return nil, fmt.Errorf("list tools: %w", err)
	}
	srv := mcp.NewServer(&mcp.Implementation{Name: "brmcp-" + uid[:8], Version: "1"}, nil)
	for _, t := range tl.Tools {
		tool := *t
		srv.AddTool(&tool, b.passthrough(link, tool.Name))
	}
	link.proxy = srv
	b.logf("brmcp bridge: proxy for bot %s serving %d tools", uid[:8], len(tl.Tools))
	return srv, nil
}

func (b *Bridge) dialLocked(link *botLink) (*mcp.ClientSession, error) {
	if link.session != nil {
		return link.session, nil
	}
	conn, err := b.router.Dial(link.uid)
	if err != nil {
		return nil, err
	}
	cl := mcp.NewClient(&mcp.Implementation{Name: b.cfg.Name, Version: "1"}, nil)
	dctx, cancel := context.WithTimeout(b.baseCtx(), 2*time.Minute)
	defer cancel()
	session, err := cl.Connect(dctx, conn.AsTransport(), nil)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("connect to bot: %w", err)
	}
	link.conn = conn
	link.session = session
	return session, nil
}

// passthrough relays one tool call to the bot, transparently settling a
// payment_required refusal when the spending policy permits, then retrying.
func (b *Bridge) passthrough(link *botLink, tool string) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var args json.RawMessage
		if req.Params != nil {
			args = req.Params.Arguments
		}
		// One idempotency key per logical call: the transport retry and the
		// post-payment re-issue reuse it, so the bot can never execute or
		// charge the same call twice on a lost reply (brmcp deduplicates
		// per caller and replays the recorded outcome).
		var keyB [16]byte
		if _, err := rand.Read(keyB[:]); err != nil {
			return nil, err
		}
		callMeta := mcp.Meta{brmcp.CallKeyMetaKey: hex.EncodeToString(keyB[:])}
		paid := false
		for attempt := 0; ; attempt++ {
			link.mu.Lock()
			session, err := b.dialLocked(link)
			link.mu.Unlock()
			if err != nil {
				return nil, err
			}
			res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: args, Meta: callMeta})
			if err != nil {
				// One transport-level retry on a fresh session.
				link.reset()
				if attempt == 0 {
					continue
				}
				return nil, err
			}
			pr := brmcp.ParsePaymentRequired(res)
			if pr == nil {
				return res, nil
			}
			if paid {
				// Already settled once: the credit may still be in
				// flight (tips settle asynchronously). Poll briefly.
				if attempt < 6 {
					select {
					case <-b.clk.After(3 * time.Second):
						continue
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				return res, nil
			}
			if err := b.settle(ctx, link.uid, tool, pr); err != nil {
				b.logf("brmcp bridge: payment for %s/%s refused: %v", link.uid[:8], tool, err)
				res.Content = append(res.Content, &mcp.TextContent{
					Text: b.cfg.Name + ": payment not made: " + err.Error(),
				})
				return res, nil
			}
			paid = true
		}
	}
}
