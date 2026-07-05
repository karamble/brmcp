// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bridge

import (
	"context"
	"crypto/subtle"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Handler returns the bridge's HTTP surface: constant-time bearer auth (an
// empty token never authorizes), the /mcp/<64-hex-uid> path gate (404 for
// malformed or non-allowlisted uids), and the streamable-HTTP MCP proxy.
// While the bridge is disabled it answers 404 to everything. Hosts that
// mount the handler themselves own the listener lifecycle, including
// severing long-lived streams when the token changes.
func (b *Bridge) Handler() http.Handler {
	mcpHandler := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server {
		uid := strings.ToLower(strings.TrimPrefix(r.URL.Path, "/mcp/"))
		srv, err := b.proxyServerFor(uid)
		if err != nil {
			// Unreachable after the middleware preflight; kept as a
			// backstop so the handshake stays valid.
			b.logf("brmcp bridge: proxy for %s: %v", uid, err)
			return mcp.NewServer(&mcp.Implementation{Name: b.cfg.Name, Version: "0"}, nil)
		}
		return srv
	}, nil)
	return b.authMiddleware(mcpHandler)
}

func (b *Bridge) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.mu.Lock()
		enabled := b.settings.Enabled
		token := b.settings.Token
		b.mu.Unlock()
		if !enabled {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if token == "" || subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		uid := strings.ToLower(strings.TrimPrefix(r.URL.Path, "/mcp/"))
		if !uidRe.MatchString(uid) || !b.botAllowed(uid) {
			http.Error(w, "unknown bot", http.StatusNotFound)
			return
		}
		// Building (or fetching) the proxy up front turns an unreachable
		// bot into a clean 503 instead of a hollow MCP handshake the agent
		// would cache as an empty tool list.
		if _, err := b.proxyServerFor(uid); err != nil {
			b.logf("brmcp bridge: proxy for %s: %v", uid, err)
			http.Error(w, "bot unavailable", http.StatusServiceUnavailable)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ListenAddr reports the owned listener's bound address, or nil when the
// bridge is not listening (disabled, Handler-only mode, or a bind failure).
func (b *Bridge) ListenAddr() net.Addr {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lnAddr
}

func (b *Bridge) startListenerLocked() error {
	ln, err := net.Listen("tcp", b.cfg.ListenAddr)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Handler: b.Handler(),
		// No read/write deadlines: calls legitimately block for relay
		// round trips and approval decisions. Only the header read is
		// bounded.
		ReadHeaderTimeout: 15 * time.Second,
	}
	b.httpSrv = srv
	b.lnAddr = ln.Addr()
	b.logf("brmcp bridge: listener on http://%s (streamable HTTP, bearer auth)", ln.Addr())
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			b.logf("brmcp bridge: listener: %v", err)
		}
	}()
	return nil
}

func (b *Bridge) stopListenerLocked() {
	srv := b.httpSrv
	b.httpSrv = nil
	b.lnAddr = nil
	if srv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	b.logf("brmcp bridge: listener stopped")
}
