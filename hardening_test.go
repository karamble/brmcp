// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package brmcp_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/karamble/brmcp"
	"github.com/karamble/brmcp/brmcptest"
)

func TestIdleSessionExpiry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	server := newEchoServer(t)
	var accepts atomic.Int64
	f := brmcptest.NewFabric()
	t.Cleanup(f.Close)
	clientRouter := f.NewRouter(clientUID, brmcp.RouterConfig{Logf: t.Logf})
	f.NewRouter(serverUID, brmcp.RouterConfig{
		Logf:        t.Logf,
		IdleTimeout: 200 * time.Millisecond,
		Accept: func(conn *brmcp.Conn) {
			accepts.Add(1)
			if _, err := server.Connect(ctx, conn.AsTransport(), nil); err != nil {
				t.Logf("server connect: %v", err)
			}
		},
	})

	conn, err := clientRouter.Dial(serverUID)
	if err != nil {
		t.Fatal(err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "c", Version: "0"}, nil)
	session, err := client.Connect(ctx, conn.AsTransport(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if _, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "echo", Arguments: map[string]any{"text": "hi"},
	}); err != nil {
		t.Fatal(err)
	}
	if got := accepts.Load(); got != 1 {
		t.Fatalf("accepts before idle: %d != 1", got)
	}

	// No traffic for well over the idle timeout: the server side expires
	// the session, so the next message opens a fresh one.
	time.Sleep(700 * time.Millisecond)
	cctx, ccancel := context.WithTimeout(ctx, 3*time.Second)
	defer ccancel()
	_, _ = session.CallTool(cctx, &mcp.CallToolParams{
		Name: "echo", Arguments: map[string]any{"text": "again"},
	})
	if got := accepts.Load(); got != 2 {
		t.Fatalf("idle session was not expired: accepts %d != 2", got)
	}
}

func TestSessionCapPerPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	server := newEchoServer(t)
	var accepts atomic.Int64
	f := brmcptest.NewFabric()
	t.Cleanup(f.Close)
	clientRouter := f.NewRouter(clientUID, brmcp.RouterConfig{Logf: t.Logf})
	f.NewRouter(serverUID, brmcp.RouterConfig{
		Logf:               t.Logf,
		MaxSessionsPerPeer: 2,
		Accept: func(conn *brmcp.Conn) {
			accepts.Add(1)
			if _, err := server.Connect(ctx, conn.AsTransport(), nil); err != nil {
				t.Logf("server connect: %v", err)
			}
		},
	})

	for i := 0; i < 2; i++ {
		conn, err := clientRouter.Dial(serverUID)
		if err != nil {
			t.Fatal(err)
		}
		client := mcp.NewClient(&mcp.Implementation{Name: "c", Version: "0"}, nil)
		session, err := client.Connect(ctx, conn.AsTransport(), nil)
		if err != nil {
			t.Fatalf("session %d: %v", i, err)
		}
		defer session.Close()
	}

	// The third concurrent session is dropped before Accept.
	conn, err := clientRouter.Dial(serverUID)
	if err != nil {
		t.Fatal(err)
	}
	cctx, ccancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer ccancel()
	client := mcp.NewClient(&mcp.Implementation{Name: "c", Version: "0"}, nil)
	if _, err := client.Connect(cctx, conn.AsTransport(), nil); err == nil {
		t.Fatal("third session connected past the per-peer bound")
	}
	if got := accepts.Load(); got != 2 {
		t.Fatalf("accepts: %d != 2", got)
	}
}
