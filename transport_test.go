// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package brmcp_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/karamble/brmcp"
	"github.com/karamble/brmcp/brmcptest"
)

var (
	clientUID = brmcptest.UID(1)
	serverUID = brmcptest.UID(2)
)

type echoIn struct {
	Text string `json:"text"`
}

func newEchoServer(t *testing.T) *mcp.Server {
	t.Helper()
	s := mcp.NewServer(&mcp.Implementation{Name: "brmcp-test", Version: "0"}, nil)
	mcp.AddTool(s, &mcp.Tool{Name: "echo", Description: "echo text back"},
		func(_ context.Context, _ *mcp.CallToolRequest, in echoIn) (*mcp.CallToolResult, any, error) {
			return nil, map[string]string{"echo": in.Text}, nil
		})
	mcp.AddTool(s, &mcp.Tool{Name: "big", Description: "return a large payload"},
		func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, any, error) {
			return nil, map[string]string{"blob": strings.Repeat("x", 8192)}, nil
		})
	return s
}

func TestMCPSessionOverPM(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	server := newEchoServer(t)
	// ChunkSize 512 forces the big tool's result to cross as many parts.
	f := brmcptest.NewFabric()
	t.Cleanup(f.Close)
	clientRouter := f.NewRouter(clientUID, brmcp.RouterConfig{ChunkSize: 512, Logf: t.Logf})
	f.NewRouter(serverUID, brmcp.RouterConfig{
		ChunkSize: 512,
		Logf:      t.Logf,
		Accept: func(conn *brmcp.Conn) {
			if _, err := server.Connect(ctx, conn.AsTransport(), nil); err != nil {
				t.Errorf("server connect: %v", err)
			}
		},
	})

	conn, err := clientRouter.Dial(serverUID)
	if err != nil {
		t.Fatal(err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "brmcp-test-client", Version: "0"}, nil)
	session, err := client.Connect(ctx, conn.AsTransport(), nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer session.Close()

	tl, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(tl.Tools) != 2 {
		t.Fatalf("want 2 tools, got %d", len(tl.Tools))
	}

	res, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "echo",
		Arguments: map[string]any{"text": "hello over BR"},
	})
	if err != nil {
		t.Fatalf("CallTool echo: %v", err)
	}
	if res.IsError {
		t.Fatalf("echo returned tool error: %+v", res.Content)
	}
	if text := brmcptest.Text(res); !strings.Contains(text, "hello over BR") {
		t.Fatalf("echo result missing input: %s", text)
	}

	pmsBefore := f.Sent()
	res, err = session.CallTool(ctx, &mcp.CallToolParams{Name: "big", Arguments: map[string]any{}})
	if err != nil {
		t.Fatalf("CallTool big: %v", err)
	}
	if text := brmcptest.Text(res); !strings.Contains(text, strings.Repeat("x", 512)) {
		t.Fatalf("big result truncated: %d bytes", len(text))
	}
	// The oversized result must have crossed the fabric chunked: one part
	// for the request plus many for the response.
	if crossed := f.Sent() - pmsBefore; crossed < 10 {
		t.Fatalf("expected chunked response, only %d PMs crossed", crossed)
	}
}

func TestDisallowedPeerIgnored(t *testing.T) {
	accepted := make(chan *brmcp.Conn, 1)
	f := brmcptest.NewFabric()
	t.Cleanup(f.Close)
	clientRouter := f.NewRouter(clientUID, brmcp.RouterConfig{Logf: t.Logf})
	f.NewRouter(serverUID, brmcp.RouterConfig{
		Logf:   t.Logf,
		Accept: func(conn *brmcp.Conn) { accepted <- conn },
		Allow:  func(peer string) bool { return peer != clientUID },
	})

	conn, err := clientRouter.Dial(serverUID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client := mcp.NewClient(&mcp.Implementation{Name: "c", Version: "0"}, nil)
	if _, err := client.Connect(ctx, conn.AsTransport(), nil); err == nil {
		t.Fatal("connect succeeded despite disallowed peer")
	}
	select {
	case <-accepted:
		t.Fatal("disallowed peer reached Accept")
	default:
	}
}

func TestHumanChatIgnored(t *testing.T) {
	f := brmcptest.NewFabric()
	t.Cleanup(f.Close)
	srv := f.NewRouter(serverUID, brmcp.RouterConfig{
		Logf: t.Logf,
		Accept: func(conn *brmcp.Conn) {
			t.Error("plain chat created a session")
		},
	})
	srv.HandlePM(clientUID, "hey bot, how are you?")
	srv.HandlePM(clientUID, "> **nick:** quoted chat\n\nreply")
	srv.HandlePM(clientUID, "--embed[type=image/png,data=QUJD]--")
	time.Sleep(50 * time.Millisecond)
}

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
