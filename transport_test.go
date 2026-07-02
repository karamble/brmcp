// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package brmcp

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type senderFunc func(ctx context.Context, peer, text string) error

func (f senderFunc) SendPM(ctx context.Context, peer, text string) error {
	return f(ctx, peer, text)
}

const (
	clientUID = "1111111111111111111111111111111111111111111111111111111111111111"
	serverUID = "2222222222222222222222222222222222222222222222222222222222222222"
)

// fabric wires two routers as Bison Relay would: a PM sent by one side is
// delivered to the other side's HandlePM attributed to the sender's uid.
// Delivery is asynchronous, like the real relay.
type fabric struct {
	client *Router
	server *Router
	sent   atomic.Int64 // PMs crossing the fabric, to assert chunk counts
}

func newFabric(t *testing.T, accept func(*Conn), allow func(string) bool, chunkSize int) *fabric {
	t.Helper()
	f := &fabric{}
	f.client = NewRouter(RouterConfig{
		Sender: senderFunc(func(_ context.Context, peer, text string) error {
			if peer != serverUID {
				t.Errorf("client sent to unexpected peer %s", peer)
			}
			f.sent.Add(1)
			go f.server.HandlePM(clientUID, text)
			return nil
		}),
		ChunkSize: chunkSize,
		Logf:      t.Logf,
	})
	f.server = NewRouter(RouterConfig{
		Sender: senderFunc(func(_ context.Context, peer, text string) error {
			if peer != clientUID {
				t.Errorf("server sent to unexpected peer %s", peer)
			}
			f.sent.Add(1)
			go f.client.HandlePM(serverUID, text)
			return nil
		}),
		Accept:    accept,
		Allow:     allow,
		ChunkSize: chunkSize,
		Logf:      t.Logf,
	})
	t.Cleanup(func() {
		f.client.Close()
		f.server.Close()
	})
	return f
}

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
	f := newFabric(t, func(conn *Conn) {
		if _, err := server.Connect(ctx, conn.AsTransport(), nil); err != nil {
			t.Errorf("server connect: %v", err)
		}
	}, nil, 512)

	conn, err := f.client.Dial(serverUID)
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
	if text := contentText(res); !strings.Contains(text, "hello over BR") {
		t.Fatalf("echo result missing input: %s", text)
	}

	pmsBefore := f.sent.Load()
	res, err = session.CallTool(ctx, &mcp.CallToolParams{Name: "big", Arguments: map[string]any{}})
	if err != nil {
		t.Fatalf("CallTool big: %v", err)
	}
	if text := contentText(res); !strings.Contains(text, strings.Repeat("x", 512)) {
		t.Fatalf("big result truncated: %d bytes", len(text))
	}
	// The oversized result must have crossed the fabric chunked: one part
	// for the request plus many for the response.
	if crossed := f.sent.Load() - pmsBefore; crossed < 10 {
		t.Fatalf("expected chunked response, only %d PMs crossed", crossed)
	}
}

func TestDisallowedPeerIgnored(t *testing.T) {
	accepted := make(chan *Conn, 1)
	f := newFabric(t, func(conn *Conn) { accepted <- conn }, func(peer string) bool {
		return peer != clientUID
	}, 0)

	conn, err := f.client.Dial(serverUID)
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
	f := newFabric(t, func(conn *Conn) {
		t.Error("plain chat created a session")
	}, nil, 0)
	f.server.HandlePM(clientUID, "hey bot, how are you?")
	f.server.HandlePM(clientUID, "> **nick:** quoted chat\n\nreply")
	f.server.HandlePM(clientUID, "--embed[type=image/png,data=QUJD]--")
	time.Sleep(50 * time.Millisecond)
}

func contentText(res *mcp.CallToolResult) string {
	var sb strings.Builder
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			sb.WriteString(tc.Text)
		}
	}
	return sb.String()
}
