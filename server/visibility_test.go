// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package server_test

import (
	"context"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/karamble/brmcp"
	"github.com/karamble/brmcp/brmcptest"
	"github.com/karamble/brmcp/server"
)

func TestToolVisibility(t *testing.T) {
	adminUID := brmcptest.UID(7)
	userUID := brmcptest.UID(8)
	botUID := brmcptest.UID(9)

	h, err := server.NewHarness(&mcp.Implementation{Name: "t", Version: "0"}, server.HarnessConfig{
		DataDir:        t.TempDir(),
		AllowFunc:      func(string) bool { return true },
		CallsPerMinute: 100,
		ToolVisible: func(peer, tool string) bool {
			return tool != "admin_only" || peer == adminUID
		},
		Logf: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	server.AddTool(h, &mcp.Tool{Name: "public", Description: "for everyone"}, 0,
		func(context.Context, string, struct{}) (any, error) {
			return map[string]string{"ok": "public"}, nil
		})
	server.AddTool(h, &mcp.Tool{Name: "admin_only", Description: "operator surface"}, 0,
		func(_ context.Context, peer string, _ struct{}) (any, error) {
			return map[string]string{"admin": peer[:4]}, nil
		})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	f := brmcptest.NewFabric()
	defer f.Close()
	botRouter := h.Start(ctx, f.Sender(botUID))
	f.Attach(botUID, botRouter.HandlePM)

	dial := func(uid string) *mcp.ClientSession {
		t.Helper()
		r := f.NewRouter(uid, brmcp.RouterConfig{Logf: t.Logf})
		conn, err := r.Dial(botUID)
		if err != nil {
			t.Fatal(err)
		}
		cl := mcp.NewClient(&mcp.Implementation{Name: "t", Version: "0"}, nil)
		session, err := cl.Connect(ctx, conn.AsTransport(), nil)
		if err != nil {
			t.Fatalf("connect %s: %v", uid[:4], err)
		}
		t.Cleanup(func() { session.Close() })
		return session
	}

	names := func(s *mcp.ClientSession) map[string]bool {
		t.Helper()
		tl, err := s.ListTools(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		m := make(map[string]bool, len(tl.Tools))
		for _, tool := range tl.Tools {
			m[tool.Name] = true
		}
		return m
	}

	admin := dial(adminUID)
	got := names(admin)
	if !got["public"] || !got["admin_only"] {
		t.Fatalf("admin tool list wrong: %v", got)
	}
	res, err := admin.CallTool(ctx, &mcp.CallToolParams{Name: "admin_only", Arguments: map[string]any{}})
	if err != nil || res.IsError {
		t.Fatalf("admin call refused: err=%v res=%+v", err, res)
	}

	user := dial(userUID)
	got = names(user)
	if !got["public"] || got["admin_only"] {
		t.Fatalf("user tool list wrong: %v", got)
	}
	res, err = user.CallTool(ctx, &mcp.CallToolParams{Name: "admin_only", Arguments: map[string]any{}})
	if err == nil && !res.IsError {
		t.Fatal("hidden tool callable by non-admin")
	}
}
