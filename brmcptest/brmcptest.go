// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package brmcptest provides an in-memory private message fabric so brmcp
// endpoints (routers, harnesses, bridges) can be exercised end to end
// without a relay.
package brmcptest

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/karamble/brmcp"
)

// SenderFunc adapts a function to brmcp.PMSender.
type SenderFunc func(ctx context.Context, peer, text string) error

// SendPM implements brmcp.PMSender.
func (f SenderFunc) SendPM(ctx context.Context, peer, text string) error {
	return f(ctx, peer, text)
}

// UID returns a deterministic 64-hex Bison Relay uid built from the low
// four bits of n, e.g. UID(1) is the string of 64 '1' characters.
func UID(n int) string {
	const digits = "0123456789abcdef"
	return strings.Repeat(string(digits[n&0xf]), 64)
}

// Fabric delivers private messages between test endpoints the way the
// relay would: asynchronously, attributed to the sending uid.
type Fabric struct {
	mu        sync.Mutex
	endpoints map[string]func(fromUID, text string)
	routers   []*brmcp.Router
	closed    bool
	sent      atomic.Int64
}

func NewFabric() *Fabric {
	return &Fabric{endpoints: make(map[string]func(string, string))}
}

// Attach registers deliver as the endpoint living at uid. Every PM sent to
// uid is delivered as deliver(fromUID, text) on its own goroutine. A
// router's HandlePM is a valid deliver function.
func (f *Fabric) Attach(uid string, deliver func(fromUID, text string)) {
	f.mu.Lock()
	f.endpoints[uid] = deliver
	f.mu.Unlock()
}

// Sender returns the PM sender for an endpoint living at fromUID. PMs to
// unknown peers (or after Close) are dropped, like a relay without the
// recipient.
func (f *Fabric) Sender(fromUID string) brmcp.PMSender {
	return SenderFunc(func(_ context.Context, peer, text string) error {
		f.mu.Lock()
		deliver := f.endpoints[peer]
		closed := f.closed
		f.mu.Unlock()
		if closed || deliver == nil {
			return nil
		}
		f.sent.Add(1)
		go deliver(fromUID, text)
		return nil
	})
}

// NewRouter builds a router living at uid on this fabric: cfg.Sender is
// replaced with the fabric's sender for uid and inbound PMs feed the
// router's HandlePM. The router is closed with the fabric.
func (f *Fabric) NewRouter(uid string, cfg brmcp.RouterConfig) *brmcp.Router {
	cfg.Sender = f.Sender(uid)
	r := brmcp.NewRouter(cfg)
	f.Attach(uid, r.HandlePM)
	f.mu.Lock()
	f.routers = append(f.routers, r)
	f.mu.Unlock()
	return r
}

// Sent reports how many PMs crossed the fabric, for chunk-count assertions.
func (f *Fabric) Sent() int64 { return f.sent.Load() }

// Close stops delivery and closes every fabric-built router.
func (f *Fabric) Close() {
	f.mu.Lock()
	f.closed = true
	routers := f.routers
	f.routers = nil
	f.mu.Unlock()
	for _, r := range routers {
		r.Close()
	}
}

// Text concatenates the text content of a tool result.
func Text(res *mcp.CallToolResult) string {
	var sb strings.Builder
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			sb.WriteString(tc.Text)
		}
	}
	return sb.String()
}
