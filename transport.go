// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package brmcp carries Model Context Protocol sessions over Bison Relay
// private messages. It binds the MCP go-sdk's Transport contract to an
// abstract PM send/receive pair, so the same code serves both a
// bisonbotkit-backed bot process and an embedded Bison Relay client.
package brmcp

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/karamble/brmcp/wire"
)

// PMSender delivers one private message body to a Bison Relay peer,
// identified by its lowercase 64-hex user id.
type PMSender interface {
	SendPM(ctx context.Context, peer string, text string) error
}

// RouterConfig wires a Router to its host.
type RouterConfig struct {
	// Sender delivers outgoing parts.
	Sender PMSender
	// Accept, when non-nil, is invoked for each NEW inbound session
	// (server role). A nil Accept drops unknown inbound sessions, which
	// is the client role: only sessions created via Dial exist.
	Accept func(conn *Conn)
	// Allow gates peers before any state is allocated for them. nil
	// allows everyone; servers should install their allowlist here so
	// unknown peers cannot even fill reassembly buffers.
	Allow func(peer string) bool
	// TTL is stamped as the deadline on outgoing messages. Bison Relay
	// stores and forwards, so without a deadline a request could execute
	// long after the caller gave up. Zero selects 10 minutes.
	TTL time.Duration
	// ChunkSize overrides wire.DefaultChunkSize (tests use small values).
	ChunkSize int
	// Assembler bounds reassembly state.
	Assembler wire.AssemblerConfig
	// InboxSize is the per-session queue of decoded inbound messages.
	// A session that overflows it is closed. Zero selects 128.
	InboxSize int
	// Logf, when non-nil, receives diagnostic lines.
	Logf func(format string, args ...any)
}

// Router demuxes envelope parts arriving on the host's single PM stream
// into per-(peer, sid) MCP connections.
type Router struct {
	cfg RouterConfig
	asm *wire.Assembler

	mu       sync.Mutex
	sessions map[string]*Conn // key: peer + "/" + sid
	closed   bool
}

func NewRouter(cfg RouterConfig) *Router {
	if cfg.TTL <= 0 {
		cfg.TTL = 10 * time.Minute
	}
	if cfg.InboxSize <= 0 {
		cfg.InboxSize = 128
	}
	return &Router{
		cfg:      cfg,
		asm:      wire.NewAssembler(cfg.Assembler),
		sessions: make(map[string]*Conn),
	}
}

func (r *Router) logf(format string, args ...any) {
	if r.cfg.Logf != nil {
		r.cfg.Logf(format, args...)
	}
}

// HandlePM feeds one inbound private message. Text that is not a valid
// envelope part is ignored so the DM thread stays usable for human chat.
func (r *Router) HandlePM(peer, text string) {
	part, ok := wire.Parse(text)
	if !ok {
		return
	}
	if r.cfg.Allow != nil && !r.cfg.Allow(peer) {
		r.logf("brmcp: dropping part from disallowed peer %s", peer)
		return
	}
	payload, err := r.asm.Add(peer, part, time.Now())
	if err != nil {
		r.logf("brmcp: reassembly from %s: %v", peer, err)
		return
	}
	if payload == nil {
		return
	}
	msg, err := jsonrpc.DecodeMessage(payload)
	if err != nil {
		r.logf("brmcp: bad JSON-RPC payload from %s: %v", peer, err)
		return
	}

	key := peer + "/" + part.SID
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	conn := r.sessions[key]
	var accepted *Conn
	if conn == nil {
		if r.cfg.Accept == nil {
			r.mu.Unlock()
			r.logf("brmcp: no session %s and no Accept; dropping", key)
			return
		}
		conn = r.newConnLocked(peer, part.SID)
		accepted = conn
	}
	r.mu.Unlock()

	// Accept runs outside the lock: it typically hands the connection to
	// an MCP server, which may immediately Write.
	if accepted != nil {
		r.cfg.Accept(accepted)
	}
	select {
	case conn.inbox <- msg:
	case <-conn.done:
	default:
		// A stalled reader means the session is wedged; close it rather
		// than buffer without bound.
		r.logf("brmcp: inbox overflow on %s; closing session", key)
		conn.Close()
	}
}

// Dial creates the client end of a fresh session to peer. The caller owns
// the returned connection and typically passes conn.AsTransport() to an
// MCP client Connect.
func (r *Router) Dial(peer string) (*Conn, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, fmt.Errorf("brmcp: router closed")
	}
	return r.newConnLocked(peer, wire.NewID()), nil
}

func (r *Router) newConnLocked(peer, sid string) *Conn {
	key := peer + "/" + sid
	conn := &Conn{
		peer:      peer,
		sid:       sid,
		sender:    r.cfg.Sender,
		ttl:       r.cfg.TTL,
		chunkSize: r.cfg.ChunkSize,
		inbox:     make(chan jsonrpc.Message, r.cfg.InboxSize),
		done:      make(chan struct{}),
	}
	conn.onClose = func() {
		r.mu.Lock()
		delete(r.sessions, key)
		r.mu.Unlock()
	}
	r.sessions[key] = conn
	return conn
}

// Close tears down every session.
func (r *Router) Close() {
	r.mu.Lock()
	r.closed = true
	conns := make([]*Conn, 0, len(r.sessions))
	for _, c := range r.sessions {
		conns = append(conns, c)
	}
	r.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
}

// Conn is one MCP session carried over Bison Relay PMs. It implements the
// go-sdk Connection contract.
type Conn struct {
	peer      string
	sid       string
	sender    PMSender
	ttl       time.Duration
	chunkSize int
	inbox     chan jsonrpc.Message
	done      chan struct{}
	closeOnce sync.Once
	onClose   func()
}

// Peer returns the Bison Relay uid this session talks to.
func (c *Conn) Peer() string { return c.peer }

func (c *Conn) SessionID() string { return c.peer + "/" + c.sid }

func (c *Conn) Read(ctx context.Context) (jsonrpc.Message, error) {
	select {
	case msg := <-c.inbox:
		return msg, nil
	case <-c.done:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Conn) Write(ctx context.Context, msg jsonrpc.Message) error {
	select {
	case <-c.done:
		return io.EOF
	default:
	}
	data, err := jsonrpc.EncodeMessage(msg)
	if err != nil {
		return err
	}
	parts, err := wire.Encode(c.sid, data, time.Now().Add(c.ttl), c.chunkSize)
	if err != nil {
		return err
	}
	for _, pm := range parts {
		if err := c.sender.SendPM(ctx, c.peer, pm); err != nil {
			return fmt.Errorf("brmcp: send to %s: %w", c.peer, err)
		}
	}
	return nil
}

func (c *Conn) Close() error {
	c.closeOnce.Do(func() {
		close(c.done)
		if c.onClose != nil {
			c.onClose()
		}
	})
	return nil
}

// AsTransport adapts the connection to the go-sdk Transport contract for
// Server.Connect / Client.Connect, which expect to dial themselves.
func (c *Conn) AsTransport() mcp.Transport { return preconnected{c} }

type preconnected struct{ c *Conn }

func (t preconnected) Connect(context.Context) (mcp.Connection, error) { return t.c, nil }

// IsEnvelope reports whether a PM body is brmcp traffic. Hosts that also
// parse chat commands on the same identity use this to keep MCP parts out
// of their command dispatch.
func IsEnvelope(text string) bool {
	_, ok := wire.Parse(text)
	return ok
}
