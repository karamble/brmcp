// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package wire frames opaque payloads (JSON-RPC messages in practice) into
// Bison Relay private messages. Each PM carries exactly one part:
//
//	--mcp[v=1,sid=<hex>,mid=<hex>,seq=<n>/<total>,exp=<unix>]--<base64 chunk>
//
// A part with seq=1/1 is a complete message. Bison Relay delivers PMs as
// discrete, ordered, acknowledged units, so the envelope only has to
// distinguish MCP traffic from human chat on the same DM thread, group the
// chunks of oversized payloads, and carry a deadline: relay delivery is
// store-and-forward, so a request can arrive long after the caller stopped
// waiting and must be droppable.
package wire

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Version is the envelope version emitted by Encode and accepted by Parse.
const Version = 1

// DefaultChunkSize is the raw payload bytes per part before base64. Base64
// expands 4/3, so parts stay far below Bison Relay's 1 MiB routed-message
// floor with room for the tag and JSON framing.
const DefaultChunkSize = 200 * 1024

// MaxParts bounds a single message's chunk count. 64 parts at the default
// chunk size is ~12.8 MiB raw, beyond any payload the transport should carry.
const MaxParts = 64

var (
	ErrExpired  = errors.New("message deadline passed")
	ErrTooLarge = errors.New("message exceeds size bounds")
	ErrOverflow = errors.New("reassembly buffer full")
)

// Part is one parsed envelope.
type Part struct {
	SID   string
	MID   string
	Seq   int
	Total int
	// Exp is the sender-set deadline in unix seconds; 0 means none.
	Exp   int64
	Chunk []byte
}

// Expired reports whether the part's deadline has passed at now.
func (p *Part) Expired(now time.Time) bool {
	return p.Exp > 0 && now.Unix() > p.Exp
}

var idRE = regexp.MustCompile(`^[0-9a-f]{1,32}$`)

// partRE matches a whole PM body that is one envelope part. The payload
// alphabet is restricted to base64 so a crafted body cannot smuggle a second
// tag or trailing content.
var partRE = regexp.MustCompile(`^--mcp\[([^\]]*)\]--([A-Za-z0-9+/=\s]*)$`)

// NewID returns a fresh random 16-hex identifier for sessions and messages.
func NewID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("wire: rand failed: %v", err))
	}
	return hex.EncodeToString(b[:])
}

// Encode frames payload into one PM body per part under sid. A zero exp
// emits no deadline. chunkSize <= 0 selects DefaultChunkSize.
func Encode(sid string, payload []byte, exp time.Time, chunkSize int) ([]string, error) {
	if !idRE.MatchString(sid) {
		return nil, fmt.Errorf("wire: invalid sid %q", sid)
	}
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	if len(payload) == 0 {
		return nil, errors.New("wire: empty payload")
	}
	total := (len(payload) + chunkSize - 1) / chunkSize
	if total > MaxParts {
		return nil, ErrTooLarge
	}
	var expUnix int64
	if !exp.IsZero() {
		expUnix = exp.Unix()
	}
	mid := NewID()
	out := make([]string, 0, total)
	for i := 0; i < total; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > len(payload) {
			end = len(payload)
		}
		b64 := base64.StdEncoding.EncodeToString(payload[start:end])
		out = append(out, fmt.Sprintf("--mcp[v=%d,sid=%s,mid=%s,seq=%d/%d,exp=%d]--%s",
			Version, sid, mid, i+1, total, expUnix, b64))
	}
	return out, nil
}

// Parse decodes one PM body. ok is false for anything that is not a
// well-formed part of a supported version, so plain chat text on the same
// thread is silently ignored by callers.
func Parse(text string) (p *Part, ok bool) {
	m := partRE.FindStringSubmatch(strings.TrimSpace(text))
	if m == nil {
		return nil, false
	}
	p = &Part{}
	var version int
	for _, kv := range strings.Split(m[1], ",") {
		k, v, found := strings.Cut(kv, "=")
		if !found {
			return nil, false
		}
		switch k {
		case "v":
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, false
			}
			version = n
		case "sid":
			if !idRE.MatchString(v) {
				return nil, false
			}
			p.SID = v
		case "mid":
			if !idRE.MatchString(v) {
				return nil, false
			}
			p.MID = v
		case "seq":
			seq, tot, found := strings.Cut(v, "/")
			if !found {
				return nil, false
			}
			var err error
			if p.Seq, err = strconv.Atoi(seq); err != nil {
				return nil, false
			}
			if p.Total, err = strconv.Atoi(tot); err != nil {
				return nil, false
			}
		case "exp":
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, false
			}
			p.Exp = n
		default:
			// Unknown keys are tolerated for forward compatibility.
		}
	}
	if version != Version || p.SID == "" || p.MID == "" {
		return nil, false
	}
	if p.Seq < 1 || p.Total < 1 || p.Seq > p.Total || p.Total > MaxParts {
		return nil, false
	}
	chunk, err := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(m[2]), ""))
	if err != nil || len(chunk) == 0 {
		return nil, false
	}
	p.Chunk = chunk
	return p, true
}

// AssemblerConfig bounds a peer's in-flight reassembly state. The zero value
// selects the defaults.
type AssemblerConfig struct {
	// MaxPendingPerPeer caps concurrently reassembling messages per peer.
	MaxPendingPerPeer int
	// MaxMessageBytes caps one reassembled message.
	MaxMessageBytes int64
	// MaxTotalBytes caps all buffered chunks across peers.
	MaxTotalBytes int64
	// Timeout evicts partial messages that stopped arriving.
	Timeout time.Duration
}

func (c AssemblerConfig) withDefaults() AssemblerConfig {
	if c.MaxPendingPerPeer <= 0 {
		c.MaxPendingPerPeer = 8
	}
	if c.MaxMessageBytes <= 0 {
		c.MaxMessageBytes = 16 << 20
	}
	if c.MaxTotalBytes <= 0 {
		c.MaxTotalBytes = 64 << 20
	}
	if c.Timeout <= 0 {
		c.Timeout = 10 * time.Minute
	}
	return c
}

type pendingMsg struct {
	parts     map[int][]byte
	total     int
	bytes     int64
	exp       int64
	firstSeen time.Time
	peer      string
}

// Assembler reassembles chunked messages with hard bounds so a hostile peer
// cannot grow the buffers without limit.
type Assembler struct {
	mu      sync.Mutex
	cfg     AssemblerConfig
	pending map[string]*pendingMsg // key: peer/sid/mid
	perPeer map[string]int
	bytes   int64
}

func NewAssembler(cfg AssemblerConfig) *Assembler {
	return &Assembler{
		cfg:     cfg.withDefaults(),
		pending: make(map[string]*pendingMsg),
		perPeer: make(map[string]int),
	}
}

// Add ingests one part from peer. It returns the full payload once every
// chunk arrived, nil while the message is still partial, and an error when
// the part is expired or a bound would be exceeded (the message's state is
// dropped on error).
func (a *Assembler) Add(peer string, p *Part, now time.Time) ([]byte, error) {
	if p.Expired(now) {
		return nil, ErrExpired
	}
	if p.Total == 1 {
		if int64(len(p.Chunk)) > a.cfg.MaxMessageBytes {
			return nil, ErrTooLarge
		}
		return p.Chunk, nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.gcLocked(now)

	key := peer + "/" + p.SID + "/" + p.MID
	msg := a.pending[key]
	if msg == nil {
		if a.perPeer[peer] >= a.cfg.MaxPendingPerPeer {
			return nil, ErrOverflow
		}
		msg = &pendingMsg{
			parts:     make(map[int][]byte),
			total:     p.Total,
			exp:       p.Exp,
			firstSeen: now,
			peer:      peer,
		}
		a.pending[key] = msg
		a.perPeer[peer]++
	}
	if p.Total != msg.total {
		a.dropLocked(key, msg)
		return nil, fmt.Errorf("wire: inconsistent part count for %s", key)
	}
	if _, dup := msg.parts[p.Seq]; dup {
		return nil, nil
	}
	newBytes := int64(len(p.Chunk))
	if msg.bytes+newBytes > a.cfg.MaxMessageBytes || a.bytes+newBytes > a.cfg.MaxTotalBytes {
		a.dropLocked(key, msg)
		return nil, ErrTooLarge
	}
	msg.parts[p.Seq] = p.Chunk
	msg.bytes += newBytes
	a.bytes += newBytes

	if len(msg.parts) < msg.total {
		return nil, nil
	}
	full := make([]byte, 0, msg.bytes)
	for i := 1; i <= msg.total; i++ {
		full = append(full, msg.parts[i]...)
	}
	a.dropLocked(key, msg)
	return full, nil
}

func (a *Assembler) dropLocked(key string, msg *pendingMsg) {
	a.bytes -= msg.bytes
	if a.perPeer[msg.peer] <= 1 {
		delete(a.perPeer, msg.peer)
	} else {
		a.perPeer[msg.peer]--
	}
	delete(a.pending, key)
}

func (a *Assembler) gcLocked(now time.Time) {
	for key, msg := range a.pending {
		expired := msg.exp > 0 && now.Unix() > msg.exp
		if expired || now.Sub(msg.firstSeen) > a.cfg.Timeout {
			a.dropLocked(key, msg)
		}
	}
}
