// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package wire

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func mustParse(t *testing.T, text string) *Part {
	t.Helper()
	p, ok := Parse(text)
	if !ok {
		t.Fatalf("Parse rejected %q", text)
	}
	return p
}

func TestSinglePartRoundTrip(t *testing.T) {
	payload := []byte(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
	pms, err := Encode("a1b2c3d4e5f60718", payload, time.Time{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(pms) != 1 {
		t.Fatalf("want 1 part, got %d", len(pms))
	}
	p := mustParse(t, pms[0])
	if p.SID != "a1b2c3d4e5f60718" || p.Seq != 1 || p.Total != 1 || p.Exp != 0 {
		t.Fatalf("unexpected part: %+v", p)
	}
	a := NewAssembler(AssemblerConfig{})
	got, err := a.Add("peer1", p, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: %q", got)
	}
}

func TestChunkedRoundTripOutOfOrder(t *testing.T) {
	payload := bytes.Repeat([]byte("0123456789abcdef"), 1024) // 16 KiB
	pms, err := Encode("00ff", payload, time.Time{}, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if len(pms) != 4 {
		t.Fatalf("want 4 parts, got %d", len(pms))
	}
	a := NewAssembler(AssemblerConfig{})
	now := time.Now()
	// Deliver out of order; only the last delivery completes.
	order := []int{2, 0, 3, 1}
	var got []byte
	for i, idx := range order {
		res, err := a.Add("peer1", mustParse(t, pms[idx]), now)
		if err != nil {
			t.Fatalf("part %d: %v", idx, err)
		}
		if i < len(order)-1 && res != nil {
			t.Fatalf("completed early at delivery %d", i)
		}
		got = res
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("reassembled payload mismatch")
	}
}

func TestParseRejectsNonEnvelope(t *testing.T) {
	for _, text := range []string{
		"hello there",
		"> **nick:** quoted chat",
		"--embed[type=image/png,data=AAAA]--",
		"--mcp[v=1,sid=zz,mid=aa,seq=1/1,exp=0]--QUJD",            // bad sid
		"--mcp[v=2,sid=aa,mid=bb,seq=1/1,exp=0]--QUJD",            // future version
		"--mcp[v=1,sid=aa,mid=bb,seq=2/1,exp=0]--QUJD",            // seq > total
		"--mcp[v=1,sid=aa,mid=bb,seq=1/999,exp=0]--QUJD",          // total > MaxParts
		"--mcp[v=1,sid=aa,mid=bb,seq=1/1,exp=0]--not!base64",      // bad payload
		"--mcp[v=1,sid=aa,mid=bb,seq=1/1,exp=0]--QUJD--mcp[]--QQ", // trailing junk
		"--mcp[v=1,sid=aa,seq=1/1,exp=0]--QUJD",                   // missing mid
	} {
		if _, ok := Parse(text); ok {
			t.Errorf("Parse accepted %q", text)
		}
	}
}

func TestExpiredPartDropped(t *testing.T) {
	pms, err := Encode("aa", []byte("payload"), time.Now().Add(-time.Minute), 0)
	if err != nil {
		t.Fatal(err)
	}
	a := NewAssembler(AssemblerConfig{})
	if _, err := a.Add("peer1", mustParse(t, pms[0]), time.Now()); !errors.Is(err, ErrExpired) {
		t.Fatalf("want ErrExpired, got %v", err)
	}
}

func TestPendingPerPeerBound(t *testing.T) {
	a := NewAssembler(AssemblerConfig{MaxPendingPerPeer: 2})
	now := time.Now()
	for i := range 2 {
		pms, err := Encode(fmt.Sprintf("a%d", i), bytes.Repeat([]byte("x"), 8), time.Time{}, 4)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := a.Add("peer1", mustParse(t, pms[0]), now); err != nil {
			t.Fatal(err)
		}
	}
	pms, err := Encode("a3", bytes.Repeat([]byte("x"), 8), time.Time{}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Add("peer1", mustParse(t, pms[0]), now); !errors.Is(err, ErrOverflow) {
		t.Fatalf("want ErrOverflow, got %v", err)
	}
	// A different peer is unaffected by peer1's pending load.
	if _, err := a.Add("peer2", mustParse(t, pms[0]), now); err != nil {
		t.Fatalf("peer2 blocked by peer1 bound: %v", err)
	}
}

func TestMessageBytesBound(t *testing.T) {
	a := NewAssembler(AssemblerConfig{MaxMessageBytes: 6})
	pms, err := Encode("aa", []byte("12345678"), time.Time{}, 4)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if _, err := a.Add("peer1", mustParse(t, pms[0]), now); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Add("peer1", mustParse(t, pms[1]), now); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("want ErrTooLarge, got %v", err)
	}
	// Oversized single-part messages are refused outright.
	one, err := Encode("bb", []byte("12345678"), time.Time{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Add("peer1", mustParse(t, one[0]), now); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("want ErrTooLarge for single part, got %v", err)
	}
}

func TestTimeoutGC(t *testing.T) {
	a := NewAssembler(AssemblerConfig{Timeout: time.Second})
	pms, err := Encode("aa", []byte("12345678"), time.Time{}, 4)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := a.Add("peer1", mustParse(t, pms[0]), start); err != nil {
		t.Fatal(err)
	}
	// After the timeout the partial message is evicted, freeing the slot,
	// and a late remaining chunk starts a fresh (incomplete) message.
	late := start.Add(2 * time.Second)
	res, err := a.Add("peer1", mustParse(t, pms[1]), late)
	if err != nil || res != nil {
		t.Fatalf("late chunk after GC: res=%v err=%v", res, err)
	}
	if len(a.pending) != 1 {
		t.Fatalf("want exactly the fresh pending message, got %d", len(a.pending))
	}
}

func TestDuplicatePartIgnored(t *testing.T) {
	pms, err := Encode("aa", []byte("12345678"), time.Time{}, 4)
	if err != nil {
		t.Fatal(err)
	}
	a := NewAssembler(AssemblerConfig{})
	now := time.Now()
	if _, err := a.Add("peer1", mustParse(t, pms[0]), now); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Add("peer1", mustParse(t, pms[0]), now); err != nil {
		t.Fatalf("duplicate part errored: %v", err)
	}
	res, err := a.Add("peer1", mustParse(t, pms[1]), now)
	if err != nil || res == nil {
		t.Fatalf("completion after duplicate: res=%v err=%v", res, err)
	}
}

func TestEncodeBounds(t *testing.T) {
	if _, err := Encode("not hex!", []byte("x"), time.Time{}, 0); err == nil {
		t.Fatal("invalid sid accepted")
	}
	if _, err := Encode("aa", nil, time.Time{}, 0); err == nil {
		t.Fatal("empty payload accepted")
	}
	huge := bytes.Repeat([]byte("x"), (MaxParts+1)*16)
	if _, err := Encode("aa", huge, time.Time{}, 16); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("want ErrTooLarge, got %v", err)
	}
}

func TestEncodeSetsDeadline(t *testing.T) {
	exp := time.Now().Add(time.Hour)
	pms, err := Encode("aa", []byte("x"), exp, 0)
	if err != nil {
		t.Fatal(err)
	}
	p := mustParse(t, pms[0])
	if p.Exp != exp.Unix() {
		t.Fatalf("exp mismatch: %d != %d", p.Exp, exp.Unix())
	}
	if p.Expired(exp.Add(time.Second)) != true || p.Expired(exp.Add(-time.Second)) {
		t.Fatal("Expired misjudges the deadline")
	}
	if !strings.Contains(pms[0], fmt.Sprintf("exp=%d", exp.Unix())) {
		t.Fatalf("encoded text lacks deadline: %s", pms[0])
	}
}
