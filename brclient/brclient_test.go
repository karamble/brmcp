// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package brclient

import (
	"context"
	"testing"

	"github.com/companyzero/bisonrelay/client/clientintf"
)

const testUID = "0707070707070707070707070707070707070707070707070707070707070707"

type fakeClient struct {
	uid      clientintf.UserID
	msg      string
	dcr      float64
	attempts int32
}

func (f *fakeClient) PM(uid clientintf.UserID, msg string) error {
	f.uid, f.msg = uid, msg
	return nil
}

func (f *fakeClient) TipUser(uid clientintf.UserID, dcr float64, attempts int32) error {
	f.uid, f.dcr, f.attempts = uid, dcr, attempts
	return nil
}

func TestSender(t *testing.T) {
	f := &fakeClient{}
	if err := (Sender{c: f}).SendPM(context.Background(), testUID, "part"); err != nil {
		t.Fatal(err)
	}
	if f.uid.String() != testUID || f.msg != "part" {
		t.Fatalf("sent %q to %s", f.msg, f.uid)
	}
	if err := (Sender{c: f}).SendPM(context.Background(), "alice", "part"); err == nil {
		t.Fatal("a nick was accepted as a peer")
	}
}

func TestTipFunc(t *testing.T) {
	f := &fakeClient{}
	if err := tipFunc(f)(context.Background(), testUID, 12_345_678); err != nil {
		t.Fatal(err)
	}
	if f.uid.String() != testUID || f.dcr != 0.12345678 || f.attempts != 1 {
		t.Fatalf("tipped %v DCR to %s in %d attempts", f.dcr, f.uid, f.attempts)
	}
	if err := tipFunc(f)(context.Background(), "alice", 1); err == nil {
		t.Fatal("a nick was accepted as a payee")
	}
}
