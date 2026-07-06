// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package directory

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestSnapshotSignVerify(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "snapshotkey.json")
	sg, err := loadOrCreateSigner(keyPath)
	if err != nil {
		t.Fatal(err)
	}

	// The key must persist across reopen.
	sg2, err := loadOrCreateSigner(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(sg.pub) != string(sg2.pub) {
		t.Fatal("signer key did not persist")
	}

	payload, err := json.Marshal(Snapshot{
		Version:     SnapshotVersion,
		GeneratedAt: 1000,
		Listings: []SnapshotListing{{
			UID:         "ab",
			Description: "test provider",
			Catalog:     json.RawMessage(`[{"name":"echo"}]`),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ss := sg.sign(payload)

	snap, err := VerifySnapshot(ss, ss.Pub)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Listings) != 1 || snap.Listings[0].UID != "ab" {
		t.Fatalf("decoded snapshot wrong: %+v", snap)
	}

	// Tampered payload.
	bad := ss
	bad.Payload = append([]byte(nil), ss.Payload...)
	bad.Payload[0] ^= 1
	if _, err := VerifySnapshot(bad, ss.Pub); err == nil {
		t.Fatal("tampered payload verified")
	}

	// Tampered signature.
	bad = ss
	bad.Sig = ss.Sig[:len(ss.Sig)-2] + "00"
	if _, err := VerifySnapshot(bad, ss.Pub); err == nil {
		t.Fatal("tampered signature verified")
	}

	// Wrong trust root.
	other, err := loadOrCreateSigner(filepath.Join(dir, "other.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifySnapshot(ss, other.sign(nil).Pub); err == nil {
		t.Fatal("unexpected signer accepted")
	}
	if _, err := VerifySnapshot(ss, ""); err == nil {
		t.Fatal("empty trust root accepted")
	}

	// Unsupported version.
	v2, _ := json.Marshal(Snapshot{Version: 2})
	if _, err := VerifySnapshot(sg.sign(v2), ss.Pub); err == nil {
		t.Fatal("future version accepted")
	}
}
