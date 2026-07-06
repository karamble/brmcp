// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package directory

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// SnapshotVersion is the current snapshot payload version.
const SnapshotVersion = 1

// Snapshot is the exportable index: every live listing, deterministically
// ordered by uid.
type Snapshot struct {
	Version     int               `json:"v"`
	GeneratedAt int64             `json:"generatedAt"`
	Listings    []SnapshotListing `json:"listings"`
}

// SnapshotListing is one provider in a snapshot.
type SnapshotListing struct {
	UID                   string          `json:"uid"`
	Description           string          `json:"description"`
	Tags                  []string        `json:"tags,omitempty"`
	Catalog               json.RawMessage `json:"catalog"`
	CatalogCheckedAt      int64           `json:"catalogCheckedAt"`
	LastVerifiedExecution int64           `json:"lastVerifiedExecution"`
	ExpiresAt             int64           `json:"expiresAt"`
}

// SignedSnapshot carries the exact signed bytes: Payload is the canon, so
// verifiers need no canonical-JSON algorithm. Sig and Pub are hex ed25519.
type SignedSnapshot struct {
	Payload []byte `json:"payload"`
	Sig     string `json:"sig"`
	Pub     string `json:"pub"`
}

type snapshotSigner struct {
	pub  ed25519.PublicKey
	priv ed25519.PrivateKey
}

type signerFile struct {
	Pub  string `json:"pub"`
	Priv string `json:"priv"`
}

// loadOrCreateSigner opens the directory's snapshot keypair, generating
// and persisting one (0600) on first run.
func loadOrCreateSigner(path string) (*snapshotSigner, error) {
	raw, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		pub, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, err
		}
		out, err := json.MarshalIndent(signerFile{
			Pub:  hex.EncodeToString(pub),
			Priv: hex.EncodeToString(priv),
		}, "", "  ")
		if err != nil {
			return nil, err
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return nil, err
		}
		if err := os.WriteFile(path, out, 0o600); err != nil {
			return nil, err
		}
		return &snapshotSigner{pub: pub, priv: priv}, nil
	case err != nil:
		return nil, err
	}
	var sf signerFile
	if err := json.Unmarshal(raw, &sf); err != nil {
		return nil, fmt.Errorf("signer key %s corrupt: %w", path, err)
	}
	pub, err := hex.DecodeString(sf.Pub)
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("signer key %s: bad public key", path)
	}
	priv, err := hex.DecodeString(sf.Priv)
	if err != nil || len(priv) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("signer key %s: bad private key", path)
	}
	return &snapshotSigner{pub: ed25519.PublicKey(pub), priv: ed25519.PrivateKey(priv)}, nil
}

func (sg *snapshotSigner) sign(payload []byte) SignedSnapshot {
	return SignedSnapshot{
		Payload: payload,
		Sig:     hex.EncodeToString(ed25519.Sign(sg.priv, payload)),
		Pub:     hex.EncodeToString(sg.pub),
	}
}

// VerifySnapshot checks a signed snapshot against the expected signer key
// (the trust root, e.g. from the curated peer record) and decodes it.
func VerifySnapshot(ss SignedSnapshot, wantPubHex string) (*Snapshot, error) {
	if wantPubHex == "" {
		return nil, errors.New("no trusted key for snapshot verification")
	}
	if ss.Pub != wantPubHex {
		return nil, errors.New("snapshot signed by an unexpected key")
	}
	pub, err := hex.DecodeString(ss.Pub)
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return nil, errors.New("snapshot public key malformed")
	}
	sig, err := hex.DecodeString(ss.Sig)
	if err != nil {
		return nil, errors.New("snapshot signature malformed")
	}
	if !ed25519.Verify(ed25519.PublicKey(pub), ss.Payload, sig) {
		return nil, errors.New("snapshot signature invalid")
	}
	var snap Snapshot
	if err := json.Unmarshal(ss.Payload, &snap); err != nil {
		return nil, fmt.Errorf("snapshot payload corrupt: %w", err)
	}
	if snap.Version != SnapshotVersion {
		return nil, fmt.Errorf("snapshot version %d unsupported", snap.Version)
	}
	return &snap, nil
}
