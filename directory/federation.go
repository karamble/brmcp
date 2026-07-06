// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package directory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// peerSnapshotBudgetFactor bounds what the directory pays a peer for one
// snapshot, as a multiple of its own snapshot price.
const peerSnapshotBudgetFactor = 10

// runPeerVerification buys a curated peer's signed snapshot, verifies it
// against the trusted key from add_peer, and turns unknown providers into
// leads. Nothing from the snapshot is ever listed directly.
func (s *Service) runPeerVerification(ctx context.Context, peerUID string) (int, error) {
	peer, ok := s.peers.get(peerUID)
	if !ok {
		return 0, errors.New("not a curated peer; add_peer first")
	}
	budget := s.policy.SnapshotPriceAtoms * peerSnapshotBudgetFactor
	cr, err := s.paidCall(ctx, peerUID, "get_snapshot", nil, newCallKey(), budget, nil)
	if err != nil {
		return 0, fmt.Errorf("snapshot purchase: %w", err)
	}
	if cr.res == nil || cr.res.IsError {
		return 0, fmt.Errorf("snapshot refused: %s", resultText(cr.res))
	}
	var ss SignedSnapshot
	if err := json.Unmarshal(resultJSON(cr.res), &ss); err != nil {
		return 0, fmt.Errorf("snapshot malformed: %w", err)
	}
	snap, err := VerifySnapshot(ss, peer.PubKey)
	if err != nil {
		return 0, err
	}

	now := s.clk.Now().Unix()
	selfUID := s.getSelfUID()
	newLeads := 0
	for _, l := range snap.Listings {
		uid := l.UID
		if len(uid) != 64 || uid == selfUID || uid == peerUID {
			continue
		}
		if _, known := s.index.get(uid); known {
			continue
		}
		if _, known := s.leads.get(uid); known {
			continue
		}
		err := s.leads.put(uid, Lead{
			ProviderUID: uid,
			PeerUID:     peerUID,
			Description: l.Description,
			Tags:        l.Tags,
			State:       LeadNew,
			CreatedAt:   now,
			UpdatedAt:   now,
		})
		if err != nil {
			return newLeads, err
		}
		newLeads++
	}
	err = s.peers.mutate(peerUID, func(p *Peer) error {
		p.LastVerifiedAt = now
		return nil
	})
	if err != nil {
		s.logf("brmcpdir: stamp peer %s: %v", peerUID[:8], err)
	}
	return newLeads, nil
}

// pursueLead invites a lead to register: first a direct listing_invite
// call, and when the provider is not reachable yet, a transitive KX
// through the peer that supplied the lead. NotifyKX completes the pursuit.
func (s *Service) pursueLead(ctx context.Context, uid string) (*Lead, error) {
	lead, ok := s.leads.get(uid)
	if !ok {
		return nil, errors.New("no lead for that uid")
	}
	if lead.State == LeadConverted {
		return &lead, nil
	}
	inviteErr := s.sendListingInvite(ctx, uid)
	if inviteErr == nil {
		lead = s.setLeadState(uid, LeadInvited, "")
		return &lead, nil
	}
	if s.cfg.Introducer == nil {
		return nil, fmt.Errorf("invite failed and no introducer is configured: %w", inviteErr)
	}
	if err := s.cfg.Introducer.Introduce(ctx, lead.PeerUID, uid); err != nil {
		return nil, fmt.Errorf("invite failed (%v); introduction failed: %w", inviteErr, err)
	}
	lead = s.setLeadState(uid, LeadPursuing, "awaiting key exchange via peer")
	return &lead, nil
}

// sendListingInvite calls the provider's Registrant-served listing_invite
// tool. The tool must be free; a priced refusal fails the invite.
func (s *Service) sendListingInvite(ctx context.Context, uid string) error {
	args, err := json.Marshal(InviteIn{
		Directory:       s.policy.Name,
		ListingFeeAtoms: s.policy.ListingFeeAtoms,
	})
	if err != nil {
		return err
	}
	cr, err := s.paidCall(ctx, uid, "listing_invite", args, newCallKey(), 0, nil)
	if err != nil {
		return err
	}
	if cr.res == nil || cr.res.IsError {
		return fmt.Errorf("invite refused: %s", resultText(cr.res))
	}
	var out InviteOut
	if err := json.Unmarshal(resultJSON(cr.res), &out); err != nil {
		return fmt.Errorf("invite reply malformed: %w", err)
	}
	if !out.Accepted {
		s.setLeadState(uid, LeadInvited, "declined: "+out.Note)
		return nil
	}
	return nil
}

// resumeLeadAfterKX continues a pursuit parked on an introduction.
func (s *Service) resumeLeadAfterKX(uid string) {
	lead, ok := s.leads.get(uid)
	if !ok || lead.State != LeadPursuing {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.wg.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.wg.Done()
		ctx := s.baseCtx()
		if err := s.sendListingInvite(ctx, uid); err != nil {
			s.setLeadState(uid, LeadPursuing, "invite after KX failed: "+err.Error())
			s.logf("brmcpdir: lead %s invite after KX: %v", uid[:8], err)
			return
		}
		s.setLeadState(uid, LeadInvited, "")
	}()
}

func (s *Service) setLeadState(uid, state, note string) Lead {
	var out Lead
	err := s.leads.mutate(uid, func(l *Lead) error {
		if l.ProviderUID == "" {
			return errNoChange
		}
		if l.State != LeadConverted {
			l.State = state
		}
		if note != "" {
			l.Note = note
		}
		l.UpdatedAt = s.clk.Now().Unix()
		out = *l
		return nil
	})
	if err != nil && !errors.Is(err, errNoChange) {
		s.logf("brmcpdir: lead %s state: %v", uid[:8], err)
	}
	return out
}
