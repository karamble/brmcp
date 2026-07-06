// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package directory

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/karamble/brmcp"
	"github.com/karamble/brmcp/server"
)

const searchHitCap = 50

// catalogTool is the slice of a crawled tool record search needs.
type catalogTool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Meta        map[string]any `json:"_meta"`
}

func (s *Service) registerPublicTools() {
	h := s.harness
	server.AddTool(h, &mcp.Tool{
		Name: "register",
		Description: "Register or renew the calling bot as a listed provider. " +
			"The result is the funding invoice: tip this directory the shortfall " +
			"and verification starts (catalog crawl plus a paid call of the " +
			"nominated test tool, spent from your deposit).",
	}, 0, s.handleRegister)

	server.AddTool(h, &mcp.Tool{
		Name:        "my_status",
		Description: "Report the calling bot's registration or listing state.",
	}, 0, func(_ context.Context, peer string, _ struct{}) (any, error) {
		return s.statusFor(peer), nil
	})

	server.AddTool(h, &mcp.Tool{
		Name: "search",
		Description: "Search listed provider tools by text, tags, and price " +
			"ceiling. Listings are live-verified at the provider's expense; " +
			"prices are advisory, the live call quotes authoritatively.",
	}, s.policy.SearchPriceAtoms, s.handleSearch)

	server.AddTool(h, &mcp.Tool{
		Name:        "get_provider",
		Description: "Fetch one provider's full listing including the crawled tool catalog.",
	}, 0, func(_ context.Context, _ string, in ProviderIn) (any, error) {
		e, ok := s.index.get(in.UID)
		if !ok || e.Live == nil {
			return nil, errors.New("no listing for that uid")
		}
		return providerOut(in.UID, e.Live), nil
	})

	server.AddTool(h, &mcp.Tool{
		Name:        "list_categories",
		Description: "Histogram listed providers by tag.",
	}, 0, func(context.Context, string, struct{}) (any, error) {
		out := CategoriesOut{Tags: make(map[string]int)}
		for _, e := range s.index.all() {
			if e.Live == nil {
				continue
			}
			for _, tag := range e.Live.Tags {
				out.Tags[tag]++
			}
		}
		return out, nil
	})

	server.AddTool(h, &mcp.Tool{
		Name:        "policy",
		Description: "This directory's terms: fees, budgets, expiry, and the snapshot signing key.",
	}, 0, func(context.Context, string, struct{}) (any, error) {
		return PolicyOut{
			Name:               s.policy.Name,
			ListingFeeAtoms:    s.policy.ListingFeeAtoms,
			SnapshotPriceAtoms: s.policy.SnapshotPriceAtoms,
			SearchPriceAtoms:   s.policy.SearchPriceAtoms,
			TestBudgetMaxAtoms: s.policy.TestBudgetMaxAtoms,
			ExpiryDays:         s.policy.ExpiryDays,
			PubKey:             s.PublicKey(),
		}, nil
	})

	server.AddTool(h, &mcp.Tool{
		Name: "get_snapshot",
		Description: "Export the full signed index. Anyone may buy, verify, " +
			"and redistribute it; peers use it to seed verify-don't-trust leads.",
	}, s.policy.SnapshotPriceAtoms, func(context.Context, string, struct{}) (any, error) {
		return s.buildSnapshot()
	})
}

func providerOut(uid string, l *Listing) ProviderOut {
	return ProviderOut{
		UID:                   uid,
		Description:           l.Description,
		Tags:                  l.Tags,
		Catalog:               l.Catalog,
		CatalogCheckedAt:      l.CatalogCheckedAt,
		LastVerifiedExecution: l.LastVerifiedExecution,
		TestLatencyMs:         l.TestLatencyMs,
		ApprovedAt:            l.ApprovedAt,
		RenewedAt:             l.RenewedAt,
		ExpiresAt:             l.ExpiresAt,
	}
}

func (s *Service) handleSearch(_ context.Context, _ string, in SearchIn) (any, error) {
	query := strings.ToLower(strings.TrimSpace(in.Query))
	wantTags := normalizeTags(in.Tags)
	out := SearchOut{Hits: []SearchHit{}}

	uids := make([]string, 0)
	entries := s.index.all()
	for uid, e := range entries {
		if e.Live != nil {
			uids = append(uids, uid)
		}
	}
	sort.Strings(uids)

	for _, uid := range uids {
		l := entries[uid].Live
		if !hasAllTags(l.Tags, wantTags) {
			continue
		}
		var tools []catalogTool
		if err := json.Unmarshal(l.Catalog, &tools); err != nil {
			continue
		}
		for _, tool := range tools {
			if query != "" &&
				!strings.Contains(strings.ToLower(tool.Name), query) &&
				!strings.Contains(strings.ToLower(tool.Description), query) {
				continue
			}
			price, dynamic := toolPrice(tool.Meta)
			if in.MaxPriceAtoms > 0 && (dynamic || price > in.MaxPriceAtoms) {
				continue
			}
			out.Total++
			if len(out.Hits) >= searchHitCap {
				continue
			}
			hit := SearchHit{
				ProviderUID:           uid,
				Tool:                  tool.Name,
				Description:           tool.Description,
				PriceAtoms:            price,
				Tags:                  l.Tags,
				CatalogCheckedAt:      l.CatalogCheckedAt,
				LastVerifiedExecution: l.LastVerifiedExecution,
			}
			if dynamic {
				hit.Pricing = "dynamic"
			}
			out.Hits = append(out.Hits, hit)
		}
	}
	return out, nil
}

func hasAllTags(have, want []string) bool {
	if len(want) == 0 {
		return true
	}
	set := make(map[string]bool, len(have))
	for _, t := range have {
		set[t] = true
	}
	for _, t := range want {
		if !set[t] {
			return false
		}
	}
	return true
}

// toolPrice reads the advertised price metadata from a crawled tool. A
// dynamic marker without a fixed price reports (0, true).
func toolPrice(meta map[string]any) (atoms int64, dynamic bool) {
	if meta == nil {
		return 0, false
	}
	switch v := meta[brmcp.PriceMetaKey].(type) {
	case float64:
		return int64(v), false
	case int64:
		return v, false
	case json.Number:
		if n, err := v.Int64(); err == nil {
			return n, false
		}
	}
	if p, ok := meta[brmcp.PricingMetaKey].(string); ok && p == "dynamic" {
		return 0, true
	}
	return 0, false
}

// buildSnapshot exports every live listing, deterministically ordered and
// signed with the directory key.
func (s *Service) buildSnapshot() (SignedSnapshot, error) {
	snap := Snapshot{
		Version:     SnapshotVersion,
		GeneratedAt: s.clk.Now().Unix(),
	}
	entries := s.index.all()
	uids := make([]string, 0, len(entries))
	for uid, e := range entries {
		if e.Live != nil {
			uids = append(uids, uid)
		}
	}
	sort.Strings(uids)
	for _, uid := range uids {
		l := entries[uid].Live
		snap.Listings = append(snap.Listings, SnapshotListing{
			UID:                   uid,
			Description:           l.Description,
			Tags:                  l.Tags,
			Catalog:               l.Catalog,
			CatalogCheckedAt:      l.CatalogCheckedAt,
			LastVerifiedExecution: l.LastVerifiedExecution,
			ExpiresAt:             l.ExpiresAt,
		})
	}
	payload, err := json.Marshal(snap)
	if err != nil {
		return SignedSnapshot{}, err
	}
	return s.signer.sign(payload), nil
}
