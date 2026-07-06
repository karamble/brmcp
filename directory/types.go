// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package directory

import "encoding/json"

// Input limits enforced by the register tool.
const (
	MaxDescriptionLen = 1024
	MaxTags           = 16
	MaxTagLen         = 32
)

// TestSpec nominates the paid call a directory executes to verify a
// provider. The provider picks a tool whose side effects it accepts and
// caps what the directory may spend on it. Args is a JSON object because
// tool arguments always are.
type TestSpec struct {
	Tool     string         `json:"tool"`
	Args     map[string]any `json:"args,omitempty"`
	MaxAtoms int64          `json:"maxAtoms"`
}

// RegisterIn is the input of the register tool. The session peer uid is
// the registered identity; nothing in the payload names it.
type RegisterIn struct {
	Description string   `json:"description"`
	Tags        []string `json:"tags,omitempty"`
	Test        TestSpec `json:"test"`
}

// StatusOut reports a caller's pipeline state. The response to register is
// the funding invoice: RequiredAtoms is what the caller's balance must
// reach before verification starts.
type StatusOut struct {
	State           string `json:"state"`
	RequiredAtoms   int64  `json:"requiredAtoms,omitempty"`
	BalanceAtoms    int64  `json:"balanceAtoms,omitempty"`
	ShortfallAtoms  int64  `json:"shortfallAtoms,omitempty"`
	FeeAtoms        int64  `json:"feeAtoms,omitempty"`
	TestBudgetAtoms int64  `json:"testBudgetAtoms,omitempty"`
	ExpiresAt       int64  `json:"expiresAt,omitempty"`
	Note            string `json:"note,omitempty"`
}

// States surfaced by StatusOut. The first four mirror the registration
// pipeline; listed and rejected are terminal, none means the caller has
// no registration and no listing.
const (
	StateNone            = "none"
	StateAwaitingFunding = "awaiting_funding"
	StateCrawling        = "crawling"
	StateTesting         = "testing"
	StatePendingReview   = "pending_review"
	StateListed          = "listed"
	StateRejected        = "rejected"
)

// InviteIn is the input of the listing_invite tool a Registrant serves on
// a provider's harness. The caller uid identifies the inviting directory.
type InviteIn struct {
	Directory       string `json:"directory"`
	ListingFeeAtoms int64  `json:"listingFeeAtoms"`
	Note            string `json:"note,omitempty"`
}

// InviteOut acknowledges an invite. Accepted means the provider will
// register back at the calling directory; the registration itself happens
// asynchronously.
type InviteOut struct {
	Accepted bool   `json:"accepted"`
	Note     string `json:"note,omitempty"`
}

// IntroduceIn asks the directory to introduce the caller to a listed
// provider.
type IntroduceIn struct {
	UID string `json:"uid"`
}

// IntroduceOut acknowledges that the KX suggestion was sent; the caller's
// client still decides whether to accept it.
type IntroduceOut struct {
	Suggested bool   `json:"suggested"`
	Note      string `json:"note,omitempty"`
}

// SearchIn queries the tool-level index. Query matches tool names and
// descriptions case-insensitively; Tags must all be present on the
// listing; MaxPriceAtoms filters out tools with a higher advertised fixed
// price (dynamically priced tools pass only when no ceiling is given).
type SearchIn struct {
	Query         string   `json:"query,omitempty"`
	Tags          []string `json:"tags,omitempty"`
	MaxPriceAtoms int64    `json:"maxPriceAtoms,omitempty"`
}

// SearchHit is one matching tool on one provider.
type SearchHit struct {
	ProviderUID           string   `json:"providerUid"`
	Tool                  string   `json:"tool"`
	Description           string   `json:"description,omitempty"`
	PriceAtoms            int64    `json:"priceAtoms,omitempty"`
	Pricing               string   `json:"pricing,omitempty"`
	Tags                  []string `json:"tags,omitempty"`
	CatalogCheckedAt      int64    `json:"catalogCheckedAt"`
	LastVerifiedExecution int64    `json:"lastVerifiedExecution"`
}

// SearchOut caps hits at 50; Total reports the uncapped match count.
type SearchOut struct {
	Hits  []SearchHit `json:"hits"`
	Total int         `json:"total"`
}

// ProviderIn selects a provider by uid.
type ProviderIn struct {
	UID string `json:"uid"`
}

// ProviderOut is a full listing: the crawled catalog verbatim plus the
// freshness stamps.
type ProviderOut struct {
	UID                   string          `json:"uid"`
	Description           string          `json:"description"`
	Tags                  []string        `json:"tags,omitempty"`
	Catalog               json.RawMessage `json:"catalog"`
	CatalogCheckedAt      int64           `json:"catalogCheckedAt"`
	LastVerifiedExecution int64           `json:"lastVerifiedExecution"`
	TestLatencyMs         int64           `json:"testLatencyMs"`
	ApprovedAt            int64           `json:"approvedAt"`
	RenewedAt             int64           `json:"renewedAt,omitempty"`
	ExpiresAt             int64           `json:"expiresAt"`
}

// CategoriesOut histograms listed providers by tag.
type CategoriesOut struct {
	Tags map[string]int `json:"tags"`
}

// PolicyOut advertises the directory's terms and its snapshot signing key.
type PolicyOut struct {
	Name               string `json:"name"`
	ListingFeeAtoms    int64  `json:"listingFeeAtoms"`
	SnapshotPriceAtoms int64  `json:"snapshotPriceAtoms"`
	SearchPriceAtoms   int64  `json:"searchPriceAtoms"`
	TestBudgetMaxAtoms int64  `json:"testBudgetMaxAtoms"`
	ExpiryDays         int    `json:"expiryDays"`
	PubKey             string `json:"pubKey"`
}
