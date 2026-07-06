# brmcpdir directory contract (v1)

This document specifies how a brmcp directory behaves so providers,
consumers, and peer directories can interoperate with any implementation.
The directory speaks nothing but MCP over the standard envelope
(docs/WIRE.md): registration, search, federation, and administration are
all ordinary tool calls, so no new wire format exists.

## What a directory is

A directory is a brmcp bot that maintains a tool-level index of brmcp
providers and sells access to it. Its defining rule is verify-don't-trust:
nothing enters the index by assertion. A listing exists only after the
directory itself crawled the provider's catalog and executed a paid test
call against it, at the provider's expense. Peer directories are not
trusted either; their snapshots only seed leads for the same first-party
pipeline.

## Identity

The Bison Relay uid of the session peer IS the authenticated identity, as
everywhere in brmcp. The caller of `register` is the provider being
registered; the caller of `listing_invite` is the inviting directory. No
payload field names an identity, so none can be forged.

## Public tools

Every KX'd peer may call these; abuse is bounded by rate limiting and the
fee schedule, not by an allowlist.

- `register` (free) - in: `{description, tags, test{tool, args, maxAtoms}}`.
  Registers or renews the caller. The result IS the funding invoice:
  `{state, requiredAtoms, balanceAtoms, shortfallAtoms, feeAtoms,
  testBudgetAtoms}`. Registering again replaces a not-yet-verified
  application (last write wins); while verification is running the previous
  registration continues and the call reports that.
- `my_status` (free) - the caller's pipeline state, listing summary, or
  rejection note. Status is pull-only; the directory never pushes.
- `search` (price per policy) - in: `{query, tags, maxPriceAtoms}`. Matches
  tool names and descriptions across live listings. Under a price ceiling,
  dynamically priced tools are excluded. Hits are capped at 50; `total`
  reports the uncapped count.
- `get_provider` (free) - one listing, including the crawled catalog
  verbatim and the freshness stamps.
- `introduce` (free) - in: `{uid}`. The directory sends the caller a Bison
  Relay KX suggestion toward a LISTED provider (see Introductions below).
- `list_categories` (free) - listed providers histogrammed by tag.
- `policy` (free) - the directory's terms: fees, the test budget ceiling,
  expiry days, the snapshot price, and the hex ed25519 snapshot key.
- `get_snapshot` (priced) - the full signed index. Anyone may buy, verify,
  and redistribute it; this is what makes the directory tip-crawlable.

## Registration lifecycle

    register -> awaiting_funding -> crawling -> testing -> pending_review
                                                       \-> listed (renewal)

- Funding. Tips from the provider credit its balance at the directory.
  Verification starts when the balance covers `feeAtoms + testBudgetAtoms`;
  the fee is debited at that moment and is not refunded. The rest stays the
  provider's balance, escrowing the test.
- Crawl. The directory dials the provider as an MCP client and stores the
  `tools/list` result verbatim, price metadata included. Listings never
  carry self-reported catalogs.
- Test. The directory executes the provider-nominated tool as a real paid
  call. The quote MUST NOT exceed the nominated `maxAtoms` or the remaining
  budget; the spend is debited from the provider's balance before each
  outbound tip. The retry after payment reuses the same `brmcp/callKey`, so
  the provider's journal executes and charges the call exactly once even
  across directory restarts. The test proves reachability, envelope
  conformance, payment settlement, and receive-side liquidity in one call.
- Review. First-time registrations park in `pending_review` for the
  operator (usually an agent driving the admin tools). Only registrations
  whose test passed can be approved. Failures park with the error attached;
  nothing retries silently.
- Renewal. A registration from an already-listed provider re-runs crawl and
  test and, on a pass, promotes without review; the live listing keeps
  serving searches throughout, and a failed renewal never degrades it.
- Expiry. Listings carry an expiry set from the policy at approval and
  renewal. Lapsed listings are removed mechanically; renewal is the garbage
  collector's other half.
- Freshness. Between renewals the directory re-reads `tools/list` for free
  on a schedule. Two stamps distinguish that from real verification:
  `catalogCheckedAt` moves on every crawl, `lastVerifiedExecution` only on
  paid tests.

## Snapshots

A snapshot is `{payload, sig, pub}`: `payload` is the exact signed bytes
(base64 in JSON), `sig` and `pub` hex ed25519. The payload bytes ARE the
canon - verifiers check the signature over them and only then decode, so no
canonical-JSON algorithm is needed. The payload decodes to `{v: 1,
generatedAt, listings[]}` with listings ordered by uid. The signing key is
directory-local (Bison Relay's client API cannot sign arbitrary data) and
is advertised by the `policy` tool; the key from a curated peer record is
the only trust root a verifier MUST accept.

## Introductions

Finding a provider is not yet reaching it: calls need a key exchange. The
directory sits between both parties as a KX'd contact of each, so it can
introduce them two ways:

- Pull (always available): the consumer's client requests a transitive KX
  through the directory as mediator (brclient: `/mi <directory> <uid>`).
  The directory's daemon mediates automatically; no directory code is
  involved.
- Push (`introduce`): the caller asks the directory, and the directory
  sends the caller's client a standard KX suggestion naming the provider.
  The receiving side MUST still accept it - in brclient via the prompted
  `/mi` command, in graphical clients via their accept action - and
  accepting completes the transitive exchange back through the directory
  automatically. Declining is fine and costs nothing.

The directory only introduces callers to live listings: it vouches for
what it suggests, and refuses unknown uids, pending registrations, and
self-introductions. Whether `introduce` is available depends on the host
daemon exposing the client library's SuggestKX; brclientd does (its status
server), stock brclient does not, and an unsupported directory answers the
tool with a clear refusal.

## Federation

Directories federate as customers, not as authorities:

- Peers are curated: `add_peer` records a peer's uid and snapshot key.
- `verify_peer` buys the peer's snapshot, verifies it against the stored
  key, and turns unknown uids into leads. Nothing from a snapshot is ever
  listed directly, and a snapshot signed by an unexpected key yields
  nothing.
- `pursue_lead` invites a lead to register by calling the provider's
  `listing_invite` tool. If the provider is not reachable yet, the
  directory requests a transitive KX through the peer that supplied the
  lead (it knows both sides) and retries the invite when the exchange
  completes.
- The invited provider registers back through the normal pipeline. Listing
  on N directories costs N fees and N passed tests; there is no transitive
  trust anywhere.

## The provider side

Providers integrate the Registrant, which is both roles at once:

- `listing_invite` (free, served on the provider's own harness) - in:
  `{directory, listingFeeAtoms, note}`. The caller uid identifies the
  directory. The auto-fund policy decides: `{enabled,
  max_atoms_per_request, max_atoms_per_month, allowed_directory_uids}`,
  disabled by default so invites only reach the operator. Acceptance
  answers `{accepted}` and the registration happens asynchronously.
- Proactive registration dials the directory, calls `register`, pays the
  shortfall if the policy allows, and reports through the operator's
  Notify callback. The fund history is persisted before money moves, so a
  crash can only over-count against the monthly cap.
- Registering while listed is a paid renewal, so a proactive Register
  first reads `my_status` and leaves a listing alone until it is within a
  week of expiry - hosts may safely Register on every startup without
  paying a renewal per restart.

## Admin surface

Admin tools are ordinary tools on the same harness, visible and callable
only for the uids in the policy's `admin_uids` (the harness hides them
from everyone else's `tools/list` entirely). The operator's agent reaches
them over Bison Relay through its own bridge: no extra listener, port, or
token exists.

`pending_registrations`, `approve`, `reject`, `leads`, `pursue_lead`,
`add_peer`, `remove_peer`, `verify_peer`, `run_verification` (a spot check
re-test funded by the provider's residual balance), `recrawl`, `unlist`,
`flag`, `stats`.

Every mutating admin call is appended to `adminlog.json`, one JSON object
per line. The audit log is the one append-only file in the data directory;
everything else persists whole-file.

The split is mechanism versus policy: funding checks, crawls, tests,
settlement, expiry, and renewals run with no operator alive; everything
requiring judgment waits in a queue for the admin tools.

## Money

- Prices in listings and search results are advisory. The live session's
  `tools/list` and `payment_required` quote authoritatively at call time,
  and the caller's own spending policy guards the payment. The directory is
  never in the money path between consumers and providers.
- The directory's outbound payments (tests, snapshot purchases) are
  recorded in a spend journal at launch and removed only on definitive
  rail failure, so a crash window never hides money that may have left.
  Test spends are additionally bounded by the provider-nominated budget.
- Fees are not refunded on rejection or expiry; residual balances remain
  the provider's, spendable on retries and renewals.

## Security considerations

- The open AllowFunc is deliberate: a directory exists to be found. Rate
  limiting bounds free-tool abuse; the listing fee prices spam out of the
  index; the review gate catches what money does not.
- Listing existence proves operation, not honesty. A malicious provider
  that passes its test can still cheat later callers; the damage is
  bounded by the caller's spending policy, and `unlist`/`flag` are the
  moderation levers.
- Nicknames are not unique on Bison Relay. Uids are the only identity;
  interfaces presenting search results SHOULD show them.
- A peer directory can lie in snapshots. Lies cost it nothing but create
  at most leads; every claim is re-verified first-party at the provider's
  expense before anything is listed.
- Providers accepting invites SHOULD allowlist directories they trust in
  the auto-fund policy; the invite channel is otherwise a spam vector for
  the operator's attention (never for funds, unless auto-funding is opened
  wide).
- Introductions are caller-requested but arrive at the caller's client as
  ordinary KX suggestions; acceptance always stays with the receiver, and
  the listed-only rule keeps the directory from being used as a generic
  introduction spammer.

## Limitations

- Status is pull-only; providers poll `my_status`.
- Admin visibility is computed per peer at first contact; changing
  `admin_uids` requires a restart. The same holds for a provider gaining
  `listing_invite`: tools appear to peers first seen after registration.
- Search is a linear scan over the index; the snapshot must fit the
  transport's message bound (about 12 MiB of payload). Both are fine at
  the scale one directory serves.
- Leads live until converted or manually pruned.
- There is no refund path in v1.
