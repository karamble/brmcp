# brmcp wire format (v1)

This document specifies the envelope so non-Go implementations can
interoperate. The payload being framed is always exactly one JSON-RPC 2.0
message as defined by the Model Context Protocol.

## Envelope

Each Bison Relay private message carries exactly one part:

    --mcp[v=1,sid=<sid>,mid=<mid>,seq=<n>/<total>,exp=<unix>]--<base64>

- `v` - envelope version. This document specifies v=1. Parsers MUST ignore
  parts with other versions.
- `sid` - session id, 1..32 lowercase hex characters. Chosen by the side
  that opens the session (random 16 hex recommended). All messages of one
  MCP session share one sid; one peer pair can run several sessions.
- `mid` - message id, 1..32 lowercase hex characters, random per message.
  Groups the chunks of one JSON-RPC message.
- `seq` - `n/total`, 1-based. `1/1` is a complete single-part message.
  `total` MUST NOT exceed 64.
- `exp` - deadline as unix seconds, `0` for none. Receivers MUST drop parts
  (and abandon partial messages) whose deadline has passed: Bison Relay
  stores and forwards, so messages can arrive long after sending.
- `<base64>` - standard base64 (RFC 4648, with padding) of this chunk of
  the JSON-RPC message bytes. Whitespace inside the base64 is permitted and
  ignored. The chunk MUST NOT be empty.

Unknown `k=v` pairs inside the brackets MUST be ignored (forward
compatibility). Any PM body that does not match the envelope exactly is not
brmcp traffic and MUST be ignored; humans and MCP share the DM thread.

## Chunking

Senders SHOULD keep raw chunks at or below 200 KiB so the base64 encoding
stays well under Bison Relay's 1 MiB routed-message limit. Receivers
reassemble by (peer, sid, mid), concatenating chunks in `seq` order once all
`total` parts arrived. Chunks may arrive out of order. Duplicate `seq`
values are ignored. Receivers MUST bound reassembly state (pending message
count and bytes) and evict partial messages after a timeout.

## Sessions

The session lifecycle is standard MCP: the client sends `initialize` as the
first message of a new sid. A server accepting a part with an unknown sid
from an authorized peer treats it as a new session. Either side ends a
session by simply ceasing to reference its sid; servers SHOULD expire idle
sessions.

## Identity and authorization

The Bison Relay uid of the message sender IS the authenticated caller
identity; the envelope carries no identity fields on purpose. Servers MUST
apply their authorization (allowlists etc.) before allocating any session
or reassembly state for a peer.

## Payments

Paid tools advertise their price in atoms in the MCP tool `_meta` object
under the key `brmcp/priceAtoms`. A call without sufficient balance returns
a tool result with `isError: true` whose first text content is a JSON
object:

    {
      "error": "payment_required",
      "tool": "<tool name>",
      "priceAtoms": <int>,
      "balanceAtoms": <int>,
      "shortfallAtoms": <int>,
      "acceptedRails": ["tip"]
    }

Clients settle by sending a Bison Relay tip of at least the shortfall,
then retry the call. Under the hood the tip is Bison Relay's native
invoice exchange: the payer's client requests an invoice from the bot's
client over the relay and pays it; neither end of this protocol carries
a raw invoice. Balances are server-side state per caller uid.

## Call metadata

Two more `_meta` keys complete the protocol surface:

- Tools priced per call from their arguments (rather than at a fixed
  price) carry `brmcp/pricing: "dynamic"` in their tools/list `_meta`; the
  authoritative quote arrives in the payment_required object.
- A tools/call request MAY carry an idempotency key in its `_meta` under
  `brmcp/callKey`: a string of 8 to 128 characters, random 32 hex
  recommended. Servers MUST execute and charge a keyed call at most once
  per (caller, key): a duplicate waits for the original and replays its
  recorded outcome (kept at least as long as the envelope deadline
  horizon; the reference implementation keeps it thirty minutes and
  journals paid-call claims to disk, so it refunds a charge whose
  execution a crash interrupted and keeps refusing duplicates of
  completed paid calls across restarts), while refusals that precede
  execution (rate limit, payment_required) release the key so a funded
  retry runs for real. Transport retries and the post-payment reissue of
  one logical call MUST reuse the same key.
