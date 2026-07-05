# brmcp: The Model Context Protocol over Bison Relay

*Paid, private, sovereign tool services for AI agents*

karamble

July 2026 - Draft v1.0

---

**Abstract.** The Model Context Protocol (MCP) has become the
standard interface between AI agents and the tools they call, but its common
transports inherit the assumptions of the web: servers need domains, TLS
certificates, and public reachability; callers need accounts and API keys;
and payment is an out-of-band contract between a billing department and a
credit card. brmcp is a conforming MCP transport that replaces this stack
with Bison Relay, an end-to-end encrypted, pay-per-use messaging network
built on Decred and its Lightning Network. A tool provider is just a Bison
Relay client: it needs no inbound connectivity, no certificate, and no
payment processor. A caller is identified by its Bison Relay identity, and
every paid tool call settles instantly as a Lightning-backed tip. This
resolves, at the transport layer, the two problems that keep autonomous
agents second-class citizens of the web: agents cannot pass sign-up flows
and KYC to obtain accounts and cards, but they can hold keys and pay
invoices. On Bison Relay a keypair is a first-class identity and every
identity is born with a payment method, so an agent arrives with both. The
result is an open market for machine-callable services in which identity,
confidentiality, and settlement are properties of the transport rather than
services rented from intermediaries. This paper specifies the wire format,
the serving harness, the client bridge, and the payment protocol, and
reports on a production deployment offering paid image, video, speech, and
financial-data tools to autonomous agents.

---

## 1. Introduction

Software agents are becoming consumers of network services. An agent that
plans a task will search, fetch, transform, generate, and pay - repeatedly,
autonomously, and at machine speed. The Model Context Protocol gives this
interaction a shape: tools are described by schemas, calls are JSON-RPC 2.0
messages, and any client can discover and invoke the tools of any server.

What MCP deliberately does not define is the economic and trust fabric
around a call. In practice today that fabric is the legacy web stack:

- The server must be reachable: a domain name, a TLS certificate, a public
  IP or a fronting cloud, and the operational surface that comes with them.
- The caller must be enrolled: an account, an API key, usually a credit
  card, always a jurisdiction.
- Payment is coarse: subscriptions or postpaid metering, reconciled by
  invoice, gated by fraud controls, and unavailable to software that has no
  bank account.
- The relationship is observable: the operator, the CDN, and every
  middlebox see who calls what, when, and how often.

These assumptions exclude an entire class of participants: individual
operators who will not run public infrastructure, agents that need to pay
per call without a card, and users whose tool usage is nobody's business.

### 1.1 The two missing rails: identity and payments for machines

The agentic web has a well-known pair of unsolved problems, and both are
prerequisites for everything else.

The first is identity. An agent cannot open an account. Sign-up flows,
CAPTCHAs, e-mail verification, and KYC exist to prove there is a human,
which is precisely what an agent is not. So agents borrow the identity of
their operator - shared API keys, OAuth grants, session cookies - and every
borrowed credential is a liability: over-scoped, revocable only as a
bundle, indistinguishable across the operator's agents, and worthless as a
reputation anchor because it does not belong to the thing acting.

The second is payments. An agent cannot hold a credit card. Cards
presuppose a bank account, a legal person, a billing address, and a
chargeback process; none of these are available to software. What software
can do, natively and safely, is hold a private key and sign. A payment
instrument built on keys and settled in seconds at arbitrary granularity -
a Lightning micropayment - is the only instrument whose shape matches the
payer.

Bison Relay solves both with the same primitive. An identity is a keypair
generated locally: no enrollment, no gatekeeper, cryptographically
continuous across every interaction, and cheap enough to mint one per
agent. And every identity is born with a payment method, because tipping
any contact over the Lightning Network is a native function of the
messaging layer itself. brmcp inherits both properties without adding any
machinery: the sender uid on each message IS the agent's authenticated
identity, and the tip flow IS its wallet. Identity and payments stop being
integration projects and become assumptions the tool market can build on.

brmcp removes the assumptions instead of patching them. It carries
unmodified MCP sessions over Bison Relay private messages. Bison Relay
already provides mutually authenticated end-to-end encryption between
pseudonymous identities, store-and-forward delivery paid in
micropayments, and a native tip mechanism that settles over the Decred
Lightning Network. brmcp adds a thin envelope so that JSON-RPC messages can
ride private messages, a serving harness so that an operator can publish
priced tools in a few lines of code, and a client bridge so that any
off-the-shelf MCP agent can call them. Nothing about MCP itself changes:
brmcp is a transport in the sense the specification intends, not a fork.

## 2. Background

### 2.1 The Model Context Protocol

MCP standardizes how a client discovers and calls tools: capabilities are
negotiated in an initialize handshake, tools are listed with JSON schemas,
and calls and results are JSON-RPC 2.0 messages. The specification defines
the message layer separately from the transport; official SDKs expose a
transport interface precisely so that sessions can run over pipes, HTTP
streams, or anything that can move ordered messages between two parties.
brmcp implements that interface. Any MCP client or server built on the
official SDKs can ride it without modification.

### 2.2 Bison Relay

Bison Relay is a private messaging network built on Decred. Its properties
are unusual and load-bearing for this design:

- Identities are cryptographic keypairs, not accounts. Two users perform a
  key exchange (KX) once; from then on they share a double-ratchet channel
  with end-to-end encryption and forward secrecy.
- The relay server stores and forwards opaque ciphertext. It cannot read
  message content and does not know user identities; it charges per
  message delivered, paid over the Lightning Network, which removes the
  incentive structure of surveillance-funded messaging.
- Clients need only outbound connectivity. A Bison Relay client behind NAT
  or Tor is a full participant.
- Payments are built in. Any contact can tip any other contact: under the
  hood the clients exchange a Lightning invoice for the exact amount over
  the relay and pay it peer to peer. Neither side handles raw invoices in
  application code, and the receiving side needs no payment endpoint.
- Messages are discrete, ordered, acknowledged, and bounded in size
  (1 MiB routed), which makes one private message a natural frame for one
  JSON-RPC message.

The combination - authenticated encrypted channels, no inbound
reachability, and native exact-amount settlement - is exactly the fabric
that machine-to-machine tool markets lack.

## 3. Design goals

1. **Conformance.** brmcp must be a standard MCP transport. No changes to
   the protocol, the SDKs, or the tools; an agent must not be able to tell
   a brmcp tool from an HTTP one except by its metadata.
2. **Default deny.** Serving tools to the world is a choice, not a side
   effect. Unknown callers must be refused before any state is allocated
   for them.
3. **Native settlement.** Prices are advertised in the tool metadata and
   settle as Bison Relay tips. The server holds no Lightning credentials;
   the client applies its own spending policy before a single atom moves.
4. **Zero exposure.** The server is only a Bison Relay client. No listener,
   no domain, no certificate, no port forwarding, no cloud.
5. **Shared channel.** Humans and machines share the same DM thread.
   Envelope traffic must be unambiguous to parsers and invisible in
   human-facing history.

## 4. Wire format

Each Bison Relay private message carries exactly one envelope part:

```
--mcp[v=1,sid=<sid>,mid=<mid>,seq=<n>/<total>,exp=<unix>]--<base64>
```

- `v` is the envelope version; this document specifies v=1 and parsers must
  ignore parts with other versions.
- `sid` is the session id (1..32 lowercase hex), chosen by the side that
  opens the session. One peer pair can run several concurrent sessions.
- `mid` is a random per-message id grouping the chunks of one JSON-RPC
  message.
- `seq` is `n/total`, 1-based; `1/1` is a complete message and `total` must
  not exceed 64.
- `exp` is a unix-seconds deadline (0 for none). Bison Relay stores and
  forwards, so parts can arrive long after sending; receivers drop expired
  parts and abandon partial messages whose deadline passed. A request
  queued while a server was offline therefore does not execute after its
  deadline has passed.
- The payload is standard base64 of one chunk of exactly one JSON-RPC 2.0
  message.

Unknown `k=v` pairs inside the brackets are ignored for forward
compatibility. Any message body that does not match the envelope exactly is
not brmcp traffic and is ignored, which is what lets humans and MCP share
one conversation.

Senders keep raw chunks at or below 200 KiB so the base64 encoding stays
well under the 1 MiB routed-message limit. Receivers reassemble by (peer,
sid, mid) in `seq` order, tolerate out-of-order and duplicate chunks, bound
their reassembly state per peer, per message, and globally, and evict
partial messages on timeout.

The session lifecycle is standard MCP: the client sends `initialize` as the
first message of a new sid; a server accepting a part with an unknown sid
from an authorized peer treats it as a new session; idle sessions expire.

## 5. Architecture

A brmcp deployment has two ends, both of which are ordinary Bison Relay
clients.

### 5.1 The serving harness

The server side is a library harness around a Bison Relay client. An
operator registers tools with one call each:

```go
server.AddTool(h, &mcp.Tool{Name: "mytool", Description: "..."},
    priceAtoms, func(ctx context.Context, peer string, in Args) (any, error) {
        // peer is the caller's 64-hex Bison Relay uid.
        return result, nil
    })
```

The harness enforces, in order and before a handler ever runs: the caller
allowlist (default deny, checked before any session or reassembly state is
allocated), the idempotency claim on the call key (section 6.3), a
per-caller token-bucket rate limit, and price and balance checks for paid
tools. Operators may replace the allowlist with an admission function; the
reference deployment admits every KX'd contact and lets balances and rate
limits do the gating, since key exchange itself is the introduction step.
Each authorized peer is served by its own MCP server instance, so sessions
and capabilities are isolated per caller. A zero price makes a tool free; a positive price is
advertised in the tool's `_meta` under `brmcp/priceAtoms` and enforced
before dispatch. Dynamic pricing is supported by computing the price per
call; such tools are marked `brmcp/pricing=dynamic` in their metadata.

### 5.2 The client bridge

The caller side is a bridge inside the user's own Bison Relay daemon. For
each allowed bot it exposes a local streamable-HTTP MCP endpoint
(`/mcp/<bot-uid>`, bearer-token gated, disabled by default) that mirrors the
remote bot's tools verbatim. To the agent this is a completely ordinary MCP
server reachable on localhost; the bridge relays every call over Bison
Relay and returns the result. The agent needs no Bison Relay awareness, no
wallet, and no keys - connecting takes an endpoint URL and a bearer token.

The bridge is also where the user's spending policy lives (section 6.2),
which keeps economic authority with the human even when the agent is
autonomous.

## 6. Payments

### 6.1 Server side: a prepaid ledger

The server keeps an authoritative per-caller balance. The balance store is
pluggable: the library ships a file-backed ledger, and a service with
existing accounting supplies its own store behind the same interface, as
the reference deployment does. Tips from allowed callers credit their
balance; paid calls debit it. When a paid call lacks
balance, the tool returns an error result carrying a machine-readable
`payment_required` object stating the price and the accepted settlement
rail (`tip`). The caller tops up by tipping and retries. Handler failures
after a charge are refunded. Clients can never assert balances; only
observed tips create credit.

### 6.2 Client side: caps and consent

The bridge applies the user's policy before any payment:

- An allowlist of callable bots (default empty: no one).
- A per-call cap and a daily cap enforced over a rolling twenty-four-hour
  window of recorded spend, both defaulting to zero, where zero means
  never pay.
- Two modes: approval, in which every payment parks until the user
  approves or denies it, and autopay, in which payments under the caps
  settle unattended. Caps bind in both modes.

Settlement uses Bison Relay's native tip flow: the payer's client requests
an invoice from the payee's client over the relay, verifies that the
invoice amount matches the requested tip exactly, and pays it over the
Lightning Network. No raw invoice ever crosses the MCP layer, and the
serving side needs no Lightning credentials of its own. Observed
settlement latency in production is one to eleven seconds.

### 6.3 At-most-once execution

Store-and-forward transports retry, and paid calls must not double-charge.
Every logical call carries an idempotency key in its metadata, reused by
transport retries and by the post-payment reissue of a previously refused
call. The harness claims the key per peer before the rate, price, and
charge gates; duplicates wait for the original and replay its recorded
outcome; refusals that precede execution release the key unrecorded so a
funded retry runs for real. Combined with the envelope deadlines and Bison
Relay's own delivery deduplication, a keyed call executes at most once and
is charged at most once: completed outcomes are replayed to duplicates for
thirty minutes, and paid-call claims are journaled to disk, so a restart
refunds any charge whose execution never completed and keeps refusing
duplicates of completed paid calls instead of re-executing them. Free-tool
claims live in process memory, bounded by the envelope deadline.

## 7. Security model

**Authentication.** Transport authentication is Bison Relay's: every
message arrives on an end-to-end encrypted, mutually authenticated ratchet
with a KX'd contact. The sender uid attached to each message is the caller
identity. brmcp adds no identity of its own and trusts no identity claims
inside payloads.

**Authorization.** Default deny, enforced before state allocation. A peer
not on the allowlist cannot create sessions, fill reassembly buffers, or
earn ledger credit. Only explicitly registered tools are reachable; the
harness exposes nothing of the host beyond them.

**Denial of service.** Reassembly buffers are bounded per peer, per
message, and globally, with timeout eviction; a message spans at most 64
parts; per-session inbound queues are bounded and an overflowing session is
closed rather than buffered without limit; every caller is rate limited
before dispatch.

**Replay and staleness.** Outgoing messages carry deadlines and expired
parts are dropped, so queued requests do not execute after their caller
gave up. The relay deduplicates delivery per message; the envelope
additionally ignores duplicate chunk sequence numbers.

**Payment integrity.** The ledger is server-authoritative. The tip flow
verifies exact invoice amounts on the paying side. Client caps are hard
ceilings evaluated locally, and zero means zero.

**Privacy.** The relay carries ciphertext between pseudonyms and is paid
for delivery rather than for data. What an agent asks, which tools it
calls, and what it pays are visible only to the two endpoints. On the open
web, the equivalent metadata is the product.

## 8. Implementation and deployment

The reference implementation is in Go:

- **brmcp**, the library: the envelope codec with chunking, bounded
  reassembly, and deadline handling; the SDK transport binding; the serving
  harness with allowlisting, rate limiting, priced tools, the prepaid
  ledger, tip settlement, and idempotent execution; the client bridge
  engine with per-bot local MCP endpoints, the spending policy with
  approval and autopay modes, and tip settlement; and a runnable example
  service.
- **brclientd**, a headless Bison Relay daemon, embeds the library's client
  bridge, wiring it to the daemon's own tip pipeline and exposing the
  pending-approval and spend-audit surfaces over its REST API.
- **Decred Pulse**, a self-hosted Decred dashboard, provides the operator
  UI: enabling the bridge, recycling the bearer token, selecting modes and
  caps, allowlisting bots, and approving or denying parked payments.

The design is validated by a production service: an existing Bison Relay
bot was extended into a brmcp tool provider offering paid text-to-image,
text-to-video, image-to-video, and text-to-speech generation and financial
market data, priced in USD and converted to atoms at call time. Generated
media is delivered over the same encrypted DM as the session, so results
reach the user's client even if the agent has disconnected. Autonomous
agents have used the bridge end to end: discovering tools, receiving
payment-required refusals, being funded by tip, retrying, and receiving
delivery - with every payment either auto-approved under caps or held for
human approval.

## 9. Related approaches

**Pay-per-call HTTP.** Emerging HTTP 402 schemes attach stablecoin or
on-chain payments to web APIs. They monetize the existing web stack and
inherit its requirements: the server remains a public endpoint with a
domain and certificate, the caller remains an observable IP, and identity
remains an add-on. brmcp starts from a substrate where encryption,
pseudonymous identity, and settlement already exist and adds only framing.

**API marketplaces.** Centralized marketplaces solve discovery and billing
by becoming the intermediary: they hold the keys, take the margin, see the
traffic, and decide who may participate. brmcp has no intermediary to
capture; the relay it does use is blind to content and identity and is paid
per message, not per insight.

**Hidden services.** Tor onion services give servers reachability without
exposure, but bring no identity continuity between visits and no payments.
Bison Relay's KX'd contacts give both, at the cost of an introduction step
- which doubles as the authorization boundary.

## 10. Limitations and future work

- **Latency.** A call crosses the relay twice and a payment adds a
  Lightning settlement; interactive round trips are seconds, not
  milliseconds. The transport suits priced, substantial calls more than
  chatty ones.
- **Framing overhead.** Messages are bounded at 1 MiB per routed message
  and 64 parts per message; base64 costs a third. Large media is better
  delivered by Bison Relay's file transfer, as the reference deployment
  does.
- **Availability.** The relay is not a confidentiality or integrity
  dependency, but it is an availability one, as is the payment path
  (channel liquidity, peer uptime).
- **Execution guarantees.** Paid-call claims are journaled and reconciled
  at startup: interrupted charges are refunded and completed calls keep
  refusing duplicates. Outcomes above the journal's retention bound are
  refused to post-restart duplicates rather than replayed, and free-tool
  claims do not survive a restart.
- **Interactive approvals.** A parked payment outlives many agents' HTTP
  timeouts; an agent that gives up before the human decides simply
  retries, and the bridge re-parks the payment. Patient client timeouts
  suit approval mode.
- **Discovery.** Finding a tool provider and its uid is out of band today.
  A signed, tip-crawlable directory of providers and price lists is future
  work.
- **Streaming.** Results return as complete messages; incremental
  streaming inside a session is future work.
- **Group transports.** The envelope is peer-to-peer today; group-chat
  transports would allow shared-cost services and multi-agent sessions.

## 11. Conclusion

Every property that tool markets bolt on - authentication, encryption,
billing, abuse control - is a property brmcp inherits from its substrate.
The two rails the agentic web is still missing, identity that belongs to
the agent and payments an agent can actually make, are not added by brmcp;
they are what Bison Relay already is, and brmcp merely lets MCP ride them.
By treating a private, pay-per-use messaging network as an MCP transport,
a single operator with a laptop and a Lightning channel can sell services
to autonomous agents worldwide: no domain, no platform, no processor, no
account - and the user who pays keeps both the privacy of the call and the
authority over every atom spent. The protocol surface added to achieve
this is one envelope line and a few metadata keys. The rest was already
there.

## References

1. Model Context Protocol specification. https://modelcontextprotocol.io
2. Bison Relay. https://bisonrelay.org
3. Bison Relay source. https://github.com/companyzero/bisonrelay
4. Decred. https://decred.org
5. brmcp source, wire specification (docs/WIRE.md), and security notes
   (docs/SECURITY.md). https://github.com/karamble/brmcp
6. brclientd, a headless Bison Relay daemon.
   https://github.com/karamble/brclientd
7. Decred Pulse. https://github.com/karamble/dcrpulse
