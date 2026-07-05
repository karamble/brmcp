# brmcp security model

## Authentication

Transport authentication is Bison Relay's: every message arrives on an
end-to-end encrypted, mutually authenticated ratchet with a KX'd contact.
The sender uid attached to each PM is the caller identity. brmcp adds no
identity of its own and trusts no identity claims inside payloads.

## Authorization

Authorization is the operator's, and it is default-deny:

- The allowlist (`allowed_uids`) gates callers BEFORE any state is
  allocated: a peer not on the list cannot create sessions, fill reassembly
  buffers, or earn ledger credit.
- Only explicitly registered tools are reachable. The harness exposes
  nothing of the host beyond them.
- Each caller is rate limited (token bucket) before dispatch.

## Denial of service bounds

- Reassembly buffers are bounded per peer (pending message count), per
  message (bytes), and globally (bytes), and partial messages are evicted
  on timeout.
- A message may span at most 64 parts.
- Per-session inbound queues are bounded; a session that overflows its
  queue is closed rather than buffered without limit.
- One peer holds at most a bounded number of concurrent sessions (default
  8); further session attempts are dropped.
- Sessions idle beyond the idle timeout (default 10 minutes on the serving
  side) are closed, so an authorized peer cannot accumulate session state
  by walking away.

## Replay and staleness

Bison Relay stores and forwards. Every outgoing message carries a deadline
(`exp`); receivers drop expired parts, so a request queued while the server
was offline does not execute after the caller stopped waiting. Bison Relay
itself deduplicates delivery per message; the envelope additionally ignores
duplicate chunk sequence numbers.

## Payments

- The ledger is server-authoritative. Clients cannot assert balances.
- Tips credit only allowlisted callers (a stranger's tip is received by the
  wallet but never creates harness state).
- Payment settlement is Bison Relay's native tip flow: the payer's client
  requests an invoice from the operator's client over the relay and pays
  it. The harness never touches LN credentials; it only sees the tip
  credit reported by the Bison Relay client.
- A paid call is debited before dispatch and refunded if the handler
  fails, so callers do not pay for operator bugs, and operators do not do
  free work for callers.

## The client bridge

The caller side (the `bridge` package) has its own guardrails, all
evaluated locally in the user's daemon:

- The local MCP endpoint is bearer-token gated (constant-time compare; an
  empty token never authorizes), bound to an address the host chooses
  (localhost by convention), and disabled by default. Changing the token
  restarts the listener, severing streams authorized under the old one.
- Callable bots are a default-deny allowlist; requests for any other uid
  are refused before any session state exists, and inbound frames from
  unlisted peers are dropped at the router.
- Spending is bounded by a per-call cap and a daily cap enforced over a
  rolling twenty-four-hour window of the recorded spend log. Both caps
  bind in approval AND autopay modes; zero means never pay. In approval
  mode every payment parks for a human decision, bounded by a timeout;
  parked payments die with the process, so a restart is a fail-safe
  denial.
- Every settled payment is recorded in the local spend log the daily cap
  is enforced against; entries inside the window are never pruned.
- No raw invoice ever crosses the MCP layer: settlement is Bison Relay's
  native tip flow between the two clients, and the exact invoice amount is
  verified on the paying side.
- A leaked bearer token is bounded by the policy: it can call tools and
  spend at most what the caps allow, and nothing at all with caps at zero
  or the bridge disabled.

## What brmcp does not do

- It does not expose the operator's wallet, Bison Relay account, or host
  beyond the registered tools.
- It does not execute anything for unauthenticated or unlisted peers.
- It does not trust tool arguments: argument validation is the tool
  author's job, as with any MCP server.
