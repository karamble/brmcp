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
- Invoice settlements are matched by payment hash against invoices this
  harness issued, credit exactly once, and survive restarts (pending
  invoices and the subscription resume index are persisted).
- A paid call is debited before dispatch and refunded if the handler
  fails, so callers do not pay for operator bugs, and operators do not do
  free work for callers.

## What brmcp does not do

- It does not expose the operator's wallet, Bison Relay account, or host
  beyond the registered tools.
- It does not execute anything for unauthenticated or unlisted peers.
- It does not trust tool arguments: argument validation is the tool
  author's job, as with any MCP server.
