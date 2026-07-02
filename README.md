# brmcp - Model Context Protocol over Bison Relay

brmcp carries MCP sessions over Bison Relay private messages. A bot operator
serves standard MCP tools to KX'd contacts; callers pay per call with Bison
Relay tips. MCP is JSON-RPC 2.0 over a pluggable
transport, so this is a conforming transport, not a protocol fork: any MCP
client or server built on the official SDKs can ride it.

Why Bison Relay instead of HTTPS:

- A KX'd contact is a mutually authenticated, end-to-end encrypted channel.
  No API keys, no TLS certificates, no domain. The caller's BR identity is
  the principal.
- The server needs zero inbound reachability. It is just another Bison Relay
  client, workable behind NAT or Tor.
- Payments are native. Per-call prices settle as Bison Relay tips (the
  clients exchange and pay Lightning invoices under the hood) with no
  billing infrastructure and no LN credentials on the server.
- The relay stores and forwards, so long-running results survive disconnects.

## Repository layout

- `wire/` - the envelope codec: framing, chunking, reassembly, deadlines.
  See WIRE.md for the byte-level specification.
- `transport.go` - the MCP go-sdk custom transport over an abstract private
  message send/receive pair, with per-session routing.
- `harness.go`, `ledger.go`, `bot.go` - the serving harness: allowlist,
  rate limiting, paid tools, tip settlement, bisonbotkit lifecycle.
- `cmd/brmcp-serve` - a runnable example service with a free tool and a paid
  tool. Copy its shape to build your own service.

## Serving tools

brmcp-serve connects to a RUNNING brclient or brclientd through the
clientrpc interface (TLS websocket). Enable clientrpc in your client, then:

    go build ./cmd/brmcp-serve
    ./brmcp-serve -datadir ~/.brmcp-serve

The first run creates two files in the data directory:

- `brmcp-serve.conf` - the clientrpc connection (bisonbotkit format: rpc
  URL, certificates, user, password). Point it at your client's clientrpc.
- `brmcp.json` - the harness config:

    {
      "allowed_uids": ["<64-hex caller uid>"],
      "calls_per_minute": 30
    }

The allowlist is default-deny: with no uids listed, every caller is refused.

Registering your own tools is one call each:

    brmcp.AddTool(h, &mcp.Tool{Name: "mytool", Description: "..."},
        priceAtoms, func(ctx context.Context, peer string, in Args) (any, error) {
            // peer is the caller's 64-hex Bison Relay uid.
            return result, nil
        })

A zero price makes the tool free. A positive price is advertised to clients
in the tool's `_meta` under `brmcp/priceAtoms` and enforced before the
handler runs.

## Calling tools

The caller side is any MCP client behind a Bison Relay client that speaks
this wire format. The reference client implementation lives in brclientd
(the `mcp` branch): it exposes a local streamable-HTTP MCP endpoint per bot
(`/mcp/<bot-uid>`) that agents such as Claude Code connect to, and relays
the session over Bison Relay, paying for tools by tip under user-configured
caps or per-payment approval.

## Payments

The server keeps an authoritative per-caller balance ledger:

- Tips from allowed callers credit their balance (milliatoms are floored to
  atoms).
- When a paid call lacks balance, the tool returns an `isError` result whose
  text content is machine-readable JSON:

      {"error":"payment_required","tool":"fortune","priceAtoms":10000,
       "balanceAtoms":0,"shortfallAtoms":10000,
       "acceptedRails":["tip"]}

- Tipping at least the shortfall funds the balance; the tip itself is
  Bison Relay's native invoice exchange between the two clients, so the
  amount arrives exactly and the bot needs no LN credentials. Retry the
  call after the tip completes.
- A handler error refunds the call price; the ledger keeps no other refund
  path.

## Latency expectations

Bison Relay is store-and-forward through a relay: a round trip takes seconds,
not milliseconds. Session initialization plus a tool call is typically 3-4
round trips; clients should cache the session and the tool list. Outgoing
messages carry a deadline (10 minutes by default) so a request delivered to
an offline server does not execute after the caller gave up.

## License

ISC. See LICENSE.
