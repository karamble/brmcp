# Embedding the brmcp client bridge

The `bridge` package is the caller side of brmcp: a library engine that
exposes one local streamable-HTTP MCP endpoint per allowed Bison Relay bot
(`/mcp/<bot-uid>`), mirrors the bot's tools verbatim, relays every call
over the relay, and settles `payment_required` refusals under the user's
spending policy. To the agent the endpoint is an ordinary MCP server: a URL
and a bearer token, no Bison Relay awareness, no wallet, no keys. brclientd
is the reference host, through the `brclient` package.

## Hosting it on an embedded Bison Relay client

A daemon that embeds `github.com/companyzero/bisonrelay/client` attaches
the bridge in one call. `brclient.Attach` supplies the PM sender, a tip
payer on the client's own tip flow, the inbound PM feed, and the recording
of late tip outcomes in the spend log:

    b, err := brclient.Attach(c, bridge.Config{
        DataDir:    dataDir,           // holds mcpclient.json + mcpspend.json
        ListenAddr: "127.0.0.1:8891",  // or "" to mount b.Handler() yourself
        Name:       "mydaemon",        // client identity + refusal-note prefix
        Log:        backend.Logger("MCPC"),
    })
    if err != nil { ... }
    if err := b.Start(ctx); err != nil { ... } // binds while enabled; ctx cancel closes

A `Start` error means the listener did not bind. The bridge stays usable
and a later settings change retries the bind, so a host may log it and go
on.

Peers are addressed by uid only, never by nick.

## Keeping envelopes out of chat

The bridge consumes brmcp envelopes from the PM feed, but the host still
receives them as ordinary private messages. Every place the host shows or
reacts to chat (conversation views, unread badges, notifications, content
filters) must skip messages for which `brmcp.IsEnvelope` reports true, or
agent traffic shows up as chat.

## Other hosts

A host on another rail wires `bridge.New` itself:

    b, err := bridge.New(bridge.Config{
        DataDir: dataDir,
        Sender:  sender, // brmcp.PMSender: uid -> private message
        Payer:   payer,  // settles payments (below)
        ...
    })
    // Feed EVERY inbound private message; non-envelopes are ignored.
    onPM(func(fromUID, text string) { b.HandlePM(fromUID, text) })

- **Sender** delivers one PM body to a peer uid. The bridge frames and
  chunks; the host only sends text.
- **Payer** performs one blocking payment of atoms to a peer uid and
  returns nil only on confirmed settlement. The ctx carries the settings'
  payment-wait deadline. Error text reaches the agent verbatim after
  "payment not made:", so word it for humans.

Hosts that pay with Bison Relay tips use `bridge.TipPayer` rather than
writing one: it starts the tip, waits for the matching tip-progress event,
and reports outcomes that arrive after the wait to a late handler.

    payer := bridge.NewTipPayer(func(ctx context.Context, uid string, atoms int64) error {
        return startTip(ctx, uid, atoms) // dispatch only; the outcome comes below
    })
    // From every tip-progress event:
    //   payer.Progress(payeeUID, amtMAtoms, completed, attemptErr, willRetry)
    b, err := bridge.New(bridge.Config{..., Payer: payer})
    payer.SetLate(func(uid string, atoms int64, err error) { b.ResolveSpend(uid, atoms, err) })

Bots that reach their client over clientrpc (bisonbotkit) get the same
pieces from the `botkit` package: `botkit.Sender`, `botkit.NewTipPayer`,
`botkit.HandleTipProgress`, and `botkit.LateBot` for a bot that connects
after start.

## The endpoint

`/mcp/<bot-uid>` speaks the standard MCP streamable-HTTP transport in
stateless mode and serves both current protocol generations per request:
agents on the 2026-07-28 wire and agents on the legacy handshake connect
unchanged. Stateless means the endpoint issues and honors no
`Mcp-Session-Id`, answers GET (the standalone SSE stream) and DELETE with
405, and caps request bodies at 16 MiB (413 beyond) - the bridge sends no
server-initiated messages, so nothing is lost. Raw non-SDK callers using
the 2026-07-28 wire must mirror the standardized HTTP headers
(`Mcp-Protocol-Version`, `Mcp-Method`) alongside the request `_meta`; SDK
clients do this on their own.

Auth is one bearer token, compared in constant time; an empty token never
authorizes. Requests may additionally be restricted to a source-IP list
(`allowed_ips`); a request from any other address is answered with the
same generic 401 as a bad token, checked per request so a change needs no
listener restart. The uid must be 64-hex and on the bot allowlist;
anything else is 404. With `ListenAddr` set the bridge owns the listener
and binds only while the settings enable it (a token change restarts it,
severing streams authorized under the old token); with `ListenAddr` empty
the host mounts `Handler()` and owns that lifecycle itself, and a disabled
bridge answers 404 to everything.

## The spending policy

Persisted as `mcpclient.json` (the `Settings` type) and applied with
`ApplySettings`, which hot-reconciles: the listener starts, stops, or
restarts; sessions of de-listed bots close; enabling with an empty token
mints a random one.

- `allowed_bots` - default-deny allowlist of callable bot uids.
- `allowed_ips` - optional source-IP restriction on the listener: single
  IPs or CIDR ranges (IPv4 or IPv6); empty means any address. The most
  recent denial is exposed via `LastDenied` (cleared on the next
  successful auth) so a host UI can offer the observed address.
- `per_call_cap_atoms`, `per_day_cap_atoms` - hard ceilings; the daily cap
  is enforced over a rolling twenty-four-hour window of the spend log;
  zero means never pay. Caps bind in BOTH modes.
- `mode` - "approval" parks every payment for a human decision (surface it
  with `PendingPayments`/`ResolvePayment`, bounded by
  `approval_timeout_secs`); "autopay" settles under the caps unattended.
- `tip_wait_secs` - how long a call waits for settlement confirmation.

`SpendLog` returns the recorded payments and the rolling daily total for
audit surfaces.

## Payment flow

A paid call without balance comes back `payment_required`. The bridge
checks the caps and the mode, pays through the Payer, then reissues the
call with the SAME idempotency key (`brmcp/callKey` - see WIRE.md), so the
bot can never execute or charge one logical call twice. Refusals (caps,
denial, timeout, payer errors) append a human-readable note to the bot's
result and return it, so the agent sees why payment was not made. After a
settlement whose credit is still in flight, the bridge briefly polls the
bot before handing back the refusal.
