# Embedding the brmcp client bridge

The `bridge` package is the caller side of brmcp: a library engine that
exposes one local streamable-HTTP MCP endpoint per allowed Bison Relay bot
(`/mcp/<bot-uid>`), mirrors the bot's tools verbatim, relays every call
over the relay, and settles `payment_required` refusals under the user's
spending policy. To the agent the endpoint is an ordinary MCP server: a URL
and a bearer token, no Bison Relay awareness, no wallet, no keys. brclientd
is the reference host.

## What the host supplies

    b, err := bridge.New(bridge.Config{
        DataDir:    dataDir,           // holds mcpclient.json + mcpspend.json
        Sender:     sender,            // brmcp.PMSender: uid -> private message
        Payer:      payer,             // settles payments (below)
        ListenAddr: "127.0.0.1:8891",  // or "" to mount b.Handler() yourself
        Name:       "mydaemon",        // client identity + refusal-note prefix
        Logf:       log.Printf,
    })
    if err != nil { ... }
    if err := b.Start(ctx); err != nil { ... } // binds while enabled; ctx cancel closes

    // Feed EVERY inbound private message; non-envelopes are ignored.
    onPM(func(fromUID, text string) { b.HandlePM(fromUID, text) })

- **Sender** delivers one PM body to a peer uid. The bridge frames and
  chunks; the host only sends text.
- **Inbound feed**: the host calls `HandlePM` for every PM it receives.
  Chat is unaffected; only envelope frames are consumed.
- **Payer** performs one blocking payment of atoms to a peer uid and
  returns nil only on confirmed settlement. The ctx carries the settings'
  payment-wait deadline. Error text reaches the agent verbatim after
  "payment not made:", so word it for humans. Hosts on Bison Relay's tip
  flow correlate their asynchronous tip-progress notifications with
  `TipMatcher`:

      matcher := bridge.NewTipMatcher()
      // From the tip progress notification, terminal events only:
      //   matcher.Resolve(payeeUID, amtMAtoms, errOrNil)
      payer := bridge.PayerFunc(func(ctx context.Context, uid string, atoms int64) error {
          w := matcher.Expect(uid, atoms*1000) // tips are milliatoms
          if err := tipUser(ctx, uid, atoms); err != nil {
              w.Cancel()
              return fmt.Errorf("tip: %w", err)
          }
          select {
          case err := <-w.Done():
              if err != nil {
                  return fmt.Errorf("tip failed: %w", err)
              }
              return nil
          case <-ctx.Done():
              w.Cancel()
              return errors.New("tip not confirmed in time; the attempt keeps " +
                  "running in the background and still credits the bot")
          }
      })

## The endpoint

`/mcp/<bot-uid>` speaks the standard MCP streamable-HTTP transport. Auth is
one bearer token, compared in constant time; an empty token never
authorizes. The uid must be 64-hex and on the bot allowlist; anything else
is 404. With `ListenAddr` set the bridge owns the listener and binds only
while the settings enable it (a token change restarts it, severing streams
authorized under the old token); with `ListenAddr` empty the host mounts
`Handler()` and owns that lifecycle itself, and a disabled bridge answers
404 to everything.

## The spending policy

Persisted as `mcpclient.json` (the `Settings` type) and applied with
`ApplySettings`, which hot-reconciles: the listener starts, stops, or
restarts; sessions of de-listed bots close; enabling with an empty token
mints a random one.

- `allowed_bots` - default-deny allowlist of callable bot uids.
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
