# Cloudflare Worker setup

Cloudflare Worker route is an optional WebSocket upstream route for TgWsProxy Android. The Android app connects to:

```text
wss://<worker-domain>/apiws?dst=<telegram-dc-ip>&dc=<dc-id>&media=<0-or-1>&sid=<session-id>
```

In the app, paste only the Worker hostname, without `https://`, `wss://`, or `/apiws`:

```text
example.username.workers.dev
```

Create your own Worker. Do not use random public Worker domains from other people.

## When To Use

- Use Worker route when Direct WebSocket or CF Proxy is unstable on your network.
- For mobile networks, a practical preset is Mobile policy -> Worker first.
- If Worker diagnostics fail, check the Worker domain first, then try CF Proxy or Direct routes.

## Setup

1. Open [Cloudflare Dashboard](https://dash.cloudflare.com).
2. Go to **Workers & Pages**.
3. Create a Worker.
4. Open **Edit code**.
5. Paste the complete deployable Worker source linked below (also update existing Workers).
6. Deploy the Worker.
7. Copy the hostname, for example `example.username.workers.dev`.
8. In TgWsProxy Android, open **Settings** -> **Cloudflare Worker** and paste the hostname.
9. Run Worker diagnostics or the active route connection test.

## Worker Code

Deployable source: [scripts/cloudflare-worker/worker.js](../../scripts/cloudflare-worker/worker.js)

The Worker uses **lazy TCP connect**: it accepts the WebSocket upgrade immediately, waits for the first client frame, then opens the Telegram TCP socket and writes that frame. This avoids Telegram closing an idle TCP connection opened before the MTProto init packet arrives.

Use the source file above as the single implementation; do not copy an older inline example. Updating the APK does not deploy JavaScript to Cloudflare.

The current source returns `X-Tgws-Worker-Revision: worker-stream-v2`. Android logs it as `worker_revision=worker-stream-v2` after HTTP 101. `unknown` means the response did not identify this revision.

Payload conversion and TCP writes run in arrival order, including asynchronous Blob conversion. The first nonempty message opens TCP. Closing either side cancels the socket immediately, without waiting for a blocked `writer.close()`. The pending outbound queue is bounded to 32 MiB; exceeding it closes the session with `tcp_backlog_limit` instead of silently dropping bytes.

[Cloudflare's socket API](https://developers.cloudflare.com/workers/runtime-apis/tcp-sockets/) defines `socket.close()` as forcibly closing both streams; a stream writer close alone is not the cancellation mechanism.

## Correlating a device stall

1. Set the Worker text variable `WORKER_DIAGNOSTICS` to `1` and deploy.
2. Open Cloudflare live logs before reproducing the problem.
3. Match Worker `sid` with Android `session_id`. For each WS message the Worker logs `ws message received`, `tcp write start` and `tcp write end`, including sequence, byte counts and timings.
4. `tcp read` identifies downstream bytes; `relay close` gives totals and the termination reason. No payload, MTProto keys or secrets are logged.
5. Disable the diagnostic variable after collecting the failing session to reduce log volume.

Android samples TCP state every two seconds during a blocked traced write: `unacked`, `retrans`, `total_retrans`, `rto_us`, `rtt_us`, `cwnd`, `bytes_acked`, `bytes_sent`, `notsent`, `snd_wnd`. Missing kernel counters are `-1`, not zero. The total send queue includes both unsent and unacknowledged bytes. TCP counters describe encrypted transport bytes, while frame-write counts describe bytes supplied to TLS.

A frame write has a 45-second ceiling (or an earlier caller deadline). A partial failed frame terminates the connection: it must not be replayed on the existing encrypted stream. A timeout is a failure observation, not evidence of successful media transfer.

## Destination baseline

Start with `PRESERVE_ORIGINAL_DST`. A transfer's bulk upload may use DC2 `media=false`; the UI category "media" is not the MTProto signed-DC flag.

The reviewed [Flowseal revision](https://github.com/Flowseal/tg-ws-proxy/tree/f200e33fd283143a9f101d62aaf9d8c1468a23fe) does not unconditionally route every Worker session to `149.154.167.220`. Its DC4 map/fallback behavior must not be confused with Android's `EXPERIMENTAL_FORCE_MEDIA_DC4` destination override. Keep that override a separate A/B and preserve the original logical signed DC.


## Worker pool

The current MTProto Flowseal-parity path uses a fresh Worker WebSocket for each session and bypasses Worker preconnect. Pool counters from other routes do not establish whether this path delivered any data.

## Diagnostics

Use **Test active routes** to check only the routes allowed by the effective policy. Worker route is shown as disabled when the current policy disables it, and as not configured when the Worker domain is empty.

Diagnostics and exported reports do not include raw SSID, SIM/operator values, full domains, or full runtime tokens by default.
