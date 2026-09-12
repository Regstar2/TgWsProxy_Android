# TgWsProxy Android v1.10.14 — release candidate

This release candidate stabilizes the working MTProto Worker transport from the Issue #29 investigation without merging the full experimental commit chain into `main`.

## What is included

- Fresh HTTPS chunk relay for MTProto Worker traffic. The Android/native side sends bounded short HTTPS requests while a Cloudflare Durable Object keeps the corresponding Telegram TCP stream.
- Stabilized upload profile: 12 KiB chunks, sliding window 3, ordered ACKs, bounded retry/backoff, HOL hedge for the oldest unacknowledged upload, and downstream queue backpressure.
- Sticky Worker pool for `ROUND_ROBIN`: a stable opaque MTProto session id selects one primary Worker for the lifetime of that session. Chunks from one session are never split between Worker hosts.
- Quota-aware Worker failover. Worker v7 converts Cloudflare Durable Objects Free Tier duration exhaustion into a recoverable 503 response with reset metadata. Native routing opens a per-domain circuit breaker, skips the exhausted Worker for new sessions, and automatically retries another enabled Worker.
- Short cooldown for unclassified Worker 5xx responses to prevent reconnect storms while still allowing automatic recovery.
- Worker preconnect disabled for the MTProto Worker route and strict preservation of explicit Worker-only route policy.
- Go race tests and Worker handler tests in pull-request CI.

## Device evidence before stabilization PR

The source checkpoint used for this release candidate is `c59f860` from PR #51. On-device testing confirmed that a v7 Worker with exhausted Durable Objects quota was automatically removed from the candidate set and new MTProto sessions failed over to a healthy v7 Worker. The healthy Worker carried bidirectional MTProto traffic and multi-megabyte uploads through the chunk relay.

## Known limitations

- The Worker route is currently slower than direct connectivity and still produces HOL hedges/timeouts under sustained bulk traffic.
- Cloudflare Durable Objects quotas remain an operational constraint; the circuit breaker prevents retry storms but does not increase quota.
- Some relay sessions can still terminate with HTTP 502 and require further error classification and transport tuning.
- The Worker route remains non-default. This PR is a stabilization baseline for further work, not a claim that Issue #29 is fully solved.

## Release gate

Do not merge or publish v1.10.14 until CI passes on the clean stabilization branch and a final Android smoke test confirms normal Telegram messages, media transfer, Worker failover, and no reconnect storm.
