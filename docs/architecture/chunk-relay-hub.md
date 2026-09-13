# Shared Durable Object relay hub

## Scope

The v1.10.14 MTProto HTTPS chunk relay uses one named Durable Object hub per Worker deployment instead of one Durable Object per MTProto session.

All `/chunk-relay/*` requests are routed through:

```text
CHUNK_RELAY.getByName("relay-hub-v1")
```

The opaque `sid` remains the session key inside the hub. Worker stickiness on the Android/native side is unchanged: one MTProto session still selects one Worker host for its lifetime.

## Session isolation

`ChunkRelayHub` owns a `Map<sid, RelaySession>`. Each `RelaySession` owns its own:

- Telegram TCP socket, reader, and writer;
- upload sequence, reorder window, ACK state, and upload promise chain;
- downstream sequence, pending ACK chunk, queue, and backpressure waiters;
- lifecycle state and cumulative upload/download byte counters.

There is no hub-global upload chain or mutex. A blocked write for one `sid` does not serialize writes for other sessions. Closing a session, an upstream EOF, or a per-session relay failure removes only that session from the map.

The existing Durable Object export name `ChunkRelaySession` is retained as an alias of `ChunkRelayHub`, so the existing `CHUNK_RELAY` binding does not require a Durable Object class-name migration.

## Diagnostics

Chunk relay responses expose:

```text
X-Tgws-Chunk-Relay-Revision: chunk-relay-mtproto-v8
X-Tgws-Relay-Hub-Revision: relay-hub-v1
```

Hub lifecycle logs include:

- active session count;
- total opened sessions;
- explicitly closed sessions;
- sessions reaped after upstream/relay termination;
- per-session cumulative upload/download bytes and sequence counters on termination.

The hub does not log the opaque `sid`, request payloads, or secrets.

The existing `do-quota-exhausted` 503 mapping and reset metadata remain unchanged. Multiplexing reduces the number of simultaneously active Durable Object instances, but the actual Cloudflare duration reduction must be confirmed from Worker account metrics under comparable traffic.

## Verification

The Worker unit tests cover:

- three parallel sessions in one hub;
- absence of a hub-global upload lock;
- duplicate and out-of-order upload ACK semantics;
- per-session target binding and 12 KiB upload limit;
- isolated close/reconnect;
- isolated upstream reap;
- downstream backpressure remaining session-local;
- Durable Object quota-exhaustion mapping.

Throughput and Durable Object duration are deployment properties and require an A/B Worker/device test before Issue #53 can be considered fully accepted.
