import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const source = (await readFile(new URL("./chunk-relay-worker.js", import.meta.url), "utf8"))
  .replace('import baseWorker from "./worker.js";', 'const baseWorker = { fetch() { return new Response("base", { status: 200 }); } };')
  .replace('import { connect } from "cloudflare:sockets";', 'const connect = (...args) => globalThis.__chunkRelayConnect(...args);')
  .replace('import { DurableObject } from "cloudflare:workers";', 'class DurableObject { constructor(ctx, env) { this.ctx = ctx; this.env = env; } }');

const mod = await import("data:text/javascript;base64," + Buffer.from(source).toString("base64"));

function makeSocket(writes) {
  return {
    opened: Promise.resolve(),
    writable: {
      getWriter() {
        return {
          async write(data) { writes.push(new Uint8Array(data).slice()); },
        };
      },
    },
    readable: {
      getReader() {
        return { read: () => new Promise(() => {}) };
      },
    },
    async close() {},
  };
}

test("duplicate upload sequence is written only once", async (t) => {
  const writes = [];
  const oldConnect = globalThis.__chunkRelayConnect;
  globalThis.__chunkRelayConnect = ({ hostname }) => {
    assert.equal(hostname, "149.154.167.51");
    return makeSocket(writes);
  };
  t.after(() => { globalThis.__chunkRelayConnect = oldConnect; });

  const relay = new mod.ChunkRelaySession({}, {});
  const open = await relay.fetch(new Request(
    "https://example.workers.dev/chunk-relay/open?sid=session_test_123&dst=149.154.167.51",
    { method: "POST" },
  ));
  assert.equal(open.status, 204);

  const body = new Uint8Array([1, 2, 3, 4]);
  const makeUp = () => relay.fetch(new Request(
    "https://example.workers.dev/chunk-relay/up?sid=session_test_123&dst=149.154.167.51&seq=1",
    { method: "POST", body },
  ));

  const [first, retry] = await Promise.all([makeUp(), makeUp()]);
  assert.equal(first.status, 204);
  assert.equal(retry.status, 204);
  assert.equal(first.headers.get("X-Tgws-Chunk-Ack"), "1");
  assert.equal(retry.headers.get("X-Tgws-Chunk-Ack"), "1");
  assert.equal(writes.length, 1);
  assert.deepEqual([...writes[0]], [...body]);
});

test("relay binds a session to one target", async (t) => {
  const writes = [];
  const oldConnect = globalThis.__chunkRelayConnect;
  globalThis.__chunkRelayConnect = () => makeSocket(writes);
  t.after(() => { globalThis.__chunkRelayConnect = oldConnect; });

  const relay = new mod.ChunkRelaySession({}, {});
  const first = await relay.fetch(new Request(
    "https://example.workers.dev/chunk-relay/open?sid=session_test_456&dst=149.154.167.51",
    { method: "POST" },
  ));
  assert.equal(first.status, 204);

  const second = await relay.fetch(new Request(
    "https://example.workers.dev/chunk-relay/open?sid=session_test_456&dst=149.154.167.91",
    { method: "POST" },
  ));
  assert.equal(second.status, 502);
});

test("relay accepts 12 KiB upload chunks and rejects larger bodies", async (t) => {
  const writes = [];
  const oldConnect = globalThis.__chunkRelayConnect;
  globalThis.__chunkRelayConnect = () => makeSocket(writes);
  t.after(() => { globalThis.__chunkRelayConnect = oldConnect; });

  const relay = new mod.ChunkRelaySession({}, {});
  const open = await relay.fetch(new Request(
    "https://example.workers.dev/chunk-relay/open?sid=session_size_123&dst=149.154.167.51",
    { method: "POST" },
  ));
  assert.equal(open.status, 204);

  const ok = await relay.fetch(new Request(
    "https://example.workers.dev/chunk-relay/up?sid=session_size_123&dst=149.154.167.51&seq=1",
    { method: "POST", body: new Uint8Array(12 * 1024) },
  ));
  assert.equal(ok.status, 204);
  assert.equal(ok.headers.get("X-Tgws-Chunk-Ack"), "1");
  assert.equal(writes.length, 1);
  assert.equal(writes[0].byteLength, 12 * 1024);

  const tooLarge = await relay.fetch(new Request(
    "https://example.workers.dev/chunk-relay/up?sid=session_size_123&dst=149.154.167.51&seq=2",
    { method: "POST", body: new Uint8Array(12 * 1024 + 1) },
  ));
  assert.equal(tooLarge.status, 413);
  assert.equal(writes.length, 1);
});

test("backpressure waits when the next downstream chunk would exceed the queue limit", async () => {
  const relay = new mod.ChunkRelaySession({}, {});
  relay.queueBytes = 2 * 1024 * 1024 - 1;

  let resolved = false;
  const wait = relay.waitForDrain(2).then(() => { resolved = true; });
  await Promise.resolve();
  assert.equal(resolved, false);

  relay.queueBytes -= 2;
  relay.wakeDrain();
  await wait;
  assert.equal(resolved, true);
});

test("top-level worker maps Durable Object free-tier duration exhaustion to a recoverable response", async () => {
  const env = {
    CHUNK_RELAY: {
      getByName() {
        return {
          async fetch() {
            throw new Error("Exceeded allowed duration in Durable Objects free tier.");
          },
        };
      },
    },
  };

  const response = await mod.default.fetch(new Request(
    "https://example.workers.dev/chunk-relay/open?sid=session_quota_123&dst=149.154.167.51",
    { method: "POST" },
  ), env, {});

  assert.equal(response.status, 503);
  assert.equal(response.headers.get("X-Tgws-Worker-State"), "do-quota-exhausted");
  assert.equal(response.headers.get("X-Tgws-Chunk-Relay-Revision"), "chunk-relay-mtproto-v7");
  assert.ok(Number.parseInt(response.headers.get("Retry-After") || "0", 10) >= 60);
  assert.ok(!Number.isNaN(Date.parse(response.headers.get("X-Tgws-Quota-Reset") || "")));
});

test("top-level worker does not hide unrelated Durable Object exceptions", async () => {
  const env = {
    CHUNK_RELAY: {
      getByName() {
        return {
          async fetch() {
            throw new Error("unexpected durable object failure");
          },
        };
      },
    },
  };

  await assert.rejects(
    () => mod.default.fetch(new Request(
      "https://example.workers.dev/chunk-relay/open?sid=session_error_123&dst=149.154.167.51",
      { method: "POST" },
    ), env, {}),
    /unexpected durable object failure/,
  );
});
