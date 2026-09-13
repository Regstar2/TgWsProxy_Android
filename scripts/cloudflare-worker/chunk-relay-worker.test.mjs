import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const source = (await readFile(new URL("./chunk-relay-worker.js", import.meta.url), "utf8"))
  .replace('import baseWorker from "./worker.js";', 'const baseWorker = { fetch() { return new Response("base", { status: 200 }); } };')
  .replace('import { connect } from "cloudflare:sockets";', 'const connect = (...args) => globalThis.__chunkRelayConnect(...args);')
  .replace('import { DurableObject } from "cloudflare:workers";', 'class DurableObject { constructor(ctx, env) { this.ctx = ctx; this.env = env; } }');

const mod = await import("data:text/javascript;base64," + Buffer.from(source).toString("base64"));

function makeSocket(writes, beforeWrite = null) {
  return {
    opened: Promise.resolve(),
    writable: {
      getWriter() {
        return {
          async write(data) {
            if (beforeWrite) await beforeWrite(data);
            writes.push(new Uint8Array(data).slice());
          },
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

function relayRequest(action, sid, { dst = "149.154.167.51", seq = null, ack = null, body = null } = {}) {
  const url = new URL(`https://example.workers.dev/chunk-relay/${action}`);
  url.searchParams.set("sid", sid);
  if (dst) url.searchParams.set("dst", dst);
  if (seq !== null) url.searchParams.set("seq", String(seq));
  if (ack !== null) url.searchParams.set("ack", String(ack));
  const method = action === "down" ? "GET" : "POST";
  return new Request(url, { method, body: method === "POST" ? body : null });
}

test("duplicate upload sequence is written only once", async (t) => {
  const writes = [];
  const oldConnect = globalThis.__chunkRelayConnect;
  globalThis.__chunkRelayConnect = ({ hostname }) => {
    assert.equal(hostname, "149.154.167.51");
    return makeSocket(writes);
  };
  t.after(() => { globalThis.__chunkRelayConnect = oldConnect; });

  const relay = new mod.ChunkRelayHub({}, {});
  const sid = "session_test_123";
  const open = await relay.fetch(relayRequest("open", sid));
  assert.equal(open.status, 204);

  const body = new Uint8Array([1, 2, 3, 4]);
  const makeUp = () => relay.fetch(relayRequest("up", sid, { seq: 1, body }));

  const [first, retry] = await Promise.all([makeUp(), makeUp()]);
  assert.equal(first.status, 204);
  assert.equal(retry.status, 204);
  assert.equal(first.headers.get("X-Tgws-Chunk-Ack"), "1");
  assert.equal(retry.headers.get("X-Tgws-Chunk-Ack"), "1");
  assert.equal(writes.length, 1);
  assert.deepEqual([...writes[0]], [...body]);
});

test("relay binds one sid to one target", async (t) => {
  const writes = [];
  const oldConnect = globalThis.__chunkRelayConnect;
  globalThis.__chunkRelayConnect = () => makeSocket(writes);
  t.after(() => { globalThis.__chunkRelayConnect = oldConnect; });

  const relay = new mod.ChunkRelayHub({}, {});
  const sid = "session_test_456";
  const first = await relay.fetch(relayRequest("open", sid, { dst: "149.154.167.51" }));
  assert.equal(first.status, 204);

  const second = await relay.fetch(relayRequest("open", sid, { dst: "149.154.167.91" }));
  assert.equal(second.status, 502);
});

test("relay accepts 12 KiB upload chunks and rejects larger bodies", async (t) => {
  const writes = [];
  const oldConnect = globalThis.__chunkRelayConnect;
  globalThis.__chunkRelayConnect = () => makeSocket(writes);
  t.after(() => { globalThis.__chunkRelayConnect = oldConnect; });

  const relay = new mod.ChunkRelayHub({}, {});
  const sid = "session_size_123";
  const open = await relay.fetch(relayRequest("open", sid));
  assert.equal(open.status, 204);

  const ok = await relay.fetch(relayRequest("up", sid, {
    seq: 1,
    body: new Uint8Array(12 * 1024),
  }));
  assert.equal(ok.status, 204);
  assert.equal(ok.headers.get("X-Tgws-Chunk-Ack"), "1");
  assert.equal(writes.length, 1);
  assert.equal(writes[0].byteLength, 12 * 1024);

  const tooLarge = await relay.fetch(relayRequest("up", sid, {
    seq: 2,
    body: new Uint8Array(12 * 1024 + 1),
  }));
  assert.equal(tooLarge.status, 413);
  assert.equal(writes.length, 1);
});

test("backpressure remains isolated inside one relay session", async (t) => {
  const oldConnect = globalThis.__chunkRelayConnect;
  globalThis.__chunkRelayConnect = () => makeSocket([]);
  t.after(() => { globalThis.__chunkRelayConnect = oldConnect; });

  const relay = new mod.ChunkRelayHub({}, {});
  const sid = "session_backpressure_123";
  assert.equal((await relay.fetch(relayRequest("open", sid))).status, 204);
  const session = relay.sessions.get(sid);
  session.queueBytes = 2 * 1024 * 1024 - 1;

  let resolved = false;
  const wait = session.waitForDrain(2).then(() => { resolved = true; });
  await Promise.resolve();
  assert.equal(resolved, false);

  session.queueBytes -= 2;
  session.wakeDrain();
  await wait;
  assert.equal(resolved, true);
});

test("top-level worker routes different sids through one named Durable Object hub", async () => {
  const names = [];
  const env = {
    CHUNK_RELAY: {
      getByName(name) {
        names.push(name);
        return { fetch: async () => new Response(null, { status: 204 }) };
      },
    },
  };

  for (const sid of ["session_hub_001", "session_hub_002", "session_hub_003"]) {
    const response = await mod.default.fetch(relayRequest("open", sid), env, {});
    assert.equal(response.status, 204);
  }

  assert.deepEqual(names, ["relay-hub-v1", "relay-hub-v1", "relay-hub-v1"]);
});

test("three parallel sessions do not share an upload lock", async (t) => {
  const oldConnect = globalThis.__chunkRelayConnect;
  const writesByHost = new Map();
  let releaseSlowWrite;
  let markSlowWriteStarted;
  const slowWriteStarted = new Promise((resolve) => { markSlowWriteStarted = resolve; });
  const slowWriteGate = new Promise((resolve) => { releaseSlowWrite = resolve; });

  globalThis.__chunkRelayConnect = ({ hostname }) => {
    const writes = [];
    writesByHost.set(hostname, writes);
    const beforeWrite = hostname === "149.154.167.51"
      ? async () => { markSlowWriteStarted(); await slowWriteGate; }
      : null;
    return makeSocket(writes, beforeWrite);
  };
  t.after(() => { globalThis.__chunkRelayConnect = oldConnect; });

  const relay = new mod.ChunkRelayHub({}, {});
  const sessions = [
    ["session_parallel_1", "149.154.167.51"],
    ["session_parallel_2", "149.154.167.91"],
    ["session_parallel_3", "149.154.167.92"],
  ];
  await Promise.all(sessions.map(([sid, dst]) => relay.fetch(relayRequest("open", sid, { dst }))));
  assert.equal(relay.sessions.size, 3);

  const slow = relay.fetch(relayRequest("up", sessions[0][0], {
    dst: sessions[0][1],
    seq: 1,
    body: new Uint8Array([1]),
  }));
  await slowWriteStarted;

  const fast = Promise.all(sessions.slice(1).map(([sid, dst], index) => relay.fetch(relayRequest("up", sid, {
    dst,
    seq: 1,
    body: new Uint8Array([index + 2]),
  }))));
  const fastCompleted = await Promise.race([
    fast.then(() => true),
    new Promise((resolve) => setTimeout(() => resolve(false), 100)),
  ]);
  assert.equal(fastCompleted, true);

  releaseSlowWrite();
  const slowResponse = await slow;
  assert.equal(slowResponse.status, 204);
  assert.equal(writesByHost.get("149.154.167.51").length, 1);
  assert.equal(writesByHost.get("149.154.167.91").length, 1);
  assert.equal(writesByHost.get("149.154.167.92").length, 1);
});

test("closing one sid does not disturb another and the sid can reconnect", async (t) => {
  const oldConnect = globalThis.__chunkRelayConnect;
  const socketsByHost = new Map();
  globalThis.__chunkRelayConnect = ({ hostname }) => {
    const writes = [];
    const sockets = socketsByHost.get(hostname) || [];
    sockets.push(writes);
    socketsByHost.set(hostname, sockets);
    return makeSocket(writes);
  };
  t.after(() => { globalThis.__chunkRelayConnect = oldConnect; });

  const relay = new mod.ChunkRelayHub({}, {});
  const sidA = "session_isolated_A";
  const sidB = "session_isolated_B";
  await relay.fetch(relayRequest("open", sidA, { dst: "149.154.167.51" }));
  await relay.fetch(relayRequest("open", sidB, { dst: "149.154.167.91" }));
  assert.equal(relay.sessions.size, 2);

  const closed = await relay.fetch(relayRequest("close", sidA, { dst: "149.154.167.51" }));
  assert.equal(closed.status, 204);
  assert.equal(relay.sessions.size, 1);

  const bUpload = await relay.fetch(relayRequest("up", sidB, {
    dst: "149.154.167.91",
    seq: 1,
    body: new Uint8Array([9]),
  }));
  assert.equal(bUpload.status, 204);

  const reopened = await relay.fetch(relayRequest("open", sidA, { dst: "149.154.167.51" }));
  assert.equal(reopened.status, 204);
  const aUpload = await relay.fetch(relayRequest("up", sidA, {
    dst: "149.154.167.51",
    seq: 1,
    body: new Uint8Array([7]),
  }));
  assert.equal(aUpload.status, 204);
  assert.equal(socketsByHost.get("149.154.167.51").length, 2);
  assert.equal(socketsByHost.get("149.154.167.91")[0].length, 1);
});

test("upstream close reaps only the affected session", async (t) => {
  const oldConnect = globalThis.__chunkRelayConnect;
  globalThis.__chunkRelayConnect = ({ hostname }) => {
    if (hostname === "149.154.167.51") {
      return {
        opened: Promise.resolve(),
        writable: { getWriter() { return { async write() {} }; } },
        readable: { getReader() { return { async read() { return { value: undefined, done: true }; } }; } },
        async close() {},
      };
    }
    return makeSocket([]);
  };
  t.after(() => { globalThis.__chunkRelayConnect = oldConnect; });

  const relay = new mod.ChunkRelayHub({}, {});
  const reapedSid = "session_reaped_01";
  const healthySid = "session_reaped_02";
  assert.equal((await relay.fetch(relayRequest("open", healthySid, { dst: "149.154.167.91" }))).status, 204);
  assert.equal((await relay.fetch(relayRequest("open", reapedSid, { dst: "149.154.167.51" }))).status, 204);
  await new Promise((resolve) => setTimeout(resolve, 0));

  assert.equal(relay.sessions.has(reapedSid), false);
  assert.equal(relay.sessions.has(healthySid), true);
  assert.equal(relay.reapedSessions, 1);
  assert.equal(relay.closedSessions, 0);
});

test("top-level worker maps Durable Object free-tier duration exhaustion to a recoverable response", async () => {
  const names = [];
  const env = {
    CHUNK_RELAY: {
      getByName(name) {
        names.push(name);
        return {
          async fetch() {
            throw new Error("Exceeded allowed duration in Durable Objects free tier.");
          },
        };
      },
    },
  };

  const response = await mod.default.fetch(relayRequest("open", "session_quota_123"), env, {});

  assert.equal(response.status, 503);
  assert.equal(response.headers.get("X-Tgws-Worker-State"), "do-quota-exhausted");
  assert.equal(response.headers.get("X-Tgws-Chunk-Relay-Revision"), "chunk-relay-mtproto-v8");
  assert.equal(response.headers.get("X-Tgws-Relay-Hub-Revision"), "relay-hub-v1");
  assert.deepEqual(names, ["relay-hub-v1"]);
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
    () => mod.default.fetch(relayRequest("open", "session_error_123"), env, {}),
    /unexpected durable object failure/,
  );
});
