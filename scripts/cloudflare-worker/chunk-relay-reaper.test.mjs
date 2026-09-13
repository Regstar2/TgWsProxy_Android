import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const source = (await readFile(new URL("./chunk-relay-worker.js", import.meta.url), "utf8"))
  .replace('import baseWorker from "./worker.js";', 'const baseWorker = { fetch() { return new Response("base", { status: 200 }); } };')
  .replace('import { connect } from "cloudflare:sockets";', 'const connect = (...args) => globalThis.__chunkRelayConnect(...args);')
  .replace('import { DurableObject } from "cloudflare:workers";', 'class DurableObject { constructor(ctx, env) { this.ctx = ctx; this.env = env; } }');

const mod = await import("data:text/javascript;base64," + Buffer.from(source).toString("base64"));

function createStorage() {
  return {
    scheduled: [],
    async setAlarm(timestamp) {
      this.scheduled.push(timestamp);
    },
  };
}

function makeTrackedSocket() {
  let settleRead = null;
  const state = {
    closeCount: 0,
    cancelCount: 0,
    abortCount: 0,
  };
  const socket = {
    opened: Promise.resolve(),
    writable: {
      getWriter() {
        return {
          async write() {},
          async abort() { state.abortCount += 1; },
          releaseLock() {},
        };
      },
    },
    readable: {
      getReader() {
        return {
          read() {
            return new Promise((resolve) => { settleRead = resolve; });
          },
          async cancel() {
            state.cancelCount += 1;
            settleRead?.({ value: undefined, done: true });
          },
          releaseLock() {},
        };
      },
    },
    async close() { state.closeCount += 1; },
  };
  return { socket, state };
}

function relayRequest(action, sid, { dst = "149.154.167.51", seq = null, ack = null, body = null } = {}) {
  const url = new URL(`https://example.workers.dev/chunk-relay/${action}`);
  url.searchParams.set("sid", sid);
  if (dst) url.searchParams.set("dst", dst);
  if (seq !== null) url.searchParams.set("seq", String(seq));
  if (ack !== null) url.searchParams.set("ack", String(ack));
  if (action === "down") url.searchParams.set("wait", "0");
  const method = action === "down" ? "GET" : "POST";
  return new Request(url, { method, body: method === "POST" ? body : null });
}

test("orphaned session is reaped after 60 seconds without client relay touches", async (t) => {
  const oldConnect = globalThis.__chunkRelayConnect;
  const tracked = makeTrackedSocket();
  globalThis.__chunkRelayConnect = () => tracked.socket;
  t.after(() => { globalThis.__chunkRelayConnect = oldConnect; });

  let now = 1_000_000;
  const storage = createStorage();
  const relay = new mod.ChunkRelayHub({ storage }, {});
  relay.now = () => now;
  const sid = "session_orphan_001";

  assert.equal((await relay.fetch(relayRequest("open", sid))).status, 204);
  assert.equal(relay.sessions.has(sid), true);
  assert.deepEqual(storage.scheduled, [now + 60_000]);

  now += 60_001;
  await relay.alarm();

  assert.equal(relay.sessions.has(sid), false);
  assert.equal(relay.reapedSessions, 1);
  assert.equal(tracked.state.cancelCount, 1);
  assert.equal(tracked.state.abortCount, 1);
  assert.equal(tracked.state.closeCount, 1);
});

test("idle Telegram session stays alive while down polling keeps touching the relay", async (t) => {
  const oldConnect = globalThis.__chunkRelayConnect;
  const tracked = makeTrackedSocket();
  globalThis.__chunkRelayConnect = () => tracked.socket;
  t.after(() => { globalThis.__chunkRelayConnect = oldConnect; });

  let now = 2_000_000;
  const storage = createStorage();
  const relay = new mod.ChunkRelayHub({ storage }, {});
  relay.now = () => now;
  const sid = "session_idle_poll_01";

  assert.equal((await relay.fetch(relayRequest("open", sid))).status, 204);
  now += 30_000;
  assert.equal((await relay.fetch(relayRequest("down", sid, { ack: 0 }))).status, 204);
  now += 29_000;
  assert.equal((await relay.fetch(relayRequest("down", sid, { ack: 0 }))).status, 204);

  now += 1_001;
  await relay.alarm();

  const session = relay.sessions.get(sid);
  assert.ok(session);
  assert.equal(session.lastPayloadActivity, 0);
  assert.equal(tracked.state.closeCount, 0);
  assert.equal(relay.reapedSessions, 0);
  assert.equal(storage.scheduled.at(-1), 2_119_000);
});

test("concurrent client close and orphan reaper clean one session exactly once", async (t) => {
  const oldConnect = globalThis.__chunkRelayConnect;
  const tracked = makeTrackedSocket();
  globalThis.__chunkRelayConnect = () => tracked.socket;
  t.after(() => { globalThis.__chunkRelayConnect = oldConnect; });

  let now = 3_000_000;
  const relay = new mod.ChunkRelayHub({ storage: createStorage() }, {});
  relay.now = () => now;
  const sid = "session_close_race";

  assert.equal((await relay.fetch(relayRequest("open", sid))).status, 204);
  now += 60_001;

  const [, closeResponse] = await Promise.all([
    relay.alarm(),
    relay.fetch(relayRequest("close", sid)),
  ]);

  assert.equal(closeResponse.status, 204);
  assert.equal(relay.sessions.has(sid), false);
  assert.equal(relay.closedSessions + relay.reapedSessions, 1);
  assert.equal(tracked.state.cancelCount, 1);
  assert.equal(tracked.state.abortCount, 1);
  assert.equal(tracked.state.closeCount, 1);
});

test("orphan cleanup unblocks upload and downstream waiters and a late touch reconnects cleanly", async (t) => {
  const oldConnect = globalThis.__chunkRelayConnect;
  const sockets = [];
  globalThis.__chunkRelayConnect = () => {
    const tracked = makeTrackedSocket();
    sockets.push(tracked);
    return tracked.socket;
  };
  t.after(() => { globalThis.__chunkRelayConnect = oldConnect; });

  let now = 4_000_000;
  const relay = new mod.ChunkRelayHub({ storage: createStorage() }, {});
  relay.now = () => now;
  const sid = "session_waiters_001";

  assert.equal((await relay.fetch(relayRequest("open", sid))).status, 204);
  const oldSession = relay.sessions.get(sid);
  oldSession.queueBytes = 2 * 1024 * 1024;

  let rejectUpload;
  const pendingUpload = new Promise((resolve, reject) => { rejectUpload = reject; });
  pendingUpload.catch(() => {});
  oldSession.upPending.set(1, {
    data: new Uint8Array([1]),
    done: { promise: pendingUpload, resolve() {}, reject: rejectUpload },
  });
  const uploadRejected = assert.rejects(pendingUpload, /orphan_timeout/);
  const drainWaiter = oldSession.waitForDrain(1);
  const downWaiter = oldSession.wait(6000);

  now += 60_001;
  const reopened = await relay.fetch(relayRequest("open", sid));
  await Promise.all([uploadRejected, drainWaiter, downWaiter]);

  assert.equal(reopened.status, 204);
  assert.notEqual(relay.sessions.get(sid), oldSession);
  assert.equal(sockets.length, 2);
  assert.equal(sockets[0].state.closeCount, 1);
  assert.equal(sockets[1].state.closeCount, 0);
});
