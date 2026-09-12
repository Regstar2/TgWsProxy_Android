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
          async write(data) {
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

function up(relay, seq, body) {
  return relay.fetch(new Request(
    `https://example.workers.dev/chunk-relay/up?sid=session_window_123&dst=149.154.167.51&seq=${seq}`,
    { method: "POST", body },
  ));
}

test("out-of-order pipelined uploads are written and acked in sequence", async (t) => {
  const writes = [];
  const oldConnect = globalThis.__chunkRelayConnect;
  globalThis.__chunkRelayConnect = () => makeSocket(writes);
  t.after(() => { globalThis.__chunkRelayConnect = oldConnect; });

  const relay = new mod.ChunkRelaySession({}, {});
  const open = await relay.fetch(new Request(
    "https://example.workers.dev/chunk-relay/open?sid=session_window_123&dst=149.154.167.51",
    { method: "POST" },
  ));
  assert.equal(open.status, 204);

  const secondPromise = up(relay, 2, new Uint8Array([2, 2]));
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(writes.length, 0);

  const firstPromise = up(relay, 1, new Uint8Array([1, 1]));
  const [second, first] = await Promise.all([secondPromise, firstPromise]);

  assert.equal(first.status, 204);
  assert.equal(second.status, 204);
  assert.equal(first.headers.get("X-Tgws-Chunk-Ack"), "1");
  assert.equal(second.headers.get("X-Tgws-Chunk-Ack"), "2");
  assert.equal(writes.length, 2);
  assert.deepEqual([...writes[0]], [1, 1]);
  assert.deepEqual([...writes[1]], [2, 2]);
});
