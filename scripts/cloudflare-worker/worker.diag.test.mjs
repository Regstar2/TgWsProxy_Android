import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const source = (await readFile(new URL("./worker.js", import.meta.url), "utf8"))
  .replace('import { connect } from "cloudflare:sockets";',
    'const connect = () => { throw new Error("unexpected production connect"); };');
const { createWorkerHandler } = await import(
  "data:text/javascript;base64," + Buffer.from(source).toString("base64")
);

function setup(t) {
  const oldPair = globalThis.WebSocketPair;
  const oldResponse = globalThis.Response;
  const pairs = [];
  class Endpoint {
    listeners = new Map();
    sent = [];
    closes = [];
    accept() {}
    addEventListener(name, handler) { this.listeners.set(name, handler); }
    emit(name, data) { return this.listeners.get(name)?.({ data }); }
    send(data) { this.sent.push(data); }
    close(code, reason) { this.closes.push({ code, reason }); }
  }
  globalThis.WebSocketPair = class {
    constructor() {
      this[0] = new Endpoint();
      this[1] = new Endpoint();
      pairs.push(this);
    }
  };
  globalThis.Response = class {
    constructor(body, options = {}) {
      this.body = body;
      Object.assign(this, options);
      this.headers = new Headers(options.headers);
    }
  };
  t.after(() => {
    globalThis.WebSocketPair = oldPair;
    globalThis.Response = oldResponse;
  });
  const handler = createWorkerHandler(() => {
    throw new Error("diagnostic route must not open cloudflare:sockets");
  });
  return { handler, pairs };
}

async function open(harness, path) {
  const response = await harness.handler.fetch(new Request(
    `https://example.workers.dev${path}`,
    { headers: { Upgrade: "websocket", "Sec-WebSocket-Protocol": "binary" } },
  ));
  assert.equal(response.status, 101);
  assert.equal(response.headers.get("X-Tgws-Worker-Revision"), "worker-network-probe-v1");
  return harness.pairs.at(-1)[1];
}

test("diagnostic echo returns the exact binary message", async (t) => {
  const harness = setup(t);
  const server = await open(harness, "/diag/ws-echo?sid=test-echo");
  const payload = new Uint8Array([1, 2, 3, 4, 5]);
  await server.emit("message", payload);
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(server.sent.length, 1);
  assert.deepEqual([...server.sent[0]], [...payload]);
});

test("diagnostic upload acknowledges cumulative bytes", async (t) => {
  const harness = setup(t);
  const server = await open(harness, "/diag/upload?sid=test-upload");
  server.emit("message", new Uint8Array(4096));
  server.emit("message", new Uint8Array(4096));
  assert.deepEqual(server.sent, ["ack:4096", "ack:8192"]);
});

test("diagnostic download emits requested payload without TCP backend", async (t) => {
  const harness = setup(t);
  const server = await open(harness, "/diag/download?size=65536&sid=test-download");
  assert.equal(server.sent.length, 1);
  assert.equal(server.sent[0].byteLength, 65536);
});
