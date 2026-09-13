import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

// Load the deployed file itself; replace only the unavailable runtime import.
const source = (await readFile(new URL("./worker.js", import.meta.url), "utf8"))
  .replace('import { connect } from "cloudflare:sockets";',
    'const connect = () => { throw new Error("unexpected production connect"); };');
const { createWorkerHandler } = await import(
  "data:text/javascript;base64," + Buffer.from(source).toString("base64")
);
const nextTurn = () => new Promise((resolve) => setImmediate(resolve));
async function until(predicate) {
  for (let i = 0; i < 2000; i++) {
    if (predicate()) return;
    await nextTurn();
  }
  assert.fail("relay did not reach the expected state");
}

function setup(t, write = async () => {}) {
  const oldPair = globalThis.WebSocketPair;
  const oldResponse = globalThis.Response;
  const pairs = [];
  const sockets = [];
  const logs = [];
  t.mock.method(console, "log", (event, fields) => logs.push({ event, ...fields }));
  class Endpoint {
    listeners = new Map();
    sent = [];
    closes = [];
    accept() {}
    addEventListener(name, handler) { this.listeners.set(name, handler); }
    emit(name, data) { this.listeners.get(name)?.({ data }); }
    send(data) { this.sent.push(new Uint8Array(data).slice()); }
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
    for (const socket of sockets) socket.close();
    globalThis.WebSocketPair = oldPair;
    globalThis.Response = oldResponse;
  });
  const handler = createWorkerHandler((address, options) => {
    let controller;
    let resolveClosed;
    const socket = {
      address, options, closeCount: 0,
      opened: Promise.resolve(),
      closed: new Promise((resolve) => { resolveClosed = resolve; }),
      readable: new ReadableStream({ start(value) { controller = value; } }),
      writable: new WritableStream({ write }),
      close() {
        this.closeCount++;
        if (this.closeCount === 1) { controller.close(); resolveClosed(); }
        return Promise.resolve();
      },
      receive(data) { controller.enqueue(data); },
    };
    sockets.push(socket);
    return socket;
  });
  return {
    pairs, sockets, logs,
    async open() {
      const response = await handler.fetch(new Request(
        "https://example.workers.dev/apiws?dst=149.154.167.51&dc=2&media=0&sid=test",
        { headers: { Upgrade: "websocket", "Sec-WebSocket-Protocol": "binary" } },
      ), { WORKER_DIAGNOSTICS: "1" });
      assert.equal(response.status, 101);
      assert.equal(response.headers.get("X-Tgws-Worker-Revision"), "worker-stream-v2");
      return pairs.at(-1)[1];
    },
  };
}

test("TCP opens lazily and asynchronous conversions preserve message order", async (t) => {
  const writes = [];
  const harness = setup(t, async (chunk) => { writes.push([...chunk]); });
  const server = await harness.open();
  assert.equal(harness.sockets.length, 0);
  let release;
  server.emit("message", {
    size: 3,
    arrayBuffer: () => new Promise((resolve) => { release = resolve; }),
  });
  server.emit("message", new Uint8Array([4, 5]));
  await until(() => release);
  assert.equal(harness.sockets.length, 0);
  release(new Uint8Array([1, 2, 3]).buffer);
  await until(() => writes.length === 2);
  assert.deepEqual(writes, [[1, 2, 3], [4, 5]]);
  assert.deepEqual(harness.sockets[0].address, { hostname: "149.154.167.51", port: 443 });
});

test("40 x 64-KiB messages cross a slow TCP writer and downstream returns", async (t) => {
  const writes = [];
  const harness = setup(t, async (chunk) => {
    await nextTurn();
    writes.push(chunk.slice());
  });
  const server = await harness.open();
  for (let i = 0; i < 40; i++) server.emit("message", new Uint8Array(65536).fill(i));
  await until(() => harness.logs.filter((line) => line.event === "tcp write end").length === 40);
  assert.equal(writes.reduce((sum, chunk) => sum + chunk.length, 0), 2621440);
  for (let i = 0; i < 40; i++) assert.ok(writes[i].every((value) => value === i));
  harness.sockets[0].receive(new Uint8Array([91, 92, 93]));
  await until(() => server.sent.length === 1);
  assert.deepEqual([...server.sent[0]], [91, 92, 93]);
  assert.equal(server.closes.length, 0);
  assert.equal(harness.logs.filter((line) => line.event === "ws message received").length, 40);
});

test("WebSocket close cancels TCP without waiting for a pending write", async (t) => {
  let rejectWrite;
  const harness = setup(t, () => new Promise((_, reject) => { rejectWrite = reject; }));
  const server = await harness.open();
  server.emit("message", new Uint8Array(65536));
  await until(() => rejectWrite);
  server.emit("close");
  assert.equal(harness.sockets[0].closeCount, 1);
  assert.equal(server.closes[0].reason, "ws_closed");
  rejectWrite(new Error("socket closed"));
  await nextTurn();
  assert.equal(harness.sockets[0].closeCount, 1);
});

test("a failed TCP write terminates the stream without replaying queued data", async (t) => {
  let writes = 0;
  const harness = setup(t, async () => { writes++; throw new Error("broken pipe"); });
  const server = await harness.open();
  server.emit("message", new Uint8Array([1]));
  server.emit("message", new Uint8Array([2]));
  await until(() => server.closes.length === 1);
  await nextTurn();
  assert.equal(writes, 1);
  assert.equal(server.closes[0].reason, "ws_to_tcp_failed");
});

test("an oversized pending backlog fails explicitly before opening TCP", async (t) => {
  const harness = setup(t);
  const server = await harness.open();
  server.emit("message", new Uint8Array(32 * 1024 * 1024 + 1));
  assert.equal(server.closes[0].reason, "tcp_backlog_limit");
  assert.equal(harness.sockets.length, 0);
});
