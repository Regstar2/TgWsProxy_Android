import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

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

class Endpoint {
  listeners = new Map();
  sent = [];
  closes = [];
  accepted = false;

  accept() { this.accepted = true; }
  addEventListener(name, handler) { this.listeners.set(name, handler); }
  emit(name, data) { this.listeners.get(name)?.({ data }); }
  send(data) { this.sent.push(new Uint8Array(data).slice()); }
  close(code, reason) { this.closes.push({ code, reason }); }
}

function setup(t, fetchImpl) {
  const oldPair = globalThis.WebSocketPair;
  const oldResponse = globalThis.Response;
  const pairs = [];
  const logs = [];
  t.mock.method(console, "log", (event, fields) => logs.push({ event, ...fields }));

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

  const tcpConnect = () => { throw new Error("v3 must not use cloudflare:sockets"); };
  const handler = createWorkerHandler(tcpConnect, fetchImpl);
  return {
    pairs,
    logs,
    async open(path = "/apiws-ws?dst=149.154.167.51&dc=2&media=0&sid=v3-test") {
      const response = await handler.fetch(new Request(
        `https://example.workers.dev${path}`,
        { headers: { Upgrade: "websocket", "Sec-WebSocket-Protocol": "binary" } },
      ), { WORKER_DIAGNOSTICS: "1" });
      return { response, server: pairs.at(-1)?.[1] };
    },
  };
}

function upgraded(upstream) {
  return { status: 101, webSocket: upstream };
}

test("v3 opens Telegram WSS lazily and preserves outer message boundaries", async (t) => {
  const upstream = new Endpoint();
  const calls = [];
  const harness = setup(t, async (url, options) => {
    calls.push({ url, options });
    return upgraded(upstream);
  });
  const { response, server } = await harness.open();

  assert.equal(response.status, 101);
  assert.equal(response.headers.get("X-Tgws-Worker-Revision"), "worker-wss-relay-v3");
  assert.equal(calls.length, 0, "Telegram WSS must stay lazy until relay_init");

  server.emit("message", new Uint8Array([1, 2, 3]));
  server.emit("message", new Uint8Array([4, 5]));
  await until(() => upstream.sent.length === 2);

  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "https://kws2.web.telegram.org/apiws");
  assert.equal(calls[0].options.headers.Upgrade, "websocket");
  assert.equal(calls[0].options.headers["Sec-WebSocket-Protocol"], "binary");
  assert.deepEqual(upstream.sent.map((value) => [...value]), [[1, 2, 3], [4, 5]]);
});

test("v3 keeps asynchronous client payload conversion ordered and relays downstream", async (t) => {
  const upstream = new Endpoint();
  const harness = setup(t, async () => upgraded(upstream));
  const { server } = await harness.open();

  let release;
  server.emit("message", {
    size: 3,
    arrayBuffer: () => new Promise((resolve) => { release = resolve; }),
  });
  server.emit("message", new Uint8Array([4, 5]));
  await until(() => release);
  assert.equal(upstream.sent.length, 0);
  release(new Uint8Array([1, 2, 3]).buffer);
  await until(() => upstream.sent.length === 2);
  assert.deepEqual(upstream.sent.map((value) => [...value]), [[1, 2, 3], [4, 5]]);

  upstream.emit("message", new Uint8Array([9, 8, 7]));
  await until(() => server.sent.length === 1);
  assert.deepEqual([...server.sent[0]], [9, 8, 7]);
});

test("v3 media route prefers kws*-1 and fails over to the secondary Telegram WSS host", async (t) => {
  const upstream = new Endpoint();
  const calls = [];
  const harness = setup(t, async (url) => {
    calls.push(url);
    if (calls.length === 1) return { status: 503, webSocket: null };
    return upgraded(upstream);
  });
  const { server } = await harness.open(
    "/apiws-ws?dst=149.154.167.51&dc=2&media=1&sid=media-v3",
  );

  server.emit("message", new Uint8Array([1]));
  await until(() => upstream.sent.length === 1);
  assert.deepEqual(calls, [
    "https://kws2-1.web.telegram.org/apiws",
    "https://kws2.web.telegram.org/apiws",
  ]);
});

test("v3 validates DC and aliases DC203 to Telegram DC2 WebSocket hosts", async (t) => {
  const upstream = new Endpoint();
  const calls = [];
  const harness = setup(t, async (url) => {
    calls.push(url);
    return upgraded(upstream);
  });

  const invalid = await harness.open("/apiws-ws?dc=999&media=0");
  assert.equal(invalid.response.status, 400);

  const valid = await harness.open("/apiws-ws?dc=203&media=0&sid=dc203");
  assert.equal(valid.response.status, 101);
  valid.server.emit("message", new Uint8Array([7]));
  await until(() => upstream.sent.length === 1);
  assert.equal(calls[0], "https://kws2.web.telegram.org/apiws");
});
