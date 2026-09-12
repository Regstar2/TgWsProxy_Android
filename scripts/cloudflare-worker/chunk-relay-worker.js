import baseWorker from "./worker.js";
import { connect } from "cloudflare:sockets";
import { DurableObject } from "cloudflare:workers";

const REVISION = "chunk-relay-mtproto-v4";
const RELAY_MAX_CHUNK_BYTES = 8 * 1024;
const DIAG_MAX_CHUNK_BYTES = 12 * 1024;
const MAX_QUEUE_BYTES = 2 * 1024 * 1024;
const MAX_POLL_WAIT_MS = 6000;

function headers(extra = {}) {
  return { "Cache-Control": "no-store", "X-Tgws-Chunk-Relay-Revision": REVISION, ...extra };
}

function randomBytes(size) {
  const bytes = new Uint8Array(size);
  crypto.getRandomValues(bytes);
  return bytes;
}

function validTarget(value) {
  const target = (value || "").trim();
  return target.length >= 3 && target.length <= 64 && /^[0-9A-Fa-f:.]+$/.test(target);
}

export class ChunkRelaySession extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.socket = null;
    this.writer = null;
    this.reader = null;
    this.target = "";
    this.openPromise = null;
    this.upChain = Promise.resolve();
    this.upSeq = 0;
    this.upBytes = 0;
    this.downSeq = 0;
    this.downBytes = 0;
    this.pending = null;
    this.queue = [];
    this.queueBytes = 0;
    this.waiters = new Set();
    this.drainWaiters = new Set();
    this.closed = false;
  }

  wake() {
    for (const resolve of this.waiters) resolve();
    this.waiters.clear();
  }

  wakeDrain() {
    for (const resolve of this.drainWaiters) resolve();
    this.drainWaiters.clear();
  }

  async waitForDrain() {
    if (this.queueBytes < MAX_QUEUE_BYTES || this.closed) return;
    await new Promise((resolve) => this.drainWaiters.add(resolve));
  }

  async ensureSocket(targetRaw) {
    const target = (targetRaw || "").trim();
    if (!validTarget(target)) throw new Error("invalid_target");
    if (this.closed) throw new Error("session_closed");
    if (this.target && this.target !== target) throw new Error("target_mismatch");
    if (this.socket) return;
    if (this.openPromise) return this.openPromise;

    this.openPromise = (async () => {
      const socket = connect({ hostname: target, port: 443 }, { secureTransport: "off", allowHalfOpen: true });
      await socket.opened;
      if (this.closed) {
        try { await socket.close(); } catch {}
        throw new Error("session_closed");
      }
      this.target = target;
      this.socket = socket;
      this.writer = socket.writable.getWriter();
      this.reader = socket.readable.getReader();
      console.log("chunk relay opened", { revision: REVISION, target });
      this.pump().catch((error) => {
        console.log("chunk relay pump failed", { revision: REVISION, target, error: String(error) });
        this.closed = true;
        this.wake();
        this.wakeDrain();
      });
    })();

    try {
      await this.openPromise;
    } finally {
      this.openPromise = null;
    }
  }

  async pump() {
    while (!this.closed) {
      const { value, done } = await this.reader.read();
      if (done) {
        this.closed = true;
        this.wake();
        this.wakeDrain();
        return;
      }
      const bytes = value instanceof Uint8Array ? value : new Uint8Array(value || 0);
      for (let offset = 0; offset < bytes.byteLength; offset += RELAY_MAX_CHUNK_BYTES) {
        const chunk = bytes.slice(offset, Math.min(offset + RELAY_MAX_CHUNK_BYTES, bytes.byteLength));
        while (!this.closed && this.queueBytes + chunk.byteLength > MAX_QUEUE_BYTES) {
          await this.waitForDrain();
        }
        if (this.closed) return;
        this.queue.push(chunk);
        this.queueBytes += chunk.byteLength;
        this.downBytes += chunk.byteLength;
      }
      this.wake();
    }
  }

  async wait(ms) {
    if (this.pending || this.queue.length || this.closed) return;
    await new Promise((resolve) => {
      const done = () => { clearTimeout(timer); this.waiters.delete(done); resolve(); };
      const timer = setTimeout(done, Math.min(Math.max(ms, 0), MAX_POLL_WAIT_MS));
      this.waiters.add(done);
    });
  }

  async handleUp(request, url) {
    const seq = Number.parseInt(url.searchParams.get("seq") || "0", 10);
    if (!Number.isSafeInteger(seq) || seq <= 0) return new Response("bad seq", { status: 400, headers: headers() });
    if (!this.socket && this.upSeq === 0 && seq > 1) return new Response("session lost", { status: 410, headers: headers() });

    const run = async () => {
      await this.ensureSocket(url.searchParams.get("dst"));
      if (seq <= this.upSeq) {
        return new Response(null, { status: 204, headers: headers({ "X-Tgws-Chunk-Ack": String(seq) }) });
      }
      if (seq !== this.upSeq + 1) return new Response("sequence gap", { status: 409, headers: headers() });
      const body = new Uint8Array(await request.arrayBuffer());
      if (!body.byteLength || body.byteLength > RELAY_MAX_CHUNK_BYTES) return new Response("bad size", { status: 413, headers: headers() });
      await this.writer.write(body);
      this.upSeq = seq;
      this.upBytes += body.byteLength;
      if (seq <= 2 || this.upBytes % (64 * 1024) < body.byteLength) {
        console.log("chunk relay up", { revision: REVISION, target: this.target, seq, bytes: body.byteLength, up_bytes: this.upBytes });
      }
      return new Response(null, { status: 204, headers: headers({ "X-Tgws-Chunk-Ack": String(seq) }) });
    };

    const result = this.upChain.then(run, run);
    this.upChain = result.then(() => undefined, () => undefined);
    return result;
  }

  async fetch(request) {
    const url = new URL(request.url);
    const action = url.pathname.split("/").filter(Boolean).at(-1) || "";

    try {
      if (action === "open") {
        if (request.method !== "POST") return new Response("method", { status: 405, headers: headers() });
        await this.ensureSocket(url.searchParams.get("dst"));
        return new Response(null, { status: 204, headers: headers() });
      }

      if (action === "up") {
        if (request.method !== "POST") return new Response("method", { status: 405, headers: headers() });
        return await this.handleUp(request, url);
      }

      if (action === "down") {
        if (request.method !== "GET") return new Response("method", { status: 405, headers: headers() });
        const ack = Number.parseInt(url.searchParams.get("ack") || "0", 10);
        if (!Number.isSafeInteger(ack) || ack < 0) return new Response("bad ack", { status: 400, headers: headers() });
        if (!this.socket && ack > 0) return new Response("session lost", { status: 410, headers: headers() });
        await this.ensureSocket(url.searchParams.get("dst"));
        if (this.pending && ack === this.pending.seq) this.pending = null;
        await this.wait(Number.parseInt(url.searchParams.get("wait") || "0", 10));
        if (!this.pending && this.queue.length) {
          const data = this.queue.shift();
          this.queueBytes -= data.byteLength;
          this.wakeDrain();
          this.pending = { seq: ++this.downSeq, data };
        }
        if (this.pending) {
          return new Response(this.pending.data, {
            status: 200,
            headers: headers({ "Content-Type": "application/octet-stream", "X-Tgws-Chunk-Seq": String(this.pending.seq) }),
          });
        }
        if (this.closed) return new Response("closed", { status: 410, headers: headers() });
        return new Response(null, { status: 204, headers: headers() });
      }

      if (action === "close") {
        this.closed = true;
        this.wake();
        this.wakeDrain();
        try { await this.socket?.close(); } catch {}
        console.log("chunk relay closed", {
          revision: REVISION,
          target: this.target,
          up_seq: this.upSeq,
          up_bytes: this.upBytes,
          down_seq: this.downSeq,
          down_bytes: this.downBytes,
        });
        return new Response(null, { status: 204, headers: headers() });
      }
    } catch (error) {
      console.log("chunk relay request failed", { revision: REVISION, action, target: this.target, error: String(error) });
      return new Response("relay failed", { status: 502, headers: headers() });
    }

    return new Response("not found", { status: 404, headers: headers() });
  }
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/diag/fresh-upload") {
      if (request.method !== "POST") return new Response("method", { status: 405, headers: headers() });
      const body = new Uint8Array(await request.arrayBuffer());
      if (!body.byteLength || body.byteLength > DIAG_MAX_CHUNK_BYTES) return new Response("bad size", { status: 413, headers: headers() });
      return Response.json({ ok: true, bytes: body.byteLength, revision: REVISION }, { headers: headers() });
    }

    if (url.pathname === "/diag/fresh-download") {
      const size = Number.parseInt(url.searchParams.get("size") || "0", 10);
      if (!Number.isSafeInteger(size) || size <= 0 || size > DIAG_MAX_CHUNK_BYTES) return new Response("bad size", { status: 400, headers: headers() });
      return new Response(randomBytes(size), { status: 200, headers: headers({ "Content-Type": "application/octet-stream" }) });
    }

    if (url.pathname.startsWith("/chunk-relay/")) {
      const sid = (url.searchParams.get("sid") || "").trim();
      if (!/^[A-Za-z0-9_-]{8,128}$/.test(sid)) return new Response("invalid sid", { status: 400 });
      return env.CHUNK_RELAY.getByName(sid).fetch(request);
    }
    return baseWorker.fetch(request, env, ctx);
  },
};
