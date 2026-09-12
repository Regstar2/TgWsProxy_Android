import baseWorker from "./worker.js";
import { connect } from "cloudflare:sockets";
import { DurableObject } from "cloudflare:workers";

const REVISION = "chunk-relay-dc2-v2";
const TELEGRAM_DC2 = "149.154.167.51";
const RELAY_MAX_CHUNK_BYTES = 8 * 1024;
const DIAG_MAX_CHUNK_BYTES = 12 * 1024;
const MAX_QUEUE_BYTES = 2 * 1024 * 1024;

function headers(extra = {}) {
  return { "Cache-Control": "no-store", "X-Tgws-Chunk-Relay-Revision": REVISION, ...extra };
}

function randomBytes(size) {
  const bytes = new Uint8Array(size);
  crypto.getRandomValues(bytes);
  return bytes;
}

export class ChunkRelaySession extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.socket = null;
    this.writer = null;
    this.reader = null;
    this.upSeq = 0;
    this.downSeq = 0;
    this.pending = null;
    this.queue = [];
    this.queueBytes = 0;
    this.waiters = new Set();
    this.closed = false;
  }

  wake() {
    for (const resolve of this.waiters) resolve();
    this.waiters.clear();
  }

  async ensureSocket() {
    if (this.socket) return;
    const socket = connect({ hostname: TELEGRAM_DC2, port: 443 }, { secureTransport: "off", allowHalfOpen: true });
    await socket.opened;
    this.socket = socket;
    this.writer = socket.writable.getWriter();
    this.reader = socket.readable.getReader();
    this.pump().catch(() => { this.closed = true; this.wake(); });
  }

  async pump() {
    while (!this.closed) {
      const { value, done } = await this.reader.read();
      if (done) { this.closed = true; this.wake(); return; }
      const bytes = value instanceof Uint8Array ? value : new Uint8Array(value || 0);
      for (let offset = 0; offset < bytes.byteLength; offset += RELAY_MAX_CHUNK_BYTES) {
        const chunk = bytes.slice(offset, Math.min(offset + RELAY_MAX_CHUNK_BYTES, bytes.byteLength));
        if (this.queueBytes + chunk.byteLength > MAX_QUEUE_BYTES) throw new Error("queue_limit");
        this.queue.push(chunk);
        this.queueBytes += chunk.byteLength;
      }
      this.wake();
    }
  }

  async wait(ms) {
    if (this.pending || this.queue.length || this.closed) return;
    await new Promise((resolve) => {
      const done = () => { clearTimeout(timer); this.waiters.delete(done); resolve(); };
      const timer = setTimeout(done, Math.min(Math.max(ms, 0), 1000));
      this.waiters.add(done);
    });
  }

  async fetch(request) {
    const url = new URL(request.url);
    const action = url.pathname.split("/").filter(Boolean).at(-1) || "";
    if (action === "open") {
      await this.ensureSocket();
      return new Response(null, { status: 204, headers: headers() });
    }
    if (action === "up") {
      await this.ensureSocket();
      const seq = Number.parseInt(url.searchParams.get("seq") || "0", 10);
      if (!Number.isSafeInteger(seq) || seq <= 0) return new Response("bad seq", { status: 400 });
      if (seq <= this.upSeq) return new Response(null, { status: 204, headers: headers({ "X-Tgws-Chunk-Ack": String(seq) }) });
      if (seq !== this.upSeq + 1) return new Response("sequence gap", { status: 409 });
      const body = new Uint8Array(await request.arrayBuffer());
      if (!body.byteLength || body.byteLength > RELAY_MAX_CHUNK_BYTES) return new Response("bad size", { status: 413 });
      await this.writer.write(body);
      this.upSeq = seq;
      return new Response(null, { status: 204, headers: headers({ "X-Tgws-Chunk-Ack": String(seq) }) });
    }
    if (action === "down") {
      await this.ensureSocket();
      const ack = Number.parseInt(url.searchParams.get("ack") || "0", 10);
      if (this.pending && ack === this.pending.seq) this.pending = null;
      await this.wait(Number.parseInt(url.searchParams.get("wait") || "0", 10));
      if (!this.pending && this.queue.length) {
        const data = this.queue.shift();
        this.queueBytes -= data.byteLength;
        this.pending = { seq: ++this.downSeq, data };
      }
      if (this.pending) return new Response(this.pending.data, { status: 200, headers: headers({ "Content-Type": "application/octet-stream", "X-Tgws-Chunk-Seq": String(this.pending.seq) }) });
      if (this.closed) return new Response("closed", { status: 410, headers: headers() });
      return new Response(null, { status: 204, headers: headers() });
    }
    if (action === "close") {
      this.closed = true;
      this.wake();
      try { await this.socket?.close(); } catch {}
      return new Response(null, { status: 204, headers: headers() });
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
