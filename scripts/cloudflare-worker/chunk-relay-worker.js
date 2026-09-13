import baseWorker from "./worker.js";
import { connect } from "cloudflare:sockets";
import { DurableObject } from "cloudflare:workers";

const REVISION = "chunk-relay-mtproto-v8";
const RELAY_MAX_UPLOAD_CHUNK_BYTES = 12 * 1024;
const RELAY_MAX_DOWN_CHUNK_BYTES = 8 * 1024;
const DIAG_MAX_CHUNK_BYTES = 12 * 1024;
const MAX_QUEUE_BYTES = 2 * 1024 * 1024;
const MAX_POLL_WAIT_MS = 6000;
const MAX_UPLOAD_REORDER_WINDOW = 16;
const WORKER_STATE_HEADER = "X-Tgws-Worker-State";
const QUOTA_RESET_HEADER = "X-Tgws-Quota-Reset";
const RELAY_ERROR_HEADER = "X-Tgws-Relay-Error";
const DO_QUOTA_EXHAUSTED_STATE = "do-quota-exhausted";
const DO_QUOTA_ERROR_FRAGMENT = "Exceeded allowed duration in Durable Objects free tier";

function headers(extra = {}) {
  return { "Cache-Control": "no-store", "X-Tgws-Chunk-Relay-Revision": REVISION, ...extra };
}

function relayHeaders(relayError, extra = {}) {
  return headers({ [RELAY_ERROR_HEADER]: relayError, ...extra });
}

function relayFailureResponse(relayError, status, text = "relay failed") {
  return new Response(text, { status, headers: relayHeaders(relayError) });
}

class RelayFailure extends Error {
  constructor(relayError, status, message = relayError) {
    super(message);
    this.name = "RelayFailure";
    this.relayError = relayError;
    this.status = status;
  }
}

function classifyRelayFailure(error) {
  if (error instanceof RelayFailure) return error;
  const message = String(error?.message || error || "");
  switch (message) {
    case "invalid_target":
      return new RelayFailure("invalid_target", 400);
    case "target_mismatch":
      return new RelayFailure("target_mismatch", 409);
    case "sequence_conflict":
      return new RelayFailure("sequence_conflict", 409);
    case "session_closed":
      return new RelayFailure("session_lost", 410);
    case "upstream_closed":
      return new RelayFailure("upstream_closed", 410);
    case "socket_read_failed":
      return new RelayFailure("socket_read_failed", 410);
    case "socket_write_failed":
      return new RelayFailure("socket_write_failed", 410);
    default:
      return new RelayFailure("transient_worker_error", 502);
  }
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

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  // A request can fail while drainUploads() still owns admission, before the
  // request reaches its explicit await below. Keep the rejection observable to
  // awaiters without letting it become an unhandled rejection in that window.
  promise.catch(() => {});
  return { promise, resolve, reject };
}

function isDurableObjectQuotaError(error) {
  return String(error || "").includes(DO_QUOTA_ERROR_FRAGMENT);
}

function nextQuotaReset(now = new Date()) {
  const reset = new Date(Date.UTC(
    now.getUTCFullYear(),
    now.getUTCMonth(),
    now.getUTCDate() + 1,
    0,
    1,
    0,
    0,
  ));
  return reset;
}

function durableObjectQuotaResponse(now = new Date()) {
  const reset = nextQuotaReset(now);
  const retryAfterSeconds = Math.max(60, Math.ceil((reset.getTime() - now.getTime()) / 1000));
  return new Response("durable object quota exhausted", {
    status: 503,
    headers: headers({
      [WORKER_STATE_HEADER]: DO_QUOTA_EXHAUSTED_STATE,
      [QUOTA_RESET_HEADER]: reset.toISOString(),
      "Retry-After": String(retryAfterSeconds),
    }),
  });
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
    this.upPending = new Map();
    this.downSeq = 0;
    this.downBytes = 0;
    this.pending = null;
    this.queue = [];
    this.queueBytes = 0;
    this.waiters = new Set();
    this.drainWaiters = new Set();
    this.closed = false;
    this.relayError = "";
  }

  wake() {
    for (const resolve of this.waiters) resolve();
    this.waiters.clear();
  }

  wakeDrain() {
    for (const resolve of this.drainWaiters) resolve();
    this.drainWaiters.clear();
  }

  rejectPendingUploads(error) {
    for (const entry of this.upPending.values()) entry.done.reject(error);
    this.upPending.clear();
  }

  failSession(relayError, error = null) {
    const failure = error instanceof RelayFailure
      ? error
      : new RelayFailure(relayError, 410, relayError);
    this.closed = true;
    if (!this.relayError) this.relayError = relayError;
    this.rejectPendingUploads(failure);
    this.wake();
    this.wakeDrain();
    if (error) {
      console.log("chunk relay session failed", {
        revision: REVISION,
        target: this.target,
        relay_error: this.relayError,
        error: String(error),
      });
    }
  }

  async waitForDrain(requiredBytes) {
    if (this.queueBytes + requiredBytes <= MAX_QUEUE_BYTES || this.closed) return;
    await new Promise((resolve) => this.drainWaiters.add(resolve));
  }

  async ensureSocket(targetRaw) {
    const target = (targetRaw || "").trim();
    if (!validTarget(target)) throw new RelayFailure("invalid_target", 400);
    if (this.closed) throw new RelayFailure(this.relayError || "session_lost", 410);
    if (this.target && this.target !== target) throw new RelayFailure("target_mismatch", 409);
    if (this.socket) return;
    if (this.openPromise) return this.openPromise;

    this.openPromise = (async () => {
      const socket = connect({ hostname: target, port: 443 }, { secureTransport: "off", allowHalfOpen: true });
      await socket.opened;
      if (this.closed) {
        try { await socket.close(); } catch {}
        throw new RelayFailure(this.relayError || "session_lost", 410);
      }
      this.target = target;
      this.socket = socket;
      this.writer = socket.writable.getWriter();
      this.reader = socket.readable.getReader();
      console.log("chunk relay opened", { revision: REVISION, target });
      this.pump().catch((error) => {
        this.failSession("socket_read_failed", error);
      });
    })();

    try {
      await this.openPromise;
    } finally {
      this.openPromise = null;
    }
  }

  async pump() {
    try {
      while (!this.closed) {
        const { value, done } = await this.reader.read();
        if (done) {
          this.failSession("upstream_closed", new RelayFailure("upstream_closed", 410));
          return;
        }
        const bytes = value instanceof Uint8Array ? value : new Uint8Array(value || 0);
        for (let offset = 0; offset < bytes.byteLength; offset += RELAY_MAX_DOWN_CHUNK_BYTES) {
          const chunk = bytes.slice(offset, Math.min(offset + RELAY_MAX_DOWN_CHUNK_BYTES, bytes.byteLength));
          while (!this.closed && this.queueBytes + chunk.byteLength > MAX_QUEUE_BYTES) {
            await this.waitForDrain(chunk.byteLength);
          }
          if (this.closed) return;
          this.queue.push(chunk);
          this.queueBytes += chunk.byteLength;
          this.downBytes += chunk.byteLength;
        }
        this.wake();
      }
    } catch (error) {
      if (!this.closed) {
        this.failSession("socket_read_failed", new RelayFailure("socket_read_failed", 410, String(error)));
      }
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

  async drainUploads() {
    while (!this.closed) {
      const nextSeq = this.upSeq + 1;
      const entry = this.upPending.get(nextSeq);
      if (!entry) return;

      try {
        await this.writer.write(entry.data);
      } catch (error) {
        const failure = new RelayFailure("socket_write_failed", 410, String(error));
        entry.done.reject(failure);
        this.upPending.delete(nextSeq);
        this.failSession("socket_write_failed", failure);
        throw failure;
      }

      this.upPending.delete(nextSeq);
      this.upSeq = nextSeq;
      this.upBytes += entry.data.byteLength;
      entry.done.resolve();
      if (nextSeq <= 2 || this.upBytes % (64 * 1024) < entry.data.byteLength) {
        console.log("chunk relay up", {
          revision: REVISION,
          target: this.target,
          seq: nextSeq,
          bytes: entry.data.byteLength,
          up_bytes: this.upBytes,
          pending_uploads: this.upPending.size,
        });
      }
    }
  }

  async handleUp(request, url) {
    const seq = Number.parseInt(url.searchParams.get("seq") || "0", 10);
    if (!Number.isSafeInteger(seq) || seq <= 0) return new Response("bad seq", { status: 400, headers: relayHeaders("invalid_sequence") });
    if (this.closed) return relayFailureResponse(this.relayError || "session_lost", 410, "session lost");
    if (!this.socket && this.upSeq === 0 && seq > MAX_UPLOAD_REORDER_WINDOW) {
      return relayFailureResponse("session_lost", 410, "session lost");
    }

    const body = new Uint8Array(await request.arrayBuffer());
    if (!body.byteLength || body.byteLength > RELAY_MAX_UPLOAD_CHUNK_BYTES) {
      return new Response("bad size", { status: 413, headers: relayHeaders("invalid_size") });
    }

    const admit = async () => {
      await this.ensureSocket(url.searchParams.get("dst"));
      if (seq <= this.upSeq) return { immediate: true };
      if (seq > this.upSeq + MAX_UPLOAD_REORDER_WINDOW) return { status: 409 };

      let entry = this.upPending.get(seq);
      if (!entry) {
        entry = { data: body, done: deferred() };
        this.upPending.set(seq, entry);
      }

      await this.drainUploads();
      return { entry };
    };

    const admittedPromise = this.upChain.then(admit, admit);
    this.upChain = admittedPromise.then(() => undefined, () => undefined);
    const admitted = await admittedPromise;

    if (admitted.immediate) {
      return new Response(null, { status: 204, headers: headers({ "X-Tgws-Chunk-Ack": String(seq) }) });
    }
    if (admitted.status) {
      return relayFailureResponse("sequence_conflict", admitted.status, "sequence window");
    }

    await admitted.entry.done.promise;
    return new Response(null, { status: 204, headers: headers({ "X-Tgws-Chunk-Ack": String(seq) }) });
  }

  async fetch(request) {
    const url = new URL(request.url);
    const action = url.pathname.split("/").filter(Boolean).at(-1) || "";

    try {
      if (action === "open") {
        if (request.method !== "POST") return new Response("method", { status: 405, headers: relayHeaders("method_not_allowed") });
        await this.ensureSocket(url.searchParams.get("dst"));
        return new Response(null, { status: 204, headers: headers() });
      }

      if (action === "up") {
        if (request.method !== "POST") return new Response("method", { status: 405, headers: relayHeaders("method_not_allowed") });
        return await this.handleUp(request, url);
      }

      if (action === "down") {
        if (request.method !== "GET") return new Response("method", { status: 405, headers: relayHeaders("method_not_allowed") });
        const ack = Number.parseInt(url.searchParams.get("ack") || "0", 10);
        if (!Number.isSafeInteger(ack) || ack < 0) return new Response("bad ack", { status: 400, headers: relayHeaders("invalid_ack") });
        if (this.closed) return relayFailureResponse(this.relayError || "session_lost", 410, "closed");
        if (!this.socket && ack > 0) return relayFailureResponse("session_lost", 410, "session lost");
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
        if (this.closed) return relayFailureResponse(this.relayError || "session_lost", 410, "closed");
        return new Response(null, { status: 204, headers: headers() });
      }

      if (action === "close") {
        this.closed = true;
        if (!this.relayError) this.relayError = "session_lost";
        this.rejectPendingUploads(new RelayFailure("session_lost", 410));
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
      const failure = classifyRelayFailure(error);
      console.log("chunk relay request failed", {
        revision: REVISION,
        action,
        target: this.target,
        relay_error: failure.relayError,
        status: failure.status,
        error: String(error),
      });
      return relayFailureResponse(failure.relayError, failure.status);
    }

    return new Response("not found", { status: 404, headers: relayHeaders("not_found") });
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
      if (!/^[A-Za-z0-9_-]{8,128}$/.test(sid)) return new Response("invalid sid", { status: 400, headers: relayHeaders("invalid_session_id") });
      try {
        return await env.CHUNK_RELAY.getByName(sid).fetch(request);
      } catch (error) {
        if (isDurableObjectQuotaError(error)) {
          console.log("chunk relay durable object quota exhausted", { revision: REVISION, sid });
          return durableObjectQuotaResponse();
        }
        console.log("chunk relay durable object request failed", {
          revision: REVISION,
          sid,
          relay_error: "transient_worker_error",
          error: String(error),
        });
        return relayFailureResponse("transient_worker_error", 502);
      }
    }
    return baseWorker.fetch(request, env, ctx);
  },
};
