import baseWorker from "./worker.js";
import { connect } from "cloudflare:sockets";
import { DurableObject } from "cloudflare:workers";

const REVISION = "chunk-relay-mtproto-v8";
const HUB_REVISION = "relay-hub-v1";
const RELAY_HUB_NAME = "relay-hub-v1";
const RELAY_MAX_UPLOAD_CHUNK_BYTES = 12 * 1024;
const RELAY_MAX_DOWN_CHUNK_BYTES = 8 * 1024;
const DIAG_MAX_CHUNK_BYTES = 12 * 1024;
const MAX_QUEUE_BYTES = 2 * 1024 * 1024;
const MAX_POLL_WAIT_MS = 6000;
const MAX_UPLOAD_REORDER_WINDOW = 16;
const WORKER_STATE_HEADER = "X-Tgws-Worker-State";
const QUOTA_RESET_HEADER = "X-Tgws-Quota-Reset";
const HUB_REVISION_HEADER = "X-Tgws-Relay-Hub-Revision";
const DO_QUOTA_EXHAUSTED_STATE = "do-quota-exhausted";
const DO_QUOTA_ERROR_FRAGMENT = "Exceeded allowed duration in Durable Objects free tier";

function headers(extra = {}) {
  return {
    "Cache-Control": "no-store",
    "X-Tgws-Chunk-Relay-Revision": REVISION,
    [HUB_REVISION_HEADER]: HUB_REVISION,
    ...extra,
  };
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

function validSessionId(value) {
  return /^[A-Za-z0-9_-]{8,128}$/.test((value || "").trim());
}

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
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

class RelaySession {
  constructor(sid, callbacks) {
    this.sid = sid;
    this.onOpened = callbacks.onOpened;
    this.onTerminated = callbacks.onTerminated;
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
    this.opened = false;
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

  rejectPendingUploads(error) {
    for (const entry of this.upPending.values()) entry.done.reject(error);
    this.upPending.clear();
  }

  terminate(reason, error = new Error(reason)) {
    if (this.closed) return false;
    this.closed = true;
    this.rejectPendingUploads(error);
    this.wake();
    this.wakeDrain();
    this.onTerminated(this, reason);
    return true;
  }

  summary() {
    return {
      target: this.target,
      up_seq: this.upSeq,
      up_bytes: this.upBytes,
      down_seq: this.downSeq,
      down_bytes: this.downBytes,
    };
  }

  isPristine() {
    return !this.opened
      && !this.socket
      && !this.openPromise
      && !this.target
      && this.upSeq === 0
      && this.upPending.size === 0
      && this.downSeq === 0
      && this.queue.length === 0;
  }

  async waitForDrain(requiredBytes) {
    if (this.queueBytes + requiredBytes <= MAX_QUEUE_BYTES || this.closed) return;
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
      this.opened = true;
      this.onOpened(this);
      this.pump().catch((error) => {
        console.log("chunk relay pump failed", {
          revision: REVISION,
          hub_revision: HUB_REVISION,
          target,
          error: String(error),
        });
        this.terminate("pump_failed", error);
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
        this.terminate("upstream_closed", new Error("upstream_closed"));
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
        this.upPending.delete(nextSeq);
        entry.done.reject(error);
        this.terminate("upload_write_failed", error);
        throw error;
      }

      this.upPending.delete(nextSeq);
      this.upSeq = nextSeq;
      this.upBytes += entry.data.byteLength;
      entry.done.resolve();
      if (nextSeq <= 2 || this.upBytes % (64 * 1024) < entry.data.byteLength) {
        console.log("chunk relay up", {
          revision: REVISION,
          hub_revision: HUB_REVISION,
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
    if (!Number.isSafeInteger(seq) || seq <= 0) return new Response("bad seq", { status: 400, headers: headers() });
    if (!this.socket && this.upSeq === 0 && seq > MAX_UPLOAD_REORDER_WINDOW) {
      return new Response("session lost", { status: 410, headers: headers() });
    }

    const body = new Uint8Array(await request.arrayBuffer());
    if (!body.byteLength || body.byteLength > RELAY_MAX_UPLOAD_CHUNK_BYTES) {
      return new Response("bad size", { status: 413, headers: headers() });
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
      return new Response("sequence window", { status: admitted.status, headers: headers() });
    }

    await admitted.entry.done.promise;
    return new Response(null, { status: 204, headers: headers({ "X-Tgws-Chunk-Ack": String(seq) }) });
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
        this.terminate("client_close", new Error("session_closed"));
        try { await this.socket?.close(); } catch {}
        return new Response(null, { status: 204, headers: headers() });
      }
    } catch (error) {
      console.log("chunk relay request failed", {
        revision: REVISION,
        hub_revision: HUB_REVISION,
        action,
        target: this.target,
        error: String(error),
      });
      return new Response("relay failed", { status: 502, headers: headers() });
    }

    return new Response("not found", { status: 404, headers: headers() });
  }
}

export class ChunkRelayHub extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.sessions = new Map();
    this.openedSessions = 0;
    this.closedSessions = 0;
    this.reapedSessions = 0;
  }

  getOrCreateSession(sid) {
    let session = this.sessions.get(sid);
    if (session) return session;

    session = new RelaySession(sid, {
      onOpened: (openedSession) => this.onSessionOpened(openedSession),
      onTerminated: (terminatedSession, reason) => this.onSessionTerminated(terminatedSession, reason),
    });
    this.sessions.set(sid, session);
    return session;
  }

  onSessionOpened(session) {
    this.openedSessions += 1;
    console.log("chunk relay hub session opened", {
      revision: REVISION,
      hub_revision: HUB_REVISION,
      active_sessions: this.sessions.size,
      opened_sessions: this.openedSessions,
      closed_sessions: this.closedSessions,
      reaped_sessions: this.reapedSessions,
      target: session.target,
    });
  }

  onSessionTerminated(session, reason) {
    if (this.sessions.get(session.sid) !== session) return;
    this.sessions.delete(session.sid);
    if (reason === "client_close") this.closedSessions += 1;
    else this.reapedSessions += 1;

    console.log("chunk relay hub session terminated", {
      revision: REVISION,
      hub_revision: HUB_REVISION,
      reason,
      active_sessions: this.sessions.size,
      opened_sessions: this.openedSessions,
      closed_sessions: this.closedSessions,
      reaped_sessions: this.reapedSessions,
      ...session.summary(),
    });
  }

  async fetch(request) {
    const url = new URL(request.url);
    const sid = (url.searchParams.get("sid") || "").trim();
    if (!validSessionId(sid)) return new Response("invalid sid", { status: 400, headers: headers() });

    const action = url.pathname.split("/").filter(Boolean).at(-1) || "";
    if (!new Set(["open", "up", "down", "close"]).has(action)) {
      return new Response("not found", { status: 404, headers: headers() });
    }

    if (action === "close" && !this.sessions.has(sid)) {
      return new Response(null, { status: 204, headers: headers() });
    }

    const session = this.getOrCreateSession(sid);
    const response = await session.fetch(request);
    if (response.status >= 400 && session.isPristine() && this.sessions.get(sid) === session) {
      this.sessions.delete(sid);
    }
    return response;
  }
}

// Keep the existing Durable Object export name so deployments do not need a class migration.
export { ChunkRelayHub as ChunkRelaySession };

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/diag/fresh-upload") {
      if (request.method !== "POST") return new Response("method", { status: 405, headers: headers() });
      const body = new Uint8Array(await request.arrayBuffer());
      if (!body.byteLength || body.byteLength > DIAG_MAX_CHUNK_BYTES) return new Response("bad size", { status: 413, headers: headers() });
      return Response.json({ ok: true, bytes: body.byteLength, revision: REVISION, hub_revision: HUB_REVISION }, { headers: headers() });
    }

    if (url.pathname === "/diag/fresh-download") {
      const size = Number.parseInt(url.searchParams.get("size") || "0", 10);
      if (!Number.isSafeInteger(size) || size <= 0 || size > DIAG_MAX_CHUNK_BYTES) return new Response("bad size", { status: 400, headers: headers() });
      return new Response(randomBytes(size), { status: 200, headers: headers({ "Content-Type": "application/octet-stream" }) });
    }

    if (url.pathname.startsWith("/chunk-relay/")) {
      const sid = (url.searchParams.get("sid") || "").trim();
      if (!validSessionId(sid)) return new Response("invalid sid", { status: 400, headers: headers() });
      try {
        return await env.CHUNK_RELAY.getByName(RELAY_HUB_NAME).fetch(request);
      } catch (error) {
        if (isDurableObjectQuotaError(error)) {
          console.log("chunk relay durable object quota exhausted", {
            revision: REVISION,
            hub_revision: HUB_REVISION,
          });
          return durableObjectQuotaResponse();
        }
        throw error;
      }
    }
    return baseWorker.fetch(request, env, ctx);
  },
};
