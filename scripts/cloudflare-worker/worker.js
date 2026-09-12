import { connect } from "cloudflare:sockets";

const REVISION = "worker-stream-v2";
const DIAG_REVISION = "worker-network-probe-v1";
const MAX_PENDING_BYTES = 32 * 1024 * 1024;
const MAX_DIAG_BYTES = 2 * 1024 * 1024;

async function toBytes(data) {
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (ArrayBuffer.isView(data)) {
    return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  }
  if (Array.isArray(data)) return new Uint8Array(data);
  if (typeof data === "string") return new TextEncoder().encode(data);
  if (data && typeof data.arrayBuffer === "function") {
    return new Uint8Array(await data.arrayBuffer());
  }
  throw new TypeError("Unsupported WebSocket payload");
}

function dataSize(data) {
  if (typeof data === "string") return new TextEncoder().encode(data).byteLength;
  return data?.byteLength ?? data?.size ?? data?.length ?? 0;
}

function websocketResponse(client, request, revision = REVISION) {
  const headers = { "X-Tgws-Worker-Revision": revision };
  const protocols = (request.headers.get("Sec-WebSocket-Protocol") || "")
    .split(",").map((value) => value.trim());
  if (protocols.includes("binary")) headers["Sec-WebSocket-Protocol"] = "binary";
  return new Response(null, { status: 101, webSocket: client, headers });
}

function diagnosticWebSocket(request, url) {
  const pair = new WebSocketPair();
  const client = pair[0];
  const server = pair[1];
  server.accept();
  const sid = url.searchParams.get("sid") || "?";
  const started = Date.now();
  const log = (event, fields = {}) => console.log(event, {
    sid,
    path: url.pathname,
    revision: DIAG_REVISION,
    elapsed_ms: Date.now() - started,
    ...fields,
  });

  if (url.pathname === "/diag/ws-echo") {
    let receivedBytes = 0;
    let messages = 0;
    server.addEventListener("message", async (event) => {
      try {
        const size = dataSize(event.data);
        if (size > MAX_DIAG_BYTES) {
          server.close(1009, "diag_message_too_large");
          return;
        }
        const bytes = await toBytes(event.data);
        receivedBytes += bytes.byteLength;
        messages++;
        server.send(bytes);
        if (messages <= 2 || receivedBytes % (64 * 1024) < bytes.byteLength) {
          log("diag echo", { messages, bytes: bytes.byteLength, received_bytes: receivedBytes });
        }
      } catch {
        server.close(1011, "diag_echo_failed");
      }
    });
    server.addEventListener("close", () => log("diag echo close", { messages, received_bytes: receivedBytes }));
    log("diag echo accepted");
    return websocketResponse(client, request, DIAG_REVISION);
  }

  if (url.pathname === "/diag/upload") {
    let receivedBytes = 0;
    let messages = 0;
    server.addEventListener("message", (event) => {
      const size = dataSize(event.data);
      if (size > MAX_DIAG_BYTES || receivedBytes + size > 64 * MAX_DIAG_BYTES) {
        server.close(1009, "diag_upload_too_large");
        return;
      }
      receivedBytes += size;
      messages++;
      server.send(`ack:${receivedBytes}`);
      if (messages <= 2 || receivedBytes % (64 * 1024) < size) {
        log("diag upload", { messages, bytes: size, received_bytes: receivedBytes });
      }
    });
    server.addEventListener("close", () => log("diag upload close", { messages, received_bytes: receivedBytes }));
    log("diag upload accepted");
    return websocketResponse(client, request, DIAG_REVISION);
  }

  if (url.pathname === "/diag/download") {
    const size = Number.parseInt(url.searchParams.get("size") || "0", 10);
    if (!Number.isFinite(size) || size < 0 || size > MAX_DIAG_BYTES) {
      try { server.close(1008, "invalid_diag_size"); } catch {}
      return new Response("Invalid size", { status: 400 });
    }
    const payload = new Uint8Array(size);
    for (let i = 0; i < payload.length; i += 4096) payload[i] = (i / 4096) & 0xff;
    server.send(payload);
    log("diag download sent", { bytes: size });
    server.addEventListener("close", () => log("diag download close", { bytes: size }));
    return websocketResponse(client, request, DIAG_REVISION);
  }

  return null;
}

// The injected connector lets tests exercise this handler without contacting
// Telegram. Production always uses cloudflare:sockets below.
export function createWorkerHandler(connectTCP) {
  return {
    async fetch(request, env = {}) {
      if ((request.headers.get("Upgrade") || "").toLowerCase() !== "websocket") {
        return new Response("WebSocket upgrade required", { status: 426 });
      }
      const url = new URL(request.url);
      if (url.pathname.startsWith("/diag/")) {
        const response = diagnosticWebSocket(request, url);
        if (response) return response;
        return new Response("Not found", { status: 404 });
      }
      if (url.pathname !== "/apiws") {
        return new Response("Not found", { status: 404 });
      }
      const dst = url.searchParams.get("dst");
      if (!dst) return new Response("Missing dst", { status: 400 });

      const meta = {
        sid: url.searchParams.get("sid") || "?",
        dst,
        dc: url.searchParams.get("dc"),
        media: url.searchParams.get("media"),
        revision: REVISION,
      };
      const started = Date.now();
      const diagnostic = env.WORKER_DIAGNOSTICS === "1";
      const log = (event, fields = {}) =>
        console.log(event, { ...meta, elapsed_ms: Date.now() - started, ...fields });
      const trace = (event, fields) => {
        if (diagnostic) log(event, fields);
      };

      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];
      server.accept();

      let socket;
      let writer;
      let closed = false;
      let chain = Promise.resolve();
      let sequence = 0;
      let receivedBytes = 0;
      let writtenBytes = 0;
      let downstreamBytes = 0;
      let pendingBytes = 0;

      function closeRelay(reason, code = 1000) {
        if (closed) return;
        closed = true;
        log("relay close", {
          reason,
          received_bytes: receivedBytes,
          tcp_written_bytes: writtenBytes,
          downstream_bytes: downstreamBytes,
          pending_bytes: pendingBytes,
        });
        try { Promise.resolve(socket?.close()).catch(() => {}); } catch {}
        try { server.close(code, reason); } catch {}
      }

      function startTCP() {
        if (socket) return;
        socket = connectTCP(
          { hostname: dst, port: 443 },
          { secureTransport: "off", allowHalfOpen: true },
        );
        writer = socket.writable.getWriter();
        trace("tcp connect start", {});
        socket.opened.then(() => trace("tcp opened", {}))
          .catch(() => closeRelay("tcp_open_failed", 1011));
        socket.closed.catch(() => closeRelay("tcp_failed", 1011));

        const reader = socket.readable.getReader();
        (async () => {
          let reason = "tcp_eof";
          try {
            while (!closed) {
              const { value, done } = await reader.read();
              if (done || closed) break;
              if (!value?.byteLength) continue;
              downstreamBytes += value.byteLength;
              trace("tcp read", {
                bytes: value.byteLength,
                downstream_bytes: downstreamBytes,
              });
              server.send(value);
            }
          } catch {
            reason = "tcp_read_failed";
          } finally {
            try { reader.releaseLock(); } catch {}
            closeRelay(reason, reason === "tcp_eof" ? 1000 : 1011);
          }
        })();
      }

      server.addEventListener("message", (event) => {
        if (closed) return;
        const seq = ++sequence;
        const size = dataSize(event.data);
        receivedBytes += size;
        trace("ws message received", { seq, bytes: size, received_bytes: receivedBytes });
        if (pendingBytes + size > MAX_PENDING_BYTES) {
          closeRelay("tcp_backlog_limit", 1011);
          return;
        }
        pendingBytes += size;
        chain = chain.then(async () => {
          if (closed) return;
          const chunk = await toBytes(event.data);
          if (closed || !chunk.byteLength) return;
          startTCP();
          const writeStarted = Date.now();
          trace("tcp write start", { seq, bytes: chunk.byteLength, pending_bytes: pendingBytes });
          await writer.write(chunk);
          writtenBytes += chunk.byteLength;
          trace("tcp write end", {
            seq,
            bytes: chunk.byteLength,
            duration_ms: Date.now() - writeStarted,
            tcp_written_bytes: writtenBytes,
          });
        }).catch(() => closeRelay("ws_to_tcp_failed", 1011))
          .finally(() => { pendingBytes -= size; });
      });
      server.addEventListener("close", () => closeRelay("ws_closed"));
      server.addEventListener("error", () => closeRelay("ws_error", 1011));

      log("apiws accepted", { tcp_connect: "lazy", diagnostics: diagnostic });
      return websocketResponse(client, request, REVISION);
    },
  };
}

export default createWorkerHandler(connect);
