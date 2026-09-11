import { connect } from "cloudflare:sockets";

const TCP_REVISION = "worker-stream-v2";
const WSS_REVISION = "worker-wss-relay-v3";
const MAX_PENDING_BYTES = 32 * 1024 * 1024;
const VALID_TELEGRAM_DCS = new Set([1, 2, 3, 4, 5, 203]);

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

function responseHeaders(request, revision) {
  const headers = { "X-Tgws-Worker-Revision": revision };
  const protocols = (request.headers.get("Sec-WebSocket-Protocol") || "")
    .split(",").map((value) => value.trim());
  if (protocols.includes("binary")) headers["Sec-WebSocket-Protocol"] = "binary";
  return headers;
}

function telegramWebSocketCandidates(dc, media) {
  // Android direct-WS routing aliases DC203 to the DC2 WebSocket host.
  const effectiveDC = dc === 203 ? 2 : dc;
  const primary = media
    ? `kws${effectiveDC}-1.web.telegram.org`
    : `kws${effectiveDC}.web.telegram.org`;
  const secondary = media
    ? `kws${effectiveDC}.web.telegram.org`
    : `kws${effectiveDC}-1.web.telegram.org`;
  return [primary, secondary];
}

function parseTelegramDC(url) {
  const raw = url.searchParams.get("dc");
  const dc = Number(raw);
  if (!Number.isInteger(dc) || !VALID_TELEGRAM_DCS.has(dc)) return null;
  return dc;
}

function createLogger(url, env, revision, transport) {
  const meta = {
    sid: url.searchParams.get("sid") || "?",
    dst: url.searchParams.get("dst") || "?",
    dc: url.searchParams.get("dc"),
    media: url.searchParams.get("media"),
    revision,
    transport,
  };
  const started = Date.now();
  const diagnostic = env.WORKER_DIAGNOSTICS === "1";
  const log = (event, fields = {}) =>
    console.log(event, { ...meta, elapsed_ms: Date.now() - started, ...fields });
  const trace = (event, fields = {}) => {
    if (diagnostic) log(event, fields);
  };
  return { log, trace, diagnostic };
}

function createOuterWebSocket() {
  const pair = new WebSocketPair();
  const client = pair[0];
  const server = pair[1];
  server.accept();
  return { client, server };
}

async function handleTcpRelay(request, env, url, connectTCP) {
  const dst = url.searchParams.get("dst");
  if (!dst) return new Response("Missing dst", { status: 400 });

  const { log, trace, diagnostic } = createLogger(url, env, TCP_REVISION, "tcp_v2");
  const { client, server } = createOuterWebSocket();

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
    // writer.close() queues behind a pending write and cannot cancel it.
    // Close the socket directly so both directions stop even under pressure.
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
    // Enqueue before asynchronous conversion: a later ArrayBuffer must not
    // overtake an earlier Blob. Exactly one TCP write runs at a time.
    chain = chain.then(async () => {
      if (closed) return;
      const chunk = await toBytes(event.data);
      if (closed || !chunk.byteLength) return;
      startTCP(); // The first nonempty message, including relay_init.
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
  return new Response(null, {
    status: 101,
    webSocket: client,
    headers: responseHeaders(request, TCP_REVISION),
  });
}

async function handleWssRelay(request, env, url, fetchWebSocket) {
  const dc = parseTelegramDC(url);
  if (dc === null) return new Response("Invalid dc", { status: 400 });
  const media = url.searchParams.get("media") === "1";
  const candidates = telegramWebSocketCandidates(dc, media);
  const { log, trace, diagnostic } = createLogger(url, env, WSS_REVISION, "wss_relay_v3");
  const { client, server } = createOuterWebSocket();

  let upstream;
  let upstreamPromise;
  let closed = false;
  let upstreamHost = "";
  let upstreamChain = Promise.resolve();
  let downstreamChain = Promise.resolve();
  let sequence = 0;
  let downstreamSequence = 0;
  let receivedBytes = 0;
  let upstreamBytes = 0;
  let downstreamBytes = 0;
  let pendingBytes = 0;

  function closeRelay(reason, code = 1000) {
    if (closed) return;
    closed = true;
    log("relay close", {
      reason,
      upstream_host: upstreamHost || null,
      received_bytes: receivedBytes,
      upstream_ws_bytes: upstreamBytes,
      downstream_bytes: downstreamBytes,
      pending_bytes: pendingBytes,
    });
    try { upstream?.close(1000, reason); } catch {}
    try { server.close(code, reason); } catch {}
  }

  function attachUpstream(ws, host) {
    upstream = ws;
    upstreamHost = host;
    if (typeof ws.accept === "function") ws.accept();
    ws.addEventListener("message", (event) => {
      if (closed) return;
      const seq = ++downstreamSequence;
      downstreamChain = downstreamChain.then(async () => {
        if (closed) return;
        const chunk = await toBytes(event.data);
        if (closed || !chunk.byteLength) return;
        downstreamBytes += chunk.byteLength;
        trace("upstream ws message received", {
          seq,
          bytes: chunk.byteLength,
          downstream_bytes: downstreamBytes,
          upstream_host: host,
        });
        server.send(chunk);
      }).catch(() => closeRelay("upstream_ws_to_client_failed", 1011));
    });
    ws.addEventListener("close", () => closeRelay("upstream_ws_closed"));
    ws.addEventListener("error", () => closeRelay("upstream_ws_error", 1011));
  }

  async function openUpstream() {
    if (upstream) return upstream;
    if (upstreamPromise) return upstreamPromise;
    upstreamPromise = (async () => {
      let lastStatus = 0;
      for (const host of candidates) {
        if (closed) throw new Error("relay closed");
        trace("upstream ws connect start", { upstream_host: host });
        try {
          const response = await fetchWebSocket(`https://${host}/apiws`, {
            headers: {
              Upgrade: "websocket",
              "Sec-WebSocket-Protocol": "binary",
            },
          });
          lastStatus = response?.status ?? 0;
          if (response?.status !== 101 || !response.webSocket) {
            trace("upstream ws connect rejected", {
              upstream_host: host,
              status: lastStatus,
            });
            continue;
          }
          attachUpstream(response.webSocket, host);
          trace("upstream ws connected", { upstream_host: host });
          return response.webSocket;
        } catch (error) {
          trace("upstream ws connect failed", {
            upstream_host: host,
            error: String(error),
          });
        }
      }
      throw new Error(`Telegram WebSocket unavailable (last status ${lastStatus})`);
    })();
    try {
      return await upstreamPromise;
    } catch (error) {
      upstreamPromise = undefined;
      throw error;
    }
  }

  server.addEventListener("message", (event) => {
    if (closed) return;
    const seq = ++sequence;
    const size = dataSize(event.data);
    receivedBytes += size;
    trace("ws message received", {
      seq,
      bytes: size,
      received_bytes: receivedBytes,
      transport: "wss_relay_v3",
    });
    if (pendingBytes + size > MAX_PENDING_BYTES) {
      closeRelay("upstream_ws_backlog_limit", 1011);
      return;
    }
    pendingBytes += size;
    // The Android v3 client packet-splits before this point. Preserve each
    // outer binary message as exactly one Telegram /apiws message.
    upstreamChain = upstreamChain.then(async () => {
      if (closed) return;
      const chunk = await toBytes(event.data);
      if (closed || !chunk.byteLength) return;
      const ws = await openUpstream(); // Lazy: relay_init is the first message.
      if (closed) return;
      const writeStarted = Date.now();
      trace("upstream ws send start", {
        seq,
        bytes: chunk.byteLength,
        pending_bytes: pendingBytes,
        upstream_host: upstreamHost,
      });
      ws.send(chunk);
      upstreamBytes += chunk.byteLength;
      trace("upstream ws send end", {
        seq,
        bytes: chunk.byteLength,
        duration_ms: Date.now() - writeStarted,
        upstream_ws_bytes: upstreamBytes,
        upstream_host: upstreamHost,
      });
    }).catch(() => closeRelay("client_to_upstream_ws_failed", 1011))
      .finally(() => { pendingBytes -= size; });
  });
  server.addEventListener("close", () => closeRelay("ws_closed"));
  server.addEventListener("error", () => closeRelay("ws_error", 1011));

  log("apiws-ws accepted", {
    upstream_connect: "lazy",
    upstream_candidates: candidates.join(","),
    diagnostics: diagnostic,
  });
  return new Response(null, {
    status: 101,
    webSocket: client,
    headers: responseHeaders(request, WSS_REVISION),
  });
}

// The injected transports let tests exercise both relay implementations without
// contacting Telegram. Production uses cloudflare:sockets for v2 and fetch()
// WebSocket upgrades for v3.
export function createWorkerHandler(connectTCP, fetchWebSocket = globalThis.fetch) {
  return {
    async fetch(request, env = {}) {
      if ((request.headers.get("Upgrade") || "").toLowerCase() !== "websocket") {
        return new Response("WebSocket upgrade required", { status: 426 });
      }
      const url = new URL(request.url);
      if (url.pathname === "/apiws") {
        return handleTcpRelay(request, env, url, connectTCP);
      }
      if (url.pathname === "/apiws-ws") {
        if (typeof fetchWebSocket !== "function") {
          return new Response("Outbound WebSocket unavailable", { status: 503 });
        }
        return handleWssRelay(request, env, url, fetchWebSocket);
      }
      return new Response("Not found", { status: 404 });
    },
  };
}

export default createWorkerHandler(connect);
