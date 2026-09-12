package com.amurcanov.tgwsproxy

import android.util.Log
import okhttp3.Dns
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString
import okio.ByteString.Companion.toByteString
import org.json.JSONArray
import org.json.JSONObject
import java.io.IOException
import java.net.Inet4Address
import java.net.Inet6Address
import java.net.InetAddress
import java.net.SocketTimeoutException
import java.net.UnknownHostException
import java.security.SecureRandom
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

/**
 * Android-side A/B probe for issue #29.
 *
 * All variants use the same OkHttp WebSocket implementation and Android TLS
 * provider. The only controlled variables are outbound per-message compression
 * and whether diagnostic payloads are deterministic/compressible or random.
 * Production proxy routing is not changed by this class.
 */
object OkHttpWorkerNetworkProbe {
    private const val TAG = "TgWsProxy"
    private const val CONNECT_TIMEOUT_SECONDS = 10L
    private const val OP_TIMEOUT_MILLIS = 12_000L
    private const val TARGET_UPLOAD_BYTES = 1024 * 1024
    private const val UPLOAD_CHUNK_BYTES = 4 * 1024
    private const val DEFAULT_MIN_COMPRESS_BYTES = 1024L

    private val secureRandom = SecureRandom()
    private val sizes = intArrayOf(
        1 * 1024,
        4 * 1024,
        8 * 1024,
        12 * 1024,
        16 * 1024,
        24 * 1024,
        32 * 1024,
        64 * 1024,
        128 * 1024,
        256 * 1024,
        512 * 1024,
        1024 * 1024,
    )

    private data class Config(
        val transport: String,
        val minCompressBytes: Long,
        val randomPayload: Boolean,
    ) {
        val payloadMode: String get() = if (randomPayload) "random" else "pattern"
        val compressionMode: String get() = if (minCompressBytes == Long.MAX_VALUE) "disabled" else "default"
    }

    private data class ProbeCase(
        val mode: String,
        val sizeBytes: Int = 0,
        val cumulativeBytes: Int = 0,
        val ok: Boolean = false,
        val durationMs: Long = 0,
        val transportState: String = "",
        val error: String = "",
    ) {
        fun toJson(): JSONObject = JSONObject().apply {
            put("mode", mode)
            if (sizeBytes != 0) put("size_bytes", sizeBytes)
            if (cumulativeBytes != 0) put("cumulative_bytes", cumulativeBytes)
            put("ok", ok)
            put("duration_ms", durationMs)
            if (transportState.isNotBlank()) put("transport_state", transportState)
            if (error.isNotBlank()) put("error", error)
        }
    }

    private sealed class Incoming {
        data class Binary(val value: ByteString) : Incoming()
        data class Text(val value: String) : Incoming()
        data class Failure(val message: String) : Incoming()
        data class Closed(val code: Int, val reason: String) : Incoming()
    }

    private class FamilyDns(private val family: String) : Dns {
        @Volatile
        var lastAddresses: List<InetAddress> = emptyList()
            private set

        override fun lookup(hostname: String): List<InetAddress> {
            val resolved = InetAddress.getAllByName(hostname).toList()
            val filtered = when (family) {
                "ipv4" -> resolved.filterIsInstance<Inet4Address>()
                "ipv6" -> resolved.filterIsInstance<Inet6Address>()
                else -> resolved
            }
            if (filtered.isEmpty()) {
                throw UnknownHostException("no_${family}_address_for:$hostname")
            }
            lastAddresses = filtered
            return filtered
        }

        fun summary(): String = lastAddresses.joinToString(",") { it.hostAddress ?: "?" }
    }

    private class Listener(
        private val dns: FamilyDns,
        private val config: Config,
    ) : WebSocketListener() {
        val openLatch = CountDownLatch(1)
        val incoming = LinkedBlockingQueue<Incoming>()

        @Volatile
        var openError: String = ""

        @Volatile
        var handshakeState: String = "stack=okhttp"

        override fun onOpen(webSocket: WebSocket, response: Response) {
            val handshake = response.handshake
            val revision = response.header("X-Tgws-Worker-Revision") ?: "unknown"
            val extensions = response.header("Sec-WebSocket-Extensions") ?: "none"
            handshakeState = buildString {
                append("stack=okhttp")
                append(" protocol=").append(response.protocol)
                append(" tls=").append(handshake?.tlsVersion?.javaName ?: "unknown")
                append(" cipher=").append(handshake?.cipherSuite?.javaName ?: "unknown")
                append(" worker_revision=").append(revision)
                append(" ws_extensions=").append(extensions.replace(' ', '_'))
                append(" compression=").append(config.compressionMode)
                append(" min_compress_bytes=").append(config.minCompressBytes)
                append(" payload_mode=").append(config.payloadMode)
                append(" dns=").append(dns.summary())
            }
            openLatch.countDown()
        }

        override fun onMessage(webSocket: WebSocket, bytes: ByteString) {
            incoming.offer(Incoming.Binary(bytes))
        }

        override fun onMessage(webSocket: WebSocket, text: String) {
            incoming.offer(Incoming.Text(text))
        }

        override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
            val status = response?.code?.let { " http_status=$it" }.orEmpty()
            val message = "${t.javaClass.simpleName}:${t.message ?: "unknown"}$status"
            openError = message
            incoming.offer(Incoming.Failure(message))
            openLatch.countDown()
        }

        override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
            incoming.offer(Incoming.Closed(code, reason))
        }
    }

    private data class Session(
        val webSocket: WebSocket,
        val listener: Listener,
    ) {
        fun state(): String = listener.handshakeState + " queue_bytes=" + webSocket.queueSize()

        fun receive(timeoutMillis: Long = OP_TIMEOUT_MILLIS): Incoming {
            val event = listener.incoming.poll(timeoutMillis, TimeUnit.MILLISECONDS)
                ?: throw SocketTimeoutException("okhttp_websocket_receive_timeout")
            when (event) {
                is Incoming.Failure -> throw IOException(event.message)
                is Incoming.Closed -> throw IOException("websocket_closed:${event.code}:${event.reason}")
                else -> return event
            }
        }

        fun close() {
            if (!webSocket.close(1000, "probe_done")) {
                webSocket.cancel()
            }
        }
    }

    private fun normalizeFamily(family: String): String = when (family.trim().lowercase()) {
        "ipv4" -> "ipv4"
        "ipv6" -> "ipv6"
        else -> "auto"
    }

    private fun configFor(transportRaw: String): Config {
        return when (transportRaw.trim().lowercase()) {
            "okhttpnocompression", "okhttp_no_compression" -> Config(
                transport = "okhttp_no_compression",
                minCompressBytes = Long.MAX_VALUE,
                randomPayload = false,
            )
            "okhttprandom", "okhttp_random" -> Config(
                transport = "okhttp_random",
                minCompressBytes = DEFAULT_MIN_COMPRESS_BYTES,
                randomPayload = true,
            )
            "okhttpnocompressionrandom", "okhttp_no_compression_random" -> Config(
                transport = "okhttp_no_compression_random",
                minCompressBytes = Long.MAX_VALUE,
                randomPayload = true,
            )
            else -> Config(
                transport = "okhttp",
                minCompressBytes = DEFAULT_MIN_COMPRESS_BYTES,
                randomPayload = false,
            )
        }
    }

    private fun payload(size: Int, seedOffset: Int, config: Config): ByteArray {
        if (config.randomPayload) {
            return ByteArray(size).also(secureRandom::nextBytes)
        }
        return ByteArray(size) { i -> ((i + seedOffset) and 0xff).toByte() }
    }

    private fun open(
        client: OkHttpClient,
        dns: FamilyDns,
        domain: String,
        path: String,
        config: Config,
    ): Session {
        val listener = Listener(dns, config)
        val request = Request.Builder()
            .url("wss://$domain$path")
            .header("Sec-WebSocket-Protocol", "binary")
            .build()
        val webSocket = client.newWebSocket(request, listener)
        if (!listener.openLatch.await(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            webSocket.cancel()
            throw SocketTimeoutException("okhttp_websocket_connect_timeout")
        }
        if (listener.openError.isNotBlank()) {
            webSocket.cancel()
            throw IOException(listener.openError)
        }
        return Session(webSocket, listener)
    }

    private fun logCase(case: ProbeCase, config: Config) {
        Log.i(
            TAG,
            "Worker network probe transport=${config.transport} mode=${case.mode} " +
                "size_bytes=${case.sizeBytes} cumulative_bytes=${case.cumulativeBytes} " +
                "ok=${case.ok} duration_ms=${case.durationMs} " +
                "error=${case.error.ifBlank { "none" }} ${case.transportState}",
        )
    }

    private fun echoCases(
        client: OkHttpClient,
        dns: FamilyDns,
        domain: String,
        config: Config,
    ): List<ProbeCase> {
        val cases = mutableListOf<ProbeCase>()
        val session = try {
            open(client, dns, domain, "/diag/ws-echo?sid=probe-${config.transport}-echo", config)
        } catch (t: Throwable) {
            return listOf(
                ProbeCase(
                    mode = "echo_staircase_connect",
                    error = "${t.javaClass.simpleName}:${t.message}",
                ).also { logCase(it, config) },
            )
        }

        var cumulative = 0
        try {
            for ((index, size) in sizes.withIndex()) {
                val bytes = payload(size, index, config)
                val started = System.nanoTime()
                var error = ""
                var ok = false
                try {
                    if (!session.webSocket.send(bytes.toByteString())) {
                        throw IOException("okhttp_send_rejected")
                    }
                    val received = session.receive()
                    if (received !is Incoming.Binary) {
                        throw IOException("unexpected_echo_type:${received.javaClass.simpleName}")
                    }
                    if (received.value.size != bytes.size || !received.value.toByteArray().contentEquals(bytes)) {
                        throw IOException("echo_payload_mismatch:${received.value.size}")
                    }
                    cumulative += size
                    ok = true
                } catch (t: Throwable) {
                    error = "${t.javaClass.simpleName}:${t.message}"
                }
                val case = ProbeCase(
                    mode = "echo_staircase",
                    sizeBytes = size,
                    cumulativeBytes = if (ok) cumulative else cumulative + size,
                    ok = ok,
                    durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started),
                    transportState = session.state(),
                    error = error,
                )
                logCase(case, config)
                cases += case
                if (!ok) break
            }
        } finally {
            session.close()
        }
        return cases
    }

    private fun uploadCases(
        client: OkHttpClient,
        dns: FamilyDns,
        domain: String,
        config: Config,
    ): List<ProbeCase> {
        val cases = mutableListOf<ProbeCase>()
        val session = try {
            open(client, dns, domain, "/diag/upload?sid=probe-${config.transport}-upload-4k", config)
        } catch (t: Throwable) {
            return listOf(
                ProbeCase(
                    mode = "upload_4k_connect",
                    error = "${t.javaClass.simpleName}:${t.message}",
                ).also { logCase(it, config) },
            )
        }

        var cumulative = 0
        try {
            while (cumulative < TARGET_UPLOAD_BYTES) {
                val chunkIndex = cumulative / UPLOAD_CHUNK_BYTES
                val bytes = payload(UPLOAD_CHUNK_BYTES, chunkIndex, config)
                val started = System.nanoTime()
                var error = ""
                var ok = false
                try {
                    if (!session.webSocket.send(bytes.toByteString())) {
                        throw IOException("okhttp_send_rejected")
                    }
                    val incoming = session.receive()
                    val ack = when (incoming) {
                        is Incoming.Text -> incoming.value
                        is Incoming.Binary -> incoming.value.utf8()
                        else -> throw IOException("unexpected_ack_type")
                    }
                    val next = cumulative + UPLOAD_CHUNK_BYTES
                    if (ack != "ack:$next") {
                        throw IOException("unexpected_ack:$ack")
                    }
                    cumulative = next
                    ok = true
                } catch (t: Throwable) {
                    error = "${t.javaClass.simpleName}:${t.message}"
                }

                if (!ok || cumulative == UPLOAD_CHUNK_BYTES || cumulative % (64 * 1024) == 0 || cumulative == TARGET_UPLOAD_BYTES) {
                    val case = ProbeCase(
                        mode = "upload_4k_stream",
                        sizeBytes = UPLOAD_CHUNK_BYTES,
                        cumulativeBytes = if (ok) cumulative else cumulative + UPLOAD_CHUNK_BYTES,
                        ok = ok,
                        durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started),
                        transportState = session.state(),
                        error = error,
                    )
                    logCase(case, config)
                    cases += case
                }
                if (!ok) break
            }
        } finally {
            session.close()
        }
        return cases
    }

    private fun downloadCases(
        client: OkHttpClient,
        dns: FamilyDns,
        domain: String,
        config: Config,
    ): List<ProbeCase> {
        val cases = mutableListOf<ProbeCase>()
        for (size in sizes) {
            val started = System.nanoTime()
            var session: Session? = null
            var error = ""
            var ok = false
            try {
                session = open(
                    client,
                    dns,
                    domain,
                    "/diag/download?size=$size&sid=probe-${config.transport}-download-$size",
                    config,
                )
                val incoming = session.receive()
                if (incoming !is Incoming.Binary) {
                    throw IOException("unexpected_download_type:${incoming.javaClass.simpleName}")
                }
                if (incoming.value.size != size) {
                    throw IOException("download_size_mismatch:${incoming.value.size}")
                }
                ok = true
            } catch (t: Throwable) {
                error = "${t.javaClass.simpleName}:${t.message}"
            }
            val case = ProbeCase(
                mode = "download_single",
                sizeBytes = size,
                ok = ok,
                durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started),
                transportState = session?.state().orEmpty(),
                error = error,
            )
            logCase(case, config)
            cases += case
            session?.close()
            if (!ok) break
        }
        return cases
    }

    fun run(domainRaw: String, familyRaw: String, transportRaw: String = "okhttp"): String {
        val domain = domainRaw.trim().removePrefix("https://").removePrefix("http://").trimEnd('/')
        val family = normalizeFamily(familyRaw)
        val config = configFor(transportRaw)
        val report = JSONObject()
        val casesJson = JSONArray()
        report.put("revision", "worker-network-probe-okhttp-v2")
        report.put("domain", domain)
        report.put("ip_family", family)
        report.put("transport", config.transport)
        report.put("compression", config.compressionMode)
        report.put("min_compress_bytes", config.minCompressBytes)
        report.put("payload_mode", config.payloadMode)
        report.put("started_ms", System.currentTimeMillis())
        report.put("cases", casesJson)

        if (domain.isBlank() || domain.contains('/') || domain.contains(' ')) {
            casesJson.put(ProbeCase(mode = "validation", error = "invalid_worker_domain").toJson())
            return report.toString()
        }

        val dns = FamilyDns(family)
        val client = OkHttpClient.Builder()
            .dns(dns)
            .connectTimeout(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .retryOnConnectionFailure(false)
            .minWebSocketMessageToCompress(config.minCompressBytes)
            .build()

        Log.i(
            TAG,
            "Worker network probe start domain=$domain ip_family=$family transport=${config.transport} " +
                "revision=worker-network-probe-okhttp-v2 compression=${config.compressionMode} " +
                "min_compress_bytes=${config.minCompressBytes} payload_mode=${config.payloadMode}",
        )
        val cases = mutableListOf<ProbeCase>()
        try {
            cases += echoCases(client, dns, domain, config)
            cases += uploadCases(client, dns, domain, config)
            cases += downloadCases(client, dns, domain, config)
        } finally {
            client.connectionPool.evictAll()
            client.dispatcher.executorService.shutdown()
        }
        cases.forEach { casesJson.put(it.toJson()) }
        Log.i(TAG, "Worker network probe complete domain=$domain ip_family=$family transport=${config.transport} cases=${cases.size}")
        return report.toString()
    }
}
