package com.amurcanov.tgwsproxy

import android.util.Log
import org.json.JSONArray
import org.json.JSONObject
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.ByteArrayOutputStream
import java.io.EOFException
import java.io.IOException
import java.net.Inet4Address
import java.net.Inet6Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket
import java.net.SocketTimeoutException
import java.net.UnknownHostException
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.security.SecureRandom
import java.util.Base64
import java.util.concurrent.TimeUnit
import javax.net.ssl.SNIHostName
import javax.net.ssl.SSLSocket
import javax.net.ssl.SSLSocketFactory

/**
 * Diagnostic Worker probe for issue #29 using Android's platform SSLSocket,
 * a manual HTTP/1.1 WebSocket upgrade and manual RFC 6455 framing.
 *
 * This deliberately removes OkHttp's WebSocket implementation while keeping
 * the Android TLS/socket path. Production proxy routing is not changed.
 */
object AndroidSslSocketWorkerNetworkProbe {
    private const val TAG = "TgWsProxy"
    private const val CONNECT_TIMEOUT_MILLIS = 10_000
    private const val OP_TIMEOUT_MILLIS = 12_000
    private const val TARGET_UPLOAD_BYTES = 1024 * 1024
    private const val UPLOAD_CHUNK_BYTES = 4 * 1024
    private const val MAX_MESSAGE_BYTES = 16 * 1024 * 1024
    private const val MAX_HTTP_HEADER_BYTES = 64 * 1024
    private const val WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

    private val random = SecureRandom()
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

    private data class Message(val opcode: Int, val payload: ByteArray)

    private class Session(
        private val rawSocket: Socket,
        private val sslSocket: SSLSocket,
        private val input: BufferedInputStream,
        private val output: BufferedOutputStream,
        private val workerRevision: String,
        private val dnsSummary: String,
    ) {
        fun state(): String = buildString {
            append("stack=android_sslsocket")
            append(" tls=").append(sslSocket.session.protocol ?: "unknown")
            append(" cipher=").append(sslSocket.session.cipherSuite ?: "unknown")
            append(" remote=").append(rawSocket.remoteSocketAddress ?: "unknown")
            append(" worker_revision=").append(workerRevision)
            append(" dns=").append(dnsSummary)
        }

        fun sendBinary(payload: ByteArray) {
            sendFrame(0x2, payload)
        }

        fun receiveMessage(): Message {
            var initialOpcode = -1
            val aggregate = ByteArrayOutputStream()
            while (true) {
                val first = readRequiredByte(input)
                val second = readRequiredByte(input)
                val fin = first and 0x80 != 0
                val opcode = first and 0x0f
                var length = (second and 0x7f).toLong()
                if (length == 126L) {
                    length = ((readRequiredByte(input) shl 8) or readRequiredByte(input)).toLong()
                } else if (length == 127L) {
                    length = 0
                    repeat(8) {
                        length = (length shl 8) or readRequiredByte(input).toLong()
                    }
                }
                if (length < 0 || length > MAX_MESSAGE_BYTES.toLong()) {
                    throw IOException("websocket_frame_too_large:$length")
                }

                val masked = second and 0x80 != 0
                val mask = if (masked) ByteArray(4).also { readFully(input, it) } else null
                val payload = ByteArray(length.toInt())
                readFully(input, payload)
                if (mask != null) {
                    for (i in payload.indices) {
                        payload[i] = (payload[i].toInt() xor mask[i and 3].toInt()).toByte()
                    }
                }

                when (opcode) {
                    0x8 -> throw EOFException("websocket_close")
                    0x9 -> {
                        sendFrame(0xA, payload)
                        continue
                    }
                    0xA -> continue
                    0x1, 0x2 -> {
                        if (initialOpcode != -1) throw IOException("unexpected_new_data_frame")
                        initialOpcode = opcode
                        aggregate.write(payload)
                    }
                    0x0 -> {
                        if (initialOpcode == -1) throw IOException("unexpected_continuation")
                        aggregate.write(payload)
                    }
                    else -> throw IOException("unsupported_websocket_opcode:$opcode")
                }

                if (aggregate.size() > MAX_MESSAGE_BYTES) {
                    throw IOException("websocket_message_too_large:${aggregate.size()}")
                }
                if (fin && initialOpcode != -1) {
                    return Message(initialOpcode, aggregate.toByteArray())
                }
            }
        }

        private fun sendFrame(opcode: Int, payload: ByteArray) {
            if (payload.size > MAX_MESSAGE_BYTES) throw IOException("websocket_message_too_large:${payload.size}")
            val header = ByteArrayOutputStream(14)
            header.write(0x80 or opcode)
            when {
                payload.size < 126 -> header.write(0x80 or payload.size)
                payload.size <= 0xffff -> {
                    header.write(0x80 or 126)
                    header.write((payload.size ushr 8) and 0xff)
                    header.write(payload.size and 0xff)
                }
                else -> {
                    header.write(0x80 or 127)
                    val value = payload.size.toLong()
                    for (shift in 56 downTo 0 step 8) {
                        header.write(((value ushr shift) and 0xff).toInt())
                    }
                }
            }
            val mask = ByteArray(4).also(random::nextBytes)
            header.write(mask)
            val masked = ByteArray(payload.size)
            for (i in payload.indices) {
                masked[i] = (payload[i].toInt() xor mask[i and 3].toInt()).toByte()
            }
            output.write(header.toByteArray())
            output.write(masked)
            output.flush()
        }

        fun close() {
            try {
                sslSocket.close()
            } catch (_: Throwable) {
                try {
                    rawSocket.close()
                } catch (_: Throwable) {
                }
            }
        }
    }

    private fun normalizeFamily(family: String): String = when (family.trim().lowercase()) {
        "ipv4" -> "ipv4"
        "ipv6" -> "ipv6"
        else -> "auto"
    }

    private fun resolve(domain: String, family: String): List<InetAddress> {
        val resolved = InetAddress.getAllByName(domain).toList()
        val filtered = when (family) {
            "ipv4" -> resolved.filterIsInstance<Inet4Address>()
            "ipv6" -> resolved.filterIsInstance<Inet6Address>()
            else -> resolved
        }
        if (filtered.isEmpty()) throw UnknownHostException("no_${family}_address_for:$domain")
        return filtered
    }

    private fun open(domain: String, path: String, family: String): Session {
        val addresses = resolve(domain, family)
        var rawSocket: Socket? = null
        var lastError: Throwable? = null
        for (address in addresses) {
            val candidate = Socket()
            try {
                candidate.tcpNoDelay = true
                candidate.connect(InetSocketAddress(address, 443), CONNECT_TIMEOUT_MILLIS)
                rawSocket = candidate
                break
            } catch (t: Throwable) {
                lastError = t
                try {
                    candidate.close()
                } catch (_: Throwable) {
                }
            }
        }
        val connected = rawSocket ?: throw IOException("tcp_connect_failed", lastError)
        connected.soTimeout = OP_TIMEOUT_MILLIS

        val sslSocket = try {
            (SSLSocketFactory.getDefault().createSocket(connected, domain, 443, true) as SSLSocket).apply {
                soTimeout = OP_TIMEOUT_MILLIS
                val params = sslParameters
                params.serverNames = listOf(SNIHostName(domain))
                params.endpointIdentificationAlgorithm = "HTTPS"
                sslParameters = params
                startHandshake()
            }
        } catch (t: Throwable) {
            try {
                connected.close()
            } catch (_: Throwable) {
            }
            throw t
        }

        val input = BufferedInputStream(sslSocket.inputStream, 32 * 1024)
        val output = BufferedOutputStream(sslSocket.outputStream, 32 * 1024)
        val keyBytes = ByteArray(16).also(random::nextBytes)
        val key = Base64.getEncoder().encodeToString(keyBytes)
        val request = buildString {
            append("GET ").append(path).append(" HTTP/1.1\r\n")
            append("Host: ").append(domain).append("\r\n")
            append("Upgrade: websocket\r\n")
            append("Connection: Upgrade\r\n")
            append("Sec-WebSocket-Key: ").append(key).append("\r\n")
            append("Sec-WebSocket-Version: 13\r\n")
            append("Sec-WebSocket-Protocol: binary\r\n")
            append("\r\n")
        }
        output.write(request.toByteArray(StandardCharsets.US_ASCII))
        output.flush()

        val headerBytes = readHttpHeaders(input)
        val headerText = String(headerBytes, StandardCharsets.ISO_8859_1)
        val lines = headerText.split("\r\n")
        val statusLine = lines.firstOrNull().orEmpty()
        if (!statusLine.matches(Regex("HTTP/1\\.[01] 101(?: .*)?"))) {
            sslSocket.close()
            throw IOException("websocket_upgrade_failed:$statusLine")
        }
        val headers = linkedMapOf<String, String>()
        lines.drop(1).forEach { line ->
            val index = line.indexOf(':')
            if (index > 0) {
                headers[line.substring(0, index).trim().lowercase()] = line.substring(index + 1).trim()
            }
        }
        val expectedAccept = Base64.getEncoder().encodeToString(
            MessageDigest.getInstance("SHA-1").digest((key + WS_GUID).toByteArray(StandardCharsets.US_ASCII)),
        )
        if (headers["sec-websocket-accept"] != expectedAccept) {
            sslSocket.close()
            throw IOException("invalid_websocket_accept")
        }

        return Session(
            rawSocket = connected,
            sslSocket = sslSocket,
            input = input,
            output = output,
            workerRevision = headers["x-tgws-worker-revision"] ?: "unknown",
            dnsSummary = addresses.joinToString(",") { it.hostAddress ?: "?" },
        )
    }

    private fun readHttpHeaders(input: BufferedInputStream): ByteArray {
        val out = ByteArrayOutputStream()
        var state = 0
        while (out.size() < MAX_HTTP_HEADER_BYTES) {
            val value = input.read()
            if (value < 0) throw EOFException("http_upgrade_eof")
            out.write(value)
            state = when {
                state == 0 && value == '\r'.code -> 1
                state == 1 && value == '\n'.code -> 2
                state == 2 && value == '\r'.code -> 3
                state == 3 && value == '\n'.code -> 4
                value == '\r'.code -> 1
                else -> 0
            }
            if (state == 4) return out.toByteArray()
        }
        throw IOException("http_headers_too_large")
    }

    private fun readRequiredByte(input: BufferedInputStream): Int {
        val value = input.read()
        if (value < 0) throw EOFException("websocket_eof")
        return value
    }

    private fun readFully(input: BufferedInputStream, target: ByteArray) {
        var offset = 0
        while (offset < target.size) {
            val count = input.read(target, offset, target.size - offset)
            if (count < 0) throw EOFException("websocket_payload_eof")
            offset += count
        }
    }

    private fun logCase(case: ProbeCase) {
        Log.i(
            TAG,
            "Worker network probe transport=sslsocket mode=${case.mode} " +
                "size_bytes=${case.sizeBytes} cumulative_bytes=${case.cumulativeBytes} " +
                "ok=${case.ok} duration_ms=${case.durationMs} " +
                "error=${case.error.ifBlank { "none" }} ${case.transportState}",
        )
    }

    private fun echoCases(domain: String, family: String): List<ProbeCase> {
        val cases = mutableListOf<ProbeCase>()
        val session = try {
            open(domain, "/diag/ws-echo?sid=probe-sslsocket-echo", family)
        } catch (t: Throwable) {
            return listOf(ProbeCase(mode = "echo_staircase_connect", error = "${t.javaClass.simpleName}:${t.message}").also(::logCase))
        }
        var cumulative = 0
        try {
            for ((index, size) in sizes.withIndex()) {
                val payload = ByteArray(size) { i -> ((i + index) and 0xff).toByte() }
                val started = System.nanoTime()
                var error = ""
                var ok = false
                try {
                    session.sendBinary(payload)
                    val received = session.receiveMessage()
                    if (received.opcode != 0x2) throw IOException("unexpected_echo_opcode:${received.opcode}")
                    if (!received.payload.contentEquals(payload)) throw IOException("echo_payload_mismatch:${received.payload.size}")
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
                logCase(case)
                cases += case
                if (!ok) break
            }
        } finally {
            session.close()
        }
        return cases
    }

    private fun uploadCases(domain: String, family: String): List<ProbeCase> {
        val cases = mutableListOf<ProbeCase>()
        val session = try {
            open(domain, "/diag/upload?sid=probe-sslsocket-upload-4k", family)
        } catch (t: Throwable) {
            return listOf(ProbeCase(mode = "upload_4k_connect", error = "${t.javaClass.simpleName}:${t.message}").also(::logCase))
        }
        var cumulative = 0
        try {
            while (cumulative < TARGET_UPLOAD_BYTES) {
                val chunkIndex = cumulative / UPLOAD_CHUNK_BYTES
                val payload = ByteArray(UPLOAD_CHUNK_BYTES) { i -> ((i + chunkIndex) and 0xff).toByte() }
                val started = System.nanoTime()
                var error = ""
                var ok = false
                try {
                    session.sendBinary(payload)
                    val incoming = session.receiveMessage()
                    val ack = String(incoming.payload, StandardCharsets.UTF_8)
                    val next = cumulative + UPLOAD_CHUNK_BYTES
                    if (ack != "ack:$next") throw IOException("unexpected_ack:$ack")
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
                    logCase(case)
                    cases += case
                }
                if (!ok) break
            }
        } finally {
            session.close()
        }
        return cases
    }

    private fun downloadCases(domain: String, family: String): List<ProbeCase> {
        val cases = mutableListOf<ProbeCase>()
        for (size in sizes) {
            val started = System.nanoTime()
            var session: Session? = null
            var error = ""
            var ok = false
            try {
                session = open(domain, "/diag/download?size=$size&sid=probe-sslsocket-download-$size", family)
                val incoming = session.receiveMessage()
                if (incoming.opcode != 0x2) throw IOException("unexpected_download_opcode:${incoming.opcode}")
                if (incoming.payload.size != size) throw IOException("download_size_mismatch:${incoming.payload.size}")
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
            logCase(case)
            cases += case
            session?.close()
            if (!ok) break
        }
        return cases
    }

    fun run(domainRaw: String, familyRaw: String): String {
        val domain = domainRaw.trim().removePrefix("https://").removePrefix("http://").trimEnd('/')
        val family = normalizeFamily(familyRaw)
        val report = JSONObject()
        val casesJson = JSONArray()
        report.put("revision", "worker-network-probe-sslsocket-v1")
        report.put("domain", domain)
        report.put("ip_family", family)
        report.put("transport", "sslsocket")
        report.put("started_ms", System.currentTimeMillis())
        report.put("cases", casesJson)

        if (domain.isBlank() || domain.contains('/') || domain.contains(' ')) {
            casesJson.put(ProbeCase(mode = "validation", error = "invalid_worker_domain").toJson())
            return report.toString()
        }

        Log.i(TAG, "Worker network probe start domain=$domain ip_family=$family transport=sslsocket revision=worker-network-probe-sslsocket-v1")
        val cases = mutableListOf<ProbeCase>()
        cases += echoCases(domain, family)
        cases += uploadCases(domain, family)
        cases += downloadCases(domain, family)
        cases.forEach { casesJson.put(it.toJson()) }
        Log.i(TAG, "Worker network probe complete domain=$domain ip_family=$family transport=sslsocket cases=${cases.size}")
        return report.toString()
    }
}
