package com.amurcanov.tgwsproxy

import android.util.Log
import okhttp3.ConnectionPool
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONArray
import org.json.JSONObject
import java.io.IOException
import java.security.SecureRandom
import java.util.concurrent.TimeUnit

/**
 * Tests the central #29 workaround hypothesis without changing production
 * routing: can high-entropy traffic cross the same Worker when every 8 KiB
 * chunk gets a brand-new HTTPS/TCP connection?
 */
object FreshConnectionWorkerProbe {
    private const val TAG = "TgWsProxy"
    private const val REVISION = "worker-fresh-connection-probe-v1"
    private const val CHUNK_BYTES = 8 * 1024
    private const val TARGET_BYTES = 1024 * 1024
    private const val TIMEOUT_SECONDS = 12L
    private val random = SecureRandom()
    private val octetStream = "application/octet-stream".toMediaType()

    private data class ProbeCase(
        val mode: String,
        val cumulativeBytes: Int,
        val ok: Boolean,
        val durationMs: Long,
        val error: String = "",
    ) {
        fun json(): JSONObject = JSONObject().apply {
            put("mode", mode)
            put("chunk_bytes", CHUNK_BYTES)
            put("cumulative_bytes", cumulativeBytes)
            put("ok", ok)
            put("duration_ms", durationMs)
            if (error.isNotBlank()) put("error", error)
        }
    }

    private fun client(): OkHttpClient = OkHttpClient.Builder()
        .protocols(listOf(Protocol.HTTP_1_1))
        .connectionPool(ConnectionPool(0, 1, TimeUnit.NANOSECONDS))
        .connectTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .readTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .writeTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .retryOnConnectionFailure(false)
        .build()

    private fun upload(domain: String): ProbeCase {
        val started = System.nanoTime()
        var cumulative = 0
        return try {
            while (cumulative < TARGET_BYTES) {
                val bytes = ByteArray(CHUNK_BYTES).also(random::nextBytes)
                val client = client()
                try {
                    val request = Request.Builder()
                        .url("https://$domain/diag/fresh-upload")
                        .header("Cache-Control", "no-store")
                        .header("Connection", "close")
                        .post(bytes.toRequestBody(octetStream))
                        .build()
                    client.newCall(request).execute().use { response ->
                        if (!response.isSuccessful) throw IOException("HTTP ${response.code}")
                        val body = response.body?.string().orEmpty()
                        if (!body.contains("\"bytes\":8192")) throw IOException("unexpected response: $body")
                    }
                } finally {
                    client.connectionPool.evictAll()
                    client.dispatcher.executorService.shutdown()
                }
                cumulative += CHUNK_BYTES
                if (cumulative == CHUNK_BYTES || cumulative % (64 * 1024) == 0 || cumulative == TARGET_BYTES) {
                    Log.i(TAG, "Worker fresh-connection probe mode=upload chunk_bytes=$CHUNK_BYTES cumulative_bytes=$cumulative ok=true")
                }
            }
            ProbeCase("fresh_upload", cumulative, true, elapsedMs(started))
        } catch (t: Throwable) {
            ProbeCase("fresh_upload", cumulative + CHUNK_BYTES, false, elapsedMs(started), "${t.javaClass.simpleName}:${t.message}")
        }
    }

    private fun download(domain: String): ProbeCase {
        val started = System.nanoTime()
        var cumulative = 0
        return try {
            while (cumulative < TARGET_BYTES) {
                val client = client()
                try {
                    val request = Request.Builder()
                        .url("https://$domain/diag/fresh-download?size=$CHUNK_BYTES")
                        .header("Cache-Control", "no-store")
                        .header("Connection", "close")
                        .get()
                        .build()
                    client.newCall(request).execute().use { response ->
                        if (!response.isSuccessful) throw IOException("HTTP ${response.code}")
                        val bytes = response.body?.bytes() ?: throw IOException("empty body")
                        if (bytes.size != CHUNK_BYTES) throw IOException("unexpected size=${bytes.size}")
                    }
                } finally {
                    client.connectionPool.evictAll()
                    client.dispatcher.executorService.shutdown()
                }
                cumulative += CHUNK_BYTES
                if (cumulative == CHUNK_BYTES || cumulative % (64 * 1024) == 0 || cumulative == TARGET_BYTES) {
                    Log.i(TAG, "Worker fresh-connection probe mode=download chunk_bytes=$CHUNK_BYTES cumulative_bytes=$cumulative ok=true")
                }
            }
            ProbeCase("fresh_download", cumulative, true, elapsedMs(started))
        } catch (t: Throwable) {
            ProbeCase("fresh_download", cumulative + CHUNK_BYTES, false, elapsedMs(started), "${t.javaClass.simpleName}:${t.message}")
        }
    }

    private fun elapsedMs(started: Long): Long = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)

    fun run(domainRaw: String): String {
        val domain = domainRaw.trim().removePrefix("https://").removePrefix("http://").trimEnd('/')
        val report = JSONObject()
        val cases = JSONArray()
        report.put("revision", REVISION)
        report.put("domain", domain)
        report.put("transport", "freshhttp")
        report.put("chunk_bytes", CHUNK_BYTES)
        report.put("target_bytes", TARGET_BYTES)
        report.put("cases", cases)

        if (domain.isBlank() || domain.contains('/') || domain.contains(' ')) {
            cases.put(ProbeCase("validation", 0, false, 0, "invalid_worker_domain").json())
            return report.toString()
        }

        Log.i(TAG, "Worker fresh-connection probe start domain=$domain revision=$REVISION chunk_bytes=$CHUNK_BYTES target_bytes=$TARGET_BYTES")
        val upload = upload(domain)
        Log.i(TAG, "Worker fresh-connection probe mode=${upload.mode} cumulative_bytes=${upload.cumulativeBytes} ok=${upload.ok} duration_ms=${upload.durationMs} error=${upload.error.ifBlank { "none" }}")
        cases.put(upload.json())

        val download = download(domain)
        Log.i(TAG, "Worker fresh-connection probe mode=${download.mode} cumulative_bytes=${download.cumulativeBytes} ok=${download.ok} duration_ms=${download.durationMs} error=${download.error.ifBlank { "none" }}")
        cases.put(download.json())
        Log.i(TAG, "Worker fresh-connection probe complete domain=$domain")
        return report.toString()
    }
}
