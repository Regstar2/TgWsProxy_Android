package com.amurcanov.tgwsproxy

import android.os.SystemClock
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
import kotlin.math.min

/**
 * Tests the #29 short-connection workaround without changing production
 * routing. Every chunk is sent over a brand-new HTTP/1.1/TLS connection.
 * Profiles isolate retry/backoff, pacing, and a larger 12 KiB chunk size.
 */
object FreshConnectionWorkerProbe {
    private const val TAG = "TgWsProxy"
    private const val REVISION = "worker-fresh-connection-probe-v2"
    private const val TARGET_BYTES = 1024 * 1024
    private const val TIMEOUT_SECONDS = 12L
    private val random = SecureRandom()
    private val octetStream = "application/octet-stream".toMediaType()

    private data class Profile(
        val transport: String,
        val chunkBytes: Int,
        val maxRetries: Int,
        val baseBackoffMs: Long,
        val paceMs: Long,
    )

    private data class ProbeCase(
        val mode: String,
        val confirmedBytes: Int,
        val attemptedBytes: Int,
        val ok: Boolean,
        val durationMs: Long,
        val retriesUsed: Int,
        val error: String = "",
    ) {
        fun json(profile: Profile): JSONObject = JSONObject().apply {
            put("mode", mode)
            put("chunk_bytes", profile.chunkBytes)
            put("confirmed_bytes", confirmedBytes)
            put("cumulative_bytes", confirmedBytes)
            put("attempted_cumulative_bytes", attemptedBytes)
            put("ok", ok)
            put("duration_ms", durationMs)
            put("retries_used", retriesUsed)
            if (error.isNotBlank()) put("error", error)
        }
    }

    private fun profileFor(transportRaw: String): Profile = when (transportRaw.trim().lowercase()) {
        "freshhttpretry8k" -> Profile("freshhttpretry8k", 8 * 1024, 3, 300, 0)
        "freshhttppaced8k" -> Profile("freshhttppaced8k", 8 * 1024, 3, 300, 75)
        "freshhttpretry12k" -> Profile("freshhttpretry12k", 12 * 1024, 3, 300, 0)
        else -> Profile("freshhttp", 8 * 1024, 0, 0, 0)
    }

    private fun client(): OkHttpClient = OkHttpClient.Builder()
        .protocols(listOf(Protocol.HTTP_1_1))
        .connectionPool(ConnectionPool(0, 1, TimeUnit.NANOSECONDS))
        .connectTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .readTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .writeTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .retryOnConnectionFailure(false)
        .build()

    private fun backoff(profile: Profile, retryIndex: Int) {
        if (profile.baseBackoffMs <= 0) return
        val multiplier = 1L shl min(retryIndex, 4)
        SystemClock.sleep(profile.baseBackoffMs * multiplier)
    }

    private fun <T> withFreshClient(block: (OkHttpClient) -> T): T {
        val client = client()
        return try {
            block(client)
        } finally {
            client.connectionPool.evictAll()
            client.dispatcher.executorService.shutdown()
        }
    }

    private fun uploadChunk(domain: String, bytes: ByteArray) {
        withFreshClient { client ->
            val request = Request.Builder()
                .url("https://$domain/diag/fresh-upload")
                .header("Cache-Control", "no-store")
                .header("Connection", "close")
                .post(bytes.toRequestBody(octetStream))
                .build()
            client.newCall(request).execute().use { response ->
                if (!response.isSuccessful) throw IOException("HTTP ${response.code}")
                val body = response.body?.string().orEmpty()
                if (!body.contains("\"bytes\":${bytes.size}")) {
                    throw IOException("unexpected response: $body")
                }
            }
        }
    }

    private fun downloadChunk(domain: String, size: Int) {
        withFreshClient { client ->
            val request = Request.Builder()
                .url("https://$domain/diag/fresh-download?size=$size")
                .header("Cache-Control", "no-store")
                .header("Connection", "close")
                .get()
                .build()
            client.newCall(request).execute().use { response ->
                if (!response.isSuccessful) throw IOException("HTTP ${response.code}")
                val bytes = response.body?.bytes() ?: throw IOException("empty body")
                if (bytes.size != size) throw IOException("unexpected size=${bytes.size}")
            }
        }
    }

    private fun upload(domain: String, profile: Profile): ProbeCase {
        val started = System.nanoTime()
        var confirmed = 0
        var retriesUsed = 0
        while (confirmed < TARGET_BYTES) {
            val size = min(profile.chunkBytes, TARGET_BYTES - confirmed)
            val bytes = ByteArray(size).also(random::nextBytes)
            var lastError: Throwable? = null
            var delivered = false
            for (attempt in 0..profile.maxRetries) {
                try {
                    uploadChunk(domain, bytes)
                    delivered = true
                    break
                } catch (t: Throwable) {
                    lastError = t
                    if (attempt >= profile.maxRetries) break
                    retriesUsed++
                    Log.i(
                        TAG,
                        "Worker fresh-connection probe mode=upload retry=${attempt + 1}/${profile.maxRetries} " +
                            "chunk_bytes=$size confirmed_bytes=$confirmed error=${t.javaClass.simpleName}:${t.message}",
                    )
                    backoff(profile, attempt)
                }
            }
            if (!delivered) {
                val attempted = min(TARGET_BYTES, confirmed + size)
                return ProbeCase(
                    "fresh_upload",
                    confirmed,
                    attempted,
                    false,
                    elapsedMs(started),
                    retriesUsed,
                    "${lastError?.javaClass?.simpleName}:${lastError?.message}",
                )
            }
            confirmed += size
            if (confirmed == size || confirmed % (64 * 1024) < size || confirmed == TARGET_BYTES) {
                Log.i(
                    TAG,
                    "Worker fresh-connection probe mode=upload chunk_bytes=$size cumulative_bytes=$confirmed " +
                        "retries_used=$retriesUsed ok=true",
                )
            }
            if (profile.paceMs > 0 && confirmed < TARGET_BYTES) SystemClock.sleep(profile.paceMs)
        }
        return ProbeCase("fresh_upload", confirmed, confirmed, true, elapsedMs(started), retriesUsed)
    }

    private fun download(domain: String, profile: Profile): ProbeCase {
        val started = System.nanoTime()
        var confirmed = 0
        var retriesUsed = 0
        while (confirmed < TARGET_BYTES) {
            val size = min(profile.chunkBytes, TARGET_BYTES - confirmed)
            var lastError: Throwable? = null
            var delivered = false
            for (attempt in 0..profile.maxRetries) {
                try {
                    downloadChunk(domain, size)
                    delivered = true
                    break
                } catch (t: Throwable) {
                    lastError = t
                    if (attempt >= profile.maxRetries) break
                    retriesUsed++
                    Log.i(
                        TAG,
                        "Worker fresh-connection probe mode=download retry=${attempt + 1}/${profile.maxRetries} " +
                            "chunk_bytes=$size confirmed_bytes=$confirmed error=${t.javaClass.simpleName}:${t.message}",
                    )
                    backoff(profile, attempt)
                }
            }
            if (!delivered) {
                val attempted = min(TARGET_BYTES, confirmed + size)
                return ProbeCase(
                    "fresh_download",
                    confirmed,
                    attempted,
                    false,
                    elapsedMs(started),
                    retriesUsed,
                    "${lastError?.javaClass?.simpleName}:${lastError?.message}",
                )
            }
            confirmed += size
            if (confirmed == size || confirmed % (64 * 1024) < size || confirmed == TARGET_BYTES) {
                Log.i(
                    TAG,
                    "Worker fresh-connection probe mode=download chunk_bytes=$size cumulative_bytes=$confirmed " +
                        "retries_used=$retriesUsed ok=true",
                )
            }
            if (profile.paceMs > 0 && confirmed < TARGET_BYTES) SystemClock.sleep(profile.paceMs)
        }
        return ProbeCase("fresh_download", confirmed, confirmed, true, elapsedMs(started), retriesUsed)
    }

    private fun elapsedMs(started: Long): Long = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)

    fun run(domainRaw: String, transportRaw: String = "freshhttp"): String {
        val domain = domainRaw.trim().removePrefix("https://").removePrefix("http://").trimEnd('/')
        val profile = profileFor(transportRaw)
        val report = JSONObject()
        val cases = JSONArray()
        report.put("revision", REVISION)
        report.put("domain", domain)
        report.put("transport", profile.transport)
        report.put("chunk_bytes", profile.chunkBytes)
        report.put("max_retries", profile.maxRetries)
        report.put("base_backoff_ms", profile.baseBackoffMs)
        report.put("pace_ms", profile.paceMs)
        report.put("target_bytes", TARGET_BYTES)
        report.put("cases", cases)

        if (domain.isBlank() || domain.contains('/') || domain.contains(' ')) {
            cases.put(ProbeCase("validation", 0, 0, false, 0, 0, "invalid_worker_domain").json(profile))
            return report.toString()
        }

        Log.i(
            TAG,
            "Worker fresh-connection probe start domain=$domain revision=$REVISION transport=${profile.transport} " +
                "chunk_bytes=${profile.chunkBytes} max_retries=${profile.maxRetries} " +
                "base_backoff_ms=${profile.baseBackoffMs} pace_ms=${profile.paceMs} target_bytes=$TARGET_BYTES",
        )

        val upload = upload(domain, profile)
        Log.i(
            TAG,
            "Worker fresh-connection probe mode=${upload.mode} confirmed_bytes=${upload.confirmedBytes} " +
                "attempted_cumulative_bytes=${upload.attemptedBytes} ok=${upload.ok} duration_ms=${upload.durationMs} " +
                "retries_used=${upload.retriesUsed} error=${upload.error.ifBlank { "none" }}",
        )
        cases.put(upload.json(profile))

        val download = download(domain, profile)
        Log.i(
            TAG,
            "Worker fresh-connection probe mode=${download.mode} confirmed_bytes=${download.confirmedBytes} " +
                "attempted_cumulative_bytes=${download.attemptedBytes} ok=${download.ok} duration_ms=${download.durationMs} " +
                "retries_used=${download.retriesUsed} error=${download.error.ifBlank { "none" }}",
        )
        cases.put(download.json(profile))
        Log.i(TAG, "Worker fresh-connection probe complete domain=$domain transport=${profile.transport}")
        return report.toString()
    }
}
