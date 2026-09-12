package com.amurcanov.tgwsproxy

import android.app.Activity
import android.os.Bundle
import android.util.Log

class WorkerNetworkProbeActivity : Activity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val domain = intent?.getStringExtra(EXTRA_DOMAIN).orEmpty().trim()
        val family = intent?.getStringExtra(EXTRA_FAMILY).orEmpty().trim().ifEmpty { "auto" }
        val transport = intent?.getStringExtra(EXTRA_TRANSPORT).orEmpty().trim().lowercase().ifEmpty { "go" }
        if (domain.isEmpty()) {
            Log.e(TAG, "PROBE_DONE error=missing_domain")
            finish()
            return
        }

        Thread {
            try {
                Log.i(TAG, "PROBE_START domain=$domain ip_family=$family transport=$transport")
                val report = when (transport) {
                    "okhttp",
                    "okhttpnocompression",
                    "okhttprandom",
                    "okhttpnocompressionrandom" -> OkHttpWorkerNetworkProbe.run(domain, family, transport)
                    "sslsocket" -> AndroidSslSocketWorkerNetworkProbe.run(domain, family)
                    "freshhttp" -> FreshConnectionWorkerProbe.run(domain)
                    else -> NativeProxy.runWorkerNetworkProbe(domain, family, transport).orEmpty()
                }
                if (report.isEmpty()) {
                    Log.e(TAG, "PROBE_RESULT empty")
                } else {
                    report.chunked(LOG_CHUNK_SIZE).forEachIndexed { index, chunk ->
                        Log.i(TAG, "PROBE_RESULT part=${index + 1} $chunk")
                    }
                }
                Log.i(TAG, "PROBE_DONE domain=$domain ip_family=$family transport=$transport")
            } catch (t: Throwable) {
                Log.e(TAG, "PROBE_DONE error=${t.javaClass.simpleName}:${t.message} transport=$transport", t)
            } finally {
                runOnUiThread { finish() }
            }
        }.start()
    }

    companion object {
        const val EXTRA_DOMAIN = "domain"
        const val EXTRA_FAMILY = "family"
        const val EXTRA_TRANSPORT = "transport"
        private const val TAG = "TgWsProxyProbe"
        private const val LOG_CHUNK_SIZE = 3000
    }
}
