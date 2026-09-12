package com.amurcanov.tgwsproxy

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class MtProtoRuntimeAdapterTest {
    private val fixedSecret = "0123456789abcdef0123456789abcdef"

    @Test
    fun unsupportedAdapter_returnsUnsupportedForValidConfig() {
        val adapter = UnsupportedMtProtoRuntimeAdapter()

        val result = adapter.start(validRuntimeConfig())

        assertEquals(MtProtoRuntimeState.UNSUPPORTED, result.state)
        assertEquals(MtProtoRuntimeErrorCode.UNSUPPORTED_RUNTIME, result.errorCode)
        assertEquals(MtProtoRuntimeState.UNSUPPORTED, adapter.getState())
    }

    @Test
    fun unsupportedAdapter_stopReturnsUnsupported() {
        val adapter = UnsupportedMtProtoRuntimeAdapter()

        val result = adapter.stop()

        assertEquals(MtProtoRuntimeState.UNSUPPORTED, result.state)
        assertEquals(MtProtoRuntimeErrorCode.UNSUPPORTED_RUNTIME, result.errorCode)
    }

    @Test
    fun configMapper_mapsProxyConfigToRuntimeConfig() {
        val proxyConfig = MtProtoProxyConfig.default { fixedSecret }

        val result = MtProtoRuntimeConfigMapper.fromProxyConfig(
            config = proxyConfig,
            dcIps = "1:149.154.175.50",
            verbose = 1,
        )

        assertTrue(result.isSuccess)
        val runtimeConfig = result.config!!
        assertEquals(MtProtoProxyConfig.DEFAULT_HOST, runtimeConfig.host)
        assertEquals(MtProtoProxyConfig.DEFAULT_PORT, runtimeConfig.port)
        assertEquals(fixedSecret, runtimeConfig.secret)
        assertEquals("1:149.154.175.50", runtimeConfig.dcIps)
        assertEquals("", runtimeConfig.fakeTlsDomain)
        assertFalse(runtimeConfig.fakeTlsPassthrough)
        assertFalse(runtimeConfig.mtProtoWorkerPreconnect)
        assertEquals(1, runtimeConfig.verbose)
        assertNull(result.errorCode)
    }

    @Test
    fun configMapperIncludesFakeTlsDomainForRuntimeTokens() {
        val proxyConfig = MtProtoProxyConfig.default { fixedSecret }
            .copy(fakeTlsDomain = "WWW.Google.COM", fakeTlsPassthrough = true)

        val result = MtProtoRuntimeConfigMapper.fromProxyConfig(
            config = proxyConfig,
            dcIps = "1:149.154.175.50",
            verbose = 1,
        )

        assertTrue(result.isSuccess)
        val runtimeConfig = result.config!!
        assertEquals("www.google.com", runtimeConfig.fakeTlsDomain)
        assertTrue(runtimeConfig.fakeTlsPassthrough)
    }

    @Test
    fun runtimeTokensIncludeFakeTlsPassthroughAndMtProtoWorkerPreconnect() {
        val tokens = normalizedRuntimeTokens(
            validRuntimeConfig().copy(
                dcIps = "1:149.154.175.50",
                fakeTlsDomain = "www.google.com",
                fakeTlsPassthrough = true,
                mtProtoWorkerPreconnect = true,
            ).normalized(),
        )

        assertTrue(tokens.contains("@mtproto_fake_tls_domain=www.google.com"))
        assertTrue(tokens.contains("@mtproto_masking_passthrough=1"))
        assertTrue(tokens.contains("@mtproto_worker_preconnect=1"))
    }

    @Test
    fun configMapperMasksSecretInLogsAndReport() {
        val runtimeConfig = validRuntimeConfig()

        val report = MtProtoRuntimeConfigDiagnostics.reportLines(runtimeConfig).joinToString("\n")
        val fields = MtProtoRuntimeConfigDiagnostics.logFields(runtimeConfig)

        assertFalse(report.contains(fixedSecret))
        assertTrue(report.contains("MTProto runtime secret: ${MtProtoSecretMasking.MASKED}"))
        assertEquals(MtProtoSecretMasking.MASKED, fields["secret"])
        assertEquals("no", fields["fakeTls"])
        assertEquals("no", fields["fakeTlsPassthrough"])
        assertEquals("no", fields["mtProtoWorkerPreconnect"])
        assertFalse(fields.values.any { it.contains(fixedSecret) })
    }

    @Test
    fun invalidConfigDoesNotStartRuntime() {
        val adapter = UnsupportedMtProtoRuntimeAdapter()
        val invalid = validRuntimeConfig().copy(port = 0)

        val result = adapter.start(invalid)

        assertEquals(MtProtoRuntimeState.FAILED, result.state)
        assertEquals(MtProtoRuntimeErrorCode.INVALID_CONFIG, result.errorCode)
        assertEquals(MtProtoRuntimeState.UNSUPPORTED, adapter.getState())
    }

    @Test
    fun invalidProxyConfigDoesNotMapToRuntimeConfig() {
        val proxyConfig = MtProtoProxyConfig.default { fixedSecret }.copy(secret = "bad")

        val result = MtProtoRuntimeConfigMapper.fromProxyConfig(proxyConfig)

        assertFalse(result.isSuccess)
        assertNull(result.config)
        assertEquals(MtProtoRuntimeErrorCode.INVALID_CONFIG, result.errorCode)
        assertTrue(result.validationErrors.contains(MtProtoProxyConfigValidationError.INVALID_SECRET))
    }

    @Test
    fun invalidFakeTlsDomainDoesNotMapToRuntimeConfig() {
        val proxyConfig = MtProtoProxyConfig.default { fixedSecret }.copy(fakeTlsDomain = "localhost")

        val result = MtProtoRuntimeConfigMapper.fromProxyConfig(proxyConfig)

        assertFalse(result.isSuccess)
        assertTrue(result.validationErrors.contains(MtProtoProxyConfigValidationError.INVALID_FAKE_TLS_DOMAIN))
    }

    @Test
    fun defaultFrontendIsMtProto() {
        val frontend = localProxyFrontendFor(LocalProxyFrontendType.DEFAULT)

        assertEquals(LocalProxyFrontendType.MTPROTO_EXPERIMENTAL, LocalProxyFrontendType.DEFAULT)
        assertEquals(LocalProxyFrontendType.MTPROTO_EXPERIMENTAL, frontend.type)
        assertEquals(LocalProxyFrontendStatus.STOPPED, frontend.getState().status)
    }

    @Test
    fun nativeStatusParser_mapsLocalOnlyStatus() {
        val status = MtProtoNativeStatus.parse(
            "status=LISTENING_LOCAL_ONLY;host=127.0.0.1;port=1443;" +
                "outbound=OUTBOUND_UNSUPPORTED;active=0;total=0;" +
                "last_error=;secret_fingerprint=abcdef123456",
        )

        assertEquals(MtProtoRuntimeState.LISTENING_LOCAL_ONLY, status.toRuntimeState())
        assertEquals("OUTBOUND_UNSUPPORTED", status.outbound)
        assertEquals("127.0.0.1", status.host)
        assertEquals(1443, status.port)
        assertEquals("abcdef123456", status.secretFingerprint)
        assertFalse(status.secretFingerprint.contains(fixedSecret))
        assertEquals(0L, status.activeConnections)
        assertEquals(0L, status.totalConnections)
    }

    @Test
    fun nativeStatusParser_mapsFailureStatuses() {
        val portBusy = MtProtoNativeStatus.parse("status=FAILED_PORT_IN_USE;last_error=bind")
        val invalidSecret = MtProtoNativeStatus.parse("status=FAILED_INVALID_SECRET;last_error=secret")

        assertEquals(MtProtoRuntimeState.FAILED, portBusy.toRuntimeState())
        assertEquals(MtProtoRuntimeState.FAILED, invalidSecret.toRuntimeState())
    }

    @Test
    fun nativeStatusParser_mapsDirectRouteTruth() {
        val status = MtProtoNativeStatus.parse(
            "status=LISTENING_ROUTE_READY;host=127.0.0.1;port=1443;" +
                "outbound=MTPROTO_ROUTE_DIRECT_READY;selected_backend=direct_tcp;" +
                "actual_backend=direct_tcp;fallback_used=false;route_reason=connected;" +
                "last_error=;secret_fingerprint=abcdef123456",
        )

        assertEquals(MtProtoRuntimeState.RUNNING, status.toRuntimeState())
        assertEquals("MTPROTO_ROUTE_DIRECT_READY", status.outbound)
        assertEquals("direct_tcp", status.selectedBackend)
        assertEquals("direct_tcp", status.actualBackend)
        assertFalse(status.fallbackUsed)
        assertEquals("connected", status.routeReason)
    }

    @Test
    fun nativeStatusParser_readsFakeTlsAndTestDcFlags() {
        val status = MtProtoNativeStatus.parse(
            "status=LISTENING_ROUTE_READY;fake_tls=true;masking_passthrough=true;" +
                "fake_tls_accepted=2;fake_tls_rejected=3;fake_tls_redirected=4;" +
                "fake_tls_probe=5;fake_tls_passthrough=6;" +
                "fake_tls_last_error=fake tls client hello verification failed;force_test_dc=true",
        )

        assertTrue(status.fakeTls)
        assertTrue(status.maskingPassthrough)
        assertEquals(2L, status.fakeTlsAccepted)
        assertEquals(3L, status.fakeTlsRejected)
        assertEquals(4L, status.fakeTlsRedirected)
        assertEquals(5L, status.fakeTlsProbe)
        assertEquals(6L, status.fakeTlsPassthrough)
        assertEquals("fake tls client hello verification failed", status.fakeTlsLastError)
        assertTrue(status.forceTestDc)
    }

    private fun validRuntimeConfig(): MtProtoRuntimeConfig {
        return MtProtoRuntimeConfig(
            host = MtProtoProxyConfig.DEFAULT_HOST,
            port = MtProtoProxyConfig.DEFAULT_PORT,
            secret = fixedSecret,
            dcIps = "",
            verbose = 1,
        )
    }
}
