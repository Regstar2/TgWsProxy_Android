package com.amurcanov.tgwsproxy

internal data class MtProtoRuntimeConfig(
    val host: String,
    val port: Int,
    val secret: String,
    val dcIps: String = "",
    val fakeTlsDomain: String = "",
    val fakeTlsPassthrough: Boolean = false,
    val forceTestDc: Boolean = false,
    val mtProtoWorkerPreconnect: Boolean = false,
    val verbose: Int = 1,
) {
    fun normalized(): MtProtoRuntimeConfig {
        val normalizedFakeTlsDomain = MtProtoFakeTlsDomain.normalize(fakeTlsDomain)
        return copy(
            host = host.trim(),
            secret = secret.trim().lowercase(java.util.Locale.US),
            dcIps = dcIps.trim(),
            fakeTlsDomain = normalizedFakeTlsDomain,
            fakeTlsPassthrough = fakeTlsPassthrough &&
                MtProtoFakeTlsDomain.isValid(normalizedFakeTlsDomain),
            verbose = verbose.coerceAtLeast(0),
        )
    }
}

internal enum class MtProtoRuntimeState {
    STOPPED,
    STARTING,
    RUNNING,
    LISTENING_LOCAL_ONLY,
    FAILED,
    UNSUPPORTED,
}

internal enum class MtProtoRuntimeErrorCode {
    NATIVE_LIBRARY_MISSING,
    INVALID_CONFIG,
    PORT_BUSY,
    START_FAILED,
    STOP_FAILED,
    UNSUPPORTED_RUNTIME,
}

internal data class MtProtoRuntimeStartResult(
    val state: MtProtoRuntimeState,
    val errorCode: MtProtoRuntimeErrorCode? = null,
    val message: String = "",
)

internal data class MtProtoRuntimeStopResult(
    val state: MtProtoRuntimeState,
    val errorCode: MtProtoRuntimeErrorCode? = null,
    val message: String = "",
)

internal interface MtProtoRuntimeAdapter {
    fun start(config: MtProtoRuntimeConfig): MtProtoRuntimeStartResult

    fun stop(): MtProtoRuntimeStopResult

    fun getState(): MtProtoRuntimeState
}

internal object MtProtoRuntimeConfigValidator {
    fun validate(config: MtProtoRuntimeConfig): MtProtoProxyConfigValidationResult {
        val normalized = config.normalized()
        return MtProtoProxyConfigValidator.validate(
            MtProtoProxyConfig(
                host = normalized.host,
                port = normalized.port,
                secret = normalized.secret,
                enabled = true,
                fakeTlsDomain = normalized.fakeTlsDomain,
                fakeTlsPassthrough = normalized.fakeTlsPassthrough,
                experimentalAcknowledged = true,
            ),
        )
    }
}

internal data class MtProtoRuntimeConfigMappingResult(
    val config: MtProtoRuntimeConfig? = null,
    val errorCode: MtProtoRuntimeErrorCode? = null,
    val validationErrors: Set<MtProtoProxyConfigValidationError> = emptySet(),
) {
    val isSuccess: Boolean
        get() = config != null && errorCode == null
}

internal object MtProtoRuntimeConfigMapper {
    fun fromProxyConfig(
        config: MtProtoProxyConfig,
        dcIps: String = "",
        verbose: Int = 1,
    ): MtProtoRuntimeConfigMappingResult {
        val validation = MtProtoProxyConfigValidator.validate(config)
        if (!validation.isValid) {
            return MtProtoRuntimeConfigMappingResult(
                errorCode = MtProtoRuntimeErrorCode.INVALID_CONFIG,
                validationErrors = validation.errors,
            )
        }
        val normalized = config.normalized()
        return MtProtoRuntimeConfigMappingResult(
            config = MtProtoRuntimeConfig(
                host = normalized.host,
                port = normalized.port,
                secret = normalized.secret,
                dcIps = dcIps,
                fakeTlsDomain = normalized.fakeTlsDomain,
                fakeTlsPassthrough = normalized.fakeTlsPassthrough,
                verbose = verbose,
            ).normalized(),
        )
    }
}

internal object MtProtoRuntimeConfigDiagnostics {
    fun reportLines(config: MtProtoRuntimeConfig): List<String> {
        val normalized = config.normalized()
        return listOf(
            "MTProto runtime host: ${normalized.host.ifBlank { "UNKNOWN" }}",
            "MTProto runtime port: ${normalized.port}",
            "MTProto runtime secret: ${MtProtoSecretMasking.mask(normalized.secret)}",
            "MTProto runtime dcIps configured: ${yesNo(normalized.dcIps.isNotBlank())}",
            "MTProto runtime Fake TLS: ${yesNo(normalized.fakeTlsDomain.isNotBlank())}",
            "MTProto runtime Fake TLS passthrough: ${yesNo(normalized.fakeTlsPassthrough)}",
            "MTProto runtime force test DC: ${yesNo(normalized.forceTestDc)}",
            "MTProto runtime Worker preconnect: ${yesNo(normalized.mtProtoWorkerPreconnect)}",
            "MTProto runtime verbose: ${normalized.verbose}",
        )
    }

    fun logFields(config: MtProtoRuntimeConfig): Map<String, String> {
        val normalized = config.normalized()
        return mapOf(
            "host" to normalized.host,
            "port" to normalized.port.toString(),
            "secret" to MtProtoSecretMasking.mask(normalized.secret),
            "dcIpsConfigured" to yesNo(normalized.dcIps.isNotBlank()),
            "fakeTls" to yesNo(normalized.fakeTlsDomain.isNotBlank()),
            "fakeTlsPassthrough" to yesNo(normalized.fakeTlsPassthrough),
            "forceTestDc" to yesNo(normalized.forceTestDc),
            "mtProtoWorkerPreconnect" to yesNo(normalized.mtProtoWorkerPreconnect),
            "verbose" to normalized.verbose.toString(),
        )
    }

    private fun yesNo(value: Boolean): String = if (value) "yes" else "no"
}

internal class UnsupportedMtProtoRuntimeAdapter : MtProtoRuntimeAdapter {
    override fun start(config: MtProtoRuntimeConfig): MtProtoRuntimeStartResult {
        val validation = MtProtoRuntimeConfigValidator.validate(config)
        if (!validation.isValid) {
            return MtProtoRuntimeStartResult(
                state = MtProtoRuntimeState.FAILED,
                errorCode = MtProtoRuntimeErrorCode.INVALID_CONFIG,
                message = "MTProto runtime config is invalid.",
            )
        }
        return MtProtoRuntimeStartResult(
            state = MtProtoRuntimeState.UNSUPPORTED,
            errorCode = MtProtoRuntimeErrorCode.UNSUPPORTED_RUNTIME,
            message = "MTProto runtime adapter has no native implementation yet.",
        )
    }

    override fun stop(): MtProtoRuntimeStopResult {
        return MtProtoRuntimeStopResult(
            state = MtProtoRuntimeState.UNSUPPORTED,
            errorCode = MtProtoRuntimeErrorCode.UNSUPPORTED_RUNTIME,
            message = "MTProto runtime adapter has no native implementation yet.",
        )
    }

    override fun getState(): MtProtoRuntimeState = MtProtoRuntimeState.UNSUPPORTED
}

internal class NativeMtProtoRuntimeAdapter : MtProtoRuntimeAdapter {
    override fun start(config: MtProtoRuntimeConfig): MtProtoRuntimeStartResult {
        val normalized = config.normalized()
        val validation = MtProtoRuntimeConfigValidator.validate(normalized)
        if (!validation.isValid) {
            return MtProtoRuntimeStartResult(
                state = MtProtoRuntimeState.FAILED,
                errorCode = MtProtoRuntimeErrorCode.INVALID_CONFIG,
                message = "MTProto runtime config is invalid.",
            )
        }

        val code = runCatching {
            NativeProxy.startMtProtoProxy(
                host = normalized.host,
                port = normalized.port,
                secret = normalized.secret,
                runtimeConfig = normalizedRuntimeTokens(normalized),
                verbose = normalized.verbose,
            )
        }.getOrElse { error ->
            return nativeMissing(error)
        }

        return when (code) {
            0 -> {
                val state = getState()
                    .takeIf {
                        it == MtProtoRuntimeState.RUNNING ||
                            it == MtProtoRuntimeState.LISTENING_LOCAL_ONLY
                    }
                    ?: MtProtoRuntimeState.LISTENING_LOCAL_ONLY
                MtProtoRuntimeStartResult(
                    state = state,
                    message = if (state == MtProtoRuntimeState.RUNNING) {
                        "MTProto frontend listening; outbound route is ready."
                    } else {
                        "MTProto frontend listening locally; outbound is unsupported."
                    },
                )
            }
            -2 -> MtProtoRuntimeStartResult(
                state = MtProtoRuntimeState.FAILED,
                errorCode = MtProtoRuntimeErrorCode.INVALID_CONFIG,
                message = "MTProto native runtime rejected config.",
            )
            -3 -> MtProtoRuntimeStartResult(
                state = MtProtoRuntimeState.FAILED,
                errorCode = MtProtoRuntimeErrorCode.PORT_BUSY,
                message = "MTProto local port is busy.",
            )
            else -> MtProtoRuntimeStartResult(
                state = MtProtoRuntimeState.FAILED,
                errorCode = MtProtoRuntimeErrorCode.START_FAILED,
                message = "MTProto native runtime start failed: code=$code.",
            )
        }
    }

    override fun stop(): MtProtoRuntimeStopResult {
        val code = runCatching {
            NativeProxy.stopMtProtoProxy()
        }.getOrElse { error ->
            return MtProtoRuntimeStopResult(
                state = MtProtoRuntimeState.UNSUPPORTED,
                errorCode = MtProtoRuntimeErrorCode.NATIVE_LIBRARY_MISSING,
                message = "MTProto native runtime is unavailable: ${error.javaClass.simpleName}.",
            )
        }
        return if (code == 0) {
            MtProtoRuntimeStopResult(
                state = MtProtoRuntimeState.STOPPED,
                message = "MTProto frontend stopped.",
            )
        } else {
            MtProtoRuntimeStopResult(
                state = MtProtoRuntimeState.FAILED,
                errorCode = MtProtoRuntimeErrorCode.STOP_FAILED,
                message = "MTProto native runtime stop failed: code=$code.",
            )
        }
    }

    override fun getState(): MtProtoRuntimeState {
        val status = runCatching {
            NativeProxy.getMtProtoProxyStatus()
        }.getOrNull() ?: return MtProtoRuntimeState.UNSUPPORTED
        return MtProtoNativeStatus.parse(status).toRuntimeState()
    }

    private fun nativeMissing(error: Throwable): MtProtoRuntimeStartResult {
        return MtProtoRuntimeStartResult(
            state = MtProtoRuntimeState.UNSUPPORTED,
            errorCode = MtProtoRuntimeErrorCode.NATIVE_LIBRARY_MISSING,
            message = "MTProto native runtime is unavailable: ${error.javaClass.simpleName}.",
        )
    }
}

internal fun normalizedRuntimeTokens(config: MtProtoRuntimeConfig): String {
    val tokens = buildList {
        if (config.dcIps.isNotBlank()) {
            add(config.dcIps)
        }
        if (config.fakeTlsDomain.isNotBlank()) {
            add("@mtproto_fake_tls_domain=${config.fakeTlsDomain}")
        }
        if (config.fakeTlsPassthrough && config.fakeTlsDomain.isNotBlank()) {
            add("@mtproto_masking_passthrough=1")
        }
        if (config.mtProtoWorkerPreconnect) {
            add("@mtproto_worker_preconnect=1")
        }
        if (config.forceTestDc) {
            add("@force_test_dc=1")
        }
    }
    return tokens.joinToString(",")
}

internal data class MtProtoNativeStatus(
    val status: String = "",
    val outbound: String = "",
    val host: String = "",
    val port: Int = 0,
    val lastError: String = "",
    val secretFingerprint: String = "",
    val selectedBackend: String = "",
    val actualBackend: String = "",
    val fallbackUsed: Boolean = false,
    val routeReason: String = "",
    val activeConnections: Long = 0L,
    val totalConnections: Long = 0L,
    val fakeTls: Boolean = false,
    val maskingPassthrough: Boolean = false,
    val fakeTlsAccepted: Long = 0L,
    val fakeTlsRejected: Long = 0L,
    val fakeTlsRedirected: Long = 0L,
    val fakeTlsProbe: Long = 0L,
    val fakeTlsPassthrough: Long = 0L,
    val fakeTlsLastError: String = "",
    val forceTestDc: Boolean = false,
) {
    fun toRuntimeState(): MtProtoRuntimeState {
        return when (status) {
            "LISTENING_ROUTE_READY" -> MtProtoRuntimeState.RUNNING
            "LISTENING_LOCAL_ONLY" -> MtProtoRuntimeState.LISTENING_LOCAL_ONLY
            "STARTING" -> MtProtoRuntimeState.STARTING
            "STOPPED",
            "DISABLED" -> MtProtoRuntimeState.STOPPED
            "FAILED_PORT_IN_USE",
            "FAILED_INVALID_SECRET",
            "FAILED_RUNTIME_ERROR" -> MtProtoRuntimeState.FAILED
            else -> MtProtoRuntimeState.UNSUPPORTED
        }
    }

    companion object {
        fun parse(raw: String): MtProtoNativeStatus {
            val fields = raw.split(';')
                .mapNotNull { part ->
                    val idx = part.indexOf('=')
                    if (idx <= 0) {
                        null
                    } else {
                        part.substring(0, idx) to part.substring(idx + 1)
                    }
                }
                .toMap()
            return MtProtoNativeStatus(
                status = fields["status"].orEmpty(),
                outbound = fields["outbound"].orEmpty(),
                host = fields["host"].orEmpty(),
                port = fields["port"]?.toIntOrNull() ?: 0,
                lastError = fields["last_error"].orEmpty(),
                secretFingerprint = fields["secret_fingerprint"].orEmpty(),
                selectedBackend = fields["selected_backend"].orEmpty(),
                actualBackend = fields["actual_backend"].orEmpty(),
                fallbackUsed = fields["fallback_used"]?.toBooleanStrictOrNull() ?: false,
                routeReason = fields["route_reason"].orEmpty(),
                activeConnections = fields["active"]?.toLongOrNull() ?: 0L,
                totalConnections = fields["total"]?.toLongOrNull() ?: 0L,
                fakeTls = fields["fake_tls"]?.toBooleanStrictOrNull() ?: false,
                maskingPassthrough = fields["masking_passthrough"]?.toBooleanStrictOrNull() ?: false,
                fakeTlsAccepted = fields["fake_tls_accepted"]?.toLongOrNull() ?: 0L,
                fakeTlsRejected = fields["fake_tls_rejected"]?.toLongOrNull() ?: 0L,
                fakeTlsRedirected = fields["fake_tls_redirected"]?.toLongOrNull() ?: 0L,
                fakeTlsProbe = fields["fake_tls_probe"]?.toLongOrNull() ?: 0L,
                fakeTlsPassthrough = fields["fake_tls_passthrough"]?.toLongOrNull() ?: 0L,
                fakeTlsLastError = fields["fake_tls_last_error"].orEmpty(),
                forceTestDc = fields["force_test_dc"]?.toBooleanStrictOrNull() ?: false,
            )
        }
    }
}
