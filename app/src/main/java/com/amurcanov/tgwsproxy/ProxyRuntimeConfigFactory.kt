package com.amurcanov.tgwsproxy

import android.content.Context
import android.content.SharedPreferences
import com.amurcanov.tgwsproxy.worker.WorkerDestinationMode
import com.amurcanov.tgwsproxy.worker.WorkerFailoverPayload

data class ProxyRuntimeStartConfig(
    val port: Int,
    val ips: String,
    val poolSize: Int,
    val profile: NetworkProfile,
    val effectivePolicy: EffectiveRoutePolicy,
)

class ProxyRuntimeConfigFactory(
    private val prefs: SharedPreferences,
    private val routePolicyRepository: NetworkRoutePolicyRepository,
    private val adaptiveRouteStatsRepository: AdaptiveRouteStatsRepository,
    private val cfUpstreamDomainsProvider: () -> List<String>,
    private val manualCfDomainsProvider: () -> List<String>,
    private val workerDomainProvider: () -> String,
    private val workerFailoverPayloadProvider: () -> WorkerFailoverPayload? = { null },
    private val workerDestinationModeProvider: () -> WorkerDestinationMode = { WorkerDestinationMode.PRESERVE_ORIGINAL_DST },
    private val flowsealMediaFixEnabledProvider: () -> Boolean = { false },
    private val flowsealMediaFixDcProvider: () -> Int = { WorkerDestinationMode.DEFAULT_MEDIA_FIX_DC },
    private val flowsealMediaFixIpProvider: () -> String = { WorkerDestinationMode.DEFAULT_MEDIA_FIX_IP },
    private val dcEntriesProvider: () -> List<String>,
    private val poolSizeProvider: () -> Int,
    private val portProvider: () -> Int?,
    private val profileProvider: (Context) -> NetworkProfile = { NetworkProfileProvider.current(it) },
) {
    fun build(context: Context): ProxyRuntimeStartConfig? {
        val port = portProvider() ?: return null
        val networkProfile = profileProvider(context)
        val effectivePolicy = EffectiveRoutePolicyResolver(
            repository = routePolicyRepository,
            prefs = prefs,
        ).resolve(networkProfile)
        val runtimePolicy = effectivePolicy.policy
        val ips = ConnectionRuntimeConfig.buildRuntimeTokens(
            dcEntries = dcEntriesProvider(),
            mode = effectivePolicy.legacyMode,
            cfProxyEnabled = prefs.getBoolean(KEY_CFPROXY_ENABLED, true),
            cfProxyPriority = prefs.getBoolean(KEY_CFPROXY_PRIORITY, true),
            cfProxyOnly = prefs.getBoolean(KEY_CFPROXY_ONLY, false),
            cfDomain = "",
            manualCfDomains = manualCfDomainsProvider(),
            workerEnabled = prefs.getBoolean(KEY_WORKER_ENABLED, false),
            workerDomain = WorkerDomain.normalize(workerDomainProvider()),
            workerFailover = workerFailoverPayloadProvider(),
            workerDestinationMode = workerDestinationModeProvider(),
            flowsealMediaFixEnabled = flowsealMediaFixEnabledProvider(),
            flowsealMediaFixDc = flowsealMediaFixDcProvider(),
            flowsealMediaFixIp = flowsealMediaFixIpProvider(),
            cachedCfDomains = cfUpstreamDomainsProvider(),
            networkProfile = networkProfile,
            adaptiveRouteStats = adaptiveRouteStatsRepository.loadEncodedStats(),
            autoStrategy = runtimePolicy.autoStrategy,
            routePolicy = runtimePolicy,
        )
        return ProxyRuntimeStartConfig(
            port = port,
            ips = ips,
            poolSize = poolSizeProvider(),
            profile = networkProfile,
            effectivePolicy = effectivePolicy,
        )
    }

    companion object {
        const val KEY_CFPROXY_ENABLED = "cfproxy_enabled"
        const val KEY_CFPROXY_PRIORITY = "cfproxy_priority"
        const val KEY_CFPROXY_ONLY = "cfproxy_only"
        const val KEY_WORKER_ENABLED = "worker_enabled"
        const val KEY_WORKER_DESTINATION_MODE = "worker_destination_mode"
        const val KEY_FLOWSEAL_MEDIA_FIX_ENABLED = "flowseal_media_fix_enabled"
        const val KEY_FLOWSEAL_MEDIA_FIX_DC = "flowseal_media_fix_dc"
        const val KEY_FLOWSEAL_MEDIA_FIX_IP = "flowseal_media_fix_ip"
    }
}
