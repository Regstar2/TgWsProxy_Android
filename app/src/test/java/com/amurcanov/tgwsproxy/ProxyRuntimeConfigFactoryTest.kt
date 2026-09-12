package com.amurcanov.tgwsproxy

import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class ProxyRuntimeConfigFactoryTest {
    @Test
    fun runtimePolicy_workerOnlyWithFallbackDoesNotEnableDisabledRoutes() {
        val policy = NetworkRoutePolicy(
            networkType = NetworkProfileType.WIFI,
            enabledRoutes = setOf(RouteKind.WORKER_WS),
            preferredRoute = RouteKind.WORKER_WS,
            autoStrategy = AutoStrategy.WORKER_PREFERRED,
            allowFallback = true,
        )

        val raw = ConnectionRuntimeConfig.buildRuntimeTokens(
            dcEntries = listOf("2:149.154.167.220"),
            mode = ConnectionMode.WorkerFirst,
            cfProxyEnabled = true,
            cfProxyPriority = true,
            cfProxyOnly = false,
            cfDomain = "",
            workerEnabled = true,
            workerDomain = "example.username.workers.dev",
            routePolicy = policy,
        )

        assertTrue(raw.contains("@route_worker_ws=1"))
        assertTrue(raw.contains("@route_fallback=1"))
        assertTrue(raw.contains("@cfproxy=0"))
        assertTrue(raw.contains("@route_cf_proxy_ws=0"))
        assertTrue(raw.contains("@route_tcp_fallback=0"))
        assertFalse(raw.contains("@route_cf_proxy_ws=1"))
        assertFalse(raw.contains("@route_tcp_fallback=1"))
    }

    @Test
    fun runtimePolicy_strictWorkerOnlyKeepsDisabledRoutesDisabled() {
        val policy = NetworkRoutePolicy(
            networkType = NetworkProfileType.WIFI,
            enabledRoutes = setOf(RouteKind.WORKER_WS),
            preferredRoute = RouteKind.WORKER_WS,
            autoStrategy = AutoStrategy.WORKER_PREFERRED,
            allowFallback = false,
        )

        val raw = ConnectionRuntimeConfig.buildRuntimeTokens(
            dcEntries = listOf("2:149.154.167.220"),
            mode = ConnectionMode.WorkerOnly,
            cfProxyEnabled = true,
            cfProxyPriority = true,
            cfProxyOnly = false,
            cfDomain = "",
            workerEnabled = true,
            workerDomain = "example.username.workers.dev",
            routePolicy = policy,
        )

        assertTrue(raw.contains("@route_worker_ws=1"))
        assertTrue(raw.contains("@route_fallback=0"))
        assertTrue(raw.contains("@cfproxy=0"))
        assertTrue(raw.contains("@route_cf_proxy_ws=0"))
        assertTrue(raw.contains("@route_tcp_fallback=0"))
    }
}
