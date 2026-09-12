package com.amurcanov.tgwsproxy

import com.sun.jna.Library
import com.sun.jna.Native
import com.sun.jna.Pointer

interface ProxyLibrary : Library {
    companion object {
        val INSTANCE = Native.load("tgwsproxy", ProxyLibrary::class.java) as ProxyLibrary
    }

    fun StartProxy(host: String, port: Int, dcIps: String, verbose: Int): Int
    fun StopProxy(): Int
    fun SetPoolSize(size: Int)
    fun GetStats(): Pointer?
    fun ResetCFDomainCooldowns()
    fun SetManualCFDomains(domains: String)
    fun SetCachedCFDomains(domains: String)
    fun GetAdaptiveRouteStats(): Pointer?
    fun GetProxyStatus(): Pointer?
    fun StartMtProtoProxy(host: String, port: Int, secret: String, runtimeConfig: String, verbose: Int): Int
    fun StopMtProtoProxy(): Int
    fun GetMtProtoProxyStatus(): Pointer?
    fun RunWorkerNetworkProbe(domain: String): Pointer?
    fun RunWorkerNetworkProbeWithFamily(domain: String, family: String): Pointer?
    fun ResetAdaptiveRouteStats(all: Int)
    fun ResetAdaptiveNetworkRouteStats(profileId: String)
    fun FreeString(p: Pointer)
}

object NativeProxy {
    fun startProxy(host: String, port: Int, dcIps: String, verbose: Int): Int {
        return ProxyLibrary.INSTANCE.StartProxy(host, port, dcIps, verbose)
    }
    fun stopProxy(): Int {
        return ProxyLibrary.INSTANCE.StopProxy()
    }
    fun setPoolSize(size: Int) {
        ProxyLibrary.INSTANCE.SetPoolSize(size)
    }
    fun getStats(): String? {
        val ptr = ProxyLibrary.INSTANCE.GetStats() ?: return null
        val res = ptr.getString(0)
        ProxyLibrary.INSTANCE.FreeString(ptr)
        return res
    }
    fun resetCfDomainCooldowns() {
        ProxyLibrary.INSTANCE.ResetCFDomainCooldowns()
    }
    fun setManualCfDomains(domains: List<String>) {
        val payload = domains.mapNotNull(CfDomain::normalizeOrNull).distinct().joinToString("|")
        ProxyLibrary.INSTANCE.SetManualCFDomains(payload)
    }
    fun setCachedCfDomains(domains: List<String>) {
        val payload = domains.mapNotNull(CfDomain::normalizeOrNull).distinct().joinToString("|")
        ProxyLibrary.INSTANCE.SetCachedCFDomains(payload)
    }
    fun getProxyStatus(): String? {
        val ptr = ProxyLibrary.INSTANCE.GetProxyStatus() ?: return null
        val res = ptr.getString(0)
        ProxyLibrary.INSTANCE.FreeString(ptr)
        return res
    }
    fun startMtProtoProxy(
        host: String,
        port: Int,
        secret: String,
        runtimeConfig: String,
        verbose: Int,
    ): Int {
        return ProxyLibrary.INSTANCE.StartMtProtoProxy(host, port, secret, runtimeConfig, verbose)
    }
    fun stopMtProtoProxy(): Int {
        return ProxyLibrary.INSTANCE.StopMtProtoProxy()
    }
    fun getMtProtoProxyStatus(): String? {
        val ptr = ProxyLibrary.INSTANCE.GetMtProtoProxyStatus() ?: return null
        val res = ptr.getString(0)
        ProxyLibrary.INSTANCE.FreeString(ptr)
        return res
    }
    fun runWorkerNetworkProbe(domain: String, family: String = "auto"): String? {
        val ptr = ProxyLibrary.INSTANCE.RunWorkerNetworkProbeWithFamily(domain, family) ?: return null
        val res = ptr.getString(0)
        ProxyLibrary.INSTANCE.FreeString(ptr)
        return res
    }
    fun getAdaptiveRouteStats(): String? {
        val ptr = ProxyLibrary.INSTANCE.GetAdaptiveRouteStats() ?: return null
        val res = ptr.getString(0)
        ProxyLibrary.INSTANCE.FreeString(ptr)
        return res
    }
    fun resetAdaptiveRouteStats(all: Boolean) {
        ProxyLibrary.INSTANCE.ResetAdaptiveRouteStats(if (all) 1 else 0)
    }
    fun resetAdaptiveNetworkRouteStats(profileId: String) {
        ProxyLibrary.INSTANCE.ResetAdaptiveNetworkRouteStats(profileId)
    }
}
