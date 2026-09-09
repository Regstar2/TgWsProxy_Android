package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"tg-ws-proxy/mtproxyfrontend"
	"tg-ws-proxy/tgwsroute"
)

type connectionMode = tgwsroute.ConnectionMode

const (
	modeAuto               = tgwsroute.ModeAuto
	modeDirectWithFallback = tgwsroute.ModeDirectWithFallback
	modeWorkerFirst        = tgwsroute.ModeWorkerFirst
	modeCFFirst            = tgwsroute.ModeCFFirst
	modeWorkerOnly         = tgwsroute.ModeWorkerOnly
	modeCFOnly             = tgwsroute.ModeCFOnly
	modeDirectOnly         = tgwsroute.ModeDirectOnly
)

type routeKind = tgwsroute.RouteKind

const (
	routeDirectWS    = tgwsroute.RouteDirectWS
	routeCFWorkerWS  = tgwsroute.RouteCFWorkerWS
	routeCFProxyWS   = tgwsroute.RouteCFProxyWS
	routeTCPFallback = tgwsroute.RouteTCPFallback
)

type flowsealMediaFixConfig struct {
	Enabled bool
	DC      int
	IP      string
}

type workerConfig struct {
	Enabled         bool
	Domain          string
	Failover        workerFailoverSettings
	DestinationMode string
	MediaFix        flowsealMediaFixConfig
}

type runtimeSettings struct {
	Mode                      connectionMode
	CF                        cfProxyConfig
	Worker                    workerConfig
	CFManualDomains           []string // user CF domains override cached and built-in pool entries
	CFCachedUpstream          []string // cached Flowseal upstream domains
	NetworkProfileID          string
	NetworkProfileType        string
	NetworkProfileLabel       string
	AdaptiveRouteStats        string
	AutoStrategy              string
	MtProtoFakeTLSDomain      string
	MtProtoMaskingPassthrough bool
	MtProtoWorkerPreconnect   bool
	ForceTestDC               bool
	// Per-network route policy (from Android @route_* tokens).
	PolicyPresent bool
	AllowDirect   bool
	AllowWorker   bool
	AllowCFProxy  bool
	AllowTCP      bool
	Preferred     routeKind // empty means "no explicit preferred"
	AllowFallback bool
	PolicyGen     uint64
}

func (s runtimeSettings) workerRouteAvailable() bool {
	if !s.Worker.Enabled || s.Worker.Domain == "" {
		return false
	}
	for _, route := range routesForMode(s.Mode, s, false) {
		if route == routeCFWorkerWS {
			return true
		}
	}
	return false
}

var (
	runtimeCfg   = runtimeSettings{Mode: modeDirectWithFallback, CF: cfProxyConfig{Domain: defaultCfProxyDomain, Enabled: true}}
	runtimeCfgMu sync.RWMutex
	cfPool       = newCFDomainPool()
)

func init() {
	cfPool.SetBuiltinDomains(tgwsroute.BuiltInCFDomains())
}

var policyGenCounter atomic.Uint64

func setRuntimeSettings(cfg runtimeSettings) {
	prev := getRuntimeSettings()
	cfg.CF = normalizeCfProxyConfig(cfg.CF)
	cfg.Worker.Domain = NormalizeWorkerDomain(cfg.Worker.Domain)
	cfg.MtProtoFakeTLSDomain = mtproxyfrontend.NormalizeFakeTLSDomain(cfg.MtProtoFakeTLSDomain)
	cfg.MtProtoMaskingPassthrough = cfg.MtProtoMaskingPassthrough && cfg.MtProtoFakeTLSDomain != ""
	cfg.CFManualDomains = tgwsroute.NormalizeCFDomains(cfg.CFManualDomains)
	cfg.PolicyGen = policyGenCounter.Add(1)
	runtimeCfgMu.Lock()
	runtimeCfg = cfg
	runtimeCfgMu.Unlock()
	resetProxyRouteDisplayState()
	cfPool.SetManualDomains(cfg.CFManualDomains)
	cfPool.SetCachedUpstreamDomains(cfg.CFCachedUpstream)
	applyAdaptiveProfile(cfg)
	logPolicyChange(prev, cfg)
}

func parsePreferredRoute(raw string) routeKind {
	s := strings.ToLower(strings.TrimSpace(raw))
	switch s {
	case "direct_ws", "direct":
		return routeDirectWS
	case "worker_ws", "cf_worker_ws", "worker":
		return routeCFWorkerWS
	case "cf_proxy_ws", "cf":
		return routeCFProxyWS
	case "tcp_fallback", "tcp":
		return routeTCPFallback
	default:
		return ""
	}
}

func allowedRoutesList(s runtimeSettings) []routeKind {
	if !s.PolicyPresent {
		return nil
	}
	out := make([]routeKind, 0, 4)
	if s.AllowDirect {
		out = append(out, routeDirectWS)
	}
	if s.AllowWorker {
		out = append(out, routeCFWorkerWS)
	}
	if s.AllowCFProxy {
		out = append(out, routeCFProxyWS)
	}
	if s.AllowTCP {
		out = append(out, routeTCPFallback)
	}
	return out
}

func routeKindStrings(routes []routeKind) []string {
	out := make([]string, 0, len(routes))
	for _, r := range routes {
		out = append(out, string(r))
	}
	return out
}

func logPolicyChange(old, cur runtimeSettings) {
	if !cur.PolicyPresent {
		return
	}
	oldAllowed := strings.Join(routeKindStrings(allowedRoutesList(old)), "|")
	newAllowed := strings.Join(routeKindStrings(allowedRoutesList(cur)), "|")
	if oldAllowed == "" {
		oldAllowed = "none"
	}
	if newAllowed == "" {
		newAllowed = "none"
	}
	logInfo.Printf("Policy changed generation=%d oldRoutes=%s newRoutes=%s preferred=%s fallback=%t",
		cur.PolicyGen, oldAllowed, newAllowed, cur.Preferred, cur.AllowFallback)

	// Close pools for disabled routes (best-effort). This prevents "ghost" worker/direct usage.
	if !cur.AllowWorker {
		workerPool.CloseAll()
	}
	if !cur.AllowDirect {
		wsPool.CloseAll()
	}
}

func getRuntimeSettings() runtimeSettings {
	runtimeCfgMu.RLock()
	defer runtimeCfgMu.RUnlock()
	return runtimeCfg
}

func parseConnectionMode(raw string) (connectionMode, bool) {
	return tgwsroute.ParseConnectionMode(raw)
}

func legacyModeFromCF(cfg cfProxyConfig) connectionMode {
	return tgwsroute.LegacyModeFromCF(tgwsroute.CFProxyFlags{
		Enabled:  cfg.Enabled,
		Priority: cfg.Priority,
		Only:     cfg.Only,
	})
}

func NormalizeWorkerDomain(raw string) string {
	return tgwsroute.NormalizeWorkerDomain(raw)
}

func workerDomainLooksValid(domain string) bool {
	if domain == "" || strings.Contains(domain, " ") {
		return false
	}
	if strings.Contains(domain, "/apiws") {
		return false
	}
	return strings.Contains(domain, ".")
}

func buildWorkerWSPath(dcID int, dcIP string, media bool, sessionID string) string {
	return tgwsroute.BuildWorkerWSPath(dcID, dcIP, media, sessionID)
}

func buildWorkerWSURL(workerDomain string, dcID int, dcIP string, media bool, sessionID string) string {
	return tgwsroute.BuildWorkerWSURL(workerDomain, dcID, dcIP, media, sessionID)
}

func lookupTelegramDC(dst string) (tgwsroute.TelegramDCInfo, bool) {
	return tgwsroute.LookupTelegramDC(dst)
}

func isTelegramLikeIP(dst string) bool {
	return tgwsroute.IsTelegramLikeIP(dst)
}

func isRestrictedMode(mode connectionMode) bool {
	return tgwsroute.IsRestrictedMode(mode)
}

func blocksDirectPassthrough(mode connectionMode, dst string, mapped bool) bool {
	return tgwsroute.BlocksDirectPassthrough(mode, dst, mapped)
}

func toRouteSettings(settings runtimeSettings) tgwsroute.RouteSettings {
	return tgwsroute.RouteSettings{
		Mode: settings.Mode,
		CF: tgwsroute.CFProxyFlags{
			Enabled:  settings.CF.Enabled,
			Priority: settings.CF.Priority,
			Only:     settings.CF.Only,
		},
		Worker: tgwsroute.WorkerSettings{
			Enabled: settings.Worker.Enabled,
			Domain:  settings.Worker.Domain,
		},
	}
}

func telegramDCTargetIP(dc int, fallbackDst string) string {
	if ip, ok := dcDefaultIP[dc]; ok && ip != "" {
		return ip
	}
	return fallbackTarget(dc, fallbackDst)
}

func effectiveWSHostDC(dc int) int {
	if mapped, ok := dcOverrides[dc]; ok {
		return mapped
	}
	return dc
}

// ---------------------------------------------------------------------------
// CF domain pool
// ---------------------------------------------------------------------------

func newCFDomainPool() *tgwsroute.CFDomainPool {
	return tgwsroute.NewCFDomainPool(monoNow)
}

// ---------------------------------------------------------------------------
// Route planning
// ---------------------------------------------------------------------------

func routesForMode(mode connectionMode, settings runtimeSettings, skipDirect bool) []routeKind {
	base := tgwsroute.RoutesForMode(mode, toRouteSettings(settings), skipDirect)
	return filterRoutesByPolicy(settings, base, "routesForMode")
}

func primaryRoutesBeforeDirectWS(mode connectionMode, settings runtimeSettings) []routeKind {
	return tgwsroute.PrimaryRoutesBeforeDirectWS(mode, toRouteSettings(settings))
}

func isRouteAllowedByPolicy(settings runtimeSettings, r routeKind) bool {
	if !settings.PolicyPresent {
		return true
	}
	switch r {
	case routeDirectWS:
		return settings.AllowDirect
	case routeCFWorkerWS:
		return settings.AllowWorker
	case routeCFProxyWS:
		return settings.AllowCFProxy
	case routeTCPFallback:
		return settings.AllowTCP
	default:
		return false
	}
}

func filterRoutesByPolicy(settings runtimeSettings, routes []routeKind, reason string) []routeKind {
	if !settings.PolicyPresent {
		return routes
	}
	if len(routes) == 0 {
		return routes
	}
	before := strings.Join(routeKindStrings(routes), "|")
	allowed := strings.Join(routeKindStrings(allowedRoutesList(settings)), "|")
	out := make([]routeKind, 0, len(routes))
	var dropped []routeKind
	for _, r := range routes {
		if isRouteAllowedByPolicy(settings, r) {
			out = append(out, r)
		} else {
			dropped = append(dropped, r)
		}
	}
	after := strings.Join(routeKindStrings(out), "|")
	if after == "" {
		after = "none"
	}
	if allowed == "" {
		allowed = "none"
	}
	if len(dropped) > 0 {
		logInfo.Printf("Route policy filter reason=%s allowed=%s before=%s after=%s dropped=%s",
			reason, allowed, before, after, strings.Join(routeKindStrings(dropped), "|"))
	}
	return out
}

// ---------------------------------------------------------------------------
// Worker + CF proxy dial
// ---------------------------------------------------------------------------

const cfWorkerModeSocks5Transparent = tgwsroute.WorkerModeSocks5Transparent

func snapshotDcOptMap() map[int]string {
	dcOptMu.RLock()
	defer dcOptMu.RUnlock()
	out := make(map[int]string, len(dcOpt))
	for dc, ip := range dcOpt {
		out[dc] = ip
	}
	return out
}

func resolveWorkerDestinationPlan(
	workerDomain string,
	dc int,
	isMedia bool,
	dcOK bool,
	parsedDstHost string,
	settings runtimeSettings,
) tgwsroute.WorkerDestinationPlan {
	return tgwsroute.ResolveCfWorkerDestination(tgwsroute.WorkerDestinationInput{
		WorkerDomain:  workerDomain,
		DCID:          dc,
		IsMedia:       isMedia,
		DCOk:          dcOK,
		ParsedDstHost: parsedDstHost,
		Mode:          settings.Worker.DestinationMode,
		DcIPMap:       snapshotDcOptMap(),
		MediaFix: tgwsroute.FlowsealMediaFixConfig{
			Enabled: settings.Worker.MediaFix.Enabled,
			DC:      settings.Worker.MediaFix.DC,
			IP:      settings.Worker.MediaFix.IP,
		},
	})
}

func buildCfWorkerDestinationPlanWithMedia(workerDomain string, session *initSession, parsedDstHost string, settings runtimeSettings, isMedia bool) tgwsroute.WorkerDestinationPlan {
	return resolveWorkerDestinationPlan(
		workerDomain,
		session.dc,
		isMedia,
		session.dcOk,
		parsedDstHost,
		settings,
	)
}

func buildMtProtoWorkerDestinationPlan(workerDomain string, dc int, isMedia bool, settings runtimeSettings) tgwsroute.WorkerDestinationPlan {
	parsedDstHost, dcOK := tgwsroute.WorkerCanonicalIPv4ForDC(dc)
	return resolveWorkerDestinationPlan(workerDomain, dc, isMedia, dcOK, parsedDstHost, settings)
}

func buildCfWorkerDestinationPlan(workerDomain string, session *initSession, parsedDstHost string, settings runtimeSettings) tgwsroute.WorkerDestinationPlan {
	return buildCfWorkerDestinationPlanWithMedia(workerDomain, session, parsedDstHost, settings, session.isMedia)
}

func buildCfWorkerTransparentWorkerURL(workerDomain string, session *initSession, parsedDstHost string) tgwsroute.CfWorkerTransparentResolve {
	settings := getRuntimeSettings()
	return buildCfWorkerDestinationPlan(workerDomain, session, parsedDstHost, settings).CfWorkerTransparentResolve
}

func shouldConsumeWorkerMediaSuspect(settings runtimeSettings, dc int, isMedia bool) (bool, string) {
	if isMedia || dc != 2 {
		return false, ""
	}
	if !settings.Worker.MediaFix.Enabled || !tgwsroute.IsExperimentalForceMediaDC4Mode(settings.Worker.DestinationMode) {
		return false, ""
	}
	return consumeWorkerMediaSuspect(dc)
}

func cfWorkerFallback(ctx context.Context, client net.Conn, session *initSession, label string, dst string, port int) (bool, string) {
	noteRouteConnectStarted(routeCFWorkerWS)
	settings := getRuntimeSettings()
	dc := session.dc
	isMedia := session.isMedia
	mTag := mediaTag(isMedia)
	sessionID := strings.TrimSpace(session.workerSessionID)
	if sessionID == "" {
		sessionID = newWorkerSessionID()
		session.workerSessionID = sessionID
	}

	if settings.PolicyPresent && !settings.AllowWorker {
		logWarn.Printf("[%s] session_id=%s DC%d%s Blocked endpoint build route=cf_worker_ws reason=disabled_by_policy allowed=%s",
			label, sessionID, dc, mTag, strings.Join(routeKindStrings(allowedRoutesList(settings)), "|"))
		return false, "disabled_by_policy"
	}
	if !settings.Worker.Enabled || settings.Worker.Domain == "" {
		logDebug.Printf("[%s] session_id=%s DC%d skipping Worker route: domain is empty", label, sessionID, dc)
		return false, "worker_disabled_or_empty_domain"
	}

	parsedDstHost := strings.TrimSpace(dst)
	workerDomain := settings.Worker.Domain
	if candidates := settings.Worker.Failover.effectiveCandidates(settings.Worker.Domain); len(candidates) > 0 {
		workerDomain = candidates[0].Domain
	}
	workerInputMedia := session.isMedia
	mediaPromoteReason := ""
	if promoted, reason := shouldConsumeWorkerMediaSuspect(settings, dc, workerInputMedia); promoted {
		workerInputMedia = true
		mediaPromoteReason = reason
	}
	resolved := buildCfWorkerDestinationPlanWithMedia(workerDomain, session, parsedDstHost, settings, workerInputMedia)
	configuredMode := resolved.ConfiguredDestinationMode
	if configuredMode == "" {
		configuredMode = tgwsroute.WorkerDestinationPreserveOriginalDst
	}
	effectiveMode := resolved.EffectiveDestinationMode
	if effectiveMode == "" {
		effectiveMode = tgwsroute.WorkerDestinationPreserveOriginalDst
	}
	effectiveDC := resolved.EffectiveDC
	if effectiveDC <= 0 {
		effectiveDC = dc
	}
	effectiveMedia := resolved.EffectiveIsMedia
	audit := resolved.MediaAudit
	if mediaPromoteReason != "" {
		logWarn.Printf("[%s] session_id=%s worker_media_suspect_consumed=true reason=%s original_parsed_dst=%s mapped_dc=%d effective_dc=%d worker_dst=%s",
			label, sessionID, mediaPromoteReason, parsedDstHost, dc, effectiveDC, resolved.WorkerDst)
	}

	if !resolved.OK {
		if resolved.FailReason == tgwsroute.FailWorkerIPv6Unsupported {
			mappedDC := "none"
			if session.dcOk {
				mappedDC = fmt.Sprintf("%d", dc)
			}
			logWarn.Printf("[%s] session_id=%s route_decision=blocked_or_failed reason=%s mapped_dc=%s parsed_dst=%s",
				label, sessionID, tgwsroute.FailTelegramIPv6UnknownDCNoMapping, mappedDC, session.parsedDst)
			logWarn.Printf("[%s] session_id=%s cf_worker_ws ipv6 skipped: mapped_dc=%s parsed_dst=%s reason=%s",
				label, sessionID, mappedDC, session.parsedDst, resolved.FailReason)
		}
		noteRouteConnectFailed(routeCFWorkerWS, resolved.FailReason)
		return false, resolved.FailReason
	}

	workerDst := resolved.WorkerDst

	if effectiveMode == tgwsroute.WorkerDestinationPreserveOriginalDst &&
		resolved.DstFamily == tgwsroute.DstFamilyIPv4 && workerDst != parsedDstHost {
		logError.Printf("[%s] session_id=%s cf_worker_ws transparent dst rewrite detected parsed_dst_host=%s worker_dst=%s preserve_original_dst=true forcing=%s",
			label, sessionID, parsedDstHost, workerDst, parsedDstHost)
		workerDst = parsedDstHost
		resolved.PreserveOriginalDst = true
		resolved.WorkerDstSource = tgwsroute.WorkerDstSourceParsedHost
	}

	dcOptMap := snapshotDcOptMap()
	manualDC2Override := tgwsroute.HasDC2ManualOverride(dcOptMap)
	manualDC2IP := strings.TrimSpace(dcOptMap[2])
	selectedDst, _ := selectDC2WorkerDst(dc2WorkerDstSelectInput{
		DC:                effectiveDC,
		DestinationMode:   effectiveMode,
		ParsedDstHost:     parsedDstHost,
		InitialWorkerDst:  workerDst,
		DstFamily:         resolved.DstFamily,
		PreserveOriginal:  resolved.PreserveOriginalDst,
		WorkerDstSource:   resolved.WorkerDstSource,
		ManualDC2Override: manualDC2Override,
		ManualDC2IP:       manualDC2IP,
	})
	if selectedDst != "" {
		workerDst = selectedDst
	}
	if resolved.DstFamily == tgwsroute.DstFamilyIPv6 && effectiveDC == 2 &&
		resolved.WorkerDstSource == tgwsroute.WorkerDstSourceIPv6ToDCIPv4 {
		logIPv6DC2CandidateSelection(parsedDstHost, workerDst, effectiveMode)
	}

	workerURL := buildWorkerWSURL(workerDomain, effectiveDC, workerDst, effectiveMedia, sessionID)

	if tgwsroute.WorkerURLContainsRawIPv6Dst(workerURL) {
		logError.Printf("[%s] session_id=%s raw ipv6 dst leaked into worker_url url=%s parsed_dst=%s",
			label, sessionID, workerURL, session.parsedDst)
		noteRouteConnectFailed(routeCFWorkerWS, tgwsroute.FailWorkerIPv6RawDstBlocked)
		return false, tgwsroute.FailWorkerIPv6RawDstBlocked
	}

	var reencryptCtx *workerReencryptContext
	if workerReencryptBridgeEnabled && resolved.FlowsealMediaFixApplied {
		ctx, err := newWorkerReencryptContext(session.original, effectiveDC, effectiveMedia)
		if err != nil {
			logWarn.Printf("[%s] session_id=%s worker_reencrypt_bridge_unavailable=true reason=%v fallback=patched_init",
				label, sessionID, err)
		} else {
			reencryptCtx = ctx
		}
	}
	patchWorkerInit := resolved.FlowsealMediaFixApplied && reencryptCtx == nil
	firstPacket := session.workerFirstPacketForDestination(effectiveDC, effectiveMedia, patchWorkerInit)
	if reencryptCtx != nil {
		firstPacket = reencryptCtx.relayInit
	}
	hashBefore := sha256Hex(session.original)
	hashAfter := sha256Hex(firstPacket)
	mutated := initPacketMutated(session.original, firstPacket)

	logInfo.Printf("[%s] session_id=%s route_decision=cf_worker_ws configured_destination_mode=%s effective_destination_mode=%s original_parsed_dst=%s mapped_dc=%d is_media=%t worker_dst=%s worker_dst_source=%s flowseal_media_fix_applied=%t",
		label, sessionID, configuredMode, effectiveMode, parsedDstHost, effectiveDC, effectiveMedia, workerDst, resolved.WorkerDstSource, resolved.FlowsealMediaFixApplied)
	logInfo.Printf("[%s] session_id=%s media_classification is_media=%t media_reason=%s telegram_class=%s media_fix_eligible=%t media_fix_skip_reason=%s",
		label, sessionID, audit.IsMedia, audit.MediaReason, audit.TelegramClass, audit.MediaFixEligible, audit.MediaFixSkipReason)
	if resolved.FlowsealMediaFixApplied {
		logWarn.Printf("[%s] session_id=%s flowseal_media_tcp_override=true original_parsed_dst=%s logical_dc=%d is_media=%t worker_dst=%s",
			label, sessionID, parsedDstHost, effectiveDC, effectiveMedia, workerDst)
	} else if configuredMode == tgwsroute.WorkerDestinationExperimentalForceMediaDC4 {
		logInfo.Printf("[%s] session_id=%s configured_destination_mode=%s effective_destination_mode=%s flowseal_media_fix_applied=false media_fix_skip_reason=%s",
			label, sessionID, configuredMode, effectiveMode, audit.MediaFixSkipReason)
	}
	logInfo.Printf("[%s] session_id=%s destination_mode=%s worker_url=%s", label, sessionID, effectiveMode, workerURL)
	logDebug.Printf("[%s] session_id=%s route=cf_worker_ws mode=%s parsed_dst=%s dst_family=%s mapped_dc=%d is_media=%t ipv6_worker_direct=%t worker_dst=%s worker_dst_source=%s preserve_original_dst=%t first_packet_mutated=%t worker_init_patch=%t worker_reencrypt_bridge=%t first_packet_len=%d first_packet_sha256_before=%s first_packet_sha256_after=%s worker_url=%s",
		label, sessionID, cfWorkerModeSocks5Transparent, session.parsedDst, resolved.DstFamily, effectiveDC, effectiveMedia, resolved.IPv6WorkerDirect, workerDst, resolved.WorkerDstSource, resolved.PreserveOriginalDst, mutated, patchWorkerInit, reencryptCtx != nil, len(firstPacket), hashBefore, hashAfter, workerURL)

	ws, candidate, workerAttempts, failReason, ok := tryWorkerFailoverConnect(settings, label, effectiveDC, effectiveMedia, workerDst, sessionID)
	if !ok || ws == nil {
		reason := "worker_ws_connect_failed"
		if failReason != "" {
			reason = failReason
		}
		logWarn.Printf("[%s] session_id=%s DC%d%s Worker WS failed: %s", label, sessionID, dc, mTag, reason)
		noteRouteConnectFailed(routeCFWorkerWS, reason)
		if settings.Mode == modeAuto || settings.Mode == modeDirectWithFallback {
			recordAdaptiveFailure(routeCFWorkerWS, effectiveDC, effectiveMedia, reason, 0)
		}
		return false, reason
	}

	domain := candidate.Domain
	logInfo.Printf("[%s] session_id=%s DC%d%s Worker WS connected host=%s workerId=%s worker_dst=%s attempt=%d",
		label, sessionID, dc, mTag, domain, candidate.ID, workerDst, workerAttempts)

	sendPacket := firstPacket
	if initPacketMutated(session.original, sendPacket) && !patchWorkerInit && reencryptCtx == nil {
		logError.Printf("[%s] session_id=%s cf_worker_ws first packet mutated before send sha256_before=%s sha256_after=%s",
			label, sessionID, hashBefore, sha256Hex(sendPacket))
		sendPacket = session.workerFirstPacket()
	}
	if reencryptCtx != nil {
		logInfo.Printf("[%s] session_id=%s worker_reencrypt_bridge=true relay_init_len=%d logical_dc=%d is_media=%t worker_dst=%s",
			label, sessionID, len(reencryptCtx.relayInit), effectiveDC, effectiveMedia, workerDst)
	} else {
		logDebug.Printf("[%s] session_id=%s cf_worker_ws raw stream forwarding enabled", label, sessionID)
	}

	if err := ws.Send(sendPacket); err != nil {
		ws.Close()
		logWarn.Printf("[%s] session_id=%s DC%d%s Worker first write failed; retrying fresh ws host=%s workerId=%s err=%v",
			label, sessionID, dc, mTag, domain, candidate.ID, err)
		noteWorkerConnectionAttemptStarted(candidate.ID)
		retryPath := buildWorkerWSPath(effectiveDC, workerDst, effectiveMedia, sessionID)
		retryPrefix := fmt.Sprintf("[%s] session_id=%s DC%d%s cfworker retry_first_write", label, sessionID, effectiveDC, mediaTag(effectiveMedia))
		retryWS, retryErr := dialWorkerCandidate(domain, retryPath, retryPrefix)
		workerAttempts++
		if retryErr != nil || retryWS == nil {
			noteWorkerConnectionAttemptFailed(candidate.ID, "worker_first_write_retry_connect_failed")
			noteRouteConnectFailed(routeCFWorkerWS, "worker_first_write_retry_connect_failed")
			if settings.Mode == modeAuto || settings.Mode == modeDirectWithFallback {
				recordAdaptiveFailure(routeCFWorkerWS, effectiveDC, effectiveMedia, "worker_first_write_retry_connect_failed", 0)
			}
			return false, fmt.Sprintf("worker_first_write_retry_connect_failed: %v", retryErr)
		}
		ws = retryWS
		rtRuntimeWorkerDomain.Store(domain)
		rtCurrentWorkerDomain.Store(domain)
		if err := ws.Send(sendPacket); err != nil {
			ws.Close()
			noteWorkerConnectionAttemptFailed(candidate.ID, "worker_first_write_retry_failed")
			noteRouteConnectFailed(routeCFWorkerWS, "worker_first_write_retry_failed")
			if settings.Mode == modeAuto || settings.Mode == modeDirectWithFallback {
				recordAdaptiveFailure(routeCFWorkerWS, effectiveDC, effectiveMedia, "worker_first_write_retry_failed", 0)
			}
			return false, fmt.Sprintf("worker_first_write_retry_failed: %v", err)
		}
		logInfo.Printf("[%s] session_id=%s worker_first_write_retry_success=true host=%s workerId=%s attempt=%d",
			label, sessionID, domain, candidate.ID, workerAttempts)
	}

	logInfo.Printf("[%s] session_id=%s first_packet_sent_to_worker=true worker_dst=%s attempt=%d",
		label, sessionID, workerDst, workerAttempts)
	noteWorkerConnectionAttemptSuccess(candidate.ID, 0)
	noteRouteConnectSucceeded(routeCFWorkerWS)
	logInfo.Printf("[%s] session_id=%s route_success_after_first_write=true", label, sessionID)
	noteActiveRoute(routeCFWorkerWS)
	stats.connectionsCfWorker.Add(1)

	bridgeSplitter := (*MsgSplitter)(nil)
	meta := bridgeWSMeta{
		route:              routeCFWorkerWS,
		workerHost:         domain,
		sessionID:          sessionID,
		configuredDestMode: configuredMode,
		effectiveDestMode:  effectiveMode,
		originalParsedDst:  parsedDstHost,
		workerDst:          workerDst,
		mappedDC:           effectiveDC,
		isMedia:            effectiveMedia,
		mediaFixApplied:    resolved.FlowsealMediaFixApplied,
	}
	if reencryptCtx != nil {
		if splitter, err := newMsgSplitter(reencryptCtx.relayInit); err == nil {
			bridgeSplitter = splitter
		} else {
			logWarn.Printf("[%s] session_id=%s worker_reencrypt_splitter_unavailable=true reason=%v",
				label, sessionID, err)
		}
		meta.upTransform = reencryptCtx.clientToRelay
		meta.downTransform = reencryptCtx.relayToClient
	}

	summary := bridgeWS(ctx, client, ws, label, effectiveDC, domain, 443, effectiveMedia, bridgeSplitter, meta)
	recordAdaptiveSessionSuccess(routeCFWorkerWS, effectiveDC, effectiveMedia, 0, summary.String(), settings)
	return true, summary.String()
}

func cfProxyFallbackWithPool(ctx context.Context, client net.Conn, init []byte, label string, dc int, isMedia bool, splitter *MsgSplitter) (bool, string) {
	noteRouteConnectStarted(routeCFProxyWS)
	settings := getRuntimeSettings()
	if settings.PolicyPresent && !settings.AllowCFProxy {
		return false, "disabled_by_policy"
	}
	if !settings.CF.Enabled {
		return false, "cfproxy_disabled"
	}

	mTag := mediaTag(isMedia)
	wsDC := effectiveWSHostDC(dc)
	selection := cfPool.SelectionForDC(wsDC)
	for _, skipped := range selection.SkippedCooldown {
		logDebug.Printf("[%s] DC%d%s CF domain skipped domain=%s reason=cooldown until=%s",
			label, dc, mTag, skipped.Domain, formatCooldownUntil(skipped.CooldownUntil))
	}
	if len(selection.Candidates) == 0 {
		stats.cfPoolMisses.Add(1)
		logWarn.Printf("[%s] DC%d%s CF pool exhausted mode=%s", label, dc, mTag, settings.Mode)
		return false, "cfproxy_unavailable"
	}
	stats.cfPoolHits.Add(1)

	skipCachedUpstream := false
	for _, candidate := range selection.Candidates {
		if skipCachedUpstream && candidate.Source == tgwsroute.CFDomainSourceCachedUpstream {
			logDebug.Printf("[%s] DC%d%s CF cached upstream skipped domain=%s reason=previous_dns_failure",
				label, dc, mTag, candidate.Domain)
			continue
		}
		baseDomain := candidate.Domain
		host := cfProxyHost(wsDC, baseDomain)
		url := fmt.Sprintf("wss://%s/apiws", host)
		logInfo.Printf("[%s] DC%d%s CF domain selector source=%s domain=%s score=%d",
			label, dc, mTag, candidate.Source, baseDomain, candidate.Score)
		logDebug.Printf("[%s] DC%d%s -> CF proxy %s (pool domain=%s)", label, dc, mTag, url, baseDomain)

		ok, reason, failureKind, latencyMs := cfProxyDialHost(ctx, client, init, label, dc, isMedia, splitter, host, baseDomain)
		if ok {
			cfPool.MarkSuccess(wsDC, baseDomain, latencyMs)
			noteRouteConnectSucceeded(routeCFProxyWS)
			recordAdaptiveSessionSuccess(routeCFProxyWS, dc, isMedia, latencyMs, reason, settings)
			logInfo.Printf("[%s] DC%d%s CF proxy selected domain=%s", label, dc, mTag, baseDomain)
			return true, reason
		}
		health := cfPool.MarkFailure(baseDomain, failureKind, latencyMs)
		logWarn.Printf("[%s] DC%d%s CF domain failed domain=%s reason=%s", label, dc, mTag, baseDomain, failureKind)
		logInfo.Printf("[%s] DC%d%s CF domain cooldown domain=%s reason=%s until=%s",
			label, dc, mTag, baseDomain, failureKind, formatCooldownUntil(health.CooldownUntil))
		logDebug.Printf("[%s] DC%d%s CF proxy failed domain=%s detail=%s", label, dc, mTag, baseDomain, reason)
		if candidate.Source == tgwsroute.CFDomainSourceCachedUpstream && tgwsroute.IsCFDNSFailure(failureKind) {
			skipCachedUpstream = true
			logInfo.Printf("[%s] DC%d%s CF cached upstream fast-skip enabled after dns failure domain=%s",
				label, dc, mTag, baseDomain)
		}
	}

	logWarn.Printf("[%s] DC%d%s CF pool exhausted mode=%s", label, dc, mTag, settings.Mode)
	stats.cfPoolRefillErrors.Add(1)
	noteRouteConnectFailed(routeCFProxyWS, "cfproxy_all_domains_failed")
	return false, "cfproxy_all_domains_failed"
}

func cfProxyDialHost(ctx context.Context, client net.Conn, init []byte, label string, dc int, isMedia bool, splitter *MsgSplitter, host, baseDomain string) (bool, string, tgwsroute.CFFailureKind, int64) {
	mTag := mediaTag(isMedia)
	started := time.Now()

	resolved, err := resolvePreferredIPs(host, 10)
	var candidates []string
	if err == nil {
		candidates = resolved.Preferred()
	}

	prefix := fmt.Sprintf("[%s] DC%d%s cfproxy", label, dc, mTag)
	var ws *RawWebSocket
	var lastErr error
	usedTarget := "hostname"

	ws, lastErr = wsConnect(host, host, "/apiws", 10)
	if lastErr != nil {
		logDomainConnectFailure(prefix, host, host, lastErr)
	}

	for _, ip := range candidates {
		if ws != nil {
			break
		}
		ws, lastErr = wsConnect(ip, host, "/apiws", 10)
		if lastErr == nil {
			usedTarget = ip
			break
		}
		logDomainConnectFailure(prefix, host, ip, lastErr)
		var wsErr *WsHandshakeError
		if errors.As(lastErr, &wsErr) && wsErr.IsRedirect() {
			break
		}
	}

	if ws == nil {
		if lastErr != nil {
			return false, fmt.Sprintf("ws_connect_failed: %v", lastErr), classifyCFFailure(lastErr), elapsedMillis(started)
		}
		return false, "ws_connect_failed", tgwsroute.CFFailureWebSocket, elapsedMillis(started)
	}

	logInfo.Printf("[%s] DC%d%s cfproxy connected host=%s via=%s", label, dc, mTag, host, usedTarget)
	noteActiveRoute(routeCFProxyWS)
	stats.connectionsCfProxy.Add(1)

	if err := ws.Send(init); err != nil {
		ws.Close()
		return false, fmt.Sprintf("first_client_to_ws_write_failed: %v", err), tgwsroute.CFFailureWebSocket, elapsedMillis(started)
	}

	summary := bridgeWS(ctx, client, ws, label, dc, host, 443, isMedia, splitter, bridgeWSMeta{route: routeCFProxyWS})
	return true, summary.String(), "", elapsedMillis(started)
}

func runRouteChain(ctx context.Context, client net.Conn, session *initSession, label string, dst string, port int, routes []routeKind) bool {
	settings := getRuntimeSettings()
	dc := session.dc
	isMedia := session.isMedia
	mTag := mediaTag(isMedia)
	target := fallbackTarget(dc, dst)
	cfFailed := false
	var lastFailRoute routeKind
	var lastFailReason string

	routes = filterWorkerRouteCooldown(routes, dc, isMedia)
	if len(routes) > 0 {
		noteRouteSelected(routes[0])
	}

	if settings.Mode == modeWorkerOnly && (!settings.Worker.Enabled || settings.Worker.Domain == "") {
		logWarn.Printf("[%s] DC%d%s Worker domain is empty in worker-only mode", label, dc, mTag)
		return false
	}
	if settings.Mode == modeCFOnly && !settings.CF.Enabled {
		logWarn.Printf("[%s] DC%d%s CF proxy is unavailable in cf-only mode", label, dc, mTag)
		return false
	}

	for _, route := range routes {
		if lastFailRoute != "" {
			noteFallbackActivated(route, lastFailReason)
		}
		switch route {
		case routeCFWorkerWS:
			if cfFailed {
				logInfo.Printf("[%s] DC%d%s CF pool fallback to Worker", label, dc, mTag)
			}
			if !settings.Worker.Enabled || settings.Worker.Domain == "" {
				if settings.Mode == modeWorkerOnly {
					logWarn.Printf("[%s] DC%d%s Worker only mode but Worker domain is empty", label, dc, mTag)
					return false
				}
				logInfo.Printf("[%s] DC%d%s Skipping Worker route: domain is empty", label, dc, mTag)
				continue
			}
			if ok, reason := cfWorkerFallback(ctx, client, session, label, dst, port); ok {
				logInfo.Printf("[%s] DC%d%s Worker closed: reason=%s", label, dc, mTag, reason)
				noteConnectionClosed(routeCFWorkerWS)
				return true
			}
			lastFailRoute = routeCFWorkerWS
			lastFailReason = "worker_failed"
			if settings.Mode == modeAuto || settings.Mode == modeDirectWithFallback {
				recordAdaptiveFailure(routeCFWorkerWS, dc, isMedia, "worker_failed", 0)
			}
		case routeCFProxyWS:
			routeInit, routeSplitter := session.prepareForRoute(routeCFProxyWS)
			ok, cfReason := cfProxyFallbackWithPool(ctx, client, routeInit, label, dc, isMedia, routeSplitter)
			if ok {
				logInfo.Printf("[%s] DC%d%s CF proxy closed: reason=%s", label, dc, mTag, cfReason)
				noteConnectionClosed(routeCFProxyWS)
				return true
			}
			lastFailRoute = routeCFProxyWS
			lastFailReason = cfReason
			if settings.Mode == modeCFOnly {
				if cfReason == "cfproxy_unavailable" || cfReason == "cfproxy_all_domains_failed" {
					logWarn.Printf("[%s] DC%d%s CF proxy is unavailable in cf-only mode", label, dc, mTag)
				} else {
					logWarn.Printf("[%s] DC%d%s CF only mode failed: %s", label, dc, mTag, cfReason)
				}
				return false
			}
			if settings.Mode != modeCFOnly {
				recordAdaptiveFailure(routeCFProxyWS, dc, isMedia, "cfproxy_failed", 0)
			}
			cfFailed = true
		case routeTCPFallback:
			noteRouteConnectStarted(routeTCPFallback)
			logInfo.Printf("[%s] DC%d%s -> TCP fallback to %s", label, dc, mTag, joinAddr(target, port))
			routeInit, _ := session.prepareForRoute(routeTCPFallback)
			if tcpFallback(ctx, client, target, port, routeInit, label, dc, isMedia) {
				noteRouteConnectSucceeded(routeTCPFallback)
				noteActiveRoute(routeTCPFallback)
				recordAdaptiveSessionSuccess(routeTCPFallback, dc, isMedia, 0, "", settings)
				return true
			}
			noteRouteConnectFailed(routeTCPFallback, "tcp_failed")
			lastFailRoute = routeTCPFallback
			lastFailReason = "tcp_failed"
			recordAdaptiveFailure(routeTCPFallback, dc, isMedia, "tcp_failed", 0)
		case routeDirectWS:
			// Direct WS is handled by the caller before fallback chains.
			continue
		}
	}

	if settings.Mode == modeWorkerOnly {
		logWarn.Printf("[%s] DC%d%s no Worker route available in worker-only mode", label, dc, mTag)
	}
	logWarn.Printf("[%s] DC%d%s no fallback available", label, dc, mTag)
	return false
}

func classifyCFFailure(err error) tgwsroute.CFFailureKind {
	var wsErr *WsHandshakeError
	if errors.As(err, &wsErr) {
		switch {
		case wsErr.StatusCode == 429:
			return tgwsroute.CFFailureRateLimit
		case wsErr.StatusCode == 403:
			return tgwsroute.CFFailureForbidden
		case wsErr.StatusCode >= 500 && wsErr.StatusCode <= 599:
			return tgwsroute.CFFailureServer
		default:
			return tgwsroute.CFFailureWebSocket
		}
	}

	var stageErr *wsStageError
	if errors.As(err, &stageErr) {
		var dnsErr *net.DNSError
		if errors.As(stageErr.Err, &dnsErr) {
			return tgwsroute.CFFailureDNS
		}
		if ne, ok := stageErr.Err.(net.Error); ok && ne.Timeout() {
			return tgwsroute.CFFailureTimeout
		}
		switch stageErr.Stage {
		case "tls_handshake":
			return tgwsroute.CFFailureTLS
		case "ws_upgrade_write", "ws_upgrade_read":
			return tgwsroute.CFFailureWebSocket
		}
	}

	return tgwsroute.CFFailureUnknown
}

func formatCooldownUntil(until float64) string {
	if until <= 0 {
		return "none"
	}
	return time.Unix(int64(until), 0).Format(time.RFC3339)
}

func elapsedMillis(start time.Time) int64 {
	return time.Since(start).Milliseconds()
}

func shouldSkipDirectWS(dcKey [2]int, mode connectionMode) bool {
	if mode == modeCFOnly || mode == modeWorkerOnly {
		return true
	}
	now := monoNow()
	dcFailMu.RLock()
	failUntil := dcFailUntil[dcKey]
	dcFailMu.RUnlock()
	if mode == modeAuto && now < failUntil {
		return true
	}
	if mode == modeDirectWithFallback && now < failUntil {
		return true
	}
	return false
}

func logRuntimeRouteConfig() {
	settings := getRuntimeSettings()
	logInfo.Printf("  Connection mode: %s", settings.Mode)
	if settings.Worker.Enabled {
		if settings.Worker.Domain != "" {
			logInfo.Printf("  CF Worker:     enabled domain=%s", settings.Worker.Domain)
		} else {
			logInfo.Println("  CF Worker:     enabled but domain is empty")
		}
	} else {
		logInfo.Println("  CF Worker:     disabled")
	}
	if settings.CF.Enabled {
		mode := "fallback"
		if settings.CF.Only {
			mode = "only"
		} else if settings.CF.Priority {
			mode = "cf_first"
		}
		poolNote := "builtin pool"
		switch {
		case len(settings.CFManualDomains) > 0:
			poolNote = fmt.Sprintf("manual_domains=%d", len(settings.CFManualDomains))
		case len(settings.CFCachedUpstream) > 0:
			poolNote = fmt.Sprintf("cached_upstream=%d builtin_fallback", len(settings.CFCachedUpstream))
		}
		logInfo.Printf("  CF proxy:     enabled %s legacy_mode=%s", poolNote, mode)
	} else {
		logInfo.Println("  CF proxy:     disabled")
	}
}
