package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"tg-ws-proxy/mtproxyfrontend"
	"tg-ws-proxy/tgwsroute"
)

const (
	mtProtoDirectWSBackend  = "direct_ws"
	mtProtoCFProxyBackend   = "cf_proxy_ws"
	mtProtoRouteChainReady  = "MTPROTO_ROUTE_CHAIN_READY"
	mtProtoRouteUnavailable = "MTPROTO_ROUTE_UNAVAILABLE"
)

type mtProtoRouteDial func(domain, path, logPrefix string) (mtProtoFrameSocket, error)
type mtProtoDirectWSDial func(targetIP, domain, path string, timeout float64) (mtProtoFrameSocket, error)

type mtProtoDirectWSConnector struct {
	resolveTarget mtProtoTargetResolver
	dial          mtProtoDirectWSDial
}

func newMtProtoDirectWSConnector() *mtProtoDirectWSConnector {
	return &mtProtoDirectWSConnector{
		resolveTarget: mtProtoTargetForDC,
		dial:          defaultMtProtoDirectWSDial,
	}
}

func (c *mtProtoDirectWSConnector) Capability() mtproxyfrontend.OutboundCapability {
	return mtproxyfrontend.OutboundCapability{
		Status:          mtProtoRouteChainReady,
		SelectedBackend: mtProtoDirectWSBackend,
	}
}

func (c *mtProtoDirectWSConnector) Connect(
	_ context.Context,
	request mtproxyfrontend.OutboundRequest,
) (net.Conn, mtproxyfrontend.OutboundResult) {
	result := mtproxyfrontend.OutboundResult{
		SelectedBackend: mtProtoDirectWSBackend,
	}

	var target string
	var ok bool
	if request.IsTestDC {
		target, _, ok = mtProtoTestTargetForDC(request.DCID)
	} else {
		target, _, ok = c.resolveTarget(request.DCID)
	}
	if !ok || target == "" {
		result.Reason = "direct_ws_target_unavailable"
		result.Err = fmt.Errorf("no direct WebSocket target for dc %d", request.DCID)
		return nil, result
	}

	isMedia := request.IsMedia
	domains := wsDomains(request.DCID, &isMedia)
	if len(domains) == 0 {
		result.Reason = "direct_ws_domain_unavailable"
		result.Err = fmt.Errorf("no direct WebSocket domain for dc %d", request.DCID)
		return nil, result
	}

	now := monoNow()
	if directIPCoolingDown(target, now) && directIPCooldownFallbackAvailable(getRuntimeSettings()) {
		result.Reason = "direct_ip_cooldown"
		result.Err = fmt.Errorf("direct WebSocket target %s is on IP cooldown", target)
		return nil, result
	}

	path := "/apiws"
	if request.IsTestDC {
		path = "/apiws_test"
	}

	var lastErr error
	timedOut := false
	for _, domain := range domains {
		prefix := fmt.Sprintf("[MTProto] DC%d%s direct_ws", request.DCID, mediaTag(request.IsMedia))
		if logInfo != nil {
			logInfo.Printf(
				"MTProto route truth frontend=MTProto selected_backend=%s actual_backend=none fallback_used=false reason=connecting dc=%d media=%t transport=%s target=%s domain=%s",
				mtProtoDirectWSBackend,
				request.DCID,
				request.IsMedia,
				request.Transport,
				target,
				domain,
			)
		}

		ws, err := c.dial(target, domain, path, 10)
		if err != nil {
			lastErr = err
			if directWSErrorTimedOut(err) {
				timedOut = true
			}
			var wsErr *WsHandshakeError
			if errors.As(err, &wsErr) && wsErr.IsRedirect() {
				continue
			}
			logDomainConnectFailure(prefix, domain, target, err)
			continue
		}

		stream, err := mtProtoWebSocketConn(ws, request.RelayInit, domain)
		if err != nil {
			ws.Close()
			lastErr = err
			continue
		}
		stats.connectionsWs.Add(1)
		markDirectIPSuccess(target)
		result.ActualBackend = mtProtoDirectWSBackend
		result.Reason = "connected"
		return stream, result
	}

	result.Reason = "direct_ws_connect_failed"
	if timedOut {
		markDirectIPTimeout(target, now)
		result.Reason = "direct_ws_ip_timeout"
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("direct WebSocket failed for dc %d", request.DCID)
	}
	result.Err = lastErr
	return nil, result
}

func defaultMtProtoDirectWSDial(targetIP, domain, path string, timeout float64) (mtProtoFrameSocket, error) {
	ws, err := wsConnect(targetIP, domain, path, timeout)
	if err == nil {
		return ws, nil
	}
	if !shouldTryDomainDial(err) {
		return nil, err
	}

	resolvedWS, _, _, fallbackErr := wsConnectViaResolvedDomain(domain, path, timeout)
	if fallbackErr == nil {
		return resolvedWS, nil
	}
	return nil, fallbackErr
}

type mtProtoCFProxyConnector struct {
	dial mtProtoRouteDial
}

func newMtProtoCFProxyConnector() *mtProtoCFProxyConnector {
	return &mtProtoCFProxyConnector{
		dial: defaultMtProtoCFProxyDial,
	}
}

func (c *mtProtoCFProxyConnector) Capability() mtproxyfrontend.OutboundCapability {
	return mtproxyfrontend.OutboundCapability{
		Status:          mtProtoRouteChainReady,
		SelectedBackend: mtProtoCFProxyBackend,
	}
}

func (c *mtProtoCFProxyConnector) Connect(
	_ context.Context,
	request mtproxyfrontend.OutboundRequest,
) (net.Conn, mtproxyfrontend.OutboundResult) {
	result := mtproxyfrontend.OutboundResult{
		SelectedBackend: mtProtoCFProxyBackend,
	}

	if request.IsTestDC {
		result.Reason = "test_dc_cfproxy_unsupported"
		result.Err = fmt.Errorf("CF proxy route is not used for Telegram test DC")
		return nil, result
	}

	settings := getRuntimeSettings()
	if settings.PolicyPresent && !settings.AllowCFProxy {
		result.Reason = "disabled_by_policy"
		result.Err = fmt.Errorf("CF proxy route disabled by policy")
		return nil, result
	}
	if !settings.CF.Enabled {
		result.Reason = "cfproxy_disabled"
		result.Err = fmt.Errorf("CF proxy route disabled")
		return nil, result
	}

	wsDC := effectiveWSHostDC(request.DCID)
	selection := cfPool.SelectionForDC(wsDC)
	if len(selection.Candidates) == 0 {
		stats.cfPoolMisses.Add(1)
		result.Reason = "cfproxy_unavailable"
		result.Err = fmt.Errorf("CF proxy pool has no available domains")
		return nil, result
	}
	stats.cfPoolHits.Add(1)

	var lastErr error
	var lastReason string
	skipCachedUpstream := false
	for _, candidate := range selection.Candidates {
		if skipCachedUpstream && candidate.Source == tgwsroute.CFDomainSourceCachedUpstream {
			if logDebug != nil {
				logDebug.Printf("MTProto CF cached upstream skipped domain=%s reason=previous_dns_failure", candidate.Domain)
			}
			continue
		}
		baseDomain := candidate.Domain
		host := cfProxyHost(wsDC, baseDomain)
		prefix := fmt.Sprintf("[MTProto] DC%d%s cfproxy", request.DCID, mediaTag(request.IsMedia))
		if logInfo != nil {
			logInfo.Printf(
				"MTProto route truth frontend=MTProto selected_backend=%s actual_backend=none fallback_used=false reason=connecting dc=%d media=%t transport=%s host=%s pool_domain=%s source=%s",
				mtProtoCFProxyBackend,
				request.DCID,
				request.IsMedia,
				request.Transport,
				host,
				baseDomain,
				candidate.Source,
			)
		}

		ws, err := c.dial(host, "/apiws", prefix)
		if err != nil {
			lastErr = err
			kind := classifyCFFailure(err)
			lastReason = string(kind)
			health := cfPool.MarkFailure(baseDomain, kind, 0)
			if logInfo != nil {
				logInfo.Printf("MTProto CF domain cooldown domain=%s reason=%s until=%s",
					baseDomain, kind, formatCooldownUntil(health.CooldownUntil))
			}
			if candidate.Source == tgwsroute.CFDomainSourceCachedUpstream && tgwsroute.IsCFDNSFailure(kind) {
				skipCachedUpstream = true
				if logInfo != nil {
					logInfo.Printf("MTProto CF cached upstream fast-skip enabled after dns failure domain=%s", baseDomain)
				}
			}
			continue
		}

		stream, err := mtProtoWebSocketConn(ws, request.RelayInit, host)
		if err != nil {
			ws.Close()
			lastErr = err
			lastReason = "packet_splitter_failed"
			cfPool.MarkFailure(baseDomain, tgwsroute.CFFailureWebSocket, 0)
			continue
		}

		cfPool.MarkSuccess(wsDC, baseDomain, 0)
		stats.connectionsCfProxy.Add(1)
		result.ActualBackend = mtProtoCFProxyBackend
		result.Reason = "connected"
		return stream, result
	}

	stats.cfPoolRefillErrors.Add(1)
	if lastReason == "" {
		lastReason = "cfproxy_all_domains_failed"
	}
	result.Reason = lastReason
	if lastErr == nil {
		lastErr = fmt.Errorf("CF proxy domains failed")
	}
	result.Err = lastErr
	return nil, result
}

func defaultMtProtoCFProxyDial(domain, path, logPrefix string) (mtProtoFrameSocket, error) {
	ws, err := wsConnect(domain, domain, path, 10)
	if err == nil {
		return ws, nil
	}
	logDomainConnectFailure(logPrefix, domain, domain, err)

	resolved, resolveErr := resolvePreferredIPs(domain, 10)
	if resolveErr != nil {
		return nil, err
	}

	lastErr := err
	for _, ip := range resolved.Preferred() {
		ws, lastErr = wsConnect(ip, domain, path, 10)
		if lastErr == nil {
			return ws, nil
		}
		logDomainConnectFailure(logPrefix, domain, ip, lastErr)
		var wsErr *WsHandshakeError
		if errors.As(lastErr, &wsErr) && wsErr.IsRedirect() {
			break
		}
	}
	return nil, lastErr
}

func mtProtoWebSocketConn(ws mtProtoFrameSocket, relayInit []byte, remote string) (net.Conn, error) {
	return mtProtoWebSocketConnWithFraming(ws, relayInit, remote, true)
}

// mtProtoWorkerWebSocketConn mirrors Flowseal's cf_worker fallback semantics:
// relay_init is sent first, then each transformed TCP chunk is forwarded as one
// WebSocket message without MTProto packet-aware splitting.
func mtProtoWorkerWebSocketConn(ws mtProtoFrameSocket, relayInit []byte, remote string) (net.Conn, error) {
	return mtProtoWebSocketConnWithFraming(ws, relayInit, remote, false)
}

func mtProtoWebSocketConnWithFraming(
	ws mtProtoFrameSocket,
	relayInit []byte,
	remote string,
	splitOutboundPackets bool,
) (net.Conn, error) {
	ws = wrapMtProtoFrameSocket(ws)
	if err := ws.Send(relayInit); err != nil {
		return nil, fmt.Errorf("write relay init to WebSocket %s: %w", remote, err)
	}

	var splitter *MsgSplitter
	if splitOutboundPackets {
		var err error
		splitter, err = newMsgSplitter(relayInit)
		if err != nil {
			return nil, fmt.Errorf("create packet splitter for WebSocket %s: %w", remote, err)
		}
	}

	return &mtProtoWebSocketStream{
		socket:   ws,
		splitter: splitter,
		local:    mtProtoNetAddr("mtproto-local"),
		remote:   mtProtoNetAddr(strings.TrimSpace(remote)),
	}, nil
}

var _ mtproxyfrontend.OutboundConnector = (*mtProtoDirectWSConnector)(nil)
var _ mtproxyfrontend.OutboundConnector = (*mtProtoCFProxyConnector)(nil)
