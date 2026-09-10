package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"tg-ws-proxy/mtproxyfrontend"
)

const (
	mtProtoWorkerBackend = "cf_worker_ws"
	mtProtoRouteWSReady  = "MTPROTO_ROUTE_WS_READY"
)

type mtProtoFrameSocket interface {
	Send([]byte) error
	SendBatch([][]byte) error
	Recv() ([]byte, error)
	Close()
}

type mtProtoWorkerDial func(domain, path, logPrefix string) (mtProtoFrameSocket, error)

type mtProtoRouteConnector struct {
	directWS mtproxyfrontend.OutboundConnector
	worker   mtproxyfrontend.OutboundConnector
	cfProxy  mtproxyfrontend.OutboundConnector
	tcp      mtproxyfrontend.OutboundConnector
}

func newMtProtoRouteConnector() *mtProtoRouteConnector {
	return &mtProtoRouteConnector{
		directWS: newMtProtoDirectWSConnector(),
		worker:   newMtProtoWorkerConnector(),
		cfProxy:  newMtProtoCFProxyConnector(),
		tcp:      newMtProtoDirectConnector(),
	}
}

func (c *mtProtoRouteConnector) Capability() mtproxyfrontend.OutboundCapability {
	routes := mtProtoRoutesForCapability(getRuntimeSettings())
	if len(routes) == 0 {
		return mtproxyfrontend.OutboundCapability{
			Status:          mtProtoRouteUnavailable,
			SelectedBackend: "",
		}
	}
	return mtproxyfrontend.OutboundCapability{
		Status:          mtProtoRouteChainReady,
		SelectedBackend: mtProtoBackendForRoute(routes[0]),
	}
}

func (c *mtProtoRouteConnector) Connect(
	ctx context.Context,
	request mtproxyfrontend.OutboundRequest,
) (net.Conn, mtproxyfrontend.OutboundResult) {
	routes := mtProtoRoutesForRequest(getRuntimeSettings(), request)
	if len(routes) == 0 {
		return nil, mtproxyfrontend.OutboundResult{
			SelectedBackend: "",
			Reason:          "no_mtproto_route_available",
			Err:             fmt.Errorf("no MTProto route available"),
		}
	}

	selectedBackend := mtProtoBackendForRoute(routes[0])
	var lastResult mtproxyfrontend.OutboundResult
	var lastErr error
	attempts := 0

	for _, route := range routes {
		connector := c.connectorForRoute(route)
		if connector == nil {
			lastErr = fmt.Errorf("MTProto route %s is unsupported", route)
			lastResult = mtproxyfrontend.OutboundResult{
				SelectedBackend: selectedBackend,
				Reason:          "mtproto_route_unsupported",
				Err:             lastErr,
			}
			continue
		}

		if attempts > 0 && logInfo != nil {
			logInfo.Printf("MTProto fallback activated selected_backend=%s next_backend=%s previous_reason=%s",
				selectedBackend, mtProtoBackendForRoute(route), mtProtoStatusField(lastResult.Reason))
		}
		attempts++

		conn, result := connector.Connect(ctx, request)
		result.SelectedBackend = selectedBackend
		result.FallbackUsed = attempts > 1
		if result.ActualBackend == "" && conn != nil && result.Err == nil {
			result.ActualBackend = mtProtoBackendForRoute(route)
		}
		if result.Reason == "" && result.Err == nil {
			result.Reason = "connected"
		}
		if conn != nil && result.Err == nil {
			return conn, result
		}

		lastResult = result
		lastErr = result.Err
		if lastErr == nil {
			lastErr = fmt.Errorf("MTProto route %s failed: %s", route, result.Reason)
		}
		if logInfo != nil {
			logInfo.Printf("MTProto route candidate failed route=%s selected_backend=%s reason=%s error=%v",
				route, selectedBackend, mtProtoStatusField(lastResult.Reason), lastErr)
		}
	}

	if lastResult.Reason == "" {
		lastResult.Reason = "all_mtproto_routes_failed"
	}
	lastResult.SelectedBackend = selectedBackend
	lastResult.ActualBackend = ""
	lastResult.FallbackUsed = attempts > 1
	if lastErr == nil {
		lastErr = fmt.Errorf("all MTProto routes failed")
	}
	lastResult.Err = lastErr
	return nil, lastResult
}

func (c *mtProtoRouteConnector) connectorForRoute(route routeKind) mtproxyfrontend.OutboundConnector {
	switch route {
	case routeDirectWS:
		return c.directWS
	case routeCFWorkerWS:
		return c.worker
	case routeCFProxyWS:
		return c.cfProxy
	case routeTCPFallback:
		return c.tcp
	default:
		return nil
	}
}

func mtProtoRoutesForCapability(settings runtimeSettings) []routeKind {
	return routesForMode(settings.Mode, settings, false)
}

func mtProtoRoutesForRequest(settings runtimeSettings, request mtproxyfrontend.OutboundRequest) []routeKind {
	if request.IsTestDC {
		return mtProtoTestDCRoutes(settings)
	}
	if settings.Mode == modeAuto {
		return adaptiveRoutesForMode(settings.Mode, settings, false, request.DCID, request.IsMedia)
	}
	return routesForMode(settings.Mode, settings, false)
}

func mtProtoTestDCRoutes(settings runtimeSettings) []routeKind {
	routes := []routeKind{routeDirectWS, routeTCPFallback}
	if !settings.PolicyPresent {
		return routes
	}
	filtered := make([]routeKind, 0, len(routes))
	for _, route := range routes {
		switch route {
		case routeDirectWS:
			if settings.AllowDirect {
				filtered = append(filtered, route)
			}
		case routeTCPFallback:
			if settings.AllowTCP && settings.AllowFallback {
				filtered = append(filtered, route)
			}
		}
	}
	return filtered
}

func mtProtoBackendForRoute(route routeKind) string {
	switch route {
	case routeDirectWS:
		return mtProtoDirectWSBackend
	case routeCFWorkerWS:
		return mtProtoWorkerBackend
	case routeCFProxyWS:
		return mtProtoCFProxyBackend
	case routeTCPFallback:
		return mtProtoDirectBackend
	default:
		return string(route)
	}
}

func mtProtoStatusField(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "none"
	}
	return strings.NewReplacer(";", ",", "\n", " ", "\r", " ").Replace(value)
}

type mtProtoWorkerConnector struct {
	dial mtProtoWorkerDial
}

func newMtProtoWorkerConnector() *mtProtoWorkerConnector {
	return &mtProtoWorkerConnector{
		dial: func(domain, path, logPrefix string) (mtProtoFrameSocket, error) {
			return dialWorkerCandidate(domain, path, logPrefix)
		},
	}
}

func (c *mtProtoWorkerConnector) Capability() mtproxyfrontend.OutboundCapability {
	return mtproxyfrontend.OutboundCapability{
		Status:          mtProtoRouteWSReady,
		SelectedBackend: mtProtoWorkerBackend,
	}
}

func (c *mtProtoWorkerConnector) Connect(
	_ context.Context,
	request mtproxyfrontend.OutboundRequest,
) (net.Conn, mtproxyfrontend.OutboundResult) {
	result := mtproxyfrontend.OutboundResult{
		SelectedBackend: mtProtoWorkerBackend,
		FallbackUsed:    false,
	}

	if request.IsTestDC {
		result.Reason = "test_dc_worker_unsupported"
		result.Err = fmt.Errorf("Worker route is not used for Telegram test DC")
		return nil, result
	}

	settings := getRuntimeSettings()
	candidates := settings.Worker.Failover.effectiveCandidates(settings.Worker.Domain)
	if len(candidates) == 0 {
		result.Reason = "worker_not_configured"
		result.Err = fmt.Errorf("MTProto Worker backend is not configured")
		return nil, result
	}

	destination := buildMtProtoWorkerDestinationPlan(
		candidates[0].Domain,
		request.DCID,
		request.IsMedia,
		settings,
	)
	target := strings.TrimSpace(destination.WorkerDst)
	effectiveDC := destination.EffectiveDC
	effectiveMedia := destination.EffectiveIsMedia
	if !destination.OK || target == "" || effectiveDC <= 0 {
		result.Reason = "dc_target_unavailable"
		result.Err = fmt.Errorf(
			"no Worker destination for dc %d: %s",
			request.DCID,
			mtProtoStatusField(destination.FailReason),
		)
		return nil, result
	}

	workerRelayInit := request.RelayInit
	// A Flowseal media fix changes only the physical Worker destination.
	// Rewriting relay_init from -DC2 to -DC4 makes Telegram treat the stream
	// as a different logical DC and causes the media session to close.
	if !destination.FlowsealMediaFixApplied &&
		(effectiveDC != request.DCID || effectiveMedia != request.IsMedia) {
		signedDC := effectiveDC
		if effectiveMedia {
			signedDC = -effectiveDC
		}
		workerRelayInit = patchInitDC(request.RelayInit, signedDC)
		patchedDC, patchedMedia, ok := dcFromInit(workerRelayInit)
		if !ok || patchedDC != effectiveDC || patchedMedia != effectiveMedia {
			result.Reason = "worker_relay_init_destination_mismatch"
			result.Err = fmt.Errorf(
				"Worker relay init destination mismatch: dc=%d media=%t",
				effectiveDC,
				effectiveMedia,
			)
			return nil, result
		}
	}

	maxAttempts := settings.Worker.Failover.maxAttemptsFor(len(candidates))
	if maxAttempts <= 0 {
		result.Reason = "worker_not_configured"
		result.Err = fmt.Errorf("MTProto Worker backend has no enabled candidates")
		return nil, result
	}

	// The frontend and Worker connector independently derive the same opaque
	// identifier from relay_init, so every lifecycle log can be correlated
	// without exposing relay key material or relying on log ordering.
	sessionID := mtproxyfrontend.SessionIDForRequest(request)
	var lastErr error
	var lastReason string
	for i := 0; i < maxAttempts; i++ {
		candidate := candidates[i]
		path := buildWorkerWSPath(effectiveDC, target, effectiveMedia, sessionID)
		prefix := fmt.Sprintf(
			"[MTProto] session_id=%s DC%d%s cfworker",
			sessionID,
			request.DCID,
			mediaTag(request.IsMedia),
		)
		if logInfo != nil {
			logInfo.Printf(
				"MTProto route truth frontend=MTProto session_id=%s signed_dc=%d selected_backend=%s actual_backend=none fallback_used=false reason=connecting dc=%d media=%t effective_dc=%d effective_media=%t transport=%s worker_host=%s worker_dst=%s destination_mode=%s effective_destination_mode=%s attempt=%d",
				sessionID,
				request.SignedDC,
				mtProtoWorkerBackend,
				request.DCID,
				request.IsMedia,
				effectiveDC,
				effectiveMedia,
				request.Transport,
				candidate.Domain,
				target,
				destination.ConfiguredDestinationMode,
				destination.EffectiveDestinationMode,
				i+1,
			)
		}

		poolKey := WorkerPoolKey{
			DC:           effectiveDC,
			WorkerDomain: candidate.Domain,
			Dst:          target,
			Media:        effectiveMedia,
		}
		var ws mtProtoFrameSocket
		var err error
		if pooled := workerPool.GetForSession(poolKey); pooled != nil {
			ws = pooled
			if logInfo != nil {
				logInfo.Printf(
					"MTProto Worker WS preconnect hit session_id=%s signed_dc=%d dc=%d media=%t worker_host=%s worker_dst=%s attempt=%d",
					sessionID,
					request.SignedDC,
					effectiveDC,
					effectiveMedia,
					candidate.Domain,
					target,
					i+1,
				)
			}
		} else {
			preconnectEnabled := workerWsPreconnectActive()
			if logInfo != nil && preconnectEnabled {
				logInfo.Printf(
					"MTProto Worker WS preconnect miss session_id=%s signed_dc=%d dc=%d media=%t worker_host=%s worker_dst=%s attempt=%d",
					sessionID,
					request.SignedDC,
					effectiveDC,
					effectiveMedia,
					candidate.Domain,
					target,
					i+1,
				)
			}
			if logInfo != nil {
				logInfo.Printf(
					"MTProto Worker WS fresh dial session_id=%s signed_dc=%d dc=%d media=%t worker_host=%s worker_dst=%s preconnect_enabled=%t attempt=%d",
					sessionID,
					request.SignedDC,
					effectiveDC,
					effectiveMedia,
					candidate.Domain,
					target,
					preconnectEnabled,
					i+1,
				)
			}
			ws, err = c.dial(candidate.Domain, path, prefix)
		}
		if err != nil {
			lastErr = err
			lastReason = classifyWorkerConnectFailure(err)
			continue
		}

		stream, err := mtProtoWorkerWebSocketConn(ws, workerRelayInit, candidate.Domain)
		if err != nil {
			ws.Close()
			lastErr = err
			lastReason = "relay_init_write_failed"
			if strings.Contains(err.Error(), "packet splitter") {
				lastReason = "packet_splitter_failed"
			}
			continue
		}
		if workerStream, ok := stream.(*mtProtoWebSocketStream); ok {
			workerStream.sessionID = sessionID
			workerStream.workerDst = target
		}
		stream = wrapMtProtoWorkerPayloadTrace(stream, request, sessionID, target)

		result.ActualBackend = mtProtoWorkerBackend
		result.Reason = "connected"
		return stream, result
	}

	if lastReason == "" {
		lastReason = "worker_ws_connect_failed"
	}
	result.Reason = lastReason
	if lastErr == nil {
		lastErr = fmt.Errorf("all MTProto Worker candidates failed")
	}
	result.Err = fmt.Errorf("connect Worker: %w", lastErr)
	return nil, result
}

func mtProtoWorkerConfigured() bool {
	settings := getRuntimeSettings()
	return len(settings.Worker.Failover.effectiveCandidates(settings.Worker.Domain)) > 0
}

type mtProtoWebSocketStream struct {
	socket    mtProtoFrameSocket
	splitter  *MsgSplitter
	local     net.Addr
	remote    net.Addr
	sessionID string
	workerDst string
	readMu    sync.Mutex
	readBuf   []byte
	closeMu   sync.Mutex
	closed    bool
}

func (s *mtProtoWebSocketStream) Read(dst []byte) (int, error) {
	s.readMu.Lock()
	defer s.readMu.Unlock()
	for len(s.readBuf) == 0 {
		frame, err := s.socket.Recv()
		if err != nil {
			return 0, err
		}
		if len(frame) > 0 {
			s.readBuf = frame
		}
	}
	n := copy(dst, s.readBuf)
	s.readBuf = s.readBuf[n:]
	return n, nil
}

func (s *mtProtoWebSocketStream) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// A nil splitter is intentional for the Worker route. Flowseal forwards
	// each transformed TCP read directly as one WebSocket message instead of
	// buffering until an MTProto packet boundary is reconstructed.
	if s.splitter == nil {
		if err := s.socket.Send(data); err != nil {
			return 0, err
		}
		return len(data), nil
	}

	parts := s.splitter.Split(data)
	if len(parts) == 0 {
		return len(data), nil
	}
	if err := s.socket.SendBatch(parts); err != nil {
		return 0, err
	}
	return len(data), nil
}

func (s *mtProtoWebSocketStream) Close() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.splitter != nil {
		if tail := s.splitter.Flush(); len(tail) > 0 {
			_ = s.socket.SendBatch(tail)
		}
	}
	s.socket.Close()
	return nil
}

func (s *mtProtoWebSocketStream) LocalAddr() net.Addr {
	return s.local
}

func (s *mtProtoWebSocketStream) RemoteAddr() net.Addr {
	return s.remote
}

func (s *mtProtoWebSocketStream) SetDeadline(time.Time) error {
	return nil
}

func (s *mtProtoWebSocketStream) SetReadDeadline(time.Time) error {
	return nil
}

func (s *mtProtoWebSocketStream) SetWriteDeadline(time.Time) error {
	return nil
}

func (s *mtProtoWebSocketStream) RouteDiagnostics() mtproxyfrontend.RouteDiagnostics {
	return mtproxyfrontend.RouteDiagnostics{
		SessionID: strings.TrimSpace(s.sessionID),
		WorkerDst: strings.TrimSpace(s.workerDst),
	}
}

type mtProtoNetAddr string

func (a mtProtoNetAddr) Network() string {
	return "websocket"
}

func (a mtProtoNetAddr) String() string {
	return strings.TrimSpace(string(a))
}

var (
	_ net.Conn                                 = (*mtProtoWebSocketStream)(nil)
	_ io.Closer                                = (*mtProtoWebSocketStream)(nil)
	_ mtproxyfrontend.RouteDiagnosticsProvider = (*mtProtoWebSocketStream)(nil)
)
