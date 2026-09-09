package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"tg-ws-proxy/mtproxyfrontend"
	"tg-ws-proxy/tgwsroute"
)

func TestMtProtoWorkerConnectorUsesConfiguredWorkerWithoutFallback(t *testing.T) {
	withRuntimeSettings(t, func(settings runtimeSettings) runtimeSettings {
		settings.Worker.Enabled = true
		settings.Worker.Domain = "example.workers.dev"
		settings.Worker.Failover = workerFailoverSettings{}
		settings.Worker.DestinationMode = tgwsroute.WorkerDestinationPreserveOriginalDst
		return settings
	})

	socket := &fakeMtProtoFrameSocket{}
	var dialDomain string
	var dialPath string
	connector := &mtProtoWorkerConnector{
		dial: func(domain, path, _ string) (mtProtoFrameSocket, error) {
			dialDomain = domain
			dialPath = path
			return socket, nil
		},
	}
	relayInit := buildTestInitWithSignedDC(t, 2)
	conn, result := connector.Connect(context.Background(), mtproxyfrontend.OutboundRequest{
		DCID:      2,
		Transport: mtproxyfrontend.TransportAbridged,
		RelayInit: relayInit,
	})
	if result.Err != nil {
		t.Fatalf("connect: %v", result.Err)
	}
	defer conn.Close()

	if dialDomain != "example.workers.dev" {
		t.Fatalf("domain=%s", dialDomain)
	}
	if !containsAll(dialPath, "/apiws?", "dc=2", "dst=149.154.167.51", "media=0", "sid=") {
		t.Fatalf("path=%s", dialPath)
	}
	if result.SelectedBackend != mtProtoWorkerBackend ||
		result.ActualBackend != mtProtoWorkerBackend ||
		result.FallbackUsed ||
		result.Reason != "connected" {
		t.Fatalf("route truth=%+v", result)
	}
	if len(socket.sent) != 1 || !bytes.Equal(socket.sent[0], relayInit) {
		t.Fatal("relay init was not sent to Worker")
	}
}

func TestMtProtoWorkerConnectorUsesFlowsealDCMapDestination(t *testing.T) {
	withMtProtoWorkerDCMap(t, map[int]string{2: "149.154.167.220"})
	withRuntimeSettings(t, func(settings runtimeSettings) runtimeSettings {
		settings.Worker.Enabled = true
		settings.Worker.Domain = "example.workers.dev"
		settings.Worker.Failover = workerFailoverSettings{}
		settings.Worker.DestinationMode = tgwsroute.WorkerDestinationFlowsealDCMap
		return settings
	})

	socket := &fakeMtProtoFrameSocket{}
	var dialPath string
	connector := &mtProtoWorkerConnector{
		dial: func(_, path, _ string) (mtProtoFrameSocket, error) {
			dialPath = path
			return socket, nil
		},
	}
	relayInit := buildTestInitWithSignedDC(t, 2)
	conn, result := connector.Connect(context.Background(), mtproxyfrontend.OutboundRequest{
		DCID:      2,
		Transport: mtproxyfrontend.TransportAbridged,
		RelayInit: relayInit,
	})
	if result.Err != nil {
		t.Fatalf("connect: %v", result.Err)
	}
	defer conn.Close()

	if !containsAll(dialPath, "/apiws?", "dc=2", "dst=149.154.167.220", "media=0", "sid=") {
		t.Fatalf("path=%s", dialPath)
	}
	if len(socket.sent) != 1 || !bytes.Equal(socket.sent[0], relayInit) {
		t.Fatal("FLOWSEAL_DC_MAP must keep relay init metadata when effective dc/media are unchanged")
	}
}

func TestMtProtoWorkerConnectorAlignsExperimentalMediaDestinationAndRelayInit(t *testing.T) {
	withRuntimeSettings(t, func(settings runtimeSettings) runtimeSettings {
		settings.Worker.Enabled = true
		settings.Worker.Domain = "example.workers.dev"
		settings.Worker.Failover = workerFailoverSettings{}
		settings.Worker.DestinationMode = tgwsroute.WorkerDestinationExperimentalForceMediaDC4
		settings.Worker.MediaFix = flowsealMediaFixConfig{
			Enabled: true,
			DC:      4,
			IP:      "149.154.167.220",
		}
		return settings
	})

	socket := &fakeMtProtoFrameSocket{}
	var dialPath string
	connector := &mtProtoWorkerConnector{
		dial: func(_, path, _ string) (mtProtoFrameSocket, error) {
			dialPath = path
			return socket, nil
		},
	}
	relayInit := buildTestInitWithSignedDC(t, -2)
	conn, result := connector.Connect(context.Background(), mtproxyfrontend.OutboundRequest{
		DCID:      2,
		IsMedia:   true,
		Transport: mtproxyfrontend.TransportAbridged,
		RelayInit: relayInit,
	})
	if result.Err != nil {
		t.Fatalf("connect: %v", result.Err)
	}
	defer conn.Close()

	if !containsAll(dialPath, "/apiws?", "dc=4", "dst=149.154.167.220", "media=1", "sid=") {
		t.Fatalf("path=%s", dialPath)
	}
	if len(socket.sent) != 1 {
		t.Fatalf("sent frames=%d", len(socket.sent))
	}
	if bytes.Equal(socket.sent[0], relayInit) {
		t.Fatal("expected relay init route metadata to be patched for effective DC4 media destination")
	}
	dc, isMedia, ok := dcFromInit(socket.sent[0])
	if !ok || dc != 4 || !isMedia {
		t.Fatalf("relay init dc=%d media=%t ok=%t", dc, isMedia, ok)
	}
}

func TestMtProtoWorkerConnectorUsesPreconnectedWorkerWhenMtProtoEnabled(t *testing.T) {
	withPoolSize(t, 1)
	withRuntimeSettings(t, func(settings runtimeSettings) runtimeSettings {
		settings.Worker.Enabled = true
		settings.Worker.Domain = "example.workers.dev"
		settings.MtProtoWorkerPreconnect = true
		settings.Worker.Failover = workerFailoverSettings{}
		return settings
	})
	previousPool := workerPool
	pool := newWorkerWsPool(&fakeWorkerDialer{})
	workerPool = pool
	t.Cleanup(func() {
		workerPool.CloseAll()
		workerPool = previousPool
	})
	key := WorkerPoolKey{
		DC:           2,
		WorkerDomain: "example.workers.dev",
		Dst:          "149.154.167.51",
		Media:        false,
	}
	pool.idle[key] = []poolEntry{{ws: newFakeWebSocket(), created: pool.now()}}

	dialed := false
	connector := &mtProtoWorkerConnector{
		dial: func(domain, path, logPrefix string) (mtProtoFrameSocket, error) {
			dialed = true
			return nil, errors.New("dial should not be used")
		},
	}

	conn, result := connector.Connect(context.Background(), mtproxyfrontend.OutboundRequest{
		DCID:      2,
		Transport: mtproxyfrontend.TransportAbridged,
		RelayInit: buildTestInitWithSignedDC(t, 2),
	})
	if result.Err != nil {
		t.Fatalf("connect: %v", result.Err)
	}
	defer conn.Close()

	if dialed {
		t.Fatal("expected MTProto Worker connector to use preconnected websocket")
	}
	if stats.workerWsPreconnectHits.Load() == 0 {
		t.Fatal("expected worker ws preconnect hit")
	}
}

func TestMtProtoWorkerConnectorTriesNextFailoverCandidate(t *testing.T) {
	withRuntimeSettings(t, func(settings runtimeSettings) runtimeSettings {
		settings.Worker.Enabled = true
		settings.Worker.Domain = "first.workers.dev"
		settings.Worker.Failover = workerFailoverSettings{
			Enabled:     true,
			MaxAttempts: 2,
			Candidates: []workerFailoverCandidate{
				{ID: "first", Domain: "first.workers.dev"},
				{ID: "second", Domain: "second.workers.dev"},
			},
		}
		return settings
	})

	socket := &fakeMtProtoFrameSocket{}
	var attempts []string
	connector := &mtProtoWorkerConnector{
		dial: func(domain, _ string, _ string) (mtProtoFrameSocket, error) {
			attempts = append(attempts, domain)
			if domain == "first.workers.dev" {
				return nil, errors.New("first worker down")
			}
			return socket, nil
		},
	}

	relayInit := buildTestInitWithSignedDC(t, 2)
	conn, result := connector.Connect(context.Background(), mtproxyfrontend.OutboundRequest{
		DCID:      2,
		Transport: mtproxyfrontend.TransportAbridged,
		RelayInit: relayInit,
	})
	if result.Err != nil {
		t.Fatalf("connect: %v", result.Err)
	}
	defer conn.Close()

	if len(attempts) != 2 || attempts[0] != "first.workers.dev" || attempts[1] != "second.workers.dev" {
		t.Fatalf("attempts=%v", attempts)
	}
	if result.ActualBackend != mtProtoWorkerBackend || result.Reason != "connected" {
		t.Fatalf("route truth=%+v", result)
	}
	if len(socket.sent) != 1 || !bytes.Equal(socket.sent[0], relayInit) {
		t.Fatal("relay init was not sent to successful Worker")
	}
}

func TestMtProtoRouteConnectorFallsBackFromWorkerToCFProxy(t *testing.T) {
	withRuntimeSettings(t, func(settings runtimeSettings) runtimeSettings {
		settings.Mode = modeWorkerFirst
		settings.Worker.Enabled = true
		settings.Worker.Domain = "worker.example"
		settings.CF.Enabled = true
		settings.CF.Only = false
		settings.CF.Priority = false
		return settings
	})

	cfConn := nopConn{}
	connector := &mtProtoRouteConnector{
		directWS: &fakeOutboundRouteConnector{backend: mtProtoDirectWSBackend, err: errors.New("direct should not be first")},
		worker:   &fakeOutboundRouteConnector{backend: mtProtoWorkerBackend, err: errors.New("worker down")},
		cfProxy:  &fakeOutboundRouteConnector{backend: mtProtoCFProxyBackend, conn: cfConn},
		tcp:      &fakeOutboundRouteConnector{backend: mtProtoDirectBackend, conn: nopConn{}},
	}

	conn, result := connector.Connect(context.Background(), mtproxyfrontend.OutboundRequest{
		DCID:      2,
		Transport: mtproxyfrontend.TransportAbridged,
		RelayInit: buildTestInitWithSignedDC(t, 2),
	})
	if result.Err != nil {
		t.Fatalf("connect: %v", result.Err)
	}
	if conn != cfConn {
		t.Fatal("expected CF proxy connection")
	}
	if result.SelectedBackend != mtProtoWorkerBackend ||
		result.ActualBackend != mtProtoCFProxyBackend ||
		!result.FallbackUsed ||
		result.Reason != "connected" {
		t.Fatalf("route truth=%+v", result)
	}
	if connector.directWS.(*fakeOutboundRouteConnector).calls != 0 {
		t.Fatal("worker_first should try Worker and CF before direct_ws")
	}
	if connector.tcp.(*fakeOutboundRouteConnector).calls != 0 {
		t.Fatal("tcp fallback should not be used after CF success")
	}
}

func TestMtProtoRoutesForRequestUsesDirectAndTCPForTestDC(t *testing.T) {
	routes := mtProtoRoutesForRequest(runtimeSettings{
		Mode: modeWorkerFirst,
		CF:   cfProxyConfig{Enabled: true},
		Worker: workerConfig{
			Enabled: true,
			Domain:  "worker.example",
		},
	}, mtproxyfrontend.OutboundRequest{
		DCID:     2,
		IsTestDC: true,
	})

	if len(routes) != 2 || routes[0] != routeDirectWS || routes[1] != routeTCPFallback {
		t.Fatalf("routes=%v", routes)
	}
}

func TestMtProtoRouteConnectorWorkerOnlyHasNoHiddenTCPFallback(t *testing.T) {
	withRuntimeSettings(t, func(settings runtimeSettings) runtimeSettings {
		settings.Mode = modeWorkerOnly
		settings.Worker.Enabled = true
		settings.Worker.Domain = "worker.example"
		settings.CF.Enabled = true
		return settings
	})

	tcp := &fakeOutboundRouteConnector{backend: mtProtoDirectBackend, conn: nopConn{}}
	connector := &mtProtoRouteConnector{
		directWS: &fakeOutboundRouteConnector{backend: mtProtoDirectWSBackend, conn: nopConn{}},
		worker:   &fakeOutboundRouteConnector{backend: mtProtoWorkerBackend, err: errors.New("worker down")},
		cfProxy:  &fakeOutboundRouteConnector{backend: mtProtoCFProxyBackend, conn: nopConn{}},
		tcp:      tcp,
	}

	conn, result := connector.Connect(context.Background(), mtproxyfrontend.OutboundRequest{
		DCID:      2,
		Transport: mtproxyfrontend.TransportAbridged,
		RelayInit: buildTestInitWithSignedDC(t, 2),
	})
	if conn != nil {
		t.Fatal("unexpected connection")
	}
	if result.Err == nil {
		t.Fatal("expected worker failure")
	}
	if result.SelectedBackend != mtProtoWorkerBackend ||
		result.ActualBackend != "" ||
		result.FallbackUsed {
		t.Fatalf("route truth=%+v", result)
	}
	if tcp.calls != 0 {
		t.Fatal("worker_only must not fall back to TCP")
	}
}

func TestMtProtoWebSocketStreamFramesCompletePackets(t *testing.T) {
	relayInit := buildTestInitWithSignedDC(t, 2)
	splitter, err := newMsgSplitter(relayInit)
	if err != nil {
		t.Fatalf("newMsgSplitter: %v", err)
	}
	socket := &fakeMtProtoFrameSocket{}
	stream := &mtProtoWebSocketStream{
		socket:   socket,
		splitter: splitter,
		local:    mtProtoNetAddr("local"),
		remote:   mtProtoNetAddr("worker"),
	}

	encryptor, err := newAESCTR(relayInit[8:40], relayInit[40:56])
	if err != nil {
		t.Fatalf("newAESCTR: %v", err)
	}
	encryptor.XORKeyStream(make([]byte, 64), make([]byte, 64))
	plainPacket := []byte{1, 1, 2, 3, 4}
	cipherPacket := make([]byte, len(plainPacket))
	encryptor.XORKeyStream(cipherPacket, plainPacket)

	n, err := stream.Write(cipherPacket)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if n != len(cipherPacket) {
		t.Fatalf("n=%d", n)
	}
	if len(socket.batches) != 1 ||
		len(socket.batches[0]) != 1 ||
		!bytes.Equal(socket.batches[0][0], cipherPacket) {
		t.Fatalf("unexpected frames=%x", socket.batches)
	}
}

type fakeMtProtoFrameSocket struct {
	sent    [][]byte
	batches [][][]byte
	recv    [][]byte
	closed  bool
}

func (s *fakeMtProtoFrameSocket) Send(data []byte) error {
	s.sent = append(s.sent, append([]byte(nil), data...))
	return nil
}

func (s *fakeMtProtoFrameSocket) SendBatch(parts [][]byte) error {
	batch := make([][]byte, len(parts))
	for i, part := range parts {
		batch[i] = append([]byte(nil), part...)
	}
	s.batches = append(s.batches, batch)
	return nil
}

func (s *fakeMtProtoFrameSocket) Recv() ([]byte, error) {
	if len(s.recv) == 0 {
		return nil, io.EOF
	}
	next := s.recv[0]
	s.recv = s.recv[1:]
	return append([]byte(nil), next...), nil
}

func (s *fakeMtProtoFrameSocket) Close() {
	s.closed = true
}

var _ mtProtoFrameSocket = (*fakeMtProtoFrameSocket)(nil)
var _ net.Conn = (*mtProtoWebSocketStream)(nil)

type fakeOutboundRouteConnector struct {
	backend string
	conn    net.Conn
	err     error
	calls   int
}

func (c *fakeOutboundRouteConnector) Capability() mtproxyfrontend.OutboundCapability {
	return mtproxyfrontend.OutboundCapability{
		Status:          mtProtoRouteChainReady,
		SelectedBackend: c.backend,
	}
}

func (c *fakeOutboundRouteConnector) Connect(
	_ context.Context,
	_ mtproxyfrontend.OutboundRequest,
) (net.Conn, mtproxyfrontend.OutboundResult) {
	c.calls++
	if c.err != nil {
		return nil, mtproxyfrontend.OutboundResult{
			SelectedBackend: c.backend,
			Reason:          c.backend + "_failed",
			Err:             c.err,
		}
	}
	return c.conn, mtproxyfrontend.OutboundResult{
		SelectedBackend: c.backend,
		ActualBackend:   c.backend,
		Reason:          "connected",
	}
}

func withRuntimeSettings(t *testing.T, mutate func(runtimeSettings) runtimeSettings) {
	t.Helper()
	previous := getRuntimeSettings()
	setRuntimeSettings(mutate(previous))
	t.Cleanup(func() {
		setRuntimeSettings(previous)
	})
}

func withMtProtoWorkerDCMap(t *testing.T, value map[int]string) {
	t.Helper()
	dcOptMu.Lock()
	previous := dcOpt
	dcOpt = value
	dcOptMu.Unlock()
	t.Cleanup(func() {
		dcOptMu.Lock()
		dcOpt = previous
		dcOptMu.Unlock()
	})
}
