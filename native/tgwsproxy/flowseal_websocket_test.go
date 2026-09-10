package main

import (
	"bufio"
	"bytes"
	"context"
	"strings"
	"testing"

	"tg-ws-proxy/mtproxyfrontend"
	"tg-ws-proxy/tgwsroute"
)

func TestFlowsealUpgradeRequestMatchesUpstreamHeaders(t *testing.T) {
	request := buildFlowsealUpgradeRequest("/apiws?dst=149.154.167.51&dc=2", "example.workers.dev", "test-key")
	want := "GET /apiws?dst=149.154.167.51&dc=2 HTTP/1.1\r\n" +
		"Host: example.workers.dev\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: test-key\r\n" +
		"Sec-WebSocket-Version: 13\r\n" +
		"Sec-WebSocket-Protocol: binary\r\n\r\n"
	if request != want {
		t.Fatalf("upgrade request differs from Flowseal parity request:\n%s", request)
	}
	if strings.Contains(strings.ToLower(request), "user-agent") {
		t.Fatal("Flowseal parity request must not add the Android RawWebSocket User-Agent header")
	}
}

func TestFlowsealRawWebSocketKeeps65536BytesInOneMessage(t *testing.T) {
	conn := &frameTestConn{reader: bytes.NewReader(nil), maxWrite: 16 * 1024}
	ws := &flowsealRawWebSocket{conn: conn, reader: bufio.NewReader(conn)}
	payload := bytes.Repeat([]byte{0x5A}, 65536)

	if err := ws.Send(payload); err != nil {
		t.Fatalf("send: %v", err)
	}
	if got, want := conn.writes.Len(), 65550; got != want {
		t.Fatalf("serialized frame bytes=%d want=%d", got, want)
	}
	lengths := clientFramePayloadLengths(t, conn.writes.Bytes())
	if len(lengths) != 1 || lengths[0] != len(payload) {
		t.Fatalf("WebSocket message payloads=%v want=[%d]", lengths, len(payload))
	}
	if conn.writeCalls <= 1 {
		t.Fatalf("write calls=%d want multiple transport writes for the short-write fixture", conn.writeCalls)
	}
}

func TestFlowsealRawWebSocketReassemblesFragmentedServerMessage(t *testing.T) {
	input := bytes.Join([][]byte{
		testServerFrame(opBinary, []byte("AAA"), false),
		testServerFrame(opPing, []byte("p"), true),
		testServerFrame(opContinuation, []byte("BBB"), false),
		testServerFrame(opContinuation, []byte("CCC"), true),
	}, nil)
	conn := &frameTestConn{reader: bytes.NewReader(input)}
	ws := &flowsealRawWebSocket{conn: conn, reader: bufio.NewReader(conn)}

	message, err := ws.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if got, want := string(message), "AAABBBCCC"; got != want {
		t.Fatalf("message=%q want=%q", got, want)
	}
	if conn.writes.Len() == 0 {
		t.Fatal("ping between fragments must produce a pong")
	}
}

func TestProductionWorkerConnectorEnablesFlowsealParity(t *testing.T) {
	connector := newMtProtoWorkerConnector()
	if !connector.flowsealParity {
		t.Fatal("production Worker connector must enable Flowseal parity mode")
	}
	if connector.dial == nil {
		t.Fatal("production Worker connector must have a parity dialer")
	}
}

func TestFlowsealParityWorkerConnectorBypassesPreconnectPool(t *testing.T) {
	stats.Reset()
	withPoolSize(t, 1)
	withRuntimeSettings(t, func(settings runtimeSettings) runtimeSettings {
		settings.Worker.Enabled = true
		settings.Worker.Domain = "example.workers.dev"
		settings.Worker.Failover = workerFailoverSettings{}
		settings.Worker.DestinationMode = tgwsroute.WorkerDestinationPreserveOriginalDst
		settings.MtProtoWorkerPreconnect = true
		return settings
	})

	previousPool := workerPool
	pool := newWorkerWsPool(&fakeWorkerDialer{})
	workerPool = pool
	t.Cleanup(func() {
		workerPool.CloseAll()
		workerPool = previousPool
	})
	pool.idle[WorkerPoolKey{
		DC:           2,
		WorkerDomain: "example.workers.dev",
		Dst:          "149.154.167.51",
		Media:        false,
	}] = []poolEntry{{ws: newFakeWebSocket(), created: pool.now()}}

	dials := 0
	socket := &fakeMtProtoFrameSocket{}
	connector := &mtProtoWorkerConnector{
		flowsealParity: true,
		dial: func(_, _, _ string) (mtProtoFrameSocket, error) {
			dials++
			return socket, nil
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
	if dials != 1 {
		t.Fatalf("fresh parity dials=%d want=1", dials)
	}
	if got := stats.workerWsPreconnectHits.Load(); got != 0 {
		t.Fatalf("preconnect hits=%d want=0 in Flowseal parity mode", got)
	}
}
