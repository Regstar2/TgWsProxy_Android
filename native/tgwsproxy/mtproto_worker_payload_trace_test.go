package main

import (
	"bytes"
	"log"
	"strings"
	"testing"

	"tg-ws-proxy/mtproxyfrontend"
)

func TestMtProtoWorkerPayloadTraceLogsFirst16ChunksPerDirection(t *testing.T) {
	previousLogInfo := logInfo
	var logs bytes.Buffer
	logInfo = log.New(&logs, "", 0)
	t.Cleanup(func() { logInfo = previousLogInfo })

	recv := make([][]byte, 17)
	for i := range recv {
		recv[i] = bytes.Repeat([]byte{byte(i + 1)}, i+1)
	}
	socket := &fakeMtProtoFrameSocket{recv: recv}
	base := &mtProtoWebSocketStream{socket: socket}
	request := mtproxyfrontend.OutboundRequest{
		DCID:     2,
		SignedDC: -2,
		IsMedia:  true,
	}
	traced := wrapMtProtoWorkerPayloadTrace(base, request, "media-session", "149.154.167.51")

	for i := 0; i < 17; i++ {
		payload := bytes.Repeat([]byte{0xA5}, i+1)
		if n, err := traced.Write(payload); err != nil || n != len(payload) {
			t.Fatalf("write[%d] n=%d err=%v", i, n, err)
		}
	}

	buf := make([]byte, 128)
	for i := 0; i < 17; i++ {
		n, err := traced.Read(buf)
		if err != nil {
			t.Fatalf("read[%d]: %v", i, err)
		}
		if n != i+1 {
			t.Fatalf("read[%d] n=%d want=%d", i, n, i+1)
		}
	}

	text := logs.String()
	if got := strings.Count(text, "direction=client_to_worker"); got != mtProtoWorkerPayloadTraceLimit {
		t.Fatalf("client_to_worker traces=%d want=%d\n%s", got, mtProtoWorkerPayloadTraceLimit, text)
	}
	if got := strings.Count(text, "direction=worker_to_client"); got != mtProtoWorkerPayloadTraceLimit {
		t.Fatalf("worker_to_client traces=%d want=%d\n%s", got, mtProtoWorkerPayloadTraceLimit, text)
	}
	for _, want := range []string{
		"session_id=media-session",
		"signed_dc=-2",
		"dc=2",
		"media=true",
		"worker_dst=149.154.167.51",
		"direction=client_to_worker chunk_index=16",
		"direction=worker_to_client chunk_index=16",
		"bytes=16",
		"elapsed_ms=",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in trace:\n%s", want, text)
		}
	}
	if strings.Contains(text, "chunk_index=17") {
		t.Fatalf("trace exceeded bounded limit:\n%s", text)
	}
}

func TestMtProtoWorkerPayloadTraceSkipsNonMediaAndPreservesDiagnostics(t *testing.T) {
	base := &mtProtoWebSocketStream{
		socket:    &fakeMtProtoFrameSocket{},
		sessionID: "underlying-session",
		workerDst: "149.154.167.51",
	}
	nonMedia := mtproxyfrontend.OutboundRequest{DCID: 2, SignedDC: 2, IsMedia: false}
	if got := wrapMtProtoWorkerPayloadTrace(base, nonMedia, "ignored", "ignored"); got != base {
		t.Fatal("non-media Worker connection must not be wrapped")
	}

	media := mtproxyfrontend.OutboundRequest{DCID: 2, SignedDC: -2, IsMedia: true}
	wrapped := wrapMtProtoWorkerPayloadTrace(base, media, "fallback-session", "149.154.167.220")
	diagnostics := mtproxyfrontend.RouteDiagnosticsFor(wrapped, media)
	if diagnostics.SessionID != "underlying-session" {
		t.Fatalf("session id=%q", diagnostics.SessionID)
	}
	if diagnostics.WorkerDst != "149.154.167.51" {
		t.Fatalf("worker dst=%q", diagnostics.WorkerDst)
	}
}
