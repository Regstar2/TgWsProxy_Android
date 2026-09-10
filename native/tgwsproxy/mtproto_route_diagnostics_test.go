package main

import (
	"testing"

	"tg-ws-proxy/mtproxyfrontend"
)

func TestMtProtoWebSocketStreamExposesWorkerRouteDiagnostics(t *testing.T) {
	stream := &mtProtoWebSocketStream{
		sessionID: "session-media-dc2",
		workerDst: "149.154.167.51",
	}

	provider, ok := any(stream).(mtproxyfrontend.RouteDiagnosticsProvider)
	if !ok {
		t.Fatal("mtProtoWebSocketStream must expose route diagnostics")
	}
	diagnostics := provider.RouteDiagnostics()
	if diagnostics.SessionID != "session-media-dc2" {
		t.Fatalf("session id=%q", diagnostics.SessionID)
	}
	if diagnostics.WorkerDst != "149.154.167.51" {
		t.Fatalf("worker dst=%q", diagnostics.WorkerDst)
	}
}
