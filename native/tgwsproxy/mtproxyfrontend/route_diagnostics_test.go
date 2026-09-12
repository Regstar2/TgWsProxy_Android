package mtproxyfrontend

import (
	"net"
	"testing"
)

type testRouteDiagnosticsConn struct {
	net.Conn
	diagnostics RouteDiagnostics
}

func (c *testRouteDiagnosticsConn) RouteDiagnostics() RouteDiagnostics {
	return c.diagnostics
}

func TestSessionIDForRequestStableAndDistinct(t *testing.T) {
	first := OutboundRequest{RelayInit: []byte("relay-init-a")}
	second := OutboundRequest{RelayInit: []byte("relay-init-b")}

	firstID := SessionIDForRequest(first)
	if len(firstID) != 16 {
		t.Fatalf("session id length=%d want=16: %q", len(firstID), firstID)
	}
	if SessionIDForRequest(first) != firstID {
		t.Fatal("session id must be stable for the same request")
	}
	if SessionIDForRequest(second) == firstID {
		t.Fatal("different relay init values must not share a session id")
	}
}

func TestRouteDiagnosticsForUsesBackendMetadata(t *testing.T) {
	request := OutboundRequest{RelayInit: []byte("relay-init")}
	conn := &testRouteDiagnosticsConn{diagnostics: RouteDiagnostics{
		SessionID: "worker-session",
		WorkerDst: "149.154.167.51",
	}}

	diagnostics := RouteDiagnosticsFor(conn, request)
	if diagnostics.SessionID != "worker-session" {
		t.Fatalf("session id=%q", diagnostics.SessionID)
	}
	if diagnostics.WorkerDst != "149.154.167.51" {
		t.Fatalf("worker dst=%q", diagnostics.WorkerDst)
	}
}

func TestRouteDiagnosticsForFallsBackToRequestSession(t *testing.T) {
	request := OutboundRequest{RelayInit: []byte("relay-init")}
	diagnostics := RouteDiagnosticsFor(nil, request)
	if diagnostics.SessionID != SessionIDForRequest(request) {
		t.Fatalf("session id=%q", diagnostics.SessionID)
	}
	if diagnostics.WorkerDst != "" {
		t.Fatalf("worker dst=%q want empty", diagnostics.WorkerDst)
	}
}
