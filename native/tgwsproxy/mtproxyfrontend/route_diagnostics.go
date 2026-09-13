package mtproxyfrontend

import (
	"crypto/sha256"
	"encoding/hex"
	"net"
	"strings"
)

// RouteDiagnostics contains correlation metadata that is safe to expose in
// runtime logs. SessionID is derived from the random relay init through SHA-256
// so no obfuscation key material is logged directly.
type RouteDiagnostics struct {
	SessionID string
	WorkerDst string
}

// RouteDiagnosticsProvider lets an outbound connection expose route-specific
// metadata without coupling the MTProto frontend to a concrete backend.
type RouteDiagnosticsProvider interface {
	RouteDiagnostics() RouteDiagnostics
}

// SessionIDForRequest returns a stable identifier for one MTProto relay
// session. Real requests have a freshly generated relay init, so the truncated
// SHA-256 value is unique enough for diagnostic correlation while remaining
// non-sensitive.
func SessionIDForRequest(request OutboundRequest) string {
	sum := sha256.Sum256(request.RelayInit)
	return hex.EncodeToString(sum[:8])
}

// RouteDiagnosticsFor combines request-level correlation with optional
// backend-specific metadata exposed by the connected stream.
func RouteDiagnosticsFor(conn net.Conn, request OutboundRequest) RouteDiagnostics {
	diagnostics := RouteDiagnostics{SessionID: SessionIDForRequest(request)}
	provider, ok := conn.(RouteDiagnosticsProvider)
	if !ok {
		return diagnostics
	}

	provided := provider.RouteDiagnostics()
	if strings.TrimSpace(provided.SessionID) != "" {
		diagnostics.SessionID = strings.TrimSpace(provided.SessionID)
	}
	diagnostics.WorkerDst = strings.TrimSpace(provided.WorkerDst)
	return diagnostics
}
