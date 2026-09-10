package main

import (
	"net"
	"strings"
	"sync"
	"time"

	"tg-ws-proxy/mtproxyfrontend"
)

const mtProtoWorkerPayloadTraceLimit = 16

type mtProtoWorkerPayloadTraceConn struct {
	net.Conn

	sessionID string
	signedDC  int16
	dc        int
	media     bool
	workerDst string
	started   time.Time

	mu         sync.Mutex
	upChunks   int
	downChunks int
}

func wrapMtProtoWorkerPayloadTrace(
	conn net.Conn,
	request mtproxyfrontend.OutboundRequest,
	sessionID string,
	workerDst string,
) net.Conn {
	if conn == nil {
		return conn
	}
	return &mtProtoWorkerPayloadTraceConn{
		Conn:      conn,
		sessionID: strings.TrimSpace(sessionID),
		signedDC:  request.SignedDC,
		dc:        request.DCID,
		media:     request.IsMedia,
		workerDst: strings.TrimSpace(workerDst),
		started:   time.Now(),
	}
}

func (c *mtProtoWorkerPayloadTraceConn) Read(dst []byte) (int, error) {
	n, err := c.Conn.Read(dst)
	if n > 0 {
		c.traceChunk("worker_to_client", n)
	}
	return n, err
}

func (c *mtProtoWorkerPayloadTraceConn) Write(data []byte) (int, error) {
	n, err := c.Conn.Write(data)
	if n > 0 {
		c.traceChunk("client_to_worker", n)
	}
	return n, err
}

func (c *mtProtoWorkerPayloadTraceConn) traceChunk(direction string, size int) {
	if size <= 0 || logInfo == nil {
		return
	}

	c.mu.Lock()
	var index int
	switch direction {
	case "client_to_worker":
		if c.upChunks >= mtProtoWorkerPayloadTraceLimit {
			c.mu.Unlock()
			return
		}
		c.upChunks++
		index = c.upChunks
	case "worker_to_client":
		if c.downChunks >= mtProtoWorkerPayloadTraceLimit {
			c.mu.Unlock()
			return
		}
		c.downChunks++
		index = c.downChunks
	default:
		c.mu.Unlock()
		return
	}
	elapsed := time.Since(c.started).Milliseconds()
	sessionID := c.sessionID
	signedDC := c.signedDC
	dc := c.dc
	media := c.media
	workerDst := c.workerDst
	c.mu.Unlock()

	logInfo.Printf(
		"MTProto Worker payload trace session_id=%s signed_dc=%d dc=%d media=%t worker_dst=%s direction=%s chunk_index=%d bytes=%d elapsed_ms=%d",
		mtProtoStatusField(sessionID),
		signedDC,
		dc,
		media,
		mtProtoStatusField(workerDst),
		direction,
		index,
		size,
		elapsed,
	)
}

func (c *mtProtoWorkerPayloadTraceConn) RouteDiagnostics() mtproxyfrontend.RouteDiagnostics {
	diagnostics := mtproxyfrontend.RouteDiagnostics{
		SessionID: c.sessionID,
		WorkerDst: c.workerDst,
	}
	if provider, ok := c.Conn.(mtproxyfrontend.RouteDiagnosticsProvider); ok {
		provided := provider.RouteDiagnostics()
		if strings.TrimSpace(provided.SessionID) != "" {
			diagnostics.SessionID = strings.TrimSpace(provided.SessionID)
		}
		if strings.TrimSpace(provided.WorkerDst) != "" {
			diagnostics.WorkerDst = strings.TrimSpace(provided.WorkerDst)
		}
	}
	return diagnostics
}

var _ net.Conn = (*mtProtoWorkerPayloadTraceConn)(nil)
var _ mtproxyfrontend.RouteDiagnosticsProvider = (*mtProtoWorkerPayloadTraceConn)(nil)
