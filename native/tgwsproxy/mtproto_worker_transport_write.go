package main

import (
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"time"

	"tg-ws-proxy/mtproxyfrontend"
)

const (
	// This is a diagnostic transport granularity, not a WebSocket fragment
	// size. The serialized WebSocket frame and its message boundary stay intact.
	mtProtoWorkerTransportWriteChunk = 16 * 1024
	mtProtoWorkerTransportTraceLimit = 32
)

// mtProtoWorkerTransportWriteConn tests whether #29 is sensitive to the
// application-write cadence into crypto/tls. Flowseal successfully moves bulk
// traffic through the same Worker while awaiting StreamWriter.drain() after
// each WebSocket send. Go has no direct drain equivalent here, so this layer
// hands one already-serialized WebSocket frame to the TLS connection in bounded
// slices and yields between slices. It never creates extra WebSocket frames or
// changes message boundaries.
type mtProtoWorkerTransportWriteConn struct {
	net.Conn

	sessionID string
	signedDC  int16
	dc        int
	media     bool
	workerDst string
	started   time.Time

	mu         sync.Mutex
	writeIndex int
}

func installMtProtoWorkerTransportWrite(
	conn net.Conn,
	request mtproxyfrontend.OutboundRequest,
	sessionID string,
	workerDst string,
) {
	stream, ok := conn.(*mtProtoWebSocketStream)
	if !ok || stream == nil {
		return
	}
	safeSocket, ok := stream.socket.(*mtProtoSafeFrameSocket)
	if !ok || safeSocket == nil || safeSocket.raw == nil || safeSocket.raw.conn == nil {
		return
	}
	if _, alreadyWrapped := safeSocket.raw.conn.(*mtProtoWorkerTransportWriteConn); alreadyWrapped {
		return
	}

	safeSocket.raw.conn = &mtProtoWorkerTransportWriteConn{
		Conn:      safeSocket.raw.conn,
		sessionID: strings.TrimSpace(sessionID),
		signedDC:  request.SignedDC,
		dc:        request.DCID,
		media:     request.IsMedia,
		workerDst: strings.TrimSpace(workerDst),
		started:   time.Now(),
	}
}

func (c *mtProtoWorkerTransportWriteConn) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return c.Conn.Write(data)
	}

	c.mu.Lock()
	c.writeIndex++
	writeIndex := c.writeIndex
	c.mu.Unlock()

	started := time.Now()
	total := 0
	transportWrites := 0
	var writeErr error

	for total < len(data) {
		end := total + mtProtoWorkerTransportWriteChunk
		if end > len(data) {
			end = len(data)
		}

		for total < end {
			n, err := c.Conn.Write(data[total:end])
			transportWrites++
			if n > 0 {
				total += n
			}
			if err != nil {
				writeErr = err
				break
			}
			if n == 0 {
				writeErr = io.ErrShortWrite
				break
			}
		}
		if writeErr != nil {
			break
		}

		// Give the reader goroutine and the runtime network poller an explicit
		// scheduling point between bounded TLS application writes. Unlike the
		// rejected RFC6455 experiment, this does not alter bytes on the wire at
		// the WebSocket protocol layer.
		runtime.Gosched()
	}

	if logInfo != nil && (writeIndex <= mtProtoWorkerTransportTraceLimit || writeErr != nil) {
		result := "ok"
		errorField := "none"
		if writeErr != nil {
			result = "error"
			errorField = mtProtoStatusField(writeErr.Error())
		}
		logInfo.Printf(
			"MTProto Worker transport write trace session_id=%s signed_dc=%d dc=%d media=%t worker_dst=%s write_index=%d requested_bytes=%d written_bytes=%d transport_writes=%d max_transport_chunk=%d write_ms=%d elapsed_ms=%d result=%s error=%s",
			mtProtoStatusField(c.sessionID),
			c.signedDC,
			c.dc,
			c.media,
			mtProtoStatusField(c.workerDst),
			writeIndex,
			len(data),
			total,
			transportWrites,
			mtProtoWorkerTransportWriteChunk,
			time.Since(started).Milliseconds(),
			time.Since(c.started).Milliseconds(),
			result,
			errorField,
		)
	}

	return total, writeErr
}

var _ net.Conn = (*mtProtoWorkerTransportWriteConn)(nil)
