package main

import (
	"encoding/binary"
	"net"
	"sync"
	"time"

	"tg-ws-proxy/mtproxyfrontend"
)

const mtProtoWorkerFrameTraceLimit = 32

type mtProtoWorkerFrameTraceConn struct {
	net.Conn

	sessionID string
	signedDC  int16
	dc        int
	media     bool
	workerDst string
	started   time.Time

	mu sync.Mutex

	writeCallIndex int
	frameIndex     int
	frameRemaining int
	frameOpcode    int
	framePayload   int
	frameBytes     int
}

func installMtProtoWorkerFrameTrace(
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
	if _, alreadyWrapped := safeSocket.raw.conn.(*mtProtoWorkerFrameTraceConn); alreadyWrapped {
		return
	}

	safeSocket.raw.conn = &mtProtoWorkerFrameTraceConn{
		Conn:      safeSocket.raw.conn,
		sessionID: mtProtoStatusField(sessionID),
		signedDC:  request.SignedDC,
		dc:        request.DCID,
		media:     request.IsMedia,
		workerDst: mtProtoStatusField(workerDst),
		started:   time.Now(),
	}
}

func (c *mtProtoWorkerFrameTraceConn) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return c.Conn.Write(data)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.frameRemaining == 0 {
		if opcode, payloadBytes, frameBytes, ok := mtProtoOutboundFrameInfo(data); ok {
			c.frameIndex++
			c.frameOpcode = opcode
			c.framePayload = payloadBytes
			c.frameBytes = frameBytes
			c.frameRemaining = frameBytes
		} else {
			c.frameIndex++
			c.frameOpcode = -1
			c.framePayload = -1
			c.frameBytes = len(data)
			c.frameRemaining = len(data)
		}
	}

	c.writeCallIndex++
	writeCallIndex := c.writeCallIndex
	frameIndex := c.frameIndex
	frameOpcode := c.frameOpcode
	framePayload := c.framePayload
	frameBytes := c.frameBytes
	requestedBytes := len(data)

	writeStarted := time.Now()
	n, err := c.Conn.Write(data)
	writeMS := time.Since(writeStarted).Milliseconds()

	if n > 0 {
		c.frameRemaining -= n
		if c.frameRemaining < 0 {
			c.frameRemaining = 0
		}
	}
	remaining := c.frameRemaining
	completed := remaining == 0
	shortWrite := err == nil && n < requestedBytes

	result := "ok"
	errorField := "none"
	if err != nil {
		result = "error"
		errorField = mtProtoStatusField(err.Error())
	}

	if logInfo != nil && (frameIndex <= mtProtoWorkerFrameTraceLimit || err != nil || shortWrite) {
		logInfo.Printf(
			"MTProto Worker WS frame trace session_id=%s signed_dc=%d dc=%d media=%t worker_dst=%s frame_index=%d opcode=%d frame_payload_bytes=%d frame_bytes=%d write_call=%d requested_bytes=%d written_bytes=%d frame_remaining=%d completed=%t short_write=%t write_ms=%d elapsed_ms=%d result=%s error=%s",
			c.sessionID,
			c.signedDC,
			c.dc,
			c.media,
			c.workerDst,
			frameIndex,
			frameOpcode,
			framePayload,
			frameBytes,
			writeCallIndex,
			requestedBytes,
			n,
			remaining,
			completed,
			shortWrite,
			writeMS,
			time.Since(c.started).Milliseconds(),
			result,
			errorField,
		)
	}

	return n, err
}

func mtProtoOutboundFrameInfo(data []byte) (opcode int, payloadBytes int, frameBytes int, ok bool) {
	if len(data) < 2 {
		return 0, 0, 0, false
	}

	opcode = int(data[0] & 0x0F)
	payloadLen := uint64(data[1] & 0x7F)
	headerLen := 2

	switch payloadLen {
	case 126:
		if len(data) < 4 {
			return 0, 0, 0, false
		}
		payloadLen = uint64(binary.BigEndian.Uint16(data[2:4]))
		headerLen = 4
	case 127:
		if len(data) < 10 {
			return 0, 0, 0, false
		}
		payloadLen = binary.BigEndian.Uint64(data[2:10])
		headerLen = 10
	}

	if data[1]&0x80 != 0 {
		headerLen += 4
	}
	if payloadLen > uint64(^uint(0)>>1) {
		return 0, 0, 0, false
	}

	payloadBytes = int(payloadLen)
	frameBytes = headerLen + payloadBytes
	if frameBytes > len(data) {
		return 0, 0, 0, false
	}
	return opcode, payloadBytes, frameBytes, true
}

var _ net.Conn = (*mtProtoWorkerFrameTraceConn)(nil)
