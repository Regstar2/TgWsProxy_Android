package main

import (
	"bufio"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	flowsealMaxWebSocketMessageLen       = 16 * 1024 * 1024
	flowsealDiagnosticPayloadBoundary    = 64 * 1024
	flowsealDiagnosticFirstFragmentBytes = 64*1024 - 1
)

// flowsealRawWebSocket mirrors Flowseal's RawWebSocket message semantics and
// intentionally stays separate from the Android RawWebSocket abstraction.
type flowsealRawWebSocket struct {
	conn      net.Conn
	reader    *bufio.Reader
	sessionID string
	sendMu    sync.Mutex
	writeMu   sync.Mutex
	closed    atomic.Bool
	frag      []byte
	sendCount atomic.Uint64
	recvCount atomic.Uint64
	sentBytes atomic.Uint64
	recvBytes atomic.Uint64
}

func (ws *flowsealRawWebSocket) Send(data []byte) error {
	if ws == nil || ws.closed.Load() {
		return fmt.Errorf("WebSocket closed")
	}
	if len(data) == flowsealDiagnosticPayloadBoundary && logInfo != nil {
		logInfo.Printf(
			"MTProto Worker Flowseal diagnostic fragment session_id=%s payload_bytes=%d first_fragment_bytes=%d final_fragment_bytes=%d reason=probe_rfc6455_length_127",
			ws.logSessionID(), len(data), flowsealDiagnosticFirstFragmentBytes, len(data)-flowsealDiagnosticFirstFragmentBytes,
		)
	}
	frame := buildFlowsealFrame(opBinary, data, true)
	return ws.sendFrame(frame, len(data))
}

func (ws *flowsealRawWebSocket) SendBatch(parts [][]byte) error {
	if ws == nil || ws.closed.Load() {
		return fmt.Errorf("WebSocket closed")
	}
	for _, part := range parts {
		if err := ws.Send(part); err != nil {
			return err
		}
	}
	return nil
}

func (ws *flowsealRawWebSocket) sendFrame(frame []byte, payloadBytes int) error {
	if ws == nil || ws.closed.Load() {
		return fmt.Errorf("WebSocket closed")
	}

	// Keep application data messages ordered. Control frames use writeMu only,
	// so the receive loop can still answer ping/close independently.
	ws.sendMu.Lock()
	defer ws.sendMu.Unlock()

	// Allocate the sequence before entering the potentially blocking transport
	// write. This keeps the exact blocked attempt visible in #29 diagnostics.
	sequence := ws.sendCount.Add(1)
	trace := payloadBytes >= 64*1024 || sequence <= 2

	ws.writeMu.Lock()
	queueBefore := tcpSendQueueBytes(ws.conn)
	notSentBefore := tcpNotSentBytes(ws.conn)
	sendBufferBytes := tcpSendBufferBytes(ws.conn)
	started := time.Now()
	if logInfo != nil && trace {
		logInfo.Printf(
			"MTProto Worker Flowseal parity WS write start session_id=%s seq=%d payload_bytes=%d frame_bytes=%d tcp_send_queue_before=%d tcp_not_sent_before=%d tcp_send_buffer_bytes=%d",
			ws.logSessionID(), sequence, payloadBytes, len(frame), queueBefore, notSentBefore, sendBufferBytes,
		)
	}

	writtenFrameBytes, err := writeFlowsealFullCount(ws.conn, frame)
	duration := time.Since(started)
	queueAfter := tcpSendQueueBytes(ws.conn)
	notSentAfter := tcpNotSentBytes(ws.conn)
	ws.writeMu.Unlock()

	if err != nil {
		if logInfo != nil {
			logInfo.Printf(
				"MTProto Worker Flowseal parity WS write failed session_id=%s seq=%d payload_bytes=%d frame_bytes=%d written_frame_bytes=%d duration_ms=%d tcp_send_queue_before=%d tcp_send_queue_after=%d tcp_not_sent_before=%d tcp_not_sent_after=%d tcp_send_buffer_bytes=%d error=%v",
				ws.logSessionID(), sequence, payloadBytes, len(frame), writtenFrameBytes, duration.Milliseconds(), queueBefore, queueAfter, notSentBefore, notSentAfter, sendBufferBytes, err,
			)
		}
		return err
	}

	cumulative := ws.sentBytes.Add(uint64(payloadBytes))
	if logInfo != nil && trace {
		logInfo.Printf(
			"MTProto Worker Flowseal parity WS send session_id=%s seq=%d payload_bytes=%d frame_bytes=%d written_frame_bytes=%d cumulative_payload_bytes=%d duration_ms=%d tcp_send_queue_before=%d tcp_send_queue_after=%d tcp_not_sent_before=%d tcp_not_sent_after=%d tcp_send_buffer_bytes=%d",
			ws.logSessionID(), sequence, payloadBytes, len(frame), writtenFrameBytes, cumulative, duration.Milliseconds(), queueBefore, queueAfter, notSentBefore, notSentAfter, sendBufferBytes,
		)
	}
	return nil
}

func (ws *flowsealRawWebSocket) Recv() ([]byte, error) {
	for ws != nil && !ws.closed.Load() {
		opcode, payload, fin, err := ws.readFrame()
		if err != nil {
			ws.closed.Store(true)
			_ = ws.conn.Close()
			return nil, err
		}
		switch opcode {
		case opClose:
			closePayload := payload
			if len(closePayload) > 2 {
				closePayload = closePayload[:2]
			}
			_ = ws.writeControl(opClose, closePayload)
			ws.closed.Store(true)
			_ = ws.conn.Close()
			return nil, io.EOF
		case opPing:
			if err := ws.writeControl(opPong, payload); err != nil {
				return nil, err
			}
			continue
		case opPong:
			continue
		case opContinuation, opText, opBinary:
			if fin && len(ws.frag) == 0 {
				ws.noteRecv(len(payload))
				return payload, nil
			}
			ws.frag = append(ws.frag, payload...)
			if len(ws.frag) > flowsealMaxWebSocketMessageLen {
				return nil, fmt.Errorf("WS message too large: %d bytes", len(ws.frag))
			}
			if !fin {
				continue
			}
			message := append([]byte(nil), ws.frag...)
			ws.frag = ws.frag[:0]
			ws.noteRecv(len(message))
			return message, nil
		}
	}
	return nil, io.EOF
}

func (ws *flowsealRawWebSocket) Close() {
	if ws == nil || ws.closed.Swap(true) {
		return
	}
	_ = ws.writeControl(opClose, nil)
	_ = ws.conn.Close()
}

func (ws *flowsealRawWebSocket) writeControl(opcode int, payload []byte) error {
	frame := buildFlowsealSingleFrame(opcode, payload, true, true)
	ws.writeMu.Lock()
	err := writeFlowsealFull(ws.conn, frame)
	ws.writeMu.Unlock()
	return err
}

func (ws *flowsealRawWebSocket) readFrame() (int, []byte, bool, error) {
	var header [2]byte
	if _, err := io.ReadFull(ws.reader, header[:]); err != nil {
		return 0, nil, false, err
	}
	fin := header[0]&0x80 != 0
	opcode := int(header[0] & 0x0F)
	length := uint64(header[1] & 0x7F)
	if length == 126 {
		var extended [2]byte
		if _, err := io.ReadFull(ws.reader, extended[:]); err != nil {
			return 0, nil, false, err
		}
		length = uint64(binary.BigEndian.Uint16(extended[:]))
	} else if length == 127 {
		var extended [8]byte
		if _, err := io.ReadFull(ws.reader, extended[:]); err != nil {
			return 0, nil, false, err
		}
		length = binary.BigEndian.Uint64(extended[:])
	}
	if length > flowsealMaxWebSocketMessageLen {
		return 0, nil, false, fmt.Errorf("WS frame too large: %d bytes", length)
	}
	var mask [4]byte
	hasMask := header[1]&0x80 != 0
	if hasMask {
		if _, err := io.ReadFull(ws.reader, mask[:]); err != nil {
			return 0, nil, false, err
		}
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(ws.reader, payload); err != nil {
		return 0, nil, false, err
	}
	if hasMask {
		xorMaskInPlace(payload, mask[:])
	}
	return opcode, payload, fin, nil
}

// buildFlowsealFrame normally emits one RFC6455 frame exactly like upstream
// Flowseal. For the #29 device-only diagnostic, an exact 65536-byte binary
// message is encoded as two RFC6455 fragments (65535 + 1). The logical
// WebSocket message stays 65536 bytes, but neither wire frame uses the 64-bit
// length-127 encoding. This isolates that boundary without changing MTProto
// bytes or the Worker-visible message payload.
func buildFlowsealFrame(opcode int, payload []byte, mask bool) []byte {
	if opcode == opBinary && len(payload) == flowsealDiagnosticPayloadBoundary {
		first := buildFlowsealSingleFrame(opcode, payload[:flowsealDiagnosticFirstFragmentBytes], mask, false)
		last := buildFlowsealSingleFrame(opContinuation, payload[flowsealDiagnosticFirstFragmentBytes:], mask, true)
		wire := make([]byte, 0, len(first)+len(last))
		wire = append(wire, first...)
		wire = append(wire, last...)
		return wire
	}
	return buildFlowsealSingleFrame(opcode, payload, mask, true)
}

func buildFlowsealSingleFrame(opcode int, payload []byte, mask bool, fin bool) []byte {
	length := len(payload)
	headerLen := 2
	if length >= 126 && length < 65536 {
		headerLen += 2
	} else if length >= 65536 {
		headerLen += 8
	}
	if mask {
		headerLen += 4
	}
	frame := make([]byte, headerLen+length)
	if fin {
		frame[0] = 0x80 | byte(opcode)
	} else {
		frame[0] = byte(opcode)
	}
	position := 1
	maskBit := byte(0)
	if mask {
		maskBit = 0x80
	}
	switch {
	case length < 126:
		frame[position] = maskBit | byte(length)
		position++
	case length < 65536:
		frame[position] = maskBit | 126
		position++
		binary.BigEndian.PutUint16(frame[position:], uint16(length))
		position += 2
	default:
		frame[position] = maskBit | 127
		position++
		binary.BigEndian.PutUint64(frame[position:], uint64(length))
		position += 8
	}
	if !mask {
		copy(frame[position:], payload)
		return frame
	}
	var maskKey [4]byte
	_, _ = rand.Read(maskKey[:])
	copy(frame[position:], maskKey[:])
	position += 4
	copy(frame[position:], payload)
	xorMaskInPlace(frame[position:], maskKey[:])
	return frame
}

func writeFlowsealFull(writer io.Writer, data []byte) error {
	_, err := writeFlowsealFullCount(writer, data)
	return err
}

func writeFlowsealFullCount(writer io.Writer, data []byte) (int, error) {
	total := 0
	for len(data) > 0 {
		written, err := writer.Write(data)
		if written > 0 {
			total += written
			data = data[written:]
		}
		if err != nil {
			return total, err
		}
		if written == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

func (ws *flowsealRawWebSocket) noteRecv(payloadBytes int) {
	sequence := ws.recvCount.Add(1)
	cumulative := ws.recvBytes.Add(uint64(payloadBytes))
	if logInfo != nil && (payloadBytes >= 64*1024 || sequence <= 2) {
		logInfo.Printf(
			"MTProto Worker Flowseal parity WS recv session_id=%s seq=%d payload_bytes=%d cumulative_payload_bytes=%d",
			ws.logSessionID(), sequence, payloadBytes, cumulative,
		)
	}
}

func (ws *flowsealRawWebSocket) logSessionID() string {
	if ws == nil || ws.sessionID == "" {
		return "none"
	}
	return mtProtoStatusField(ws.sessionID)
}

func underlyingTCPConn(conn net.Conn) *net.TCPConn {
	for conn != nil {
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			return tcpConn
		}
		netConnProvider, ok := conn.(interface{ NetConn() net.Conn })
		if !ok {
			return nil
		}
		next := netConnProvider.NetConn()
		if next == conn {
			return nil
		}
		conn = next
	}
	return nil
}

var _ mtProtoFrameSocket = (*flowsealRawWebSocket)(nil)
