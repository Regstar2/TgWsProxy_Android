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
)

const flowsealMaxWebSocketMessageLen = 16 * 1024 * 1024

// flowsealRawWebSocket mirrors Flowseal's RawWebSocket message semantics and
// intentionally stays separate from the Android RawWebSocket abstraction.
type flowsealRawWebSocket struct {
	conn      net.Conn
	reader    *bufio.Reader
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
	frame := buildFlowsealFrame(opBinary, data, true)
	ws.writeMu.Lock()
	err := writeFlowsealFull(ws.conn, frame)
	ws.writeMu.Unlock()
	if err == nil {
		ws.noteSend(len(data), len(frame))
	}
	return err
}

func (ws *flowsealRawWebSocket) SendBatch(parts [][]byte) error {
	if ws == nil || ws.closed.Load() {
		return fmt.Errorf("WebSocket closed")
	}
	ws.writeMu.Lock()
	defer ws.writeMu.Unlock()
	for _, part := range parts {
		frame := buildFlowsealFrame(opBinary, part, true)
		if err := writeFlowsealFull(ws.conn, frame); err != nil {
			return err
		}
		ws.noteSend(len(part), len(frame))
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
	frame := buildFlowsealFrame(opcode, payload, true)
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

func buildFlowsealFrame(opcode int, payload []byte, mask bool) []byte {
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
	frame[0] = byte(0x80 | opcode)
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
	for len(data) > 0 {
		written, err := writer.Write(data)
		if written > 0 {
			data = data[written:]
		}
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func (ws *flowsealRawWebSocket) noteSend(payloadBytes, frameBytes int) {
	sequence := ws.sendCount.Add(1)
	cumulative := ws.sentBytes.Add(uint64(payloadBytes))
	if logInfo != nil && (payloadBytes >= 64*1024 || sequence <= 2) {
		logInfo.Printf("MTProto Worker Flowseal parity WS send seq=%d payload_bytes=%d frame_bytes=%d cumulative_payload_bytes=%d", sequence, payloadBytes, frameBytes, cumulative)
	}
}

func (ws *flowsealRawWebSocket) noteRecv(payloadBytes int) {
	sequence := ws.recvCount.Add(1)
	cumulative := ws.recvBytes.Add(uint64(payloadBytes))
	if logInfo != nil && (payloadBytes >= 64*1024 || sequence <= 2) {
		logInfo.Printf("MTProto Worker Flowseal parity WS recv seq=%d payload_bytes=%d cumulative_payload_bytes=%d", sequence, payloadBytes, cumulative)
	}
}

var _ mtProtoFrameSocket = (*flowsealRawWebSocket)(nil)
