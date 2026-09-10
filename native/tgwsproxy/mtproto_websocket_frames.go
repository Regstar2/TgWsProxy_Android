package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

// mtProtoMaxWebSocketMessageLen mirrors the current Flowseal runtime guard.
// The limit is applied both to individual frames and to the accumulated
// fragmented message before it is exposed to the MTProto stream.
const mtProtoMaxWebSocketMessageLen = 16 * 1024 * 1024

// mtProtoMaxOutboundWebSocketPayloadLen deliberately keeps each outbound
// WebSocket message below the 64 KiB reads produced by the MTProto bridge.
// Real-device diagnostics for #29 showed repeated 64 KiB Worker messages with
// no downstream response, followed by Cloudflare write timeouts. Splitting the
// byte stream into smaller WebSocket messages preserves TCP stream semantics at
// the Worker while adding backpressure between writes.
const mtProtoMaxOutboundWebSocketPayloadLen = 16 * 1024

// mtProtoSafeFrameSocket adds bounded WebSocket message reassembly to the
// existing RawWebSocket without changing the Android-specific transport,
// routing, cooldown, watchdog or diagnostics code around it.
type mtProtoSafeFrameSocket struct {
	raw           *RawWebSocket
	frag          []byte
	maxMessageLen int
}

func wrapMtProtoFrameSocket(socket mtProtoFrameSocket) mtProtoFrameSocket {
	raw, ok := socket.(*RawWebSocket)
	if !ok || raw == nil {
		return socket
	}
	return &mtProtoSafeFrameSocket{
		raw:           raw,
		maxMessageLen: mtProtoMaxWebSocketMessageLen,
	}
}

func (s *mtProtoSafeFrameSocket) messageLimit() int {
	if s.maxMessageLen > 0 {
		return s.maxMessageLen
	}
	return mtProtoMaxWebSocketMessageLen
}

func (s *mtProtoSafeFrameSocket) Send(data []byte) error {
	if s == nil || s.raw == nil {
		return fmt.Errorf("WebSocket closed")
	}
	if len(data) == 0 {
		return s.writeFrameFull(opBinary, nil)
	}

	for len(data) > 0 {
		chunkLen := len(data)
		if chunkLen > mtProtoMaxOutboundWebSocketPayloadLen {
			chunkLen = mtProtoMaxOutboundWebSocketPayloadLen
		}
		if err := s.writeFrameFull(opBinary, data[:chunkLen]); err != nil {
			return err
		}
		data = data[chunkLen:]
	}
	return nil
}

func (s *mtProtoSafeFrameSocket) SendBatch(parts [][]byte) error {
	for _, part := range parts {
		if err := s.Send(part); err != nil {
			return err
		}
	}
	return nil
}

func (s *mtProtoSafeFrameSocket) writeFrameFull(opcode int, payload []byte) error {
	if s.raw.closed.Load() {
		return fmt.Errorf("WebSocket closed")
	}
	frame := s.raw.buildFrame(opcode, payload, true)
	s.raw.writeMu.Lock()
	defer s.raw.writeMu.Unlock()
	return writeFull(s.raw.conn, frame)
}

// writeFull turns net.Conn.Write into the same all-bytes-or-error contract that
// Flowseal gets from StreamWriter.write()+drain(). A short successful Write is
// legal for io.Writer and must not silently truncate a WebSocket frame.
func writeFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func (s *mtProtoSafeFrameSocket) Close() {
	s.raw.Close()
}

func (s *mtProtoSafeFrameSocket) Recv() ([]byte, error) {
	limit := s.messageLimit()
	for !s.raw.closed.Load() {
		opcode, payload, fin, err := readMtProtoWebSocketFrame(s.raw, limit)
		if err != nil {
			s.raw.closed.Store(true)
			_ = s.raw.conn.Close()
			return nil, err
		}

		switch opcode {
		case opClose:
			closePayload := payload
			if len(closePayload) > 2 {
				closePayload = closePayload[:2]
			}
			_ = s.writeFrameFull(opClose, closePayload)
			s.raw.closed.Store(true)
			_ = s.raw.conn.Close()
			return nil, io.EOF

		case opPing:
			if err := s.writeFrameFull(opPong, payload); err != nil {
				s.raw.closed.Store(true)
				_ = s.raw.conn.Close()
				return nil, err
			}
			continue

		case opPong:
			continue

		case opContinuation, opText, opBinary:
			// Keep the same tolerant semantics as current Flowseal: continuation,
			// text and binary data frames all participate in one accumulated
			// message, while control frames may appear between fragments.
			if fin && len(s.frag) == 0 {
				return payload, nil
			}
			s.frag = append(s.frag, payload...)
			if len(s.frag) > limit {
				_ = s.raw.conn.Close()
				s.raw.closed.Store(true)
				return nil, fmt.Errorf("WS message too large: %d bytes", len(s.frag))
			}
			if !fin {
				continue
			}
			message := append([]byte(nil), s.frag...)
			s.frag = s.frag[:0]
			return message, nil

		default:
			continue
		}
	}
	return nil, io.EOF
}

func readMtProtoWebSocketFrame(ws *RawWebSocket, maxMessageLen int) (opcode int, payload []byte, fin bool, err error) {
	if maxMessageLen <= 0 {
		maxMessageLen = mtProtoMaxWebSocketMessageLen
	}

	var hdr [2]byte
	if _, err = io.ReadFull(ws.bufReader, hdr[:]); err != nil {
		return 0, nil, false, err
	}

	fin = (hdr[0] & 0x80) != 0
	opcode = int(hdr[0] & 0x0F)
	length := uint64(hdr[1] & 0x7F)

	switch length {
	case 126:
		var extended [2]byte
		if _, err = io.ReadFull(ws.bufReader, extended[:]); err != nil {
			return 0, nil, false, err
		}
		length = uint64(binary.BigEndian.Uint16(extended[:]))
	case 127:
		var extended [8]byte
		if _, err = io.ReadFull(ws.bufReader, extended[:]); err != nil {
			return 0, nil, false, err
		}
		length = binary.BigEndian.Uint64(extended[:])
	}

	if length > uint64(maxMessageLen) {
		return 0, nil, false, fmt.Errorf("WS frame too large: %d bytes", length)
	}

	hasMask := (hdr[1] & 0x80) != 0
	var maskKey [4]byte
	if hasMask {
		if _, err = io.ReadFull(ws.bufReader, maskKey[:]); err != nil {
			return 0, nil, false, err
		}
	}

	payload = make([]byte, int(length))
	if length > 0 {
		if _, err = io.ReadFull(ws.bufReader, payload); err != nil {
			return 0, nil, false, err
		}
	}
	if hasMask {
		xorMaskInPlace(payload, maskKey[:])
	}
	return opcode, payload, fin, nil
}

// GetForSession mirrors the useful part of Flowseal's Worker pool refactor:
// a miss does not immediately start another background refill while the caller
// is already about to dial the same Worker synchronously. Hits still trigger a
// refill so the explicitly enabled preconnect pool can replenish itself.
func (p *WorkerWsPool) GetForSession(key WorkerPoolKey) *RawWebSocket {
	if !workerWsPreconnectActive() || workerWsPreconnectTargetSize() <= 0 || key.WorkerDomain == "" || key.Dst == "" {
		return nil
	}

	now := p.now()
	p.mu.Lock()
	defer p.mu.Unlock()

	bucket := p.idle[key]
	for len(bucket) > 0 {
		entry := bucket[0]
		bucket = bucket[1:]
		p.idle[key] = bucket

		age := now - entry.created
		if age > p.maxAge || !p.reusable(entry.ws) {
			go entry.ws.Close()
			continue
		}

		stats.workerWsPreconnectHits.Add(1)
		p.scheduleRefillLocked(key)
		return entry.ws
	}

	stats.workerWsPreconnectMisses.Add(1)
	return nil
}

var _ mtProtoFrameSocket = (*mtProtoSafeFrameSocket)(nil)
