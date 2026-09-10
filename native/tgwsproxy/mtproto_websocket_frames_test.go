package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

type frameTestConn struct {
	reader     *bytes.Reader
	writes     bytes.Buffer
	closed     bool
	maxWrite   int
	writeCalls int
}

func newFrameTestRaw(input []byte) (*RawWebSocket, *frameTestConn) {
	conn := &frameTestConn{reader: bytes.NewReader(input)}
	return &RawWebSocket{
		conn:      conn,
		bufReader: bufio.NewReader(conn),
	}, conn
}

func (c *frameTestConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *frameTestConn) Write(p []byte) (int, error) {
	c.writeCalls++
	if c.maxWrite > 0 && len(p) > c.maxWrite {
		p = p[:c.maxWrite]
	}
	return c.writes.Write(p)
}

func (c *frameTestConn) Close() error {
	c.closed = true
	return nil
}

func (c *frameTestConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *frameTestConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *frameTestConn) SetDeadline(time.Time) error      { return nil }
func (c *frameTestConn) SetReadDeadline(time.Time) error  { return nil }
func (c *frameTestConn) SetWriteDeadline(time.Time) error { return nil }

func testServerFrame(opcode int, payload []byte, fin bool) []byte {
	first := byte(opcode)
	if fin {
		first |= 0x80
	}
	length := len(payload)
	switch {
	case length < 126:
		return append([]byte{first, byte(length)}, payload...)
	case length < 65536:
		frame := make([]byte, 4+length)
		frame[0] = first
		frame[1] = 126
		binary.BigEndian.PutUint16(frame[2:4], uint16(length))
		copy(frame[4:], payload)
		return frame
	default:
		frame := make([]byte, 10+length)
		frame[0] = first
		frame[1] = 127
		binary.BigEndian.PutUint64(frame[2:10], uint64(length))
		copy(frame[10:], payload)
		return frame
	}
}

func clientFramePayloadLengths(t *testing.T, data []byte) []int {
	t.Helper()
	reader := bytes.NewReader(data)
	lengths := make([]int, 0, 4)
	for reader.Len() > 0 {
		first, err := reader.ReadByte()
		if err != nil {
			t.Fatalf("read frame first byte: %v", err)
		}
		if first&0x0F != opBinary {
			t.Fatalf("opcode=%d want=%d", first&0x0F, opBinary)
		}
		second, err := reader.ReadByte()
		if err != nil {
			t.Fatalf("read frame second byte: %v", err)
		}
		if second&0x80 == 0 {
			t.Fatal("client frame must be masked")
		}

		payloadLen := uint64(second & 0x7F)
		switch payloadLen {
		case 126:
			var extended [2]byte
			if _, err := io.ReadFull(reader, extended[:]); err != nil {
				t.Fatalf("read 16-bit payload length: %v", err)
			}
			payloadLen = uint64(binary.BigEndian.Uint16(extended[:]))
		case 127:
			var extended [8]byte
			if _, err := io.ReadFull(reader, extended[:]); err != nil {
				t.Fatalf("read 64-bit payload length: %v", err)
			}
			payloadLen = binary.BigEndian.Uint64(extended[:])
		}

		var mask [4]byte
		if _, err := io.ReadFull(reader, mask[:]); err != nil {
			t.Fatalf("read mask: %v", err)
		}
		if payloadLen > uint64(reader.Len()) {
			t.Fatalf("payload length=%d remaining=%d", payloadLen, reader.Len())
		}
		if _, err := reader.Seek(int64(payloadLen), io.SeekCurrent); err != nil {
			t.Fatalf("skip payload: %v", err)
		}
		lengths = append(lengths, int(payloadLen))
	}
	return lengths
}

func TestMtProtoSafeFrameSocketReassemblesFragmentedMessage(t *testing.T) {
	input := bytes.Join([][]byte{
		testServerFrame(opBinary, []byte("AAA"), false),
		testServerFrame(opPong, nil, true),
		testServerFrame(opContinuation, []byte("BBB"), false),
		testServerFrame(opContinuation, []byte("CCC"), true),
	}, nil)
	raw, _ := newFrameTestRaw(input)
	socket := &mtProtoSafeFrameSocket{raw: raw, maxMessageLen: 64}

	message, err := socket.Recv()
	if err != nil {
		t.Fatalf("recv fragmented message: %v", err)
	}
	if got, want := string(message), "AAABBBCCC"; got != want {
		t.Fatalf("message=%q want=%q", got, want)
	}
}

func TestMtProtoSafeFrameSocketRejectsOversizedFrameBeforePayloadRead(t *testing.T) {
	var header [10]byte
	header[0] = 0x80 | opBinary
	header[1] = 127
	binary.BigEndian.PutUint64(header[2:], 65)
	raw, conn := newFrameTestRaw(header[:])
	socket := &mtProtoSafeFrameSocket{raw: raw, maxMessageLen: 64}

	_, err := socket.Recv()
	if err == nil || !strings.Contains(err.Error(), "WS frame too large") {
		t.Fatalf("err=%v", err)
	}
	if !conn.closed {
		t.Fatal("oversized frame must close the underlying connection")
	}
}

func TestMtProtoSafeFrameSocketRejectsOversizedReassembledMessage(t *testing.T) {
	input := bytes.Join([][]byte{
		testServerFrame(opBinary, []byte("1234"), false),
		testServerFrame(opContinuation, []byte("5678"), false),
	}, nil)
	raw, conn := newFrameTestRaw(input)
	socket := &mtProtoSafeFrameSocket{raw: raw, maxMessageLen: 7}

	_, err := socket.Recv()
	if err == nil || !strings.Contains(err.Error(), "WS message too large") {
		t.Fatalf("err=%v", err)
	}
	if !conn.closed {
		t.Fatal("oversized fragmented message must close the underlying connection")
	}
}

func TestMtProtoSafeFrameSocketHandlesPingBetweenFragments(t *testing.T) {
	input := bytes.Join([][]byte{
		testServerFrame(opBinary, []byte("left"), false),
		testServerFrame(opPing, []byte("p"), true),
		testServerFrame(opContinuation, []byte("right"), true),
	}, nil)
	raw, conn := newFrameTestRaw(input)
	socket := &mtProtoSafeFrameSocket{raw: raw, maxMessageLen: 64}

	message, err := socket.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if got, want := string(message), "leftright"; got != want {
		t.Fatalf("message=%q want=%q", got, want)
	}
	if conn.writes.Len() == 0 {
		t.Fatal("ping must produce a pong frame")
	}
}

func TestMtProtoSafeFrameSocketPreservesLargeOutboundWriteAsSingleMessage(t *testing.T) {
	raw, conn := newFrameTestRaw(nil)
	socket := &mtProtoSafeFrameSocket{raw: raw}
	payload := bytes.Repeat([]byte{0x5A}, 65536)

	if err := socket.Send(payload); err != nil {
		t.Fatalf("send: %v", err)
	}

	lengths := clientFramePayloadLengths(t, conn.writes.Bytes())
	if len(lengths) != 1 || lengths[0] != len(payload) {
		t.Fatalf("frame lengths=%v want=[%d]", lengths, len(payload))
	}
}

func TestMtProtoSafeFrameSocketCompletesShortWrites(t *testing.T) {
	raw, conn := newFrameTestRaw(nil)
	conn.maxWrite = 7
	socket := &mtProtoSafeFrameSocket{raw: raw}
	payload := bytes.Repeat([]byte{0xA5}, 256)

	if err := socket.Send(payload); err != nil {
		t.Fatalf("send with short writer: %v", err)
	}
	if conn.writeCalls <= 1 {
		t.Fatalf("write calls=%d, want multiple short writes", conn.writeCalls)
	}
	lengths := clientFramePayloadLengths(t, conn.writes.Bytes())
	if len(lengths) != 1 || lengths[0] != len(payload) {
		t.Fatalf("frame lengths=%v want=[%d]", lengths, len(payload))
	}
}

func TestMtProtoWebSocketConnWrapsRawSocketWithSafeReceiver(t *testing.T) {
	raw, _ := newFrameTestRaw(testServerFrame(opBinary, []byte("payload"), true))
	conn, err := mtProtoWebSocketConn(raw, buildTestInitWithSignedDC(t, 2), "worker.example")
	if err != nil {
		t.Fatalf("mtProtoWebSocketConn: %v", err)
	}
	defer conn.Close()

	stream, ok := conn.(*mtProtoWebSocketStream)
	if !ok {
		t.Fatalf("stream type=%T", conn)
	}
	if _, ok := stream.socket.(*mtProtoSafeFrameSocket); !ok {
		t.Fatalf("socket type=%T, want *mtProtoSafeFrameSocket", stream.socket)
	}

	buf := make([]byte, 32)
	n, err := conn.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("read: %v", err)
	}
	if got, want := string(buf[:n]), "payload"; got != want {
		t.Fatalf("payload=%q want=%q", got, want)
	}
}
