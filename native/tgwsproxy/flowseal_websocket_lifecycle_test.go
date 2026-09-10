package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type observedFlowsealWriteConn struct {
	net.Conn
	started chan struct{}
	once    sync.Once
}

func (c *observedFlowsealWriteConn) Write(p []byte) (int, error) {
	c.once.Do(func() { close(c.started) })
	return c.Conn.Write(p)
}

func blockedFlowsealSocket(t *testing.T) (*flowsealRawWebSocket, net.Conn, <-chan error) {
	t.Helper()
	client, peer := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = peer.Close() })
	conn := &observedFlowsealWriteConn{Conn: client, started: make(chan struct{})}
	ws := &flowsealRawWebSocket{conn: conn, reader: bufio.NewReader(conn)}
	done := make(chan error, 1)
	go func() { done <- ws.Send(make([]byte, 65536)) }()
	select {
	case <-conn.started:
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not start")
	}
	return ws, peer, done
}

func TestFlowsealCloseInterruptsBlockedWrite(t *testing.T) {
	ws, _, writeDone := blockedFlowsealSocket(t)
	closed := make(chan struct{})
	go func() { ws.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close waited for the blocked writer")
	}
	select {
	case err := <-writeDone:
		if err == nil {
			t.Fatal("blocked write unexpectedly succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not interrupt Write")
	}
}

func TestFlowsealPeerCloseInterruptsBlockedWrite(t *testing.T) {
	ws, peer, writeDone := blockedFlowsealSocket(t)
	recvDone := make(chan error, 1)
	go func() { _, err := ws.Recv(); recvDone <- err }()
	_ = peer.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := peer.Write([]byte{0x88, 0}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-recvDone:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("Recv: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("peer close waited for the data writer")
	}
	select {
	case <-writeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("peer close did not interrupt Write")
	}
}

func TestWorkerStreamDeadlineInterruptsBlockedWrite(t *testing.T) {
	ws, _, done := blockedFlowsealSocket(t)
	stream := &mtProtoWebSocketStream{socket: ws}
	if err := stream.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		var timeout net.Error
		if !errors.As(err, &timeout) || !timeout.Timeout() {
			t.Fatalf("expected write timeout, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream write deadline was ignored")
	}
}

func TestWorkerStreamReadDeadlineIsForwarded(t *testing.T) {
	client, peer := net.Pipe()
	defer client.Close()
	defer peer.Close()
	ws := &flowsealRawWebSocket{conn: client, reader: bufio.NewReader(client)}
	stream := &mtProtoWebSocketStream{socket: ws}
	if err := stream.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	_, err := stream.Read(make([]byte, 1))
	var timeout net.Error
	if !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Fatalf("expected read timeout, got %v", err)
	}
}

// Uses real TCP/TLS and serialized masked frames. This is not a Cloudflare or
// Telegram acceptance test; it checks bulk delivery beyond the old stall point.
func TestFlowsealTLSBulkRoundTrip(t *testing.T) {
	const chunks = 32
	payload := bytes.Repeat([]byte{0x39}, 65536)
	reply := bytes.Repeat(payload, chunks)
	serverDone := make(chan error, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
		_, err = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
		if err == nil {
			err = rw.Flush()
		}
		if err != nil {
			serverDone <- err
			return
		}
		ws := &flowsealRawWebSocket{conn: conn, reader: rw.Reader}
		for i := 0; i < chunks; i++ {
			got, err := ws.Recv()
			if err != nil {
				serverDone <- err
				return
			}
			if !bytes.Equal(got, payload) {
				serverDone <- fmt.Errorf("chunk %d differs", i)
				return
			}
		}
		serverDone <- writeFlowsealFull(conn, buildFlowsealSingleFrame(opBinary, reply, false, true))
	}))
	defer server.Close()
	conn, err := tls.Dial("tcp", strings.TrimPrefix(server.URL, "https://"), &tls.Config{
		InsecureSkipVerify: true, // Local httptest certificate only.
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	if err := writeFlowsealFull(conn, []byte(buildFlowsealUpgradeRequest("/apiws", "localhost", "test-key"))); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade status=%d", response.StatusCode)
	}
	ws := &flowsealRawWebSocket{conn: conn, reader: reader}
	if err := ws.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < chunks; i++ {
		if err := ws.Send(payload); err != nil {
			t.Fatalf("chunk %d: %v", i, err)
		}
	}
	got, err := ws.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, reply) {
		t.Fatalf("bulk reply differs: got %d bytes, want %d", len(got), len(reply))
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}
