package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"
)

type flowsealRecordCaptureConn struct {
	net.Conn
	wire bytes.Buffer
}

func (c *flowsealRecordCaptureConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.wire.Write(p[:n])
	return n, err
}

func TestFlowsealFixedTLSRecordsPreserveWebSocketMessage(t *testing.T) {
	certServer := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer certServer.Close()
	for _, adaptive := range []bool{true, false} {
		t.Run(fmt.Sprintf("adaptive=%t", adaptive), func(t *testing.T) {
			raw, peer := net.Pipe()
			defer raw.Close()
			defer peer.Close()
			_ = raw.SetDeadline(time.Now().Add(3 * time.Second))
			_ = peer.SetDeadline(time.Now().Add(3 * time.Second))
			capture := &flowsealRecordCaptureConn{Conn: raw}
			config := flowsealWorkerTLSConfig("localhost")
			config.MinVersion = tls.VersionTLS13
			config.MaxVersion = tls.VersionTLS13
			if adaptive {
				config.DynamicRecordSizingDisabled = false // The previous device baseline.
			}
			client := tls.Client(capture, config)
			server := tls.Server(peer, &tls.Config{Certificates: certServer.TLS.Certificates})
			payload := bytes.Repeat([]byte{0x39}, 65536)
			serverDone := make(chan error, 1)
			go func() {
				ws := &flowsealRawWebSocket{conn: server, reader: bufio.NewReader(server)}
				for _, want := range [][]byte{make([]byte, 64), payload} {
					_, got, fin, err := ws.readFrame()
					if err != nil {
						serverDone <- err
						return
					}
					if !fin || !bytes.Equal(got, want) {
						serverDone <- fmt.Errorf("TLS profile changed WebSocket payload")
						return
					}
				}
				serverDone <- writeFlowsealFull(server, buildFlowsealSingleFrame(opBinary, payload, false, true))
			}()
			if err := client.Handshake(); err != nil {
				t.Fatal(err)
			}
			ws := &flowsealRawWebSocket{conn: client, reader: bufio.NewReader(client)}
			if err := ws.Send(make([]byte, 64)); err != nil {
				t.Fatal(err)
			}
			capture.wire.Reset()
			if err := ws.Send(payload); err != nil {
				t.Fatal(err)
			}
			wire := capture.wire.Bytes()
			var lengths []int
			for len(wire) > 0 {
				if len(wire) < 5 || wire[0] != 23 {
					t.Fatal("expected a complete TLS application record")
				}
				n := int(binary.BigEndian.Uint16(wire[3:5]))
				if len(wire) < n+5 {
					t.Fatal("truncated TLS record")
				}
				lengths = append(lengths, n)
				wire = wire[n+5:]
			}
			// Four 16-KiB records plus the remaining 14-byte WebSocket header;
			// TLS 1.3 adds an inner content type and a 16-byte authentication tag.
			fixed := []int{16401, 16401, 16401, 16401, 31}
			if !adaptive && !slices.Equal(lengths, fixed) {
				t.Fatalf("fixed profile produced TLS records %v, want %v", lengths, fixed)
			}
			if adaptive && slices.Equal(lengths, fixed) {
				t.Fatal("baseline no longer exercises adaptive TLS record sizing")
			}
			t.Logf("TLS ciphertext record lengths: %v", lengths)
			got, err := ws.Recv()
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("downstream differs: bytes=%d err=%v", len(got), err)
			}
			if err := <-serverDone; err != nil {
				t.Fatal(err)
			}
		})
	}
}
