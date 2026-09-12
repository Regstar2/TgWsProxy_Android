package mtproxyfrontend

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

const testSecret = "0123456789abcdef0123456789abcdef"

func TestIsRawSecret(t *testing.T) {
	if !IsRawSecret(testSecret) {
		t.Fatalf("expected valid raw secret")
	}
	if IsRawSecret("bad") {
		t.Fatalf("expected invalid raw secret")
	}
}

func TestSecretFingerprintDoesNotContainSecret(t *testing.T) {
	fingerprint := SecretFingerprint(testSecret)
	if fingerprint == "" {
		t.Fatalf("expected non-empty fingerprint")
	}
	if strings.Contains(testSecret, fingerprint) || strings.Contains(fingerprint, testSecret) {
		t.Fatalf("fingerprint should not expose raw secret")
	}
}

func TestRuntimeStartStopValidSecret(t *testing.T) {
	port := reservePort(t)
	runtime := NewRuntime(nil)

	result := runtime.Start(Config{
		Host:   "127.0.0.1",
		Port:   port,
		Secret: testSecret,
	})
	if result.Err != nil {
		t.Fatalf("start failed: %v", result.Err)
	}
	if result.Status != StatusListeningLocalOnly {
		t.Fatalf("status = %s, want %s", result.Status, StatusListeningLocalOnly)
	}
	status := runtime.Status()
	if status.OutboundStatus != OutboundUnsupported {
		t.Fatalf("outbound = %s, want %s", status.OutboundStatus, OutboundUnsupported)
	}
	if status.SecretFingerprint == "" || status.SecretFingerprint == testSecret {
		t.Fatalf("secret fingerprint should be masked")
	}

	if err := runtime.Stop(); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
	if runtime.Status().Status != StatusStopped {
		t.Fatalf("status = %s, want %s", runtime.Status().Status, StatusStopped)
	}
}

func TestRuntimeStartInvalidSecret(t *testing.T) {
	runtime := NewRuntime(nil)

	result := runtime.Start(Config{
		Host:   "127.0.0.1",
		Port:   1443,
		Secret: "bad",
	})
	if result.Err == nil {
		t.Fatalf("expected invalid secret error")
	}
	if result.Status != StatusFailedInvalidSecret {
		t.Fatalf("status = %s, want %s", result.Status, StatusFailedInvalidSecret)
	}
}

func TestRuntimeStartPortBusy(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve busy listener: %v", err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port

	runtime := NewRuntime(nil)
	result := runtime.Start(Config{
		Host:   "127.0.0.1",
		Port:   port,
		Secret: testSecret,
	})
	if result.Err == nil {
		defer runtime.Stop()
		t.Fatalf("expected port busy error")
	}
	if result.Status != StatusFailedPortInUse {
		t.Fatalf("status = %s, want %s", result.Status, StatusFailedPortInUse)
	}
}

func reservePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}


func TestBridgeTransformedStreamsBidirectionalLargeAndSequentialPayload(t *testing.T) {
	clientPeer, clientBridge := net.Pipe()
	outboundBridge, upstreamPeer := net.Pipe()
	defer clientPeer.Close()
	defer upstreamPeer.Close()

	resultCh := make(chan bridgeResult, 1)
	go func() {
		resultCh <- bridgeTransformedStreams(
			context.Background(),
			clientBridge,
			outboundBridge,
			xorTransformForBridgeTest(0x5a),
			xorTransformForBridgeTest(0xa5),
		)
	}()

	upPayloads := [][]byte{
		[]byte("first-packet"),
		[]byte("second-packet"),
		bytes.Repeat([]byte{0x31}, 96*1024),
	}
	var wantUpBytes int64
	for _, payload := range upPayloads {
		wantUpBytes += int64(len(payload))
		expected := xorBytesForBridgeTest(payload, 0x5a)
		writeAndReadFullForBridgeTest(t, clientPeer, upstreamPeer, payload, expected)
	}

	downPayloads := [][]byte{
		[]byte("reply-one"),
		[]byte("reply-two"),
		bytes.Repeat([]byte{0x72}, 80*1024),
	}
	var wantDownBytes int64
	for _, payload := range downPayloads {
		wantDownBytes += int64(len(payload))
		expected := xorBytesForBridgeTest(payload, 0xa5)
		writeAndReadFullForBridgeTest(t, upstreamPeer, clientPeer, payload, expected)
	}

	if err := clientPeer.Close(); err != nil {
		t.Fatalf("close client: %v", err)
	}

	select {
	case result := <-resultCh:
		if result.FirstTerminator != "client_to_upstream" {
			t.Fatalf("first terminator=%s", result.FirstTerminator)
		}
		if result.CloseReason != "eof" {
			t.Fatalf("close reason=%s err=%v", result.CloseReason, result.Err)
		}
		if result.UpBytes != wantUpBytes {
			t.Fatalf("up bytes=%d want=%d", result.UpBytes, wantUpBytes)
		}
		if result.DownBytes != wantDownBytes {
			t.Fatalf("down bytes=%d want=%d", result.DownBytes, wantDownBytes)
		}
		if result.UpChunks == 0 || result.DownChunks == 0 {
			t.Fatalf("chunks up=%d down=%d", result.UpChunks, result.DownChunks)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("bridge did not terminate")
	}
}

func TestBridgeTransformedStreamsReportsUpstreamClose(t *testing.T) {
	clientPeer, clientBridge := net.Pipe()
	outboundBridge, upstreamPeer := net.Pipe()
	defer clientPeer.Close()

	resultCh := make(chan bridgeResult, 1)
	go func() {
		resultCh <- bridgeTransformedStreams(
			context.Background(),
			clientBridge,
			outboundBridge,
			nil,
			nil,
		)
	}()

	payload := []byte("request")
	writeAndReadFullForBridgeTest(t, clientPeer, upstreamPeer, payload, payload)
	if err := upstreamPeer.Close(); err != nil {
		t.Fatalf("close upstream: %v", err)
	}

	select {
	case result := <-resultCh:
		if result.FirstTerminator != "upstream_to_client" {
			t.Fatalf("first terminator=%s", result.FirstTerminator)
		}
		if result.CloseReason != "eof" {
			t.Fatalf("close reason=%s err=%v", result.CloseReason, result.Err)
		}
		if result.UpBytes != int64(len(payload)) {
			t.Fatalf("up bytes=%d want=%d", result.UpBytes, len(payload))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("bridge did not terminate")
	}
}

func xorTransformForBridgeTest(mask byte) StreamTransform {
	return func(in []byte) []byte {
		return xorBytesForBridgeTest(in, mask)
	}
}

func xorBytesForBridgeTest(in []byte, mask byte) []byte {
	out := make([]byte, len(in))
	for i, b := range in {
		out[i] = b ^ mask
	}
	return out
}

func writeAndReadFullForBridgeTest(t *testing.T, writer net.Conn, reader net.Conn, input, expected []byte) {
	t.Helper()
	errCh := make(chan error, 1)
	go func() {
		_, err := writer.Write(input)
		errCh <- err
	}()

	got := make([]byte, len(expected))
	if _, err := io.ReadFull(reader, got); err != nil {
		t.Fatalf("read full: %v", err)
	}
	if !bytes.Equal(got, expected) {
		t.Fatalf("payload mismatch len=%d", len(expected))
	}
	if err := <-errCh; err != nil {
		t.Fatalf("write: %v", err)
	}
}
