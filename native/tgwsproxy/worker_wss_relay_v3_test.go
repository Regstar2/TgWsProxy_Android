package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestFlowsealWorkerWSSRelayPathPreservesWorkerQuery(t *testing.T) {
	got := flowsealWorkerWSSRelayPath("/apiws?dc=2&dst=149.154.167.51&media=1&sid=abc123")
	if !strings.HasPrefix(got, "/apiws-ws?") {
		t.Fatalf("path=%s", got)
	}
	for _, want := range []string{"dc=2", "dst=149.154.167.51", "media=1", "sid=abc123"} {
		if !strings.Contains(got, want) {
			t.Fatalf("path=%s missing %s", got, want)
		}
	}
}

func TestFlowsealWorkerWSSRelayPathDoesNotRewriteOtherEndpoints(t *testing.T) {
	for _, input := range []string{"/apiws_test?dc=2", "/other?dc=2", "not a uri"} {
		if got := flowsealWorkerWSSRelayPath(input); got != input {
			t.Fatalf("input=%q got=%q", input, got)
		}
	}
}

func TestWorkerWSSRelayV3FrameSocketPacketizesSplitWrites(t *testing.T) {
	relayInit := buildTestInitWithSignedDC(t, 2)
	inner := &fakeMtProtoFrameSocket{}
	socket := newWorkerWSSRelayV3FrameSocket(inner)

	if err := socket.Send(relayInit); err != nil {
		t.Fatalf("relay init: %v", err)
	}
	if len(inner.sent) != 1 || !bytes.Equal(inner.sent[0], relayInit) {
		t.Fatalf("relay init was not forwarded intact: sent=%x", inner.sent)
	}

	plainPacket := []byte{1, 9, 8, 7, 6}
	cipherPacket := encryptRelayPayloadForFramingTest(t, relayInit, plainPacket)
	cut := 2
	if err := socket.Send(cipherPacket[:cut]); err != nil {
		t.Fatalf("first partial send: %v", err)
	}
	if len(inner.sent) != 1 || len(inner.batches) != 0 {
		t.Fatalf("partial packet escaped before completion: sent=%d batches=%d", len(inner.sent), len(inner.batches))
	}

	if err := socket.Send(cipherPacket[cut:]); err != nil {
		t.Fatalf("second partial send: %v", err)
	}
	if len(inner.batches) != 1 || len(inner.batches[0]) != 1 {
		t.Fatalf("batches=%x", inner.batches)
	}
	if !bytes.Equal(inner.batches[0][0], cipherPacket) {
		t.Fatalf("packet changed: got=%x want=%x", inner.batches[0][0], cipherPacket)
	}
}

func TestWorkerWSSRelayV3FrameSocketRejectsInvalidRelayInit(t *testing.T) {
	inner := &fakeMtProtoFrameSocket{}
	socket := newWorkerWSSRelayV3FrameSocket(inner)
	if err := socket.Send([]byte{1, 2, 3}); err == nil {
		t.Fatal("expected invalid relay_init to fail before any Worker message is sent")
	}
	if len(inner.sent) != 0 || len(inner.batches) != 0 {
		t.Fatalf("invalid relay_init leaked to Worker: sent=%d batches=%d", len(inner.sent), len(inner.batches))
	}
}
