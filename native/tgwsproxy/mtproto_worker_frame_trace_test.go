package main

import (
	"bytes"
	"log"
	"strings"
	"testing"

	"tg-ws-proxy/mtproxyfrontend"
)

func TestMtProtoWorkerFrameTraceReportsSegmentedWrites(t *testing.T) {
	previousLogInfo := logInfo
	var logs bytes.Buffer
	logInfo = log.New(&logs, "", 0)
	t.Cleanup(func() { logInfo = previousLogInfo })

	raw, _ := newFrameTestRaw(nil)
	safeSocket := &mtProtoSafeFrameSocket{raw: raw}
	stream := &mtProtoWebSocketStream{socket: safeSocket}
	request := mtproxyfrontend.OutboundRequest{DCID: 2, SignedDC: 2, IsMedia: false}
	installMtProtoWorkerFrameTrace(stream, request, "frame-session", "149.154.167.51")

	payload := bytes.Repeat([]byte{0x5A}, 2*mtProtoMaxOutboundWebSocketPayloadLen+123)
	if err := safeSocket.Send(payload); err != nil {
		t.Fatalf("send: %v", err)
	}

	text := logs.String()
	if got := strings.Count(text, "MTProto Worker WS frame trace"); got != 3 {
		t.Fatalf("frame traces=%d want=3\n%s", got, text)
	}
	for _, want := range []string{
		"session_id=frame-session",
		"signed_dc=2 dc=2 media=false",
		"worker_dst=149.154.167.51",
		"frame_index=1 opcode=2 frame_payload_bytes=16384 frame_bytes=16392",
		"frame_index=2 opcode=2 frame_payload_bytes=16384 frame_bytes=16392",
		"frame_index=3 opcode=2 frame_payload_bytes=123 frame_bytes=129",
		"completed=true short_write=false",
		"result=ok error=none",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in trace:\n%s", want, text)
		}
	}
}

func TestMtProtoWorkerFrameTraceExposesShortWrites(t *testing.T) {
	previousLogInfo := logInfo
	var logs bytes.Buffer
	logInfo = log.New(&logs, "", 0)
	t.Cleanup(func() { logInfo = previousLogInfo })

	raw, conn := newFrameTestRaw(nil)
	conn.maxWrite = 7
	safeSocket := &mtProtoSafeFrameSocket{raw: raw}
	stream := &mtProtoWebSocketStream{socket: safeSocket}
	request := mtproxyfrontend.OutboundRequest{DCID: 2, SignedDC: -2, IsMedia: true}
	installMtProtoWorkerFrameTrace(stream, request, "short-write-session", "149.154.167.51")

	if err := safeSocket.Send(bytes.Repeat([]byte{0xA5}, 256)); err != nil {
		t.Fatalf("send: %v", err)
	}

	text := logs.String()
	for _, want := range []string{
		"session_id=short-write-session",
		"signed_dc=-2 dc=2 media=true",
		"frame_index=1 opcode=2 frame_payload_bytes=256 frame_bytes=264",
		"short_write=true",
		"completed=true",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in trace:\n%s", want, text)
		}
	}
	if conn.writeCalls <= 1 {
		t.Fatalf("write calls=%d want multiple", conn.writeCalls)
	}
}

func TestMtProtoWorkerPayloadTraceInstallsFrameTrace(t *testing.T) {
	raw, _ := newFrameTestRaw(nil)
	safeSocket := &mtProtoSafeFrameSocket{raw: raw}
	base := &mtProtoWebSocketStream{socket: safeSocket}
	request := mtproxyfrontend.OutboundRequest{DCID: 2, SignedDC: -2, IsMedia: true}

	wrapped := wrapMtProtoWorkerPayloadTrace(base, request, "payload-session", "149.154.167.51")
	if wrapped == nil {
		t.Fatal("payload trace wrapper is nil")
	}

	frameTrace, ok := safeSocket.raw.conn.(*mtProtoWorkerFrameTraceConn)
	if !ok {
		t.Fatalf("raw conn type=%T want *mtProtoWorkerFrameTraceConn", safeSocket.raw.conn)
	}
	if frameTrace.sessionID != "payload-session" {
		t.Fatalf("session id=%q", frameTrace.sessionID)
	}
	if frameTrace.signedDC != -2 || frameTrace.dc != 2 || !frameTrace.media {
		t.Fatalf("metadata signed_dc=%d dc=%d media=%t", frameTrace.signedDC, frameTrace.dc, frameTrace.media)
	}
	if frameTrace.workerDst != "149.154.167.51" {
		t.Fatalf("worker dst=%q", frameTrace.workerDst)
	}
}
