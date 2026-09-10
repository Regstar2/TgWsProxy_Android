package main

import (
	"bytes"
	"log"
	"strings"
	"testing"

	"tg-ws-proxy/mtproxyfrontend"
)

func TestMtProtoWorkerTransportWritePreservesSingleLargeWebSocketFrame(t *testing.T) {
	previousLogInfo := logInfo
	var logs bytes.Buffer
	logInfo = log.New(&logs, "", 0)
	t.Cleanup(func() { logInfo = previousLogInfo })

	raw, underlying := newFrameTestRaw(nil)
	safeSocket := &mtProtoSafeFrameSocket{raw: raw}
	stream := &mtProtoWebSocketStream{socket: safeSocket}
	request := mtproxyfrontend.OutboundRequest{DCID: 2, SignedDC: -2, IsMedia: true}

	installMtProtoWorkerTransportWrite(stream, request, "transport-session", "149.154.167.51")
	installMtProtoWorkerFrameTrace(stream, request, "transport-session", "149.154.167.51")

	frameTrace, ok := safeSocket.raw.conn.(*mtProtoWorkerFrameTraceConn)
	if !ok {
		t.Fatalf("raw conn type=%T want *mtProtoWorkerFrameTraceConn", safeSocket.raw.conn)
	}
	if _, ok := frameTrace.Conn.(*mtProtoWorkerTransportWriteConn); !ok {
		t.Fatalf("frame trace inner conn type=%T want *mtProtoWorkerTransportWriteConn", frameTrace.Conn)
	}

	payload := bytes.Repeat([]byte{0x5A}, 65536)
	if err := safeSocket.Send(payload); err != nil {
		t.Fatalf("send: %v", err)
	}

	lengths := clientFramePayloadLengths(t, underlying.writes.Bytes())
	if len(lengths) != 1 || lengths[0] != len(payload) {
		t.Fatalf("frame lengths=%v want=[%d]", lengths, len(payload))
	}
	if underlying.writeCalls < 5 {
		t.Fatalf("underlying TLS writes=%d want at least 5 for 65550 serialized bytes", underlying.writeCalls)
	}

	text := logs.String()
	for _, want := range []string{
		"MTProto Worker WS frame trace",
		"frame_payload_bytes=65536 frame_bytes=65550",
		"MTProto Worker transport write trace",
		"session_id=transport-session",
		"requested_bytes=65550 written_bytes=65550",
		"transport_writes=5 max_transport_chunk=16384",
		"result=ok error=none",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in trace:\n%s", want, text)
		}
	}
}

func TestMtProtoWorkerTransportWriteCompletesShortUnderlyingWrites(t *testing.T) {
	previousLogInfo := logInfo
	var logs bytes.Buffer
	logInfo = log.New(&logs, "", 0)
	t.Cleanup(func() { logInfo = previousLogInfo })

	raw, underlying := newFrameTestRaw(nil)
	underlying.maxWrite = 1024
	safeSocket := &mtProtoSafeFrameSocket{raw: raw}
	stream := &mtProtoWebSocketStream{socket: safeSocket}
	request := mtproxyfrontend.OutboundRequest{DCID: 2, SignedDC: 2, IsMedia: false}

	installMtProtoWorkerTransportWrite(stream, request, "short-transport", "149.154.167.51")

	payload := bytes.Repeat([]byte{0xA5}, 65536)
	if err := safeSocket.Send(payload); err != nil {
		t.Fatalf("send: %v", err)
	}

	lengths := clientFramePayloadLengths(t, underlying.writes.Bytes())
	if len(lengths) != 1 || lengths[0] != len(payload) {
		t.Fatalf("frame lengths=%v want=[%d]", lengths, len(payload))
	}
	if underlying.writeCalls <= 5 {
		t.Fatalf("underlying writes=%d want more than nominal 5 because of short writes", underlying.writeCalls)
	}
	if !strings.Contains(logs.String(), "result=ok error=none") {
		t.Fatalf("transport trace did not report successful completion:\n%s", logs.String())
	}
}
