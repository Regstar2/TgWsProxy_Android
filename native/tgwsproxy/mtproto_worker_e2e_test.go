package main

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"

	"tg-ws-proxy/mtproxyfrontend"
	"tg-ws-proxy/tgwsroute"
)

// workerRelayHarnessSocket models the Cloudflare Worker boundary without
// external networking: Send is WS -> Worker -> Telegram TCP and Recv is
// Telegram TCP -> Worker -> WS.
type workerRelayHarnessSocket struct {
	mu       sync.Mutex
	sent     [][]byte
	recv     chan []byte
	closeOnce sync.Once
	closed   chan struct{}
}

func newWorkerRelayHarnessSocket() *workerRelayHarnessSocket {
	return &workerRelayHarnessSocket{
		recv:   make(chan []byte, 16),
		closed: make(chan struct{}),
	}
}

func (s *workerRelayHarnessSocket) Send(data []byte) error {
	s.mu.Lock()
	s.sent = append(s.sent, append([]byte(nil), data...))
	s.mu.Unlock()
	return nil
}

func (s *workerRelayHarnessSocket) SendBatch(parts [][]byte) error {
	for _, part := range parts {
		if err := s.Send(part); err != nil {
			return err
		}
	}
	return nil
}

func (s *workerRelayHarnessSocket) Recv() ([]byte, error) {
	select {
	case data := <-s.recv:
		return append([]byte(nil), data...), nil
	case <-s.closed:
		return nil, io.EOF
	}
}

func (s *workerRelayHarnessSocket) Close() {
	s.closeOnce.Do(func() { close(s.closed) })
}

func (s *workerRelayHarnessSocket) sentFrames() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.sent))
	for i, frame := range s.sent {
		out[i] = append([]byte(nil), frame...)
	}
	return out
}

func TestMtProtoWorkerE2EHarnessNormalBidirectionalLargeAndSequential(t *testing.T) {
	withRuntimeSettings(t, func(settings runtimeSettings) runtimeSettings {
		settings.Mode = modeWorkerOnly
		settings.Worker.Enabled = true
		settings.Worker.Domain = "example.workers.dev"
		settings.Worker.Failover = workerFailoverSettings{}
		settings.Worker.DestinationMode = tgwsroute.WorkerDestinationPreserveOriginalDst
		settings.MtProtoWorkerPreconnect = false
		return settings
	})

	socket := newWorkerRelayHarnessSocket()
	var dialPath string
	dials := 0
	connector := &mtProtoWorkerConnector{
		dial: func(_, path, _ string) (mtProtoFrameSocket, error) {
			dials++
			dialPath = path
			return socket, nil
		},
	}

	relayInit := buildTestInitWithSignedDC(t, 2)
	conn, result := connector.Connect(context.Background(), mtproxyfrontend.OutboundRequest{
		DCID:      2,
		IsMedia:   false,
		Transport: mtproxyfrontend.TransportPaddedIntermediate,
		RelayInit: relayInit,
	})
	if result.Err != nil {
		t.Fatalf("connect: %v", result.Err)
	}
	defer conn.Close()

	if dials != 1 {
		t.Fatalf("fresh dials=%d want=1", dials)
	}
	if result.ActualBackend != mtProtoWorkerBackend || result.FallbackUsed {
		t.Fatalf("route truth=%+v", result)
	}
	if !containsAll(dialPath, "/apiws?", "dc=2", "dst=149.154.167.51", "media=0", "sid=") {
		t.Fatalf("path=%s", dialPath)
	}

	frames := socket.sentFrames()
	if len(frames) != 1 {
		t.Fatalf("initial frames=%d want relay_init only", len(frames))
	}
	dc, media, ok := dcFromInit(frames[0])
	if !ok || dc != 2 || media {
		t.Fatalf("relay_init dc=%d media=%t ok=%t", dc, media, ok)
	}

	uploads := [][]byte{
		[]byte("packet-one"),
		[]byte("packet-two"),
		[]byte{0x01, 0x02},
		[]byte{0x03, 0x04, 0x05},
		bytes.Repeat([]byte{0x51}, 96*1024),
	}
	for _, upload := range uploads {
		n, err := conn.Write(upload)
		if err != nil {
			t.Fatalf("upload write: %v", err)
		}
		if n != len(upload) {
			t.Fatalf("upload n=%d want=%d", n, len(upload))
		}
	}

	frames = socket.sentFrames()
	if len(frames) != 1+len(uploads) {
		t.Fatalf("frames=%d want=%d", len(frames), 1+len(uploads))
	}
	for i, upload := range uploads {
		if !bytes.Equal(frames[i+1], upload) {
			t.Fatalf("upload frame[%d] changed len=%d", i, len(upload))
		}
	}

	downloads := [][]byte{
		[]byte("reply-one"),
		[]byte("reply-two"),
		bytes.Repeat([]byte{0x63}, 80*1024),
	}
	for _, download := range downloads {
		socket.recv <- download
		got := make([]byte, len(download))
		if _, err := io.ReadFull(conn, got); err != nil {
			t.Fatalf("download read: %v", err)
		}
		if !bytes.Equal(got, download) {
			t.Fatalf("download changed len=%d", len(download))
		}
	}
}

func TestMtProtoWorkerE2EHarnessMediaDestinationOverrideUploadDownload(t *testing.T) {
	withRuntimeSettings(t, func(settings runtimeSettings) runtimeSettings {
		settings.Mode = modeWorkerOnly
		settings.Worker.Enabled = true
		settings.Worker.Domain = "example.workers.dev"
		settings.Worker.Failover = workerFailoverSettings{}
		settings.Worker.DestinationMode = tgwsroute.WorkerDestinationExperimentalForceMediaDC4
		settings.Worker.MediaFix = flowsealMediaFixConfig{
			Enabled: true,
			DC:      4,
			IP:      "149.154.167.220",
		}
		settings.MtProtoWorkerPreconnect = false
		return settings
	})

	socket := newWorkerRelayHarnessSocket()
	var dialPath string
	connector := &mtProtoWorkerConnector{
		dial: func(_, path, _ string) (mtProtoFrameSocket, error) {
			dialPath = path
			return socket, nil
		},
	}

	relayInit := buildTestInitWithSignedDC(t, -2)
	conn, result := connector.Connect(context.Background(), mtproxyfrontend.OutboundRequest{
		DCID:      2,
		IsMedia:   true,
		Transport: mtproxyfrontend.TransportPaddedIntermediate,
		RelayInit: relayInit,
	})
	if result.Err != nil {
		t.Fatalf("connect: %v", result.Err)
	}
	defer conn.Close()

	if !containsAll(dialPath, "/apiws?", "dc=2", "dst=149.154.167.220", "media=1", "sid=") {
		t.Fatalf("path=%s", dialPath)
	}
	frames := socket.sentFrames()
	if len(frames) != 1 {
		t.Fatalf("frames=%d want=1", len(frames))
	}
	if !bytes.Equal(frames[0], relayInit) {
		t.Fatal("media destination override changed the original relay_init")
	}
	dc, media, ok := dcFromInit(frames[0])
	if !ok || dc != 2 || !media {
		t.Fatalf("relay_init dc=%d media=%t ok=%t", dc, media, ok)
	}

	upload := bytes.Repeat([]byte{0x7a}, 72*1024)
	if n, err := conn.Write(upload); err != nil || n != len(upload) {
		t.Fatalf("media upload n=%d err=%v", n, err)
	}
	frames = socket.sentFrames()
	if len(frames) != 2 || !bytes.Equal(frames[1], upload) {
		t.Fatal("media upload did not cross Worker harness unchanged after relay_init")
	}

	download := bytes.Repeat([]byte{0x29}, 73*1024)
	socket.recv <- download
	got := make([]byte, len(download))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("media download: %v", err)
	}
	if !bytes.Equal(got, download) {
		t.Fatal("media download changed across Worker harness")
	}
}

var _ mtProtoFrameSocket = (*workerRelayHarnessSocket)(nil)
