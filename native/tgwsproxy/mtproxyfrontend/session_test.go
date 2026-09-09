package mtproxyfrontend

import (
	"bytes"
	"context"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func TestPrepareOutboundRequestExtractsRouteMetadata(t *testing.T) {
	wire, _, _ := buildClientHandshake(t, testSecret, protoPaddedIntermediate, -2)
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	go func() {
		_, _ = client.Write(wire)
	}()

	request, _, err := prepareOutboundRequest(server, testSecret, "", false, false)
	if err != nil {
		t.Fatalf("prepareOutboundRequest: %v", err)
	}
	if request.SignedDC != -2 || request.DCID != 2 || !request.IsMedia {
		t.Fatalf("route metadata signed_dc=%d dc=%d media=%t", request.SignedDC, request.DCID, request.IsMedia)
	}
	if request.Transport != TransportPaddedIntermediate {
		t.Fatalf("transport=%s", request.Transport)
	}
	if len(request.RelayInit) != 64 {
		t.Fatalf("relay init len=%d", len(request.RelayInit))
	}
	if request.ClientToRelay == nil || request.RelayToClient == nil {
		t.Fatal("expected bidirectional transforms")
	}
}

func TestOutboundTransformsRoundTrip(t *testing.T) {
	wire, clientEncryptor, clientDecryptor := buildClientHandshake(
		t,
		testSecret,
		protoIntermediate,
		4,
	)
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	go func() {
		_, _ = client.Write(wire)
	}()

	request, _, err := prepareOutboundRequest(server, testSecret, "", false, false)
	if err != nil {
		t.Fatalf("prepareOutboundRequest: %v", err)
	}
	if request.SignedDC != 4 || request.DCID != 4 || request.IsMedia {
		t.Fatalf("route metadata signed_dc=%d dc=%d media=%t", request.SignedDC, request.DCID, request.IsMedia)
	}

	clientPlain := []byte("client-to-telegram-payload")
	clientWire := xorCopy(clientEncryptor, clientPlain)
	relayWire := request.ClientToRelay(clientWire)

	telegramDecryptor, err := newAESCTR(request.RelayInit[8:40], request.RelayInit[40:56])
	if err != nil {
		t.Fatalf("telegram decryptor: %v", err)
	}
	telegramDecryptor.XORKeyStream(make([]byte, 64), make([]byte, 64))
	if got := xorCopy(telegramDecryptor, relayWire); !bytes.Equal(got, clientPlain) {
		t.Fatalf("client->relay transform mismatch: got=%x want=%x", got, clientPlain)
	}

	telegramPlain := []byte("telegram-to-client-payload")
	reversedRelay := reverseCopy(request.RelayInit[8:56])
	telegramEncryptor, err := newAESCTR(reversedRelay[:32], reversedRelay[32:])
	if err != nil {
		t.Fatalf("telegram encryptor: %v", err)
	}
	telegramWire := xorCopy(telegramEncryptor, telegramPlain)
	clientResponseWire := request.RelayToClient(telegramWire)
	if got := xorCopy(clientDecryptor, clientResponseWire); !bytes.Equal(got, telegramPlain) {
		t.Fatalf("relay->client transform mismatch: got=%x want=%x", got, telegramPlain)
	}
}

func TestPrepareOutboundRequestRejectsUnsupportedTransport(t *testing.T) {
	wire, _, _ := buildClientHandshake(t, testSecret, 0xAAAAAAAA, 2)
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	go func() {
		_, _ = client.Write(wire)
	}()

	_, _, err := prepareOutboundRequest(server, testSecret, "", false, false)
	if err != ErrUnsupportedTransport {
		t.Fatalf("err=%v, want %v", err, ErrUnsupportedTransport)
	}
}

func TestPrepareOutboundRequestAcceptsFakeTLSWrappedHandshake(t *testing.T) {
	wire, _, _ := buildClientHandshake(t, testSecret, protoPaddedIntermediate, 2)
	secret := decodeTestSecret(t, testSecret)
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	go func() {
		_ = writeFull(client, buildFakeTLSClientHello(t, secret, time.Now()))
		readFakeTLSServerHello(t, client)
		_ = writeFull(client, wrapFakeTLSRecords(wire))
	}()

	request, clientConn, err := prepareOutboundRequest(server, testSecret, "www.example.com", false, false)
	if err != nil {
		t.Fatalf("prepareOutboundRequest fake TLS: %v", err)
	}
	if clientConn == server {
		t.Fatal("expected fake TLS client stream wrapper")
	}
	if request.DCID != 2 || request.IsTestDC {
		t.Fatalf("route metadata dc=%d test=%t", request.DCID, request.IsTestDC)
	}
	if request.Transport != TransportPaddedIntermediate {
		t.Fatalf("transport=%s", request.Transport)
	}
}

func TestPrepareOutboundRequestRequestsMaskingPassthroughForInvalidFakeTLS(t *testing.T) {
	wrongSecret := bytes.Repeat([]byte{0x42}, 16)
	clientHello := buildFakeTLSClientHello(t, wrongSecret, time.Now())
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	go func() {
		_ = writeFull(client, clientHello)
	}()

	_, _, err := prepareOutboundRequest(server, testSecret, "www.example.com", true, false)
	var passthrough *FakeTLSPassthroughRequest
	if !errors.As(err, &passthrough) {
		t.Fatalf("err=%v, want FakeTLSPassthroughRequest", err)
	}
	if passthrough.Domain != "www.example.com" {
		t.Fatalf("domain=%s", passthrough.Domain)
	}
	if !bytes.Equal(passthrough.InitialData, clientHello) {
		t.Fatal("passthrough did not preserve original ClientHello")
	}
}

func TestPrepareOutboundRequestMarksHighDcAsTestDc(t *testing.T) {
	wire, _, _ := buildClientHandshake(t, testSecret, protoIntermediate, 10002)
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	go func() {
		_, _ = client.Write(wire)
	}()

	request, _, err := prepareOutboundRequest(server, testSecret, "", false, false)
	if err != nil {
		t.Fatalf("prepareOutboundRequest: %v", err)
	}
	if request.SignedDC != 10002 || request.DCID != 2 || !request.IsTestDC {
		t.Fatalf("route metadata signed_dc=%d dc=%d test=%t", request.SignedDC, request.DCID, request.IsTestDC)
	}

	relayDecryptor, err := newAESCTR(request.RelayInit[8:40], request.RelayInit[40:56])
	if err != nil {
		t.Fatalf("relay decryptor: %v", err)
	}
	relayPlain := xorCopy(relayDecryptor, request.RelayInit)
	if got := int16(binary.LittleEndian.Uint16(relayPlain[60:62])); got != 2 {
		t.Fatalf("relay signed dc=%d, want 2", got)
	}
}

func TestRuntimeWithConnectorReportsRouteReady(t *testing.T) {
	runtime := NewRuntime(nil, fakeOutboundConnector{})
	result := runtime.Start(Config{
		Host:   "127.0.0.1",
		Port:   reservePort(t),
		Secret: testSecret,
	})
	if result.Err != nil {
		t.Fatalf("start failed: %v", result.Err)
	}
	defer runtime.Stop()

	if result.Status != StatusListeningRouteReady {
		t.Fatalf("status=%s", result.Status)
	}
	status := runtime.Status()
	if status.OutboundStatus != "MTPROTO_ROUTE_DIRECT_READY" {
		t.Fatalf("outbound=%s", status.OutboundStatus)
	}
	if status.SelectedBackend != "direct_tcp" {
		t.Fatalf("selected backend=%s", status.SelectedBackend)
	}
	if status.ActualBackend != "" || status.FallbackUsed {
		t.Fatalf("unexpected route truth: %+v", status)
	}
}

func TestRuntimeRoutesTransformedTrafficThroughConnector(t *testing.T) {
	telegramReply := []byte("telegram-reply")
	connector := &integrationOutboundConnector{
		clientPayloadLength: len("client-request"),
		telegramReply:       telegramReply,
		received:            make(chan []byte, 1),
		errors:              make(chan error, 1),
		release:             make(chan struct{}),
	}
	runtime := NewRuntime(nil, connector)
	port := reservePort(t)
	result := runtime.Start(Config{
		Host:   "127.0.0.1",
		Port:   port,
		Secret: testSecret,
	})
	if result.Err != nil {
		t.Fatalf("start failed: %v", result.Err)
	}
	defer runtime.Stop()

	client, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", fmt.Sprint(port)))
	if err != nil {
		t.Fatalf("dial runtime: %v", err)
	}
	defer client.Close()

	handshake, clientEncryptor, clientDecryptor := buildClientHandshake(
		t,
		testSecret,
		protoPaddedIntermediate,
		2,
	)
	if err := writeFull(client, handshake); err != nil {
		t.Fatalf("write handshake: %v", err)
	}
	clientRequest := []byte("client-request")
	if err := writeFull(client, xorCopy(clientEncryptor, clientRequest)); err != nil {
		t.Fatalf("write client request: %v", err)
	}

	replyWire := make([]byte, len(telegramReply))
	if _, err := io.ReadFull(client, replyWire); err != nil {
		t.Fatalf("read reply: %v", err)
	}
	if got := xorCopy(clientDecryptor, replyWire); !bytes.Equal(got, telegramReply) {
		t.Fatalf("reply=%q", got)
	}
	select {
	case got := <-connector.received:
		if !bytes.Equal(got, clientRequest) {
			t.Fatalf("telegram received=%q", got)
		}
	case err := <-connector.errors:
		t.Fatalf("mock telegram: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for mock telegram")
	}

	deadline := time.Now().Add(2 * time.Second)
	for runtime.Status().ActualBackend != "direct_tcp" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	status := runtime.Status()
	if status.ActualBackend != "direct_tcp" || status.FallbackUsed {
		t.Fatalf("route truth=%+v", status)
	}
	close(connector.release)
}

type fakeOutboundConnector struct{}

func (fakeOutboundConnector) Capability() OutboundCapability {
	return OutboundCapability{
		Status:          "MTPROTO_ROUTE_DIRECT_READY",
		SelectedBackend: "direct_tcp",
	}
}

func (fakeOutboundConnector) Connect(
	_ context.Context,
	_ OutboundRequest,
) (net.Conn, OutboundResult) {
	return nil, OutboundResult{
		SelectedBackend: "direct_tcp",
		Reason:          "not_used",
	}
}

type integrationOutboundConnector struct {
	clientPayloadLength int
	telegramReply       []byte
	received            chan []byte
	errors              chan error
	release             chan struct{}
}

func (c *integrationOutboundConnector) Capability() OutboundCapability {
	return OutboundCapability{
		Status:          "MTPROTO_ROUTE_DIRECT_READY",
		SelectedBackend: "direct_tcp",
	}
}

func (c *integrationOutboundConnector) Connect(
	_ context.Context,
	request OutboundRequest,
) (net.Conn, OutboundResult) {
	outbound, telegram := net.Pipe()
	go c.runMockTelegram(telegram, request)
	if err := writeFull(outbound, request.RelayInit); err != nil {
		_ = outbound.Close()
		return nil, OutboundResult{
			SelectedBackend: "direct_tcp",
			Reason:          "relay_init_write_failed",
			Err:             err,
		}
	}
	return outbound, OutboundResult{
		SelectedBackend: "direct_tcp",
		ActualBackend:   "direct_tcp",
		Reason:          "connected",
	}
}

func (c *integrationOutboundConnector) runMockTelegram(
	conn net.Conn,
	request OutboundRequest,
) {
	defer conn.Close()
	relayInit := make([]byte, 64)
	if _, err := io.ReadFull(conn, relayInit); err != nil {
		c.errors <- err
		return
	}

	decryptor, err := newAESCTR(relayInit[8:40], relayInit[40:56])
	if err != nil {
		c.errors <- err
		return
	}
	decryptor.XORKeyStream(make([]byte, 64), make([]byte, 64))
	requestWire := make([]byte, c.clientPayloadLength)
	if _, err := io.ReadFull(conn, requestWire); err != nil {
		c.errors <- err
		return
	}
	c.received <- xorCopy(decryptor, requestWire)

	reversed := reverseCopy(relayInit[8:56])
	encryptor, err := newAESCTR(reversed[:32], reversed[32:])
	if err != nil {
		c.errors <- err
		return
	}
	if err := writeFull(conn, xorCopy(encryptor, c.telegramReply)); err != nil {
		c.errors <- err
		return
	}
	<-c.release
}

func buildClientHandshake(
	t *testing.T,
	secretHex string,
	proto uint32,
	signedDC int16,
) ([]byte, cipher.Stream, cipher.Stream) {
	t.Helper()
	secret, err := hex.DecodeString(secretHex)
	if err != nil {
		t.Fatalf("decode secret: %v", err)
	}
	plain := bytes.Repeat([]byte{0x42}, 64)
	for i := 8; i < 56; i++ {
		plain[i] = byte(i + 1)
	}
	binary.LittleEndian.PutUint32(plain[56:60], proto)
	binary.LittleEndian.PutUint16(plain[60:62], uint16(signedDC))

	keyMaterial := append(append([]byte(nil), plain[8:40]...), secret...)
	key := sha256.Sum256(keyMaterial)
	clientEncryptor, err := newAESCTR(key[:], plain[40:56])
	if err != nil {
		t.Fatalf("client encryptor: %v", err)
	}
	encrypted := xorCopy(clientEncryptor, plain)
	wire := append([]byte(nil), plain...)
	copy(wire[56:64], encrypted[56:64])

	reversed := reverseCopy(wire[8:56])
	clientDecryptor, err := secretStream(reversed[:32], reversed[32:], secret)
	if err != nil {
		t.Fatalf("client decryptor: %v", err)
	}
	return wire, clientEncryptor, clientDecryptor
}

func xorCopy(stream cipher.Stream, value []byte) []byte {
	out := make([]byte, len(value))
	stream.XORKeyStream(out, value)
	return out
}

func decodeTestSecret(t *testing.T, secretHex string) []byte {
	t.Helper()
	secret, err := hex.DecodeString(secretHex)
	if err != nil {
		t.Fatalf("decode secret: %v", err)
	}
	return secret
}

func buildFakeTLSClientHello(t *testing.T, secret []byte, now time.Time) []byte {
	t.Helper()
	body := make([]byte, 1+3+2+fakeTLSClientRandomLen+1+fakeTLSSessionIDLen)
	body[0] = 0x01
	body[3] = byte(len(body) - 4)
	body[4] = 0x03
	body[5] = 0x03
	body[1] = byte((len(body) - 4) >> 16)
	body[2] = byte((len(body) - 4) >> 8)
	body[1+3+2+fakeTLSClientRandomLen] = 0x20
	for i := 0; i < fakeTLSSessionIDLen; i++ {
		body[1+3+2+fakeTLSClientRandomLen+1+i] = byte(i + 1)
	}

	record := make([]byte, 5+len(body))
	record[0] = tlsRecordHandshake
	record[1] = 0x03
	record[2] = 0x01
	binary.BigEndian.PutUint16(record[3:5], uint16(len(body)))
	copy(record[5:], body)

	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(record)
	expected := mac.Sum(nil)
	copy(record[fakeTLSClientRandomOffset:fakeTLSClientRandomOffset+28], expected[:28])
	ts := uint32(now.Unix())
	var tsBytes [4]byte
	binary.LittleEndian.PutUint32(tsBytes[:], ts)
	for i := 0; i < 4; i++ {
		record[fakeTLSClientRandomOffset+28+i] = tsBytes[i] ^ expected[28+i]
	}
	return record
}

func readFakeTLSServerHello(t *testing.T, conn net.Conn) {
	t.Helper()
	readTLSRecord(t, conn, tlsRecordHandshake)
	readTLSRecord(t, conn, tlsRecordCCS)
	readTLSRecord(t, conn, tlsRecordAppData)
}

func readTLSRecord(t *testing.T, conn net.Conn, wantType byte) {
	t.Helper()
	header := make([]byte, 5)
	if _, err := io.ReadFull(conn, header); err != nil {
		t.Fatalf("read TLS header: %v", err)
	}
	if header[0] != wantType {
		t.Fatalf("TLS record type=%x want=%x", header[0], wantType)
	}
	recordLen := int(binary.BigEndian.Uint16(header[3:5]))
	if recordLen > 0 {
		if _, err := io.CopyN(io.Discard, conn, int64(recordLen)); err != nil {
			t.Fatalf("read TLS record body: %v", err)
		}
	}
}
