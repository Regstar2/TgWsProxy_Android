package mtproxyfrontend

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

type Transport string

const (
	TransportAbridged           Transport = "ABRIDGED"
	TransportIntermediate       Transport = "INTERMEDIATE"
	TransportPaddedIntermediate Transport = "PADDED_INTERMEDIATE"
)

const (
	protoAbridged           uint32 = 0xEFEFEFEF
	protoIntermediate       uint32 = 0xEEEEEEEE
	protoPaddedIntermediate uint32 = 0xDDDDDDDD
)

var (
	ErrHandshakeRead        = errors.New("mtproto handshake read failed")
	ErrHTTPTransport        = errors.New("http transport is not supported")
	ErrFakeTLSExpected      = errors.New("fake tls client hello expected")
	ErrFakeTLSVerification  = errors.New("fake tls client hello verification failed")
	ErrUnsupportedTransport = errors.New("unsupported mtproto transport")
	ErrInvalidDC            = errors.New("invalid mtproto dc")
	ErrRelayInitGeneration  = errors.New("relay init generation failed")
)

type StreamTransform func([]byte) []byte

type OutboundRequest struct {
	DCID          int
	SignedDC      int16
	IsMedia       bool
	IsTestDC      bool
	Transport     Transport
	RelayInit     []byte
	ClientToRelay StreamTransform
	RelayToClient StreamTransform
}

type OutboundCapability struct {
	Status          string
	SelectedBackend string
}

type OutboundResult struct {
	SelectedBackend string
	ActualBackend   string
	FallbackUsed    bool
	Reason          string
	Err             error
}

type OutboundConnector interface {
	Capability() OutboundCapability
	Connect(context.Context, OutboundRequest) (net.Conn, OutboundResult)
}

func prepareOutboundRequest(
	conn net.Conn,
	secretHex string,
	fakeTLSDomain string,
	fakeTLSMaskingPassthrough bool,
	forceTestDC bool,
) (OutboundRequest, net.Conn, error) {
	secret, err := hex.DecodeString(secretHex)
	if err != nil || len(secret) != 16 {
		return OutboundRequest{}, conn, ErrInvalidSecret
	}

	handshake, clientConn, err := readClientInit(conn, secret, fakeTLSDomain, fakeTLSMaskingPassthrough)
	if err != nil {
		return OutboundRequest{}, conn, err
	}
	if isHTTPTransport(handshake) {
		return OutboundRequest{}, clientConn, ErrHTTPTransport
	}

	clientDecryptor, err := secretStream(handshake[8:40], handshake[40:56], secret)
	if err != nil {
		return OutboundRequest{}, clientConn, fmt.Errorf("client decryptor: %w", err)
	}
	decrypted := make([]byte, len(handshake))
	clientDecryptor.XORKeyStream(decrypted, handshake)

	proto := binary.LittleEndian.Uint32(decrypted[56:60])
	transport, ok := transportForProto(proto)
	if !ok {
		return OutboundRequest{}, clientConn, ErrUnsupportedTransport
	}

	signedDC := int16(binary.LittleEndian.Uint16(decrypted[60:62]))
	dc := int(signedDC)
	if dc < 0 {
		dc = -dc
	}
	isTestDC := forceTestDC || dc >= 10000
	relaySignedDC := signedDC
	if dc >= 10000 {
		dc -= 10000
		if signedDC < 0 {
			relaySignedDC = -int16(dc)
		} else {
			relaySignedDC = int16(dc)
		}
	}
	if !validDC(dc) {
		return OutboundRequest{}, clientConn, ErrInvalidDC
	}

	reversedClient := reverseCopy(handshake[8:56])
	clientEncryptor, err := secretStream(reversedClient[:32], reversedClient[32:], secret)
	if err != nil {
		return OutboundRequest{}, clientConn, fmt.Errorf("client encryptor: %w", err)
	}

	relayInit, relayEncryptor, relayDecryptor, err := generateRelayInit(rand.Reader, proto, relaySignedDC)
	if err != nil {
		return OutboundRequest{}, clientConn, err
	}

	return OutboundRequest{
		DCID:          dc,
		SignedDC:      signedDC,
		IsMedia:       signedDC < 0,
		IsTestDC:      isTestDC,
		Transport:     transport,
		RelayInit:     relayInit,
		ClientToRelay: chainStreams(clientDecryptor, relayEncryptor),
		RelayToClient: chainStreams(relayDecryptor, clientEncryptor),
	}, clientConn, nil
}

func readClientInit(
	conn net.Conn,
	secret []byte,
	fakeTLSDomain string,
	fakeTLSMaskingPassthrough bool,
) ([]byte, net.Conn, error) {
	if strings.TrimSpace(fakeTLSDomain) == "" {
		handshake := make([]byte, 64)
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		_, readErr := io.ReadFull(conn, handshake)
		_ = conn.SetReadDeadline(time.Time{})
		if readErr != nil {
			return nil, conn, fmt.Errorf("%w: %v", ErrHandshakeRead, readErr)
		}
		return handshake, conn, nil
	}

	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	first := make([]byte, 1)
	if _, err := io.ReadFull(conn, first); err != nil {
		_ = conn.SetReadDeadline(time.Time{})
		return nil, conn, fmt.Errorf("%w: %v", ErrHandshakeRead, err)
	}
	if first[0] != tlsRecordHandshake {
		_ = conn.SetReadDeadline(time.Time{})
		_ = writeFakeTLSRedirect(conn, fakeTLSDomain)
		return nil, conn, ErrFakeTLSExpected
	}

	headerRest := make([]byte, 4)
	if _, err := io.ReadFull(conn, headerRest); err != nil {
		_ = conn.SetReadDeadline(time.Time{})
		return nil, conn, fmt.Errorf("%w: %v", ErrHandshakeRead, err)
	}
	recordLen := int(binary.BigEndian.Uint16(headerRest[2:4]))
	body := make([]byte, recordLen)
	if _, err := io.ReadFull(conn, body); err != nil {
		_ = conn.SetReadDeadline(time.Time{})
		return nil, conn, fmt.Errorf("%w: %v", ErrHandshakeRead, err)
	}
	_ = conn.SetReadDeadline(time.Time{})

	clientHello := append(append([]byte{first[0]}, headerRest...), body...)
	clientRandom, sessionID, ok := verifyFakeTLSClientHello(clientHello, secret, time.Now())
	if !ok {
		if fakeTLSMaskingPassthrough {
			return nil, conn, &FakeTLSPassthroughRequest{
				Domain:      fakeTLSDomain,
				InitialData: clientHello,
			}
		}
		return nil, conn, ErrFakeTLSVerification
	}
	if err := writeFull(conn, buildFakeTLSServerHello(secret, clientRandom, sessionID)); err != nil {
		return nil, conn, fmt.Errorf("%w: %v", ErrHandshakeRead, err)
	}

	stream := newFakeTLSStream(conn)
	handshake := make([]byte, 64)
	_ = stream.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, readErr := io.ReadFull(stream, handshake)
	_ = stream.SetReadDeadline(time.Time{})
	if readErr != nil {
		return nil, stream, fmt.Errorf("%w: %v", ErrHandshakeRead, readErr)
	}
	return handshake, stream, nil
}

func generateRelayInit(
	random io.Reader,
	proto uint32,
	signedDC int16,
) ([]byte, cipher.Stream, cipher.Stream, error) {
	var relayPlain []byte
	for attempt := 0; attempt < 1024; attempt++ {
		candidate := make([]byte, 64)
		if _, err := io.ReadFull(random, candidate); err != nil {
			return nil, nil, nil, fmt.Errorf("%w: %v", ErrRelayInitGeneration, err)
		}
		if forbiddenInitPrefix(candidate) {
			continue
		}
		relayPlain = candidate
		break
	}
	if relayPlain == nil {
		return nil, nil, nil, ErrRelayInitGeneration
	}

	binary.LittleEndian.PutUint32(relayPlain[56:60], proto)
	binary.LittleEndian.PutUint16(relayPlain[60:62], uint16(signedDC))

	relayEncryptor, err := newAESCTR(relayPlain[8:40], relayPlain[40:56])
	if err != nil {
		return nil, nil, nil, fmt.Errorf("relay encryptor: %w", err)
	}
	reversedRelay := reverseCopy(relayPlain[8:56])
	relayDecryptor, err := newAESCTR(reversedRelay[:32], reversedRelay[32:])
	if err != nil {
		return nil, nil, nil, fmt.Errorf("relay decryptor: %w", err)
	}

	encrypted := make([]byte, len(relayPlain))
	relayEncryptor.XORKeyStream(encrypted, relayPlain)
	relayWire := append([]byte(nil), relayPlain...)
	copy(relayWire[56:64], encrypted[56:64])
	return relayWire, relayEncryptor, relayDecryptor, nil
}

func chainStreams(decryptor, encryptor cipher.Stream) StreamTransform {
	return func(chunk []byte) []byte {
		plain := make([]byte, len(chunk))
		decryptor.XORKeyStream(plain, chunk)
		transformed := make([]byte, len(chunk))
		encryptor.XORKeyStream(transformed, plain)
		return transformed
	}
}

func secretStream(prekey, iv, secret []byte) (cipher.Stream, error) {
	keyMaterial := make([]byte, 0, len(prekey)+len(secret))
	keyMaterial = append(keyMaterial, prekey...)
	keyMaterial = append(keyMaterial, secret...)
	key := sha256.Sum256(keyMaterial)
	return newAESCTR(key[:], iv)
}

func newAESCTR(key, iv []byte) (cipher.Stream, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewCTR(block, iv), nil
}

func transportForProto(proto uint32) (Transport, bool) {
	switch proto {
	case protoAbridged:
		return TransportAbridged, true
	case protoIntermediate:
		return TransportIntermediate, true
	case protoPaddedIntermediate:
		return TransportPaddedIntermediate, true
	default:
		return "", false
	}
}

func validDC(dc int) bool {
	return dc >= 1 && dc <= 5 || dc == 203
}

func reverseCopy(value []byte) []byte {
	out := make([]byte, len(value))
	for i := range value {
		out[i] = value[len(value)-1-i]
	}
	return out
}

func forbiddenInitPrefix(init []byte) bool {
	if len(init) < 8 || init[0] == 0xef || bytes.Equal(init[4:8], []byte{0, 0, 0, 0}) {
		return true
	}
	switch binary.LittleEndian.Uint32(init[:4]) {
	case 0x44414548, // HEAD
		0x54534f50, // POST
		0x20544547, // GET
		0x4954504f, // OPTI
		0x02010316, // TLS ClientHello prefix
		protoAbridged,
		protoIntermediate,
		protoPaddedIntermediate:
		return true
	default:
		return false
	}
}

func isHTTPTransport(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	return bytes.HasPrefix(data, []byte("GET ")) ||
		bytes.HasPrefix(data, []byte("POST")) ||
		bytes.HasPrefix(data, []byte("HEAD")) ||
		bytes.HasPrefix(data, []byte("OPTI"))
}
