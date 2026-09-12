package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	utls "github.com/refraction-networking/utls"
)

const workerProbeUTLSFingerprint = "chrome_auto_http1"

func connectFlowsealRawWebSocketUTLSContext(ctx context.Context, host, domain, path string, timeout float64) (*flowsealRawWebSocket, error) {
	if path == "" {
		path = "/apiws"
	}
	if timeout <= 0 {
		timeout = 10
	}
	dialTimeout := timeout
	if dialTimeout > 10 {
		dialTimeout = 10
	}

	dialer := &net.Dialer{Timeout: time.Duration(dialTimeout * float64(time.Second))}
	rawConn, err := dialer.DialContext(ctx, "tcp", joinAddr(host, 443))
	if err != nil {
		return nil, &wsStageError{Stage: "tcp_dial", Err: err}
	}
	setSockOpts(rawConn)
	stopCancel := context.AfterFunc(ctx, func() { _ = rawConn.Close() })
	defer stopCancel()

	config := &utls.Config{
		InsecureSkipVerify:          true, // Preserve the existing Flowseal verification policy.
		ServerName:                  domain,
		NextProtos:                  []string{"http/1.1"},
		DynamicRecordSizingDisabled: true,
	}
	utlsConn := utls.UClient(rawConn, config, utls.HelloChrome_Auto)
	if err := prepareWorkerUTLSForHTTP1(utlsConn); err != nil {
		_ = rawConn.Close()
		return nil, &wsStageError{Stage: "tls_client_hello", Err: err}
	}

	deadline := time.Now().Add(time.Duration(timeout * float64(time.Second)))
	_ = utlsConn.SetDeadline(deadline)
	if err := utlsConn.HandshakeContext(ctx); err != nil {
		_ = utlsConn.Close()
		return nil, &wsStageError{Stage: "tls_handshake", Err: err}
	}
	_ = utlsConn.SetDeadline(time.Time{})

	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		_ = utlsConn.Close()
		return nil, &wsStageError{Stage: "ws_upgrade_write", Err: err}
	}
	request := buildFlowsealUpgradeRequest(path, domain, base64.StdEncoding.EncodeToString(keyBytes))
	_ = utlsConn.SetWriteDeadline(deadline)
	if err := writeFlowsealFull(utlsConn, []byte(request)); err != nil {
		_ = utlsConn.Close()
		return nil, &wsStageError{Stage: "ws_upgrade_write", Err: err}
	}
	_ = utlsConn.SetWriteDeadline(time.Time{})

	reader := bufio.NewReaderSize(utlsConn, 4096)
	_ = utlsConn.SetReadDeadline(deadline)
	responseLines := make([]string, 0, 16)
	for {
		line, readErr := reader.ReadString('\n')
		if readErr != nil {
			_ = utlsConn.Close()
			return nil, &wsStageError{Stage: "ws_upgrade_read", Err: readErr}
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		responseLines = append(responseLines, line)
		if len(responseLines) > 100 {
			_ = utlsConn.Close()
			return nil, &wsStageError{Stage: "ws_upgrade_read", Err: fmt.Errorf("too many HTTP headers")}
		}
	}
	_ = utlsConn.SetReadDeadline(time.Time{})

	if len(responseLines) == 0 {
		_ = utlsConn.Close()
		return nil, &WsHandshakeError{StatusCode: 0, StatusLine: "empty response"}
	}

	firstLine := responseLines[0]
	parts := strings.SplitN(firstLine, " ", 3)
	statusCode := 0
	if len(parts) >= 2 {
		statusCode, _ = strconv.Atoi(parts[1])
	}
	if statusCode == 101 {
		if logInfo != nil {
			state := utlsConn.ConnectionState()
			logInfo.Printf(
				"MTProto Worker transport ready session_id=%s remote=%s tls_version=%x cipher=%x tls_record_sizing=fixed tls_fingerprint=%s alpn=%s worker_revision=%s",
				flowsealSessionIDFromPath(path), rawConn.RemoteAddr(), state.Version, state.CipherSuite,
				workerProbeUTLSFingerprint, mtProtoStatusField(state.NegotiatedProtocol),
				flowsealResponseHeader(responseLines, "X-Tgws-Worker-Revision"),
			)
		}
		return &flowsealRawWebSocket{
			conn:      utlsConn,
			reader:    reader,
			sessionID: flowsealSessionIDFromPath(path),
		}, nil
	}

	headers := make(map[string]string)
	for _, headerLine := range responseLines[1:] {
		if index := strings.IndexByte(headerLine, ':'); index >= 0 {
			key := strings.TrimSpace(strings.ToLower(headerLine[:index]))
			value := strings.TrimSpace(headerLine[index+1:])
			headers[key] = value
		}
	}
	_ = utlsConn.Close()
	return nil, &WsHandshakeError{
		StatusCode: statusCode,
		StatusLine: firstLine,
		Headers:    headers,
		Location:   headers["location"],
	}
}

// Chrome parrots normally advertise h2 as well. The existing probe speaks a
// raw HTTP/1.1 WebSocket upgrade after TLS, so keep the browser-like ClientHello
// while constraining ALPN to HTTP/1.1. This isolates ClientHello/fingerprint
// effects without changing the WebSocket implementation under test.
func prepareWorkerUTLSForHTTP1(conn *utls.UConn) error {
	if err := conn.BuildHandshakeState(); err != nil {
		return err
	}
	found := false
	for _, extension := range conn.Extensions {
		if alpn, ok := extension.(*utls.ALPNExtension); ok {
			alpn.AlpnProtocols = []string{"http/1.1"}
			found = true
			break
		}
	}
	if !found {
		conn.Extensions = append(conn.Extensions, &utls.ALPNExtension{AlpnProtocols: []string{"http/1.1"}})
	}
	return conn.BuildHandshakeState()
}
