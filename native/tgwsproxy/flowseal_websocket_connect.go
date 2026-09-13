package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const workerChunkRelayExperimentEnabled = true

func dialFlowsealWorkerCandidate(domain, path, logPrefix string) (mtProtoFrameSocket, error) {
	return dialFlowsealWorkerCandidateContext(context.Background(), domain, path, logPrefix)
}

func dialFlowsealWorkerCandidateContext(ctx context.Context, domain, path, logPrefix string) (mtProtoFrameSocket, error) {
	if workerChunkRelayExperimentEnabled {
		socket, err := dialMtProtoChunkRelayFrameSocket(ctx, domain, path, logPrefix)
		if err != nil {
			logDomainConnectFailure(logPrefix, domain, domain, err)
			return nil, err
		}
		if logInfo != nil {
			logInfo.Printf("%s Worker transport=chunk_relay_http host=%s path=%s chunk_bytes=%d max_retries=%d",
				logPrefix, domain, path, mtProtoChunkRelayBytes, mtProtoChunkRelayMaxRetries)
		}
		return socket, nil
	}

	ws, err := connectFlowsealRawWebSocketContext(ctx, domain, domain, path, 10)
	if err != nil {
		logDomainConnectFailure(logPrefix, domain, domain, err)
		return nil, err
	}
	if logInfo != nil {
		logInfo.Printf("%s Flowseal parity transport connected host=%s path=%s pool=false preconnect=false", logPrefix, domain, path)
	}
	return ws, nil
}

func connectFlowsealRawWebSocket(host, domain, path string, timeout float64) (*flowsealRawWebSocket, error) {
	return connectFlowsealRawWebSocketContext(context.Background(), host, domain, path, timeout)
}

func connectFlowsealRawWebSocketContext(ctx context.Context, host, domain, path string, timeout float64) (*flowsealRawWebSocket, error) {
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

	tlsConn := tls.Client(rawConn, flowsealWorkerTLSConfig(domain))
	deadline := time.Now().Add(time.Duration(timeout * float64(time.Second)))
	_ = tlsConn.SetDeadline(deadline)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		_ = tlsConn.Close()
		return nil, &wsStageError{Stage: "tls_handshake", Err: err}
	}
	_ = tlsConn.SetDeadline(time.Time{})

	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		_ = tlsConn.Close()
		return nil, &wsStageError{Stage: "ws_upgrade_write", Err: err}
	}
	request := buildFlowsealUpgradeRequest(path, domain, base64.StdEncoding.EncodeToString(keyBytes))
	_ = tlsConn.SetWriteDeadline(deadline)
	if err := writeFlowsealFull(tlsConn, []byte(request)); err != nil {
		_ = tlsConn.Close()
		return nil, &wsStageError{Stage: "ws_upgrade_write", Err: err}
	}
	_ = tlsConn.SetWriteDeadline(time.Time{})

	reader := bufio.NewReaderSize(tlsConn, 4096)
	_ = tlsConn.SetReadDeadline(deadline)
	responseLines := make([]string, 0, 16)
	for {
		line, readErr := reader.ReadString('\n')
		if readErr != nil {
			_ = tlsConn.Close()
			return nil, &wsStageError{Stage: "ws_upgrade_read", Err: readErr}
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		responseLines = append(responseLines, line)
		if len(responseLines) > 100 {
			_ = tlsConn.Close()
			return nil, &wsStageError{Stage: "ws_upgrade_read", Err: fmt.Errorf("too many HTTP headers")}
		}
	}
	_ = tlsConn.SetReadDeadline(time.Time{})

	if len(responseLines) == 0 {
		_ = tlsConn.Close()
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
			state := tlsConn.ConnectionState()
			logInfo.Printf("MTProto Worker transport ready session_id=%s remote=%s tls_version=%x cipher=%x tls_record_sizing=fixed worker_revision=%s",
				flowsealSessionIDFromPath(path), rawConn.RemoteAddr(), state.Version, state.CipherSuite,
				flowsealResponseHeader(responseLines, "X-Tgws-Worker-Revision"))
		}
		return &flowsealRawWebSocket{
			conn:      tlsConn,
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
	_ = tlsConn.Close()
	return nil, &WsHandshakeError{
		StatusCode: statusCode,
		StatusLine: firstLine,
		Headers:    headers,
		Location:   headers["location"],
	}
}

func flowsealWorkerTLSConfig(domain string) *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true, // Preserve the existing Flowseal verification policy.
		ServerName:         domain,
		// Flowseal uses OpenSSL's full-size records. Keep WS messages intact and
		// isolate Go's adaptive TLS record sizing in the #29 device experiment.
		// This is not evidence that record sizing caused the observed TCP loss.
		DynamicRecordSizingDisabled: true,
	}
}

func flowsealSessionIDFromPath(path string) string {
	parsed, err := url.ParseRequestURI(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(parsed.Query().Get("sid"))
}

func buildFlowsealUpgradeRequest(path, domain, websocketKey string) string {
	return fmt.Sprintf(
		"GET %s HTTP/1.1\r\n"+
			"Host: %s\r\n"+
			"Upgrade: websocket\r\n"+
			"Connection: Upgrade\r\n"+
			"Sec-WebSocket-Key: %s\r\n"+
			"Sec-WebSocket-Version: 13\r\n"+
			"Sec-WebSocket-Protocol: binary\r\n"+
			"\r\n",
		path, domain, websocketKey,
	)
}

func flowsealResponseHeader(lines []string, name string) string {
	for _, line := range lines {
		key, value, ok := strings.Cut(line, ":")
		if ok && strings.EqualFold(strings.TrimSpace(key), name) {
			return mtProtoStatusField(value)
		}
	}
	return "unknown"
}
