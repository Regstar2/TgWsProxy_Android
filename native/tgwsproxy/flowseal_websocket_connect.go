package main

import (
	"bufio"
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

func dialFlowsealWorkerCandidate(domain, path, logPrefix string) (mtProtoFrameSocket, error) {
	ws, err := connectFlowsealRawWebSocket(domain, domain, path, 10)
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
	rawConn, err := dialer.Dial("tcp", joinAddr(host, 443))
	if err != nil {
		return nil, &wsStageError{Stage: "tcp_dial", Err: err}
	}
	setSockOpts(rawConn)

	tlsConn := tls.Client(rawConn, &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         domain,
	})
	deadline := time.Now().Add(time.Duration(timeout * float64(time.Second)))
	_ = tlsConn.SetDeadline(deadline)
	if err := tlsConn.Handshake(); err != nil {
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
