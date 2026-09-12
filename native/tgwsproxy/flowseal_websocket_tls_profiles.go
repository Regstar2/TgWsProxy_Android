package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type workerProbeTLSProfile struct {
	Name                        string
	DynamicRecordSizingDisabled bool
	WriteChunkBytes             int
	WritePace                   time.Duration
}

var (
	workerProbeTLSProfileDynamic = workerProbeTLSProfile{
		Name:                        "go_dynamic",
		DynamicRecordSizingDisabled: false,
	}
	workerProbeTLSProfileSplit1200 = workerProbeTLSProfile{
		Name:                        "go_split_1200",
		DynamicRecordSizingDisabled: true,
		WriteChunkBytes:             1200,
	}
	workerProbeTLSProfileSplit4K = workerProbeTLSProfile{
		Name:                        "go_split_4k",
		DynamicRecordSizingDisabled: true,
		WriteChunkBytes:             4 * 1024,
	}
	workerProbeTLSProfileSplit16K = workerProbeTLSProfile{
		Name:                        "go_split_16k",
		DynamicRecordSizingDisabled: true,
		WriteChunkBytes:             16 * 1024,
	}
	workerProbeTLSProfilePaced4K = workerProbeTLSProfile{
		Name:                        "go_paced_4k",
		DynamicRecordSizingDisabled: true,
		WriteChunkBytes:             4 * 1024,
		WritePace:                   2 * time.Millisecond,
	}
)

// connectFlowsealRawWebSocketTLSProfileContext is diagnostic-only. It keeps the
// same Go TCP/TLS/raw-WebSocket implementation as the production Flowseal path
// while changing only TLS record sizing and/or the size/timing of application
// writes handed to crypto/tls.
func connectFlowsealRawWebSocketTLSProfileContext(
	ctx context.Context,
	host, domain, path string,
	timeout float64,
	profile workerProbeTLSProfile,
) (*flowsealRawWebSocket, error) {
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

	config := flowsealWorkerTLSConfig(domain)
	config.DynamicRecordSizingDisabled = profile.DynamicRecordSizingDisabled
	tlsConn := tls.Client(rawConn, config)
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
	if err := writeFlowsealShapedFull(tlsConn, []byte(request), profile.WriteChunkBytes, profile.WritePace); err != nil {
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
			recordSizing := "dynamic"
			if profile.DynamicRecordSizingDisabled {
				recordSizing = "fixed"
			}
			logInfo.Printf(
				"MTProto Worker transport ready session_id=%s remote=%s tls_version=%x cipher=%x tls_record_sizing=%s tls_write_profile=%s tls_write_chunk_bytes=%d tls_write_pace_us=%d worker_revision=%s",
				flowsealSessionIDFromPath(path), rawConn.RemoteAddr(), state.Version, state.CipherSuite,
				recordSizing, mtProtoStatusField(profile.Name), profile.WriteChunkBytes, profile.WritePace.Microseconds(),
				flowsealResponseHeader(responseLines, "X-Tgws-Worker-Revision"),
			)
		}
		return &flowsealRawWebSocket{
			conn:                tlsConn,
			reader:              reader,
			sessionID:           flowsealSessionIDFromPath(path),
			tlsWriteChunkBytes:  profile.WriteChunkBytes,
			tlsWritePace:        profile.WritePace,
			tlsWriteProfileName: profile.Name,
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
