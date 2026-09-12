package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"tg-ws-proxy/mtproxyfrontend"
)

const (
	mtProtoChunkRelayBytes      = 8 * 1024
	mtProtoChunkRelayMaxRetries = 3
	mtProtoChunkRelayTimeout    = 12 * time.Second
	mtProtoChunkRelayPollWaitMS = 900
)

type chunkRelayRequestFunc func(
	ctx context.Context,
	action string,
	query url.Values,
	body []byte,
) (status int, headers http.Header, responseBody []byte, err error)

type mtProtoChunkRelayConn struct {
	domain    string
	sessionID string
	workerDst string
	request   chunkRelayRequestFunc
	cancel    context.CancelFunc
	lifeCtx   context.Context

	writeMu   sync.Mutex
	upSeq     int64
	upBytes   int64

	readMu     sync.Mutex
	readBuf    []byte
	pendingSeq int64
	ackSeq     int64
	downBytes  int64

	deadlineMu    sync.RWMutex
	readDeadline  time.Time
	writeDeadline time.Time

	closeMu sync.Mutex
	closed  bool
}

func dialMtProtoChunkRelay(
	ctx context.Context,
	domain, sessionID, workerDst, logPrefix string,
) (net.Conn, error) {
	domain = strings.TrimSpace(domain)
	sessionID = strings.TrimSpace(sessionID)
	workerDst = strings.TrimSpace(workerDst)
	if domain == "" || sessionID == "" || workerDst == "" {
		return nil, fmt.Errorf("chunk relay requires domain, session id and destination")
	}

	lifeCtx, cancel := context.WithCancel(context.Background())
	conn := &mtProtoChunkRelayConn{
		domain:    domain,
		sessionID: sessionID,
		workerDst: workerDst,
		lifeCtx:   lifeCtx,
		cancel:    cancel,
	}
	conn.request = conn.freshHTTPRequest

	openQuery := url.Values{
		"sid": {sessionID},
		"dst": {workerDst},
	}
	status, headers, _, err := conn.requestWithRetry(ctx, "open", openQuery, nil, "open", 0)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("open chunk relay: %w", err)
	}
	if status != http.StatusNoContent {
		cancel()
		return nil, fmt.Errorf("open chunk relay: HTTP %d", status)
	}
	if strings.TrimSpace(headers.Get("X-Tgws-Chunk-Relay-Revision")) == "" {
		cancel()
		return nil, fmt.Errorf("open chunk relay: missing relay revision header")
	}

	if logInfo != nil {
		logInfo.Printf(
			"%s MTProto Worker chunk relay ready session_id=%s worker_host=%s worker_dst=%s chunk_bytes=%d max_retries=%d revision=%s",
			logPrefix,
			sessionID,
			domain,
			workerDst,
			mtProtoChunkRelayBytes,
			mtProtoChunkRelayMaxRetries,
			mtProtoStatusField(headers.Get("X-Tgws-Chunk-Relay-Revision")),
		)
	}
	return conn, nil
}

func (c *mtProtoChunkRelayConn) freshHTTPRequest(
	ctx context.Context,
	action string,
	query url.Values,
	body []byte,
) (int, http.Header, []byte, error) {
	endpoint := url.URL{
		Scheme:   "https",
		Host:     c.domain,
		Path:     "/chunk-relay/" + action,
		RawQuery: query.Encode(),
	}

	method := http.MethodPost
	if action == "down" {
		method = http.MethodGet
	}
	var reader io.Reader
	if len(body) > 0 {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), reader)
	if err != nil {
		return 0, nil, nil, err
	}
	req.Header.Set("Cache-Control", "no-store")
	req.Header.Set("Connection", "close")
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/octet-stream")
	}

	dialer := &net.Dialer{Timeout: 10 * time.Second}
	transport := &http.Transport{
		Proxy:               nil,
		DialContext:         dialer.DialContext,
		DisableKeepAlives:   true,
		ForceAttemptHTTP2:   false,
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // Match the existing Flowseal Worker TLS policy in this experiment.
			ServerName:         c.domain,
			NextProtos:         []string{"http/1.1"},
		},
	}
	client := &http.Client{Transport: transport}
	defer transport.CloseIdleConnections()

	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, mtProtoChunkRelayBytes+4096))
	if err != nil {
		return resp.StatusCode, resp.Header.Clone(), nil, err
	}
	return resp.StatusCode, resp.Header.Clone(), responseBody, nil
}

func (c *mtProtoChunkRelayConn) requestWithRetry(
	parent context.Context,
	action string,
	query url.Values,
	body []byte,
	direction string,
	seq int64,
) (int, http.Header, []byte, error) {
	var lastErr error
	for attempt := 0; attempt <= mtProtoChunkRelayMaxRetries; attempt++ {
		ctx, cancel := c.requestContext(parent, direction)
		status, headers, responseBody, err := c.request(ctx, action, query, body)
		cancel()
		if err == nil {
			return status, headers, responseBody, nil
		}
		lastErr = err
		if attempt >= mtProtoChunkRelayMaxRetries {
			break
		}
		if logInfo != nil {
			logInfo.Printf(
				"MTProto Worker chunk relay retry session_id=%s direction=%s seq=%d attempt=%d/%d error=%s",
				c.sessionID,
				direction,
				seq,
				attempt+1,
				mtProtoChunkRelayMaxRetries,
				mtProtoStatusField(err.Error()),
			)
		}
		backoff := 300 * time.Millisecond * time.Duration(1<<attempt)
		timer := time.NewTimer(backoff)
		select {
		case <-c.lifeCtx.Done():
			timer.Stop()
			return 0, nil, nil, net.ErrClosed
		case <-parent.Done():
			timer.Stop()
			return 0, nil, nil, parent.Err()
		case <-timer.C:
		}
	}
	return 0, nil, nil, lastErr
}

func (c *mtProtoChunkRelayConn) requestContext(parent context.Context, direction string) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	base, baseCancel := context.WithCancel(parent)
	stopLife := context.AfterFunc(c.lifeCtx, baseCancel)

	deadline := time.Now().Add(mtProtoChunkRelayTimeout)
	c.deadlineMu.RLock()
	if direction == "up" && !c.writeDeadline.IsZero() && c.writeDeadline.Before(deadline) {
		deadline = c.writeDeadline
	}
	if direction == "down" && !c.readDeadline.IsZero() && c.readDeadline.Before(deadline) {
		deadline = c.readDeadline
	}
	c.deadlineMu.RUnlock()

	ctx, timeoutCancel := context.WithDeadline(base, deadline)
	return ctx, func() {
		timeoutCancel()
		stopLife()
		baseCancel()
	}
}

func (c *mtProtoChunkRelayConn) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.isClosed() {
		return 0, net.ErrClosed
	}

	written := 0
	for written < len(data) {
		end := written + mtProtoChunkRelayBytes
		if end > len(data) {
			end = len(data)
		}
		chunk := data[written:end]
		seq := c.upSeq + 1
		query := url.Values{
			"sid": {c.sessionID},
			"dst": {c.workerDst},
			"seq": {strconv.FormatInt(seq, 10)},
		}
		status, headers, _, err := c.requestWithRetry(context.Background(), "up", query, chunk, "up", seq)
		if err != nil {
			return written, err
		}
		if status == http.StatusGone {
			return written, io.EOF
		}
		if status != http.StatusNoContent {
			return written, fmt.Errorf("chunk relay up seq %d: HTTP %d", seq, status)
		}
		ack, err := strconv.ParseInt(strings.TrimSpace(headers.Get("X-Tgws-Chunk-Ack")), 10, 64)
		if err != nil || ack != seq {
			return written, fmt.Errorf("chunk relay up seq %d: invalid ack %q", seq, headers.Get("X-Tgws-Chunk-Ack"))
		}
		c.upSeq = seq
		c.upBytes += int64(len(chunk))
		written = end
		if logInfo != nil && (seq <= 2 || c.upBytes%(64*1024) < int64(len(chunk))) {
			logInfo.Printf(
				"MTProto Worker chunk relay up session_id=%s seq=%d bytes=%d confirmed_bytes=%d",
				c.sessionID, seq, len(chunk), c.upBytes,
			)
		}
	}
	return written, nil
}

func (c *mtProtoChunkRelayConn) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	c.readMu.Lock()
	defer c.readMu.Unlock()

	for {
		if len(c.readBuf) > 0 {
			n := copy(dst, c.readBuf)
			c.readBuf = c.readBuf[n:]
			if len(c.readBuf) == 0 && c.pendingSeq > 0 {
				c.ackSeq = c.pendingSeq
				c.pendingSeq = 0
			}
			return n, nil
		}
		if c.isClosed() {
			return 0, net.ErrClosed
		}

		query := url.Values{
			"sid":  {c.sessionID},
			"dst":  {c.workerDst},
			"ack":  {strconv.FormatInt(c.ackSeq, 10)},
			"wait": {strconv.Itoa(mtProtoChunkRelayPollWaitMS)},
		}
		status, headers, body, err := c.requestWithRetry(context.Background(), "down", query, nil, "down", c.ackSeq)
		if err != nil {
			return 0, err
		}
		switch status {
		case http.StatusNoContent:
			continue
		case http.StatusGone:
			return 0, io.EOF
		case http.StatusOK:
		default:
			return 0, fmt.Errorf("chunk relay down: HTTP %d", status)
		}
		seq, err := strconv.ParseInt(strings.TrimSpace(headers.Get("X-Tgws-Chunk-Seq")), 10, 64)
		if err != nil || seq <= 0 {
			return 0, fmt.Errorf("chunk relay down: invalid seq %q", headers.Get("X-Tgws-Chunk-Seq"))
		}
		if len(body) == 0 || len(body) > mtProtoChunkRelayBytes {
			return 0, fmt.Errorf("chunk relay down seq %d: invalid body size %d", seq, len(body))
		}
		if seq <= c.ackSeq {
			continue
		}
		c.pendingSeq = seq
		c.readBuf = body
		c.downBytes += int64(len(body))
		if logInfo != nil && (seq <= 2 || c.downBytes%(64*1024) < int64(len(body))) {
			logInfo.Printf(
				"MTProto Worker chunk relay down session_id=%s seq=%d bytes=%d received_bytes=%d",
				c.sessionID, seq, len(body), c.downBytes,
			)
		}
	}
}

func (c *mtProtoChunkRelayConn) Close() error {
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return nil
	}
	c.closed = true
	c.closeMu.Unlock()
	c.cancel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	query := url.Values{"sid": {c.sessionID}, "dst": {c.workerDst}}
	_, _, _, _ = c.request(ctx, "close", query, nil)
	if logInfo != nil {
		logInfo.Printf(
			"MTProto Worker chunk relay closed session_id=%s worker_dst=%s up_bytes=%d down_bytes=%d up_seq=%d down_ack=%d",
			c.sessionID, c.workerDst, c.upBytes, c.downBytes, c.upSeq, c.ackSeq,
		)
	}
	return nil
}

func (c *mtProtoChunkRelayConn) isClosed() bool {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	return c.closed
}

func (c *mtProtoChunkRelayConn) LocalAddr() net.Addr {
	return mtProtoNetAddr("mtproto-chunk-relay-local")
}

func (c *mtProtoChunkRelayConn) RemoteAddr() net.Addr {
	return mtProtoNetAddr(strings.TrimSpace(c.domain))
}

func (c *mtProtoChunkRelayConn) SetDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	c.readDeadline = t
	c.writeDeadline = t
	c.deadlineMu.Unlock()
	return nil
}

func (c *mtProtoChunkRelayConn) SetReadDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	c.readDeadline = t
	c.deadlineMu.Unlock()
	return nil
}

func (c *mtProtoChunkRelayConn) SetWriteDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	c.writeDeadline = t
	c.deadlineMu.Unlock()
	return nil
}

func (c *mtProtoChunkRelayConn) RouteDiagnostics() mtproxyfrontend.RouteDiagnostics {
	return mtproxyfrontend.RouteDiagnostics{
		SessionID: strings.TrimSpace(c.sessionID),
		WorkerDst: strings.TrimSpace(c.workerDst),
	}
}

var (
	_ net.Conn                                 = (*mtProtoChunkRelayConn)(nil)
	_ mtproxyfrontend.RouteDiagnosticsProvider = (*mtProtoChunkRelayConn)(nil)
)
