package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"time"
)

type mtProtoChunkRelayFrameSocket struct {
	conn net.Conn
}

func dialMtProtoChunkRelayFrameSocket(ctx context.Context, domain, path, logPrefix string) (mtProtoFrameSocket, error) {
	parsed, err := url.ParseRequestURI(path)
	if err != nil {
		return nil, fmt.Errorf("parse chunk relay path: %w", err)
	}
	sessionID := strings.TrimSpace(parsed.Query().Get("sid"))
	workerDst := strings.TrimSpace(parsed.Query().Get("dst"))
	if sessionID == "" || workerDst == "" {
		return nil, fmt.Errorf("chunk relay path missing sid/dst")
	}
	conn, err := dialMtProtoChunkRelay(ctx, domain, sessionID, workerDst, logPrefix)
	if err != nil {
		return nil, err
	}
	if chunkConn, ok := conn.(*mtProtoChunkRelayConn); ok {
		enableMtProtoChunkRelayRequestLimit(chunkConn)
		if logInfo != nil {
			logInfo.Printf(
				"%s MTProto Worker chunk relay upload pipeline session_id=%s window=%d global_http_requests=%d",
				logPrefix,
				sessionID,
				mtProtoChunkRelayUpWindow,
				mtProtoChunkRelayGlobalHTTPRequests,
			)
		}
	}
	return &mtProtoChunkRelayFrameSocket{conn: conn}, nil
}

func (s *mtProtoChunkRelayFrameSocket) Send(data []byte) error {
	if conn, ok := s.conn.(*mtProtoChunkRelayConn); ok {
		n, err := writePipelinedMtProtoChunkRelay(conn, data)
		if err != nil {
			return err
		}
		if n != len(data) {
			return io.ErrShortWrite
		}
		return nil
	}
	return writeFullConn(s.conn, data)
}

func (s *mtProtoChunkRelayFrameSocket) SendBatch(parts [][]byte) error {
	for _, part := range parts {
		if err := s.Send(part); err != nil {
			return err
		}
	}
	return nil
}

func (s *mtProtoChunkRelayFrameSocket) Recv() ([]byte, error) {
	buf := make([]byte, mtProtoChunkRelayBytes)
	n, err := s.conn.Read(buf)
	if n > 0 {
		return buf[:n], nil
	}
	return nil, err
}

func (s *mtProtoChunkRelayFrameSocket) Close() {
	_ = s.conn.Close()
}

func (s *mtProtoChunkRelayFrameSocket) SetDeadline(t time.Time) error {
	return s.conn.SetDeadline(t)
}

func (s *mtProtoChunkRelayFrameSocket) SetReadDeadline(t time.Time) error {
	return s.conn.SetReadDeadline(t)
}

func (s *mtProtoChunkRelayFrameSocket) SetWriteDeadline(t time.Time) error {
	return s.conn.SetWriteDeadline(t)
}

var _ mtProtoFrameSocket = (*mtProtoChunkRelayFrameSocket)(nil)
