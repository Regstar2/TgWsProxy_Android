package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"testing"
)

func TestChunkRelayPipelineBadStatusCannotWinHedge(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	conn := newTestChunkRelayConn(t, func(_ context.Context, action string, query url.Values, _ []byte) (int, http.Header, []byte, error) {
		if action != "up" {
			t.Fatalf("action=%s", action)
		}
		seq, err := strconv.ParseInt(query.Get("seq"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		calls++
		call := calls
		mu.Unlock()

		headers := make(http.Header)
		if call == 1 {
			headers.Set(chunkRelayErrorHeader, "transient_worker_error")
			return http.StatusBadGateway, headers, nil, nil
		}
		headers.Set("X-Tgws-Chunk-Ack", strconv.FormatInt(seq, 10))
		return http.StatusNoContent, headers, nil, nil
	})

	payload := []byte("pipeline-status-aware")
	n, err := writePipelinedMtProtoChunkRelay(conn, payload)
	if err != nil {
		t.Fatalf("pipeline write: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("n=%d want=%d", n, len(payload))
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Fatalf("calls=%d want=2", calls)
	}
}

func TestChunkRelayPipeline410IsTerminalWithoutRetry(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	conn := newTestChunkRelayConn(t, func(_ context.Context, action string, _ url.Values, _ []byte) (int, http.Header, []byte, error) {
		if action != "up" {
			t.Fatalf("action=%s", action)
		}
		mu.Lock()
		calls++
		mu.Unlock()
		headers := make(http.Header)
		headers.Set(chunkRelayErrorHeader, "session_lost")
		return http.StatusGone, headers, nil, nil
	})

	_, err := writePipelinedMtProtoChunkRelay(conn, []byte("terminal"))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err=%v want EOF", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("calls=%d want=1", calls)
	}
}
