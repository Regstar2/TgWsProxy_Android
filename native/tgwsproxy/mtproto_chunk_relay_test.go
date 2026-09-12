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

func newTestChunkRelayConn(t *testing.T, request chunkRelayRequestFunc) *mtProtoChunkRelayConn {
	t.Helper()
	lifeCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &mtProtoChunkRelayConn{
		domain:    "example.workers.dev",
		sessionID: "session_test_123",
		workerDst: "149.154.167.51",
		request:   request,
		lifeCtx:   lifeCtx,
		cancel:    cancel,
	}
}

func TestChunkRelayWriteRetriesSameSequence(t *testing.T) {
	var mu sync.Mutex
	var seqs []int64
	var sizes []int
	failedFirst := false
	conn := newTestChunkRelayConn(t, func(_ context.Context, action string, query url.Values, body []byte) (int, http.Header, []byte, error) {
		if action != "up" {
			t.Fatalf("action=%s", action)
		}
		seq, err := strconv.ParseInt(query.Get("seq"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		seqs = append(seqs, seq)
		sizes = append(sizes, len(body))
		if seq == 1 && !failedFirst {
			failedFirst = true
			mu.Unlock()
			return 0, nil, nil, errors.New("synthetic timeout")
		}
		mu.Unlock()
		headers := make(http.Header)
		headers.Set("X-Tgws-Chunk-Ack", strconv.FormatInt(seq, 10))
		return http.StatusNoContent, headers, nil, nil
	})

	payload := make([]byte, mtProtoChunkRelayBytes+808)
	n, err := conn.Write(payload)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("n=%d want=%d", n, len(payload))
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seqs) != 3 || seqs[0] != 1 || seqs[1] != 1 || seqs[2] != 2 {
		t.Fatalf("seqs=%v", seqs)
	}
	if sizes[0] != mtProtoChunkRelayBytes || sizes[1] != mtProtoChunkRelayBytes || sizes[2] != 808 {
		t.Fatalf("sizes=%v", sizes)
	}
}

func TestChunkRelayReadAcknowledgesAfterBufferedChunkIsConsumed(t *testing.T) {
	var mu sync.Mutex
	var acks []string
	calls := 0
	conn := newTestChunkRelayConn(t, func(_ context.Context, action string, query url.Values, _ []byte) (int, http.Header, []byte, error) {
		if action != "down" {
			t.Fatalf("action=%s", action)
		}
		mu.Lock()
		defer mu.Unlock()
		acks = append(acks, query.Get("ack"))
		calls++
		if calls == 1 {
			headers := make(http.Header)
			headers.Set("X-Tgws-Chunk-Seq", "1")
			return http.StatusOK, headers, []byte("abcdefgh"), nil
		}
		return http.StatusGone, make(http.Header), nil, nil
	})

	first := make([]byte, 3)
	n, err := conn.Read(first)
	if err != nil || n != 3 || string(first) != "abc" {
		t.Fatalf("first n=%d err=%v data=%q", n, err, first)
	}
	second := make([]byte, 5)
	n, err = conn.Read(second)
	if err != nil || n != 5 || string(second) != "defgh" {
		t.Fatalf("second n=%d err=%v data=%q", n, err, second)
	}
	if conn.ackSeq != 1 {
		t.Fatalf("ackSeq=%d want=1", conn.ackSeq)
	}

	_, err = conn.Read(make([]byte, 1))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("third err=%v want EOF", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(acks) != 2 || acks[0] != "0" || acks[1] != "1" {
		t.Fatalf("acks=%v", acks)
	}
}

func TestChunkRelayFrameSocketRequiresSessionAndDestination(t *testing.T) {
	_, err := dialMtProtoChunkRelayFrameSocket(context.Background(), "example.workers.dev", "/apiws?sid=abc", "test")
	if err == nil {
		t.Fatal("expected missing destination error")
	}
}
