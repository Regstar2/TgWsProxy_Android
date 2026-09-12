package main

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestChunkRelayWriteHedgesSlowUploadWithSameSequence(t *testing.T) {
	var mu sync.Mutex
	var seqs []int64
	calls := 0

	conn := newTestChunkRelayConn(t, func(ctx context.Context, action string, query url.Values, body []byte) (int, http.Header, []byte, error) {
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
		seqs = append(seqs, seq)
		mu.Unlock()

		if call == 1 {
			<-ctx.Done()
			return 0, nil, nil, ctx.Err()
		}

		headers := make(http.Header)
		headers.Set("X-Tgws-Chunk-Ack", strconv.FormatInt(seq, 10))
		return http.StatusNoContent, headers, nil, nil
	})

	started := time.Now()
	payload := make([]byte, 1024)
	n, err := conn.Write(payload)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("n=%d want=%d", n, len(payload))
	}
	if elapsed := time.Since(started); elapsed >= mtProtoChunkRelayUpTimeout {
		t.Fatalf("hedged write took %s, expected less than upload timeout %s", elapsed, mtProtoChunkRelayUpTimeout)
	}

	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Fatalf("calls=%d want=2", calls)
	}
	if len(seqs) != 2 || seqs[0] != 1 || seqs[1] != 1 {
		t.Fatalf("seqs=%v want=[1 1]", seqs)
	}
}

func TestChunkRelayWriteDoesNotHedgeFastUpload(t *testing.T) {
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
		mu.Unlock()
		headers := make(http.Header)
		headers.Set("X-Tgws-Chunk-Ack", strconv.FormatInt(seq, 10))
		return http.StatusNoContent, headers, nil, nil
	})

	if _, err := conn.Write([]byte("fast")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Give any incorrectly scheduled hedge enough time to fire.
	time.Sleep(mtProtoChunkRelayUpHedgeDelay + 50*time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("calls=%d want=1", calls)
	}
}
