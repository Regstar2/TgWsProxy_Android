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

func TestChunkRelayPipelineHedgesOnlyOldestUnacked(t *testing.T) {
	oldDelay := mtProtoChunkRelayHOLHedgeDelay
	mtProtoChunkRelayHOLHedgeDelay = 50 * time.Millisecond
	defer func() { mtProtoChunkRelayHOLHedgeDelay = oldDelay }()

	var mu sync.Mutex
	calls := map[int64]int{}

	conn := newTestChunkRelayConn(t, func(ctx context.Context, action string, query url.Values, _ []byte) (int, http.Header, []byte, error) {
		if action != "up" {
			t.Fatalf("action=%s", action)
		}
		seq, err := strconv.ParseInt(query.Get("seq"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}

		mu.Lock()
		calls[seq]++
		call := calls[seq]
		mu.Unlock()

		if seq == 1 && call == 1 {
			<-ctx.Done()
			return 0, nil, nil, ctx.Err()
		}

		headers := make(http.Header)
		headers.Set("X-Tgws-Chunk-Ack", strconv.FormatInt(seq, 10))
		return http.StatusNoContent, headers, nil, nil
	})

	payload := make([]byte, mtProtoChunkRelayBytes*mtProtoChunkRelayUpWindow)
	started := time.Now()
	n, err := writePipelinedMtProtoChunkRelay(conn, payload)
	if err != nil {
		t.Fatalf("pipeline write: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("n=%d want=%d", n, len(payload))
	}
	if elapsed := time.Since(started); elapsed >= mtProtoChunkRelayUpTimeout {
		t.Fatalf("HOL hedge took %s, expected less than upload timeout %s", elapsed, mtProtoChunkRelayUpTimeout)
	}

	mu.Lock()
	defer mu.Unlock()
	if calls[1] != 2 {
		t.Fatalf("seq1 calls=%d want=2", calls[1])
	}
	for seq := int64(2); seq <= int64(mtProtoChunkRelayUpWindow); seq++ {
		if calls[seq] != 1 {
			t.Fatalf("seq%d calls=%d want=1; only oldest unacked seq may hedge", seq, calls[seq])
		}
	}
}
