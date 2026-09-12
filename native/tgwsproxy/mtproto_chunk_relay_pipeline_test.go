package main

import (
	"context"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestChunkRelayPipelineStartsWindowBeforeWaitingForAck(t *testing.T) {
	started := make(chan int64, mtProtoChunkRelayUpWindow)
	release := make(chan struct{})

	conn := newTestChunkRelayConn(t, func(_ context.Context, action string, query url.Values, _ []byte) (int, http.Header, []byte, error) {
		if action != "up" {
			t.Fatalf("action=%s", action)
		}
		seq, err := strconv.ParseInt(query.Get("seq"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		started <- seq
		<-release
		headers := make(http.Header)
		headers.Set("X-Tgws-Chunk-Ack", strconv.FormatInt(seq, 10))
		return http.StatusNoContent, headers, nil, nil
	})

	payload := make([]byte, mtProtoChunkRelayUploadBytes*mtProtoChunkRelayUpWindow)
	done := make(chan error, 1)
	go func() {
		_, err := writePipelinedMtProtoChunkRelay(conn, payload)
		done <- err
	}()

	seqs := make([]int64, 0, mtProtoChunkRelayUpWindow)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for len(seqs) < mtProtoChunkRelayUpWindow {
		select {
		case seq := <-started:
			seqs = append(seqs, seq)
		case <-deadline.C:
			t.Fatalf("only %d uploads started before ACK release: %v", len(seqs), seqs)
		}
	}

	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	for i, seq := range seqs {
		want := int64(i + 1)
		if seq != want {
			t.Fatalf("seqs=%v want 1..%d", seqs, mtProtoChunkRelayUpWindow)
		}
	}

	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("pipeline write: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pipeline write did not complete")
	}
	if conn.upSeq != int64(mtProtoChunkRelayUpWindow) {
		t.Fatalf("upSeq=%d want=%d", conn.upSeq, mtProtoChunkRelayUpWindow)
	}
	if conn.upBytes != int64(len(payload)) {
		t.Fatalf("upBytes=%d want=%d", conn.upBytes, len(payload))
	}
}

func TestChunkRelaySlidingWindowRefillsAfterSingleAck(t *testing.T) {
	const totalChunks = mtProtoChunkRelayUpWindow + 1

	started := make(chan int64, totalChunks)
	releases := make(map[int64]chan struct{}, totalChunks)
	for seq := int64(1); seq <= totalChunks; seq++ {
		releases[seq] = make(chan struct{})
	}

	conn := newTestChunkRelayConn(t, func(_ context.Context, action string, query url.Values, _ []byte) (int, http.Header, []byte, error) {
		if action != "up" {
			t.Fatalf("action=%s", action)
		}
		seq, err := strconv.ParseInt(query.Get("seq"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		started <- seq
		<-releases[seq]
		headers := make(http.Header)
		headers.Set("X-Tgws-Chunk-Ack", strconv.FormatInt(seq, 10))
		return http.StatusNoContent, headers, nil, nil
	})

	payload := make([]byte, mtProtoChunkRelayUploadBytes*totalChunks)
	done := make(chan error, 1)
	go func() {
		_, err := writePipelinedMtProtoChunkRelay(conn, payload)
		done <- err
	}()

	seen := make(map[int64]bool, mtProtoChunkRelayUpWindow)
	deadline := time.NewTimer(time.Second)
	for len(seen) < mtProtoChunkRelayUpWindow {
		select {
		case seq := <-started:
			seen[seq] = true
		case <-deadline.C:
			deadline.Stop()
			t.Fatalf("only %d initial uploads started: %v", len(seen), seen)
		}
	}
	deadline.Stop()

	for seq := int64(1); seq <= mtProtoChunkRelayUpWindow; seq++ {
		if !seen[seq] {
			t.Fatalf("initial window missing seq=%d: %v", seq, seen)
		}
	}

	// Release only seq=1. A fixed window-sized batch would still wait for the
	// other in-flight chunks; a sliding window must immediately refill the
	// freed slot with the next sequence.
	close(releases[1])
	select {
	case seq := <-started:
		if seq != totalChunks {
			t.Fatalf("next started seq=%d want=%d", seq, totalChunks)
		}
	case <-time.After(time.Second):
		t.Fatal("sliding window did not launch the next chunk after seq=1 ACK")
	}

	for seq := int64(2); seq <= totalChunks; seq++ {
		close(releases[seq])
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("pipeline write: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sliding pipeline write did not complete")
	}

	if conn.upSeq != totalChunks {
		t.Fatalf("upSeq=%d want=%d", conn.upSeq, totalChunks)
	}
	if conn.upBytes != int64(len(payload)) {
		t.Fatalf("upBytes=%d want=%d", conn.upBytes, len(payload))
	}
}

func TestChunkRelayPipelineUses12KiBUploadChunks(t *testing.T) {
	if mtProtoChunkRelayUploadBytes != 12*1024 {
		t.Fatalf("upload chunk bytes=%d want=%d", mtProtoChunkRelayUploadBytes, 12*1024)
	}

	var mu sync.Mutex
	var sizes []int
	conn := newTestChunkRelayConn(t, func(_ context.Context, action string, query url.Values, body []byte) (int, http.Header, []byte, error) {
		if action != "up" {
			t.Fatalf("action=%s", action)
		}
		seq, err := strconv.ParseInt(query.Get("seq"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		sizes = append(sizes, len(body))
		mu.Unlock()
		headers := make(http.Header)
		headers.Set("X-Tgws-Chunk-Ack", strconv.FormatInt(seq, 10))
		return http.StatusNoContent, headers, nil, nil
	})

	payload := make([]byte, mtProtoChunkRelayUploadBytes+100)
	n, err := writePipelinedMtProtoChunkRelay(conn, payload)
	if err != nil {
		t.Fatalf("pipeline write: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("n=%d want=%d", n, len(payload))
	}

	mu.Lock()
	defer mu.Unlock()
	sort.Ints(sizes)
	if len(sizes) != 2 || sizes[0] != 100 || sizes[1] != mtProtoChunkRelayUploadBytes {
		t.Fatalf("sizes=%v", sizes)
	}
}

func TestChunkRelayWindow3Profile(t *testing.T) {
	if mtProtoChunkRelayUpWindow != 3 {
		t.Fatalf("window=%d want=3", mtProtoChunkRelayUpWindow)
	}
	if mtProtoChunkRelayPrimaryRequests != 12 {
		t.Fatalf("primary requests=%d want=12", mtProtoChunkRelayPrimaryRequests)
	}
	if mtProtoChunkRelayGlobalHTTPRequests != 18 {
		t.Fatalf("global HTTP requests=%d want=18", mtProtoChunkRelayGlobalHTTPRequests)
	}
	if mtProtoChunkRelayPipelineMode != "sliding-w3" {
		t.Fatalf("pipeline mode=%q want=%q", mtProtoChunkRelayPipelineMode, "sliding-w3")
	}
}
