package main

import (
	"context"
	"net/http"
	"net/url"
	"sort"
	"strconv"
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

	payload := make([]byte, mtProtoChunkRelayBytes*mtProtoChunkRelayUpWindow)
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
