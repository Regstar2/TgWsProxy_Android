package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

type chunkRelayUploadObservation struct {
	seq   int64
	bytes int
}

func TestWorkerStreamUsesVerifiedChunkRelayUploadProfile(t *testing.T) {
	if mtProtoChunkRelayUploadBytes != 12*1024 {
		t.Fatalf("upload chunk bytes=%d want=%d", mtProtoChunkRelayUploadBytes, 12*1024)
	}
	if mtProtoChunkRelayUpWindow != 3 {
		t.Fatalf("window=%d want=3", mtProtoChunkRelayUpWindow)
	}
	if mtProtoChunkRelayPrimaryRequests != 12 {
		t.Fatalf("primary requests=%d want=12", mtProtoChunkRelayPrimaryRequests)
	}
	if mtProtoChunkRelayGlobalHTTPRequests != 18 {
		t.Fatalf("global HTTP requests=%d want=18", mtProtoChunkRelayGlobalHTTPRequests)
	}
	if mtProtoChunkRelayHOLHedgeDelay != 900*time.Millisecond {
		t.Fatalf("HOL hedge delay=%s want=%s", mtProtoChunkRelayHOLHedgeDelay, 900*time.Millisecond)
	}
	if mtProtoChunkRelayPipelineMode != "sliding-w3" {
		t.Fatalf("pipeline mode=%q want=%q", mtProtoChunkRelayPipelineMode, "sliding-w3")
	}

	started := make(chan chunkRelayUploadObservation, mtProtoChunkRelayUpWindow)
	release := make(chan struct{})
	conn := newTestChunkRelayConn(t, func(_ context.Context, action string, query url.Values, body []byte) (int, http.Header, []byte, error) {
		if action != "up" {
			t.Fatalf("action=%s", action)
		}
		seq, err := strconv.ParseInt(query.Get("seq"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		started <- chunkRelayUploadObservation{seq: seq, bytes: len(body)}
		<-release
		headers := make(http.Header)
		headers.Set("X-Tgws-Chunk-Ack", strconv.FormatInt(seq, 10))
		return http.StatusNoContent, headers, nil, nil
	})

	stream := &mtProtoWebSocketStream{
		socket: &mtProtoChunkRelayFrameSocket{conn: conn},
	}
	payload := make([]byte, mtProtoChunkRelayUploadBytes*mtProtoChunkRelayUpWindow)
	done := make(chan error, 1)
	go func() {
		n, err := stream.Write(payload)
		if err == nil && n != len(payload) {
			err = fmt.Errorf("written=%d want=%d", n, len(payload))
		}
		done <- err
	}()

	observations := make([]chunkRelayUploadObservation, 0, mtProtoChunkRelayUpWindow)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for len(observations) < mtProtoChunkRelayUpWindow {
		select {
		case observation := <-started:
			observations = append(observations, observation)
		case <-deadline.C:
			t.Fatalf("only %d uploads reached the scheduler before ACK release: %v", len(observations), observations)
		}
	}

	sort.Slice(observations, func(i, j int) bool { return observations[i].seq < observations[j].seq })
	for index, observation := range observations {
		wantSeq := int64(index + 1)
		if observation.seq != wantSeq {
			t.Fatalf("observations=%v want seq 1..%d", observations, mtProtoChunkRelayUpWindow)
		}
		if observation.bytes != mtProtoChunkRelayUploadBytes {
			t.Fatalf("seq=%d bytes=%d want=%d", observation.seq, observation.bytes, mtProtoChunkRelayUploadBytes)
		}
	}

	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Worker stream write: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Worker stream write did not complete")
	}
}

func TestWorkerStreamRetryKeepsSequenceAndCommitsPayloadOnce(t *testing.T) {
	payload := bytes.Repeat([]byte{0x5a}, mtProtoChunkRelayUploadBytes)
	var mu sync.Mutex
	var seqs []int64
	var bodies [][]byte
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
		bodies = append(bodies, append([]byte(nil), body...))
		if !failedFirst {
			failedFirst = true
			mu.Unlock()
			return 0, nil, nil, errors.New("synthetic timeout")
		}
		mu.Unlock()

		headers := make(http.Header)
		headers.Set("X-Tgws-Chunk-Ack", strconv.FormatInt(seq, 10))
		return http.StatusNoContent, headers, nil, nil
	})

	stream := &mtProtoWebSocketStream{
		socket: &mtProtoChunkRelayFrameSocket{conn: conn},
	}
	n, err := stream.Write(payload)
	if err != nil {
		t.Fatalf("Worker stream write: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("written=%d want=%d", n, len(payload))
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seqs) < 2 {
		t.Fatalf("attempts=%d want at least 2", len(seqs))
	}
	for index, seq := range seqs {
		if seq != 1 {
			t.Fatalf("attempt %d seq=%d want=1; all retries must keep the same sequence", index+1, seq)
		}
		if !bytes.Equal(bodies[index], payload) {
			t.Fatalf("attempt %d changed payload", index+1)
		}
	}
	if conn.upSeq != 1 {
		t.Fatalf("upSeq=%d want=1", conn.upSeq)
	}
	if conn.upBytes != int64(len(payload)) {
		t.Fatalf("upBytes=%d want=%d", conn.upBytes, len(payload))
	}
}
