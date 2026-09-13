package main

import (
	"bytes"
	"testing"
)

type flowsealWriteCapture struct {
	writes []int
	data   bytes.Buffer
}

func (w *flowsealWriteCapture) Write(p []byte) (int, error) {
	w.writes = append(w.writes, len(p))
	return w.data.Write(p)
}

func TestWriteFlowsealShapedFullCount(t *testing.T) {
	payload := bytes.Repeat([]byte{0x5a}, 2500)
	capture := &flowsealWriteCapture{}

	n, err := writeFlowsealShapedFullCount(capture, payload, 1200, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(payload) {
		t.Fatalf("written=%d want=%d", n, len(payload))
	}
	wantWrites := []int{1200, 1200, 100}
	if len(capture.writes) != len(wantWrites) {
		t.Fatalf("writes=%v want=%v", capture.writes, wantWrites)
	}
	for i, want := range wantWrites {
		if capture.writes[i] != want {
			t.Fatalf("writes=%v want=%v", capture.writes, wantWrites)
		}
	}
	if !bytes.Equal(capture.data.Bytes(), payload) {
		t.Fatal("shaping changed payload bytes")
	}
}

func TestWorkerProbeTLSProfileForTransport(t *testing.T) {
	tests := []struct {
		transport string
		chunk     int
		dynamic   bool
	}{
		{workerProbeTransportGoDynamic, 0, true},
		{workerProbeTransportGoSplit1200, 1200, false},
		{workerProbeTransportGoSplit4K, 4 * 1024, false},
		{workerProbeTransportGoSplit16K, 16 * 1024, false},
		{workerProbeTransportGoPaced4K, 4 * 1024, false},
	}
	for _, tt := range tests {
		profile, ok := workerProbeTLSProfileForTransport(tt.transport)
		if !ok {
			t.Fatalf("missing profile for %s", tt.transport)
		}
		if profile.WriteChunkBytes != tt.chunk {
			t.Fatalf("%s chunk=%d want=%d", tt.transport, profile.WriteChunkBytes, tt.chunk)
		}
		if gotDynamic := !profile.DynamicRecordSizingDisabled; gotDynamic != tt.dynamic {
			t.Fatalf("%s dynamic=%t want=%t", tt.transport, gotDynamic, tt.dynamic)
		}
	}
	if workerProbeTLSProfilePaced4K.WritePace <= 0 {
		t.Fatal("paced profile must have a positive delay")
	}
}
