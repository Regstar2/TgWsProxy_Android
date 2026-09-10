//go:build (linux || android) && (amd64 || arm64)

package main

import (
	"encoding/binary"
	"strings"
	"testing"
)

func TestTCPInfoOldKernelDoesNotInventAbsentCounters(t *testing.T) {
	value := make([]byte, 104)
	binary.LittleEndian.PutUint32(value[24:], 7)
	got := formatTCPInfo(value)
	for _, want := range []string{"unacked=7", "bytes_acked=-1", "bytes_sent=-1", "notsent=-1", "snd_wnd=-1"} {
		if !strings.Contains(got, want) {
			t.Fatalf("%q missing %q", got, want)
		}
	}
}

func TestTCPInfoReportsDeliveryAndUnsentCounters(t *testing.T) {
	value := make([]byte, 232)
	binary.LittleEndian.PutUint64(value[120:], 512000)
	binary.LittleEndian.PutUint64(value[200:], 520000)
	binary.LittleEndian.PutUint32(value[144:], 8000)
	got := formatTCPInfo(value)
	for _, want := range []string{"bytes_acked=512000", "bytes_sent=520000", "notsent=8000"} {
		if !strings.Contains(got, want) {
			t.Fatalf("%q missing %q", got, want)
		}
	}
}
