package main

import "testing"

func TestNormalizeWorkerProbeIPFamily(t *testing.T) {
	tests := []struct {
		input string
		want  string
		ok    bool
	}{
		{"", workerProbeIPAuto, true},
		{"auto", workerProbeIPAuto, true},
		{"AUTO", workerProbeIPAuto, true},
		{"ipv4", workerProbeIPv4, true},
		{"4", workerProbeIPv4, true},
		{"tcp4", workerProbeIPv4, true},
		{"IPv6", workerProbeIPv6, true},
		{"6", workerProbeIPv6, true},
		{"tcp6", workerProbeIPv6, true},
		{"bogus", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, ok := normalizeWorkerProbeIPFamily(tt.input)
			if got != tt.want || ok != tt.ok {
				t.Fatalf("normalizeWorkerProbeIPFamily(%q) = (%q, %t), want (%q, %t)", tt.input, got, ok, tt.want, tt.ok)
			}
		})
	}
}
