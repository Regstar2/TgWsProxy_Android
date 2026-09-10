package main

import "testing"

func TestFlowsealBackpressureQueueLimitUsesRequestedSendBuffer(t *testing.T) {
	if got, want := flowsealBackpressureQueueLimit(524288), 262144; got != want {
		t.Fatalf("not-sent limit=%d want=%d", got, want)
	}
	if got := flowsealBackpressureQueueLimit(-1); got != -1 {
		t.Fatalf("unavailable not-sent limit=%d want=-1", got)
	}
}

func TestFlowsealBackpressureUsesOnlyNotSentBulkBytes(t *testing.T) {
	const (
		frameBytes      = 65550
		sendBufferBytes = 524288
	)

	cases := []struct {
		name         string
		frameBytes   int
		notSentBytes int
		want         bool
	}{
		{
			name:         "large_unsent_backlog",
			frameBytes:   frameBytes,
			notSentBytes: 459800,
			want:         true,
		},
		{
			name:         "half_buffer_is_allowed",
			frameBytes:   frameBytes,
			notSentBytes: 262144,
			want:         false,
		},
		{
			name:         "just_above_half_buffer_is_paced",
			frameBytes:   frameBytes,
			notSentBytes: 262145,
			want:         true,
		},
		{
			name:         "small_control_frame_is_not_paced",
			frameBytes:   128,
			notSentBytes: 500000,
			want:         false,
		},
		{
			name:         "not_sent_introspection_unavailable",
			frameBytes:   frameBytes,
			notSentBytes: -1,
			want:         false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldFlowsealBackpressure(tc.frameBytes, tc.notSentBytes, sendBufferBytes); got != tc.want {
				t.Fatalf("should backpressure=%t want=%t", got, tc.want)
			}
		})
	}
}
