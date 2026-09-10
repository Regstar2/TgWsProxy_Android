package main

import "testing"

func TestFlowsealBackpressureQueueLimitUsesHalfObservedSendBuffer(t *testing.T) {
	if got, want := flowsealBackpressureQueueLimit(524288), 262144; got != want {
		t.Fatalf("queue limit=%d want=%d", got, want)
	}
	if got := flowsealBackpressureQueueLimit(-1); got != -1 {
		t.Fatalf("unavailable queue limit=%d want=-1", got)
	}
}

func TestFlowsealBackpressureDetectsIssue29BulkQueueSaturation(t *testing.T) {
	const (
		frameBytes      = 65550
		sendBufferBytes = 524288
	)

	cases := []struct {
		name       string
		frameBytes int
		queueBytes int
		want       bool
	}{
		{
			name:       "issue29_seq9_queue",
			frameBytes: frameBytes,
			queueBytes: 459800,
			want:       true,
		},
		{
			name:       "queue_above_reported_buffer",
			frameBytes: frameBytes,
			queueBytes: 533019,
			want:       true,
		},
		{
			name:       "half_buffer_is_allowed",
			frameBytes: frameBytes,
			queueBytes: 262144,
			want:       false,
		},
		{
			name:       "just_above_half_buffer_is_paced",
			frameBytes: frameBytes,
			queueBytes: 262145,
			want:       true,
		},
		{
			name:       "small_control_frame_is_not_paced",
			frameBytes: 128,
			queueBytes: 500000,
			want:       false,
		},
		{
			name:       "queue_introspection_unavailable",
			frameBytes: frameBytes,
			queueBytes: -1,
			want:       false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldFlowsealBackpressure(tc.frameBytes, tc.queueBytes, sendBufferBytes); got != tc.want {
				t.Fatalf("should backpressure=%t want=%t", got, tc.want)
			}
		})
	}
}
