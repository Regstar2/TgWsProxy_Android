package main

import "testing"

func TestFlowsealDrainWatermarksMatchAsyncioDefaults(t *testing.T) {
	if got, want := flowsealDrainHighWaterBytes, 64*1024; got != want {
		t.Fatalf("high water=%d want=%d", got, want)
	}
	if got, want := flowsealDrainLowWaterBytes, 16*1024; got != want {
		t.Fatalf("low water=%d want=%d", got, want)
	}
}

func TestShouldFlowsealDrainBulkOutstandingQueue(t *testing.T) {
	const frameBytes = 65550

	cases := []struct {
		name       string
		frameBytes int
		queueBytes int
		want       bool
	}{
		{
			name:       "at_high_water_is_allowed",
			frameBytes: frameBytes,
			queueBytes: 64 * 1024,
			want:       false,
		},
		{
			name:       "above_high_water_drains",
			frameBytes: frameBytes,
			queueBytes: 64*1024 + 1,
			want:       true,
		},
		{
			name:       "issue29_plateau_drains",
			frameBytes: frameBytes,
			queueBytes: 459800,
			want:       true,
		},
		{
			name:       "small_frame_does_not_trigger_bulk_drain",
			frameBytes: 4096,
			queueBytes: 459800,
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
			if got := shouldFlowsealDrain(tc.frameBytes, tc.queueBytes); got != tc.want {
				t.Fatalf("should drain=%t want=%t", got, tc.want)
			}
		})
	}
}
