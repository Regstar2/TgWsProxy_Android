package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"testing"
)

func TestChunkRelayResponseClassification(t *testing.T) {
	tests := []struct {
		name       string
		action     string
		status     int
		relayError string
		wantErr    bool
		retryable  bool
		terminal   bool
	}{
		{name: "up success", action: "up", status: http.StatusNoContent},
		{name: "down payload", action: "down", status: http.StatusOK},
		{name: "down empty", action: "down", status: http.StatusNoContent},
		{name: "session lost", action: "down", status: http.StatusGone, relayError: "session_lost", wantErr: true, terminal: true},
		{name: "target conflict", action: "open", status: http.StatusConflict, relayError: "target_mismatch", wantErr: true},
		{name: "transient 502", action: "down", status: http.StatusBadGateway, relayError: "socket_read_failed", wantErr: true, retryable: true},
		{name: "transient 503", action: "open", status: http.StatusServiceUnavailable, relayError: "transient_worker_error", wantErr: true, retryable: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			headers := make(http.Header)
			if tc.relayError != "" {
				headers.Set(chunkRelayErrorHeader, tc.relayError)
			}
			err := chunkRelayResponseError(tc.action, tc.status, headers)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
			}
			if got := chunkRelayErrorRetryable(err); got != tc.retryable {
				t.Fatalf("retryable=%v want=%v err=%v", got, tc.retryable, err)
			}
			if got := chunkRelayTerminalSessionError(err); got != tc.terminal {
				t.Fatalf("terminal=%v want=%v err=%v", got, tc.terminal, err)
			}
			if tc.wantErr {
				status, relayError, ok := chunkRelayHTTPErrorDetails(err)
				if !ok || status != tc.status || relayError != tc.relayError {
					t.Fatalf("details status=%d relayError=%q ok=%v", status, relayError, ok)
				}
			}
		})
	}
}

func TestChunkRelayReadTreats410AsTerminalWithoutRetry(t *testing.T) {
	calls := 0
	conn := newTestChunkRelayConn(t, func(_ context.Context, action string, _ url.Values, _ []byte) (int, http.Header, []byte, error) {
		calls++
		headers := make(http.Header)
		headers.Set(chunkRelayErrorHeader, "session_lost")
		return http.StatusGone, headers, nil, nil
	})

	_, err := conn.Read(make([]byte, 1))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err=%v want EOF", err)
	}
	if calls != 1 {
		t.Fatalf("calls=%d want=1", calls)
	}
}
