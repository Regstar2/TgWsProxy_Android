package main

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
)

const chunkRelayErrorHeader = "X-Tgws-Relay-Error"

type chunkRelayHTTPError struct {
	Action     string
	Status     int
	RelayError string
}

func (e *chunkRelayHTTPError) Error() string {
	if e == nil {
		return "chunk relay HTTP error"
	}
	relayError := strings.TrimSpace(e.RelayError)
	if relayError == "" {
		relayError = "unspecified"
	}
	return fmt.Sprintf("chunk relay action=%s HTTP %d relay_error=%s", e.Action, e.Status, relayError)
}

func chunkRelayExpectedStatus(action string, status int) bool {
	switch action {
	case "open", "up", "close":
		return status == http.StatusNoContent
	case "down":
		return status == http.StatusOK || status == http.StatusNoContent
	default:
		return status >= 200 && status < 300
	}
}

func chunkRelayResponseError(action string, status int, headers http.Header) error {
	if chunkRelayExpectedStatus(action, status) {
		return nil
	}
	relayError := ""
	if headers != nil {
		relayError = strings.TrimSpace(headers.Get(chunkRelayErrorHeader))
	}
	if relayError == "" {
		relayError = fmt.Sprintf("http_%d", status)
	}
	return &chunkRelayHTTPError{
		Action:     action,
		Status:     status,
		RelayError: relayError,
	}
}

func chunkRelayHTTPErrorDetails(err error) (status int, relayError string, ok bool) {
	var target *chunkRelayHTTPError
	if !errors.As(err, &target) || target == nil {
		return 0, "", false
	}
	return target.Status, strings.TrimSpace(target.RelayError), true
}

func chunkRelayErrorRetryable(err error) bool {
	if err == nil {
		return false
	}
	if _, circuitOpen := workerCircuitError(err); circuitOpen {
		return false
	}
	status, _, ok := chunkRelayHTTPErrorDetails(err)
	if !ok {
		// Network/TLS/timeouts remain bounded-retryable.
		return true
	}
	return status >= 500
}

func chunkRelayTerminalSessionError(err error) bool {
	status, _, ok := chunkRelayHTTPErrorDetails(err)
	return ok && status == http.StatusGone
}

func chunkRelayRetryDecision(err error, attempt int) string {
	if err == nil {
		return "success"
	}
	if _, circuitOpen := workerCircuitError(err); circuitOpen {
		return "circuit"
	}
	if chunkRelayTerminalSessionError(err) {
		return "terminal"
	}
	if !chunkRelayErrorRetryable(err) {
		return "reject"
	}
	if attempt >= mtProtoChunkRelayMaxRetries {
		return "fail"
	}
	return "retry"
}

func logChunkRelayResponseDecision(
	c *mtProtoChunkRelayConn,
	action, direction string,
	seq int64,
	status int,
	err error,
	attempt int,
) {
	if logInfo == nil || c == nil || err == nil {
		return
	}
	relayError := "transport_error"
	if _, value, ok := chunkRelayHTTPErrorDetails(err); ok && value != "" {
		relayError = value
	} else if circuit, ok := workerCircuitError(err); ok {
		relayError = circuit.Reason
	}
	logInfo.Printf(
		"MTProto Worker chunk relay response session_id=%s action=%s direction=%s seq=%d status=%d relay_error=%s retry_decision=%s attempt=%d/%d",
		c.sessionID,
		action,
		direction,
		seq,
		status,
		mtProtoStatusField(relayError),
		chunkRelayRetryDecision(err, attempt),
		attempt+1,
		mtProtoChunkRelayMaxRetries+1,
	)
}
