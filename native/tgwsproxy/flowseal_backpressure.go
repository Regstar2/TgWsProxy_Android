package main

import (
	"fmt"
	"net"
	"time"
)

const (
	flowsealBulkFrameMinBytes            = 64 * 1024
	flowsealBackpressurePollInterval     = 5 * time.Millisecond
	flowsealBackpressureProgressInterval = 10 * time.Second
)

// flowsealBackpressureQueueLimit returns the high-water mark used for data
// which has not yet left the local TCP sender. Linux reports SO_SNDBUF as twice
// the value requested by setsockopt, so half of the observed value corresponds
// to the 256 KiB buffer requested by Flowseal-compatible socket setup.
func flowsealBackpressureQueueLimit(sendBufferBytes int) int {
	if sendBufferBytes <= 0 {
		return -1
	}
	return sendBufferBytes / 2
}

func shouldFlowsealBackpressure(frameBytes, notSentBytes, sendBufferBytes int) bool {
	if frameBytes < flowsealBulkFrameMinBytes || notSentBytes < 0 {
		return false
	}
	limit := flowsealBackpressureQueueLimit(sendBufferBytes)
	return limit >= 0 && notSentBytes > limit
}

// waitFlowsealBackpressure applies pressure only to bytes which TCP has not
// sent yet (SIOCOUTQNSD). TIOCOUTQ includes sent-but-unacknowledged data and is
// retained only for diagnostics; waiting on that total queue caused #29 test
// sessions to be terminated even when the local sender had already handed data
// to the network.
//
// There is deliberately no synthetic timeout here. If genuine not-sent data
// remains above the high-water mark, keep applying backpressure until the
// queue drains or the WebSocket is closed by the real network/read path.
func waitFlowsealBackpressure(
	conn net.Conn,
	closed func() bool,
	sessionID string,
	sequence uint64,
	payloadBytes int,
	frameBytes int,
) error {
	sendBufferBytes := tcpSendBufferBytes(conn)
	notSentBytes := tcpNotSentBytes(conn)
	if !shouldFlowsealBackpressure(frameBytes, notSentBytes, sendBufferBytes) {
		return nil
	}

	limit := flowsealBackpressureQueueLimit(sendBufferBytes)
	started := time.Now()
	lastProgressLog := started
	if logInfo != nil {
		logInfo.Printf(
			"MTProto Worker Flowseal parity backpressure wait start session_id=%s seq=%d payload_bytes=%d frame_bytes=%d tcp_not_sent=%d tcp_not_sent_limit=%d tcp_send_queue=%d tcp_send_buffer_bytes=%d",
			sessionID, sequence, payloadBytes, frameBytes, notSentBytes, limit, tcpSendQueueBytes(conn), sendBufferBytes,
		)
	}

	for notSentBytes > limit {
		if closed != nil && closed() {
			return fmt.Errorf("WebSocket closed while waiting for Worker TCP not-sent queue to drain")
		}

		time.Sleep(flowsealBackpressurePollInterval)
		notSentBytes = tcpNotSentBytes(conn)
		if notSentBytes < 0 {
			// Queue introspection is Linux/Android-specific. If it becomes
			// unavailable, preserve the transport behavior instead of turning a
			// diagnostic capability failure into a connection failure.
			return nil
		}

		if logInfo != nil && time.Since(lastProgressLog) >= flowsealBackpressureProgressInterval {
			logInfo.Printf(
				"MTProto Worker Flowseal parity backpressure still waiting session_id=%s seq=%d payload_bytes=%d frame_bytes=%d wait_ms=%d tcp_not_sent=%d tcp_not_sent_limit=%d tcp_send_queue=%d tcp_send_buffer_bytes=%d",
				sessionID, sequence, payloadBytes, frameBytes, time.Since(started).Milliseconds(), notSentBytes, limit, tcpSendQueueBytes(conn), sendBufferBytes,
			)
			lastProgressLog = time.Now()
		}
	}

	if logInfo != nil {
		logInfo.Printf(
			"MTProto Worker Flowseal parity backpressure wait end session_id=%s seq=%d payload_bytes=%d frame_bytes=%d wait_ms=%d tcp_not_sent=%d tcp_not_sent_limit=%d tcp_send_queue=%d tcp_send_buffer_bytes=%d",
			sessionID, sequence, payloadBytes, frameBytes, time.Since(started).Milliseconds(), notSentBytes, limit, tcpSendQueueBytes(conn), sendBufferBytes,
		)
	}
	return nil
}
