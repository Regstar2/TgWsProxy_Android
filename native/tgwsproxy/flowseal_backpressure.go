package main

import (
	"fmt"
	"net"
	"time"
)

const (
	flowsealBulkFrameMinBytes        = 64 * 1024
	flowsealBackpressurePollInterval = 5 * time.Millisecond
	flowsealBackpressureMaxWait      = 10 * time.Second
)

// flowsealBackpressureQueueLimit returns the application-side high-water mark
// used for Android/Linux Worker writes. Linux reports SO_SNDBUF as twice the
// value requested by setsockopt, so half of the observed value corresponds to
// the 256 KiB buffer requested by the Flowseal-compatible socket setup.
//
// This is deliberately not described as equivalent to asyncio
// StreamWriter.drain(). It is a queue-aware guard derived from the #29 device
// trace: unpaced Go TLS writes filled TIOCOUTQ to roughly 450-533 KiB with an
// observed SO_SNDBUF of 524288, after which the next 64 KiB frame blocked.
func flowsealBackpressureQueueLimit(sendBufferBytes int) int {
	if sendBufferBytes <= 0 {
		return -1
	}
	return sendBufferBytes / 2
}

func shouldFlowsealBackpressure(frameBytes, queueBytes, sendBufferBytes int) bool {
	if frameBytes < flowsealBulkFrameMinBytes || queueBytes < 0 {
		return false
	}
	limit := flowsealBackpressureQueueLimit(sendBufferBytes)
	return limit >= 0 && queueBytes > limit
}

// waitFlowsealBackpressure prevents a bulk Worker WebSocket producer from
// filling the kernel TCP send queue to the point where tls.Conn.Write blocks
// for minutes. Small/control frames are intentionally unaffected.
func waitFlowsealBackpressure(
	conn net.Conn,
	closed func() bool,
	sessionID string,
	sequence uint64,
	payloadBytes int,
	frameBytes int,
) error {
	sendBufferBytes := tcpSendBufferBytes(conn)
	queueBytes := tcpSendQueueBytes(conn)
	if !shouldFlowsealBackpressure(frameBytes, queueBytes, sendBufferBytes) {
		return nil
	}

	limit := flowsealBackpressureQueueLimit(sendBufferBytes)
	started := time.Now()
	if logInfo != nil {
		logInfo.Printf(
			"MTProto Worker Flowseal parity backpressure wait start session_id=%s seq=%d payload_bytes=%d frame_bytes=%d tcp_send_queue=%d tcp_send_queue_limit=%d tcp_send_buffer_bytes=%d",
			sessionID, sequence, payloadBytes, frameBytes, queueBytes, limit, sendBufferBytes,
		)
	}

	deadline := started.Add(flowsealBackpressureMaxWait)
	for queueBytes > limit {
		if closed != nil && closed() {
			return fmt.Errorf("WebSocket closed while waiting for Worker send queue to drain")
		}
		if time.Now().After(deadline) {
			if logInfo != nil {
				logInfo.Printf(
					"MTProto Worker Flowseal parity backpressure timeout session_id=%s seq=%d payload_bytes=%d frame_bytes=%d wait_ms=%d tcp_send_queue=%d tcp_send_queue_limit=%d tcp_send_buffer_bytes=%d",
					sessionID, sequence, payloadBytes, frameBytes, time.Since(started).Milliseconds(), queueBytes, limit, sendBufferBytes,
				)
			}
			return fmt.Errorf(
				"Worker TCP send queue did not drain below %d bytes within %s (queue=%d)",
				limit,
				flowsealBackpressureMaxWait,
				queueBytes,
			)
		}

		time.Sleep(flowsealBackpressurePollInterval)
		queueBytes = tcpSendQueueBytes(conn)
		if queueBytes < 0 {
			// Queue introspection is diagnostic/platform-specific. If it becomes
			// unavailable, preserve the existing transport behavior rather than
			// turning that into a connection failure.
			return nil
		}
	}

	if logInfo != nil {
		logInfo.Printf(
			"MTProto Worker Flowseal parity backpressure wait end session_id=%s seq=%d payload_bytes=%d frame_bytes=%d wait_ms=%d tcp_send_queue=%d tcp_send_queue_limit=%d tcp_send_buffer_bytes=%d",
			sessionID, sequence, payloadBytes, frameBytes, time.Since(started).Milliseconds(), queueBytes, limit, sendBufferBytes,
		)
	}
	return nil
}
