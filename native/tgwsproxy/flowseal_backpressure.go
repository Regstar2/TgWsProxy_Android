package main

import (
	"fmt"
	"net"
	"time"
)

const (
	flowsealBulkFrameMinBytes        = 64 * 1024
	flowsealDrainHighWaterBytes      = 64 * 1024
	flowsealDrainLowWaterBytes       = 16 * 1024
	flowsealDrainPollInterval        = 5 * time.Millisecond
	flowsealDrainProgressInterval    = 10 * time.Second
)

func shouldFlowsealDrain(frameBytes, queueBytes int) bool {
	return frameBytes >= flowsealBulkFrameMinBytes && queueBytes > flowsealDrainHighWaterBytes
}

// drainFlowsealAfterWrite approximates the point at which Flowseal's
// asyncio StreamWriter.write(frame); await writer.drain() applies flow control.
//
// CPython's default transport write-buffer watermarks are 64 KiB high and
// 16 KiB low. Android exposes TIOCOUTQ reliably while SIOCOUTQNSD is not
// available on the tested device, so this deliberately uses the total TCP
// outstanding queue as a conservative pacing signal. The important semantic
// difference from the previous #29 experiment is that pacing happens AFTER a
// completed WebSocket frame, matching Flowseal's write-then-drain order.
//
// There is no synthetic timeout. If the peer stops making progress, the real
// read/network path closes the WebSocket and wakes this loop through closed().
func drainFlowsealAfterWrite(
	conn net.Conn,
	closed func() bool,
	sessionID string,
	sequence uint64,
	payloadBytes int,
	frameBytes int,
) error {
	queueBytes := tcpSendQueueBytes(conn)
	if !shouldFlowsealDrain(frameBytes, queueBytes) {
		return nil
	}

	started := time.Now()
	lastProgressLog := started
	if logInfo != nil {
		logInfo.Printf(
			"MTProto Worker Flowseal parity drain wait start session_id=%s seq=%d payload_bytes=%d frame_bytes=%d tcp_send_queue=%d drain_high_water=%d drain_low_water=%d tcp_not_sent=%d tcp_send_buffer_bytes=%d",
			sessionID,
			sequence,
			payloadBytes,
			frameBytes,
			queueBytes,
			flowsealDrainHighWaterBytes,
			flowsealDrainLowWaterBytes,
			tcpNotSentBytes(conn),
			tcpSendBufferBytes(conn),
		)
	}

	for queueBytes > flowsealDrainLowWaterBytes {
		if closed != nil && closed() {
			return fmt.Errorf("WebSocket closed while waiting for Flowseal-style drain")
		}

		time.Sleep(flowsealDrainPollInterval)
		queueBytes = tcpSendQueueBytes(conn)
		if queueBytes < 0 {
			// TIOCOUTQ is platform-specific. If introspection becomes unavailable,
			// preserve transport behavior instead of turning diagnostics into a
			// connection failure.
			return nil
		}

		if logInfo != nil && time.Since(lastProgressLog) >= flowsealDrainProgressInterval {
			logInfo.Printf(
				"MTProto Worker Flowseal parity drain still waiting session_id=%s seq=%d payload_bytes=%d frame_bytes=%d wait_ms=%d tcp_send_queue=%d drain_high_water=%d drain_low_water=%d tcp_not_sent=%d tcp_send_buffer_bytes=%d",
				sessionID,
				sequence,
				payloadBytes,
				frameBytes,
				time.Since(started).Milliseconds(),
				queueBytes,
				flowsealDrainHighWaterBytes,
				flowsealDrainLowWaterBytes,
				tcpNotSentBytes(conn),
				tcpSendBufferBytes(conn),
			)
			lastProgressLog = time.Now()
		}
	}

	if logInfo != nil {
		logInfo.Printf(
			"MTProto Worker Flowseal parity drain wait end session_id=%s seq=%d payload_bytes=%d frame_bytes=%d wait_ms=%d tcp_send_queue=%d drain_high_water=%d drain_low_water=%d tcp_not_sent=%d tcp_send_buffer_bytes=%d",
			sessionID,
			sequence,
			payloadBytes,
			frameBytes,
			time.Since(started).Milliseconds(),
			queueBytes,
			flowsealDrainHighWaterBytes,
			flowsealDrainLowWaterBytes,
			tcpNotSentBytes(conn),
			tcpSendBufferBytes(conn),
		)
	}
	return nil
}
