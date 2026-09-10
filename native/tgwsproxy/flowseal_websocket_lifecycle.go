package main

import "time"

func (ws *flowsealRawWebSocket) SetDeadline(t time.Time) error {
	if err := ws.SetReadDeadline(t); err != nil {
		return err
	}
	return ws.SetWriteDeadline(t)
}

func (ws *flowsealRawWebSocket) SetReadDeadline(t time.Time) error {
	return ws.conn.SetReadDeadline(t)
}

func (ws *flowsealRawWebSocket) SetWriteDeadline(t time.Time) error {
	ws.deadlineMu.Lock()
	defer ws.deadlineMu.Unlock()
	ws.writeDeadline = t
	return ws.applyWriteDeadline()
}

func (ws *flowsealRawWebSocket) beginFrameWrite() error {
	ws.deadlineMu.Lock()
	defer ws.deadlineMu.Unlock()
	ws.frameDeadline = time.Now().Add(flowsealFrameWriteTimeout)
	return ws.applyWriteDeadline()
}

func (ws *flowsealRawWebSocket) endFrameWrite() {
	ws.deadlineMu.Lock()
	defer ws.deadlineMu.Unlock()
	ws.frameDeadline = time.Time{}
	_ = ws.applyWriteDeadline()
}

// Called with deadlineMu held. A caller's earlier deadline always wins.
func (ws *flowsealRawWebSocket) applyWriteDeadline() error {
	deadline := ws.writeDeadline
	if !ws.frameDeadline.IsZero() && (deadline.IsZero() || ws.frameDeadline.Before(deadline)) {
		deadline = ws.frameDeadline
	}
	return ws.conn.SetWriteDeadline(deadline)
}

// Observe TCP while Write is blocked, not only after it eventually returns.
// No payload or TLS key material is logged.
func (ws *flowsealRawWebSocket) traceBlockedWrite(sequence uint64, enabled bool) func() {
	if !enabled || logInfo == nil {
		return func() {}
	}
	done := make(chan struct{})
	stopped := make(chan struct{})
	started := time.Now()
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				logInfo.Printf("MTProto Worker TCP stalled session_id=%s seq=%d elapsed_ms=%d %s",
					ws.logSessionID(), sequence, time.Since(started).Milliseconds(), tcpTransportState(ws.conn))
			}
		}
	}()
	return func() {
		close(done)
		<-stopped
	}
}
