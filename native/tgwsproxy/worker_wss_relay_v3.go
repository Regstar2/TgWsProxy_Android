package main

import "fmt"

// workerWSSRelayV3FrameSocket adapts the existing raw Worker stream to the
// message semantics expected by Telegram's /apiws endpoint. The first Send is
// the 64-byte MTProto relay_init. Subsequent transformed TCP writes are fed to
// MsgSplitter and emitted only as complete MTProto packet WebSocket messages.
//
// This wrapper is installed only by the experimental Flowseal-parity Worker
// dial. Legacy /apiws -> raw TCP Worker paths retain their v2 chunk semantics.
type workerWSSRelayV3FrameSocket struct {
	inner       mtProtoFrameSocket
	splitter    *MsgSplitter
	initialized bool
}

func newWorkerWSSRelayV3FrameSocket(inner mtProtoFrameSocket) mtProtoFrameSocket {
	return &workerWSSRelayV3FrameSocket{inner: inner}
}

func (s *workerWSSRelayV3FrameSocket) Send(data []byte) error {
	if !s.initialized {
		splitter, err := newMsgSplitter(data)
		if err != nil {
			return fmt.Errorf("initialize Worker v3 MTProto packet splitter: %w", err)
		}
		s.splitter = splitter
		s.initialized = true
		return s.inner.Send(data)
	}

	parts := s.splitter.Split(data)
	if len(parts) == 0 {
		return nil
	}
	return s.inner.SendBatch(parts)
}

func (s *workerWSSRelayV3FrameSocket) SendBatch(parts [][]byte) error {
	return s.inner.SendBatch(parts)
}

func (s *workerWSSRelayV3FrameSocket) Recv() ([]byte, error) {
	return s.inner.Recv()
}

func (s *workerWSSRelayV3FrameSocket) Close() {
	s.inner.Close()
}

var _ mtProtoFrameSocket = (*workerWSSRelayV3FrameSocket)(nil)
