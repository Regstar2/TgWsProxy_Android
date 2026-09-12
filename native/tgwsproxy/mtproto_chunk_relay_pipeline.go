package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	mtProtoChunkRelayUploadBytes       = 12 * 1024
	mtProtoChunkRelayUpWindow          = 2
	mtProtoChunkRelayPrimaryRequests   = 8
	mtProtoChunkRelayGlobalHTTPRequests = 12
	mtProtoChunkRelayPipelineMode      = "sliding-w2"
)

var (
	mtProtoChunkRelayHTTPSlots     = make(chan struct{}, mtProtoChunkRelayGlobalHTTPRequests)
	mtProtoChunkRelayPrimarySlots  = make(chan struct{}, mtProtoChunkRelayPrimaryRequests)
	mtProtoChunkRelayHOLHedgeDelay = 900 * time.Millisecond
)

func enableMtProtoChunkRelayRequestLimit(c *mtProtoChunkRelayConn) {
	base := c.request
	c.request = func(ctx context.Context, action string, query url.Values, body []byte) (int, http.Header, []byte, error) {
		select {
		case mtProtoChunkRelayHTTPSlots <- struct{}{}:
			defer func() { <-mtProtoChunkRelayHTTPSlots }()
		case <-ctx.Done():
			return 0, nil, nil, ctx.Err()
		case <-c.lifeCtx.Done():
			return 0, nil, nil, net.ErrClosed
		}
		return base(ctx, action, query, body)
	}
}

func acquireMtProtoChunkRelayPrimarySlot(c *mtProtoChunkRelayConn) error {
	select {
	case mtProtoChunkRelayPrimarySlots <- struct{}{}:
		return nil
	case <-c.lifeCtx.Done():
		return net.ErrClosed
	}
}

func releaseMtProtoChunkRelayPrimarySlot() {
	<-mtProtoChunkRelayPrimarySlots
}

type mtProtoChunkUploadSpec struct {
	seq   int64
	start int
	end   int
}

type mtProtoChunkUploadResult struct {
	spec    mtProtoChunkUploadSpec
	status  int
	headers http.Header
	err     error
}

type mtProtoChunkRelayHeadTracker struct {
	mu      sync.Mutex
	head    int64
	done    map[int64]struct{}
	changed chan struct{}
}

func newMtProtoChunkRelayHeadTracker(head int64) *mtProtoChunkRelayHeadTracker {
	return &mtProtoChunkRelayHeadTracker{
		head:    head,
		done:    make(map[int64]struct{}),
		changed: make(chan struct{}),
	}
}

func (t *mtProtoChunkRelayHeadTracker) state(seq int64) (isHead bool, alreadyDone bool, changed <-chan struct{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return seq == t.head, seq < t.head, t.changed
}

func (t *mtProtoChunkRelayHeadTracker) markDone(seq int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if seq < t.head {
		return
	}
	t.done[seq] = struct{}{}
	advanced := false
	for {
		if _, ok := t.done[t.head]; !ok {
			break
		}
		delete(t.done, t.head)
		t.head++
		advanced = true
	}
	if advanced {
		close(t.changed)
		t.changed = make(chan struct{})
	}
}

func (c *mtProtoChunkRelayConn) requestPipelinedUploadWithRetry(
	parent context.Context,
	query url.Values,
	body []byte,
	seq int64,
	head *mtProtoChunkRelayHeadTracker,
) (int, http.Header, []byte, error) {
	if parent == nil {
		parent = context.Background()
	}
	var lastErr error
	for attempt := 0; attempt <= mtProtoChunkRelayMaxRetries; attempt++ {
		status, headers, responseBody, err := c.requestPipelinedUploadRound(parent, query, body, seq, head)
		if err == nil {
			return status, headers, responseBody, nil
		}
		lastErr = err
		if attempt >= mtProtoChunkRelayMaxRetries {
			break
		}
		if logInfo != nil {
			logInfo.Printf(
				"MTProto Worker chunk relay retry session_id=%s direction=up seq=%d attempt=%d/%d error=%s",
				c.sessionID,
				seq,
				attempt+1,
				mtProtoChunkRelayMaxRetries,
				mtProtoStatusField(err.Error()),
			)
		}
		backoff := 300 * time.Millisecond * time.Duration(1<<attempt)
		timer := time.NewTimer(backoff)
		select {
		case <-c.lifeCtx.Done():
			timer.Stop()
			return 0, nil, nil, net.ErrClosed
		case <-parent.Done():
			timer.Stop()
			return 0, nil, nil, parent.Err()
		case <-timer.C:
		}
	}
	return 0, nil, nil, lastErr
}

func (c *mtProtoChunkRelayConn) requestPipelinedUploadRound(
	parent context.Context,
	query url.Values,
	body []byte,
	seq int64,
	head *mtProtoChunkRelayHeadTracker,
) (int, http.Header, []byte, error) {
	roundCtx, roundCancel := context.WithCancel(parent)
	defer roundCancel()

	results := make(chan chunkRelayAttemptResult, 2)
	launch := func(leg string) {
		go func() {
			ctx, cancel := c.requestContext(roundCtx, "up")
			status, headers, responseBody, err := c.request(ctx, "up", query, body)
			cancel()
			results <- chunkRelayAttemptResult{
				status:       status,
				headers:      headers,
				responseBody: responseBody,
				err:          err,
				leg:          leg,
			}
		}()
	}

	launch("primary")
	timer := time.NewTimer(mtProtoChunkRelayHOLHedgeDelay)
	defer timer.Stop()

	hedgeLaunched := false
	delayElapsed := false
	completed := 0
	var lastErr error

	launchHedge := func(reason string) {
		if hedgeLaunched {
			return
		}
		hedgeLaunched = true
		launch("hedge")
		if logInfo != nil {
			logInfo.Printf(
				"MTProto Worker chunk relay hedge session_id=%s direction=up seq=%d delay_ms=%d reason=%s policy=oldest_unacked",
				c.sessionID,
				seq,
				mtProtoChunkRelayHOLHedgeDelay.Milliseconds(),
				reason,
			)
		}
	}

	for {
		var headChanged <-chan struct{}
		if delayElapsed && !hedgeLaunched {
			isHead, alreadyDone, changed := head.state(seq)
			if alreadyDone {
				headChanged = nil
			} else if isHead {
				launchHedge("head_of_line")
			} else {
				headChanged = changed
			}
		}

		select {
		case result := <-results:
			completed++
			if result.err == nil {
				roundCancel()
				if result.leg == "hedge" && logInfo != nil {
					logInfo.Printf(
						"MTProto Worker chunk relay hedge won session_id=%s direction=up seq=%d policy=oldest_unacked",
						c.sessionID,
						seq,
					)
				}
				return result.status, result.headers, result.responseBody, nil
			}
			lastErr = result.err
			if !hedgeLaunched {
				isHead, alreadyDone, _ := head.state(seq)
				if isHead && !alreadyDone {
					launchHedge("primary_error_head")
				} else {
					return 0, nil, nil, lastErr
				}
			}
			if hedgeLaunched && completed >= 2 {
				return 0, nil, nil, lastErr
			}
		case <-timer.C:
			delayElapsed = true
		case <-headChanged:
			// Re-evaluate whether this sequence is now the oldest unacknowledged chunk.
		case <-parent.Done():
			return 0, nil, nil, parent.Err()
		case <-c.lifeCtx.Done():
			return 0, nil, nil, net.ErrClosed
		}
	}
}

func writePipelinedMtProtoChunkRelay(c *mtProtoChunkRelayConn, data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.isClosed() {
		return 0, net.ErrClosed
	}

	specs := make([]mtProtoChunkUploadSpec, 0, (len(data)+mtProtoChunkRelayUploadBytes-1)/mtProtoChunkRelayUploadBytes)
	baseSeq := c.upSeq
	for start := 0; start < len(data); start += mtProtoChunkRelayUploadBytes {
		end := start + mtProtoChunkRelayUploadBytes
		if end > len(data) {
			end = len(data)
		}
		specs = append(specs, mtProtoChunkUploadSpec{
			seq:   baseSeq + int64(len(specs)) + 1,
			start: start,
			end:   end,
		})
	}

	pipelineCtx, pipelineCancel := context.WithCancel(context.Background())
	defer pipelineCancel()

	results := make(chan mtProtoChunkUploadResult, len(specs))
	head := newMtProtoChunkRelayHeadTracker(specs[0].seq)

	launch := func(spec mtProtoChunkUploadSpec) {
		go func() {
			if err := acquireMtProtoChunkRelayPrimarySlot(c); err != nil {
				results <- mtProtoChunkUploadResult{spec: spec, err: err}
				return
			}
			defer releaseMtProtoChunkRelayPrimarySlot()

			query := url.Values{
				"sid": {c.sessionID},
				"dst": {c.workerDst},
				"seq": {strconv.FormatInt(spec.seq, 10)},
			}
			status, headers, _, err := c.requestPipelinedUploadWithRetry(
				pipelineCtx,
				query,
				data[spec.start:spec.end],
				spec.seq,
				head,
			)
			if err == nil && status == http.StatusNoContent {
				ack, ackErr := strconv.ParseInt(strings.TrimSpace(headers.Get("X-Tgws-Chunk-Ack")), 10, 64)
				if ackErr == nil && ack == spec.seq {
					head.markDone(spec.seq)
				}
			}
			results <- mtProtoChunkUploadResult{spec: spec, status: status, headers: headers, err: err}
		}()
	}

	nextLaunch := 0
	for nextLaunch < len(specs) && nextLaunch < mtProtoChunkRelayUpWindow {
		launch(specs[nextLaunch])
		nextLaunch++
	}

	completed := make(map[int64]mtProtoChunkUploadResult, mtProtoChunkRelayUpWindow)
	nextCommit := 0
	written := 0

	for nextCommit < len(specs) {
		result := <-results

		if result.err != nil {
			pipelineCancel()
			return written, result.err
		}
		if result.status == http.StatusGone {
			pipelineCancel()
			return written, io.EOF
		}
		if result.status != http.StatusNoContent {
			pipelineCancel()
			return written, fmt.Errorf("chunk relay up seq %d: HTTP %d", result.spec.seq, result.status)
		}
		ack, err := strconv.ParseInt(strings.TrimSpace(result.headers.Get("X-Tgws-Chunk-Ack")), 10, 64)
		if err != nil || ack != result.spec.seq {
			pipelineCancel()
			return written, fmt.Errorf("chunk relay up seq %d: invalid ack %q", result.spec.seq, result.headers.Get("X-Tgws-Chunk-Ack"))
		}

		completed[result.spec.seq] = result

		// True sliding window: as soon as any in-flight chunk receives a valid
		// ACK, immediately launch the next chunk instead of waiting for the
		// remaining members of a fixed batch.
		if nextLaunch < len(specs) {
			launch(specs[nextLaunch])
			nextLaunch++
		}

		for nextCommit < len(specs) {
			spec := specs[nextCommit]
			if _, ok := completed[spec.seq]; !ok {
				break
			}
			delete(completed, spec.seq)

			chunkBytes := spec.end - spec.start
			c.upSeq = spec.seq
			c.upBytes += int64(chunkBytes)
			written = spec.end
			nextCommit++

			if logInfo != nil && (spec.seq <= 2 || c.upBytes%(64*1024) < int64(chunkBytes)) {
				logInfo.Printf(
					"MTProto Worker chunk relay up session_id=%s seq=%d bytes=%d confirmed_bytes=%d upload_chunk_bytes=%d upload_window=%d pipeline=%s",
					c.sessionID,
					spec.seq,
					chunkBytes,
					c.upBytes,
					mtProtoChunkRelayUploadBytes,
					mtProtoChunkRelayUpWindow,
					mtProtoChunkRelayPipelineMode,
				)
			}
		}
	}

	return written, nil
}
