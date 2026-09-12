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
)

const (
	mtProtoChunkRelayUpWindow          = 4
	mtProtoChunkRelayGlobalHTTPRequests = 12
)

var mtProtoChunkRelayHTTPSlots = make(chan struct{}, mtProtoChunkRelayGlobalHTTPRequests)

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

func writePipelinedMtProtoChunkRelay(c *mtProtoChunkRelayConn, data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.isClosed() {
		return 0, net.ErrClosed
	}

	specs := make([]mtProtoChunkUploadSpec, 0, (len(data)+mtProtoChunkRelayBytes-1)/mtProtoChunkRelayBytes)
	baseSeq := c.upSeq
	for start := 0; start < len(data); start += mtProtoChunkRelayBytes {
		end := start + mtProtoChunkRelayBytes
		if end > len(data) {
			end = len(data)
		}
		specs = append(specs, mtProtoChunkUploadSpec{
			seq:   baseSeq + int64(len(specs)) + 1,
			start: start,
			end:   end,
		})
	}

	written := 0
	for batchStart := 0; batchStart < len(specs); batchStart += mtProtoChunkRelayUpWindow {
		batchEnd := batchStart + mtProtoChunkRelayUpWindow
		if batchEnd > len(specs) {
			batchEnd = len(specs)
		}
		batch := specs[batchStart:batchEnd]
		results := make(chan mtProtoChunkUploadResult, len(batch))

		for _, spec := range batch {
			spec := spec
			go func() {
				query := url.Values{
					"sid": {c.sessionID},
					"dst": {c.workerDst},
					"seq": {strconv.FormatInt(spec.seq, 10)},
				}
				status, headers, _, err := c.requestWithRetry(
					context.Background(),
					"up",
					query,
					data[spec.start:spec.end],
					"up",
					spec.seq,
				)
				results <- mtProtoChunkUploadResult{spec: spec, status: status, headers: headers, err: err}
			}()
		}

		bySeq := make(map[int64]mtProtoChunkUploadResult, len(batch))
		for range batch {
			result := <-results
			bySeq[result.spec.seq] = result
		}

		for _, spec := range batch {
			result := bySeq[spec.seq]
			if result.err != nil {
				return written, result.err
			}
			if result.status == http.StatusGone {
				return written, io.EOF
			}
			if result.status != http.StatusNoContent {
				return written, fmt.Errorf("chunk relay up seq %d: HTTP %d", spec.seq, result.status)
			}
			ack, err := strconv.ParseInt(strings.TrimSpace(result.headers.Get("X-Tgws-Chunk-Ack")), 10, 64)
			if err != nil || ack != spec.seq {
				return written, fmt.Errorf("chunk relay up seq %d: invalid ack %q", spec.seq, result.headers.Get("X-Tgws-Chunk-Ack"))
			}

			chunkBytes := spec.end - spec.start
			c.upSeq = spec.seq
			c.upBytes += int64(chunkBytes)
			written = spec.end
			if logInfo != nil && (spec.seq <= 2 || c.upBytes%(64*1024) < int64(chunkBytes)) {
				logInfo.Printf(
					"MTProto Worker chunk relay up session_id=%s seq=%d bytes=%d confirmed_bytes=%d upload_window=%d",
					c.sessionID,
					spec.seq,
					chunkBytes,
					c.upBytes,
					mtProtoChunkRelayUpWindow,
				)
			}
		}
	}

	return written, nil
}
