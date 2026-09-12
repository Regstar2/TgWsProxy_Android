package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"
)

const workerNetworkProbeDeadline = 12 * time.Second

var workerNetworkProbeSizes = []int{
	1 * 1024,
	4 * 1024,
	8 * 1024,
	12 * 1024,
	16 * 1024,
	24 * 1024,
	32 * 1024,
	64 * 1024,
	128 * 1024,
	256 * 1024,
	512 * 1024,
	1024 * 1024,
}

type workerNetworkProbeCase struct {
	Mode            string `json:"mode"`
	SizeBytes       int    `json:"size_bytes,omitempty"`
	CumulativeBytes int    `json:"cumulative_bytes,omitempty"`
	OK              bool   `json:"ok"`
	DurationMS      int64  `json:"duration_ms"`
	TransportState  string `json:"transport_state,omitempty"`
	Error           string `json:"error,omitempty"`
}

type workerNetworkProbeReport struct {
	Revision string                   `json:"revision"`
	Domain   string                   `json:"domain"`
	Started  string                   `json:"started"`
	Cases    []workerNetworkProbeCase `json:"cases"`
}

func workerProbeOpen(domain, path string) (*flowsealRawWebSocket, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ws, err := connectFlowsealRawWebSocketContext(ctx, domain, domain, path, 10)
	if err != nil {
		return nil, err
	}
	_ = ws.SetDeadline(time.Now().Add(workerNetworkProbeDeadline))
	return ws, nil
}

func workerProbeCaseLog(c workerNetworkProbeCase) {
	if logInfo == nil {
		return
	}
	logInfo.Printf(
		"Worker network probe mode=%s size_bytes=%d cumulative_bytes=%d ok=%t duration_ms=%d error=%s %s",
		c.Mode,
		c.SizeBytes,
		c.CumulativeBytes,
		c.OK,
		c.DurationMS,
		mtProtoStatusField(c.Error),
		c.TransportState,
	)
}

func runEchoStaircase(domain string) []workerNetworkProbeCase {
	path := "/diag/ws-echo?sid=probe-echo"
	ws, err := workerProbeOpen(domain, path)
	if err != nil {
		c := workerNetworkProbeCase{Mode: "echo_staircase_connect", Error: err.Error()}
		workerProbeCaseLog(c)
		return []workerNetworkProbeCase{c}
	}
	defer ws.Close()

	cases := make([]workerNetworkProbeCase, 0, len(workerNetworkProbeSizes))
	cumulative := 0
	for index, size := range workerNetworkProbeSizes {
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = byte((i + index) & 0xff)
		}
		_ = ws.SetDeadline(time.Now().Add(workerNetworkProbeDeadline))
		started := time.Now()
		sendErr := ws.Send(payload)
		if sendErr != nil {
			c := workerNetworkProbeCase{
				Mode:            "echo_staircase",
				SizeBytes:       size,
				CumulativeBytes: cumulative,
				DurationMS:      time.Since(started).Milliseconds(),
				TransportState:  tcpTransportState(ws.conn),
				Error:           sendErr.Error(),
			}
			workerProbeCaseLog(c)
			cases = append(cases, c)
			break
		}
		received, recvErr := ws.Recv()
		cumulative += size
		c := workerNetworkProbeCase{
			Mode:            "echo_staircase",
			SizeBytes:       size,
			CumulativeBytes: cumulative,
			DurationMS:      time.Since(started).Milliseconds(),
			TransportState:  tcpTransportState(ws.conn),
		}
		if recvErr != nil {
			c.Error = recvErr.Error()
		} else if len(received) != len(payload) {
			c.Error = fmt.Sprintf("echo_size_mismatch:%d", len(received))
		} else {
			c.OK = true
		}
		workerProbeCaseLog(c)
		cases = append(cases, c)
		if !c.OK {
			break
		}
	}
	return cases
}

func runUpload4KStream(domain string) []workerNetworkProbeCase {
	path := "/diag/upload?sid=probe-upload-4k"
	ws, err := workerProbeOpen(domain, path)
	if err != nil {
		c := workerNetworkProbeCase{Mode: "upload_4k_connect", Error: err.Error()}
		workerProbeCaseLog(c)
		return []workerNetworkProbeCase{c}
	}
	defer ws.Close()

	const chunkSize = 4 * 1024
	const target = 1024 * 1024
	payload := make([]byte, chunkSize)
	cases := make([]workerNetworkProbeCase, 0, target/(64*1024))
	cumulative := 0
	for cumulative < target {
		for i := range payload {
			payload[i] = byte((i + cumulative/chunkSize) & 0xff)
		}
		_ = ws.SetDeadline(time.Now().Add(workerNetworkProbeDeadline))
		started := time.Now()
		if err := ws.Send(payload); err != nil {
			c := workerNetworkProbeCase{
				Mode:            "upload_4k_stream",
				SizeBytes:       chunkSize,
				CumulativeBytes: cumulative,
				DurationMS:      time.Since(started).Milliseconds(),
				TransportState:  tcpTransportState(ws.conn),
				Error:           err.Error(),
			}
			workerProbeCaseLog(c)
			cases = append(cases, c)
			break
		}
		ack, err := ws.Recv()
		if err != nil {
			c := workerNetworkProbeCase{
				Mode:            "upload_4k_stream",
				SizeBytes:       chunkSize,
				CumulativeBytes: cumulative + chunkSize,
				DurationMS:      time.Since(started).Milliseconds(),
				TransportState:  tcpTransportState(ws.conn),
				Error:           err.Error(),
			}
			workerProbeCaseLog(c)
			cases = append(cases, c)
			break
		}
		cumulative += chunkSize
		expected := fmt.Sprintf("ack:%d", cumulative)
		if string(ack) != expected {
			c := workerNetworkProbeCase{
				Mode:            "upload_4k_stream",
				SizeBytes:       chunkSize,
				CumulativeBytes: cumulative,
				DurationMS:      time.Since(started).Milliseconds(),
				TransportState:  tcpTransportState(ws.conn),
				Error:           "unexpected_ack",
			}
			workerProbeCaseLog(c)
			cases = append(cases, c)
			break
		}
		if cumulative == chunkSize || cumulative%(64*1024) == 0 || cumulative == target {
			c := workerNetworkProbeCase{
				Mode:            "upload_4k_stream",
				SizeBytes:       chunkSize,
				CumulativeBytes: cumulative,
				OK:              true,
				DurationMS:      time.Since(started).Milliseconds(),
				TransportState:  tcpTransportState(ws.conn),
			}
			workerProbeCaseLog(c)
			cases = append(cases, c)
		}
	}
	return cases
}

func runDownloadSizes(domain string) []workerNetworkProbeCase {
	cases := make([]workerNetworkProbeCase, 0, len(workerNetworkProbeSizes))
	for _, size := range workerNetworkProbeSizes {
		path := "/diag/download?size=" + fmt.Sprint(size) + "&sid=" + url.QueryEscape(fmt.Sprintf("probe-download-%d", size))
		started := time.Now()
		ws, err := workerProbeOpen(domain, path)
		if err != nil {
			c := workerNetworkProbeCase{Mode: "download_single", SizeBytes: size, DurationMS: time.Since(started).Milliseconds(), Error: err.Error()}
			workerProbeCaseLog(c)
			cases = append(cases, c)
			break
		}
		_ = ws.SetDeadline(time.Now().Add(workerNetworkProbeDeadline))
		payload, recvErr := ws.Recv()
		c := workerNetworkProbeCase{
			Mode:           "download_single",
			SizeBytes:      size,
			DurationMS:     time.Since(started).Milliseconds(),
			TransportState: tcpTransportState(ws.conn),
		}
		if recvErr != nil {
			c.Error = recvErr.Error()
		} else if len(payload) != size {
			c.Error = fmt.Sprintf("download_size_mismatch:%d", len(payload))
		} else {
			c.OK = true
		}
		workerProbeCaseLog(c)
		cases = append(cases, c)
		ws.Close()
		if !c.OK {
			break
		}
	}
	return cases
}

func runWorkerNetworkProbe(domain string) workerNetworkProbeReport {
	domain = NormalizeWorkerDomain(strings.TrimSpace(domain))
	report := workerNetworkProbeReport{
		Revision: "worker-network-probe-v1",
		Domain:   domain,
		Started:  time.Now().UTC().Format(time.RFC3339),
	}
	if domain == "" {
		report.Cases = []workerNetworkProbeCase{{Mode: "validation", Error: "invalid_worker_domain"}}
		return report
	}
	if logInfo != nil {
		logInfo.Printf("Worker network probe start domain=%s revision=%s", domain, report.Revision)
	}
	report.Cases = append(report.Cases, runEchoStaircase(domain)...)
	report.Cases = append(report.Cases, runUpload4KStream(domain)...)
	report.Cases = append(report.Cases, runDownloadSizes(domain)...)
	if logInfo != nil {
		logInfo.Printf("Worker network probe complete domain=%s cases=%d", domain, len(report.Cases))
	}
	return report
}

//export RunWorkerNetworkProbe
func RunWorkerNetworkProbe(cDomain *C.char) *C.char {
	initLogging(true)
	report := runWorkerNetworkProbe(C.GoString(cDomain))
	encoded, err := json.Marshal(report)
	if err != nil {
		return C.CString(`{"revision":"worker-network-probe-v1","error":"json_encode_failed"}`)
	}
	return C.CString(string(encoded))
}
