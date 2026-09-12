package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

const workerNetworkProbeDeadline = 12 * time.Second

const (
	workerProbeIPAuto = "auto"
	workerProbeIPv4   = "ipv4"
	workerProbeIPv6   = "ipv6"
)

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
	IPFamily string                   `json:"ip_family"`
	Started  string                   `json:"started"`
	Cases    []workerNetworkProbeCase `json:"cases"`
}

func normalizeWorkerProbeIPFamily(raw string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", workerProbeIPAuto:
		return workerProbeIPAuto, true
	case workerProbeIPv4, "4", "tcp4":
		return workerProbeIPv4, true
	case workerProbeIPv6, "6", "tcp6":
		return workerProbeIPv6, true
	default:
		return "", false
	}
}

func resolveWorkerProbeHost(ctx context.Context, domain, family string) (string, error) {
	if family == workerProbeIPAuto {
		return domain, nil
	}
	network := "ip4"
	if family == workerProbeIPv6 {
		network = "ip6"
	}
	ips, err := net.DefaultResolver.LookupIP(ctx, network, domain)
	if err != nil {
		return "", fmt.Errorf("resolve_%s: %w", family, err)
	}
	if len(ips) == 0 {
		return "", fmt.Errorf("resolve_%s: no addresses", family)
	}
	return ips[0].String(), nil
}

func workerProbeOpen(domain, path, family string) (*flowsealRawWebSocket, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	host, err := resolveWorkerProbeHost(ctx, domain, family)
	if err != nil {
		return nil, err
	}
	ws, err := connectFlowsealRawWebSocketContext(ctx, host, domain, path, 10)
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

func runEchoStaircase(domain, family string) []workerNetworkProbeCase {
	path := "/diag/ws-echo?sid=probe-echo"
	ws, err := workerProbeOpen(domain, path, family)
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

func runUpload4KStream(domain, family string) []workerNetworkProbeCase {
	path := "/diag/upload?sid=probe-upload-4k"
	ws, err := workerProbeOpen(domain, path, family)
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

func runDownloadSizes(domain, family string) []workerNetworkProbeCase {
	cases := make([]workerNetworkProbeCase, 0, len(workerNetworkProbeSizes))
	for _, size := range workerNetworkProbeSizes {
		path := "/diag/download?size=" + fmt.Sprint(size) + "&sid=" + url.QueryEscape(fmt.Sprintf("probe-download-%d", size))
		started := time.Now()
		ws, err := workerProbeOpen(domain, path, family)
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

func runWorkerNetworkProbe(domain, familyRaw string) workerNetworkProbeReport {
	domain = NormalizeWorkerDomain(strings.TrimSpace(domain))
	family, familyOK := normalizeWorkerProbeIPFamily(familyRaw)
	report := workerNetworkProbeReport{
		Revision: "worker-network-probe-v2",
		Domain:   domain,
		IPFamily: family,
		Started:  time.Now().UTC().Format(time.RFC3339),
	}
	if domain == "" {
		report.Cases = []workerNetworkProbeCase{{Mode: "validation", Error: "invalid_worker_domain"}}
		return report
	}
	if !familyOK {
		report.IPFamily = strings.TrimSpace(familyRaw)
		report.Cases = []workerNetworkProbeCase{{Mode: "validation", Error: "invalid_ip_family"}}
		return report
	}
	if logInfo != nil {
		logInfo.Printf("Worker network probe start domain=%s ip_family=%s revision=%s", domain, family, report.Revision)
	}
	report.Cases = append(report.Cases, runEchoStaircase(domain, family)...)
	report.Cases = append(report.Cases, runUpload4KStream(domain, family)...)
	report.Cases = append(report.Cases, runDownloadSizes(domain, family)...)
	if logInfo != nil {
		logInfo.Printf("Worker network probe complete domain=%s ip_family=%s cases=%d", domain, family, len(report.Cases))
	}
	return report
}

func encodeWorkerNetworkProbeReport(report workerNetworkProbeReport) *C.char {
	encoded, err := json.Marshal(report)
	if err != nil {
		return C.CString(`{"revision":"worker-network-probe-v2","error":"json_encode_failed"}`)
	}
	return C.CString(string(encoded))
}

//export RunWorkerNetworkProbe
func RunWorkerNetworkProbe(cDomain *C.char) *C.char {
	initLogging(true)
	return encodeWorkerNetworkProbeReport(runWorkerNetworkProbe(C.GoString(cDomain), workerProbeIPAuto))
}

//export RunWorkerNetworkProbeWithFamily
func RunWorkerNetworkProbeWithFamily(cDomain *C.char, cFamily *C.char) *C.char {
	initLogging(true)
	return encodeWorkerNetworkProbeReport(runWorkerNetworkProbe(C.GoString(cDomain), C.GoString(cFamily)))
}
