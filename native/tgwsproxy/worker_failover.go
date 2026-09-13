package main

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

const defaultMaxWorkerFailoverAttempts = 3

type workerFailoverCandidate struct {
	ID     string
	Domain string
}

type workerFailoverSettings struct {
	Enabled           bool
	SelectedID        string
	Candidates        []workerFailoverCandidate
	MaxAttempts       int
	SkippedBackoff    int
	SelectionStrategy string
	SelectionReason   string
	CandidateCount    int
	RoundRobinCursor  string
}

func (s workerFailoverSettings) effectiveCandidates(fallbackDomain string) []workerFailoverCandidate {
	if s.Enabled && len(s.Candidates) > 0 {
		return s.Candidates
	}
	if fallbackDomain != "" {
		return []workerFailoverCandidate{{ID: s.SelectedID, Domain: fallbackDomain}}
	}
	return nil
}

func (s workerFailoverSettings) maxAttemptsFor(count int) int {
	max := s.MaxAttempts
	if max <= 0 {
		max = defaultMaxWorkerFailoverAttempts
	}
	if count <= 0 {
		return 0
	}
	if max > count {
		max = count
	}
	return max
}

func parseWorkerFailoverCandidates(raw string) []workerFailoverCandidate {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	out := make([]workerFailoverCandidate, 0)
	for _, part := range strings.Split(raw, "|") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		sep := strings.Index(part, ":")
		if sep <= 0 {
			continue
		}
		id := strings.TrimSpace(part[:sep])
		domain := NormalizeWorkerDomain(strings.TrimSpace(part[sep+1:]))
		if id == "" || domain == "" {
			continue
		}
		out = append(out, workerFailoverCandidate{ID: id, Domain: domain})
	}
	return out
}

var (
	rtSelectedWorkerID         atomic.Value // string
	rtRuntimeWorkerID          atomic.Value // string
	rtRuntimeWorkerDomain      atomic.Value // string
	rtLastSuccessfulWorkerID   atomic.Value // string
	rtLastFailedWorkerID       atomic.Value // string
	rtWorkerFailoverReason     atomic.Value // string
	rtWorkerFailoverAttemptCnt atomic.Int32
	rtWorkerFailoverActive     atomic.Int32
	rtWorkerFailoverSkipped    atomic.Int32
	rtWorkerSelectionStrategy  atomic.Value // string
	rtWorkerSelectionReason    atomic.Value // string
	rtWorkerCandidateCount     atomic.Int32
)

func initWorkerFailoverRuntimeState() {
	empty := ""
	rtSelectedWorkerID.Store(empty)
	rtRuntimeWorkerID.Store(empty)
	rtRuntimeWorkerDomain.Store(empty)
	rtLastSuccessfulWorkerID.Store(empty)
	rtLastFailedWorkerID.Store(empty)
	rtWorkerFailoverReason.Store(empty)
	rtWorkerFailoverAttemptCnt.Store(0)
	rtWorkerFailoverActive.Store(0)
	rtWorkerFailoverSkipped.Store(0)
	rtWorkerSelectionStrategy.Store(empty)
	rtWorkerSelectionReason.Store(empty)
	rtWorkerCandidateCount.Store(0)
}

func syncWorkerFailoverFromSettings(failover workerFailoverSettings) {
	rtSelectedWorkerID.Store(failover.SelectedID)
	rtWorkerFailoverSkipped.Store(int32(failover.SkippedBackoff))
	rtWorkerSelectionStrategy.Store(failover.SelectionStrategy)
	rtWorkerSelectionReason.Store(failover.SelectionReason)
	rtWorkerCandidateCount.Store(int32(failover.CandidateCount))
}

func noteWorkerFailoverAttemptStarted(selectedID string, candidateCount int) {
	attemptID := fmt.Sprintf("%d", time.Now().UnixNano())
	rtWorkerFailoverActive.Store(1)
	rtWorkerFailoverAttemptCnt.Store(0)
	rtWorkerFailoverReason.Store("")
	strategy := workerFailoverString(rtWorkerSelectionStrategy)
	logInfo.Printf("Worker failover attempt started: attemptId=%s, selectedWorker=%s, candidates=%d",
		attemptID, selectedID, candidateCount)
	if strategy != "" {
		logInfo.Printf("Worker selection started: strategy=%s", strategy)
	}
}

func noteWorkerFailoverCandidateSelected(workerID, workerName, domain string) {
	logInfo.Printf("Worker failover candidate selected: workerId=%s, name=%s, domain=%s",
		workerID, workerName, domain)
}

func noteWorkerConnectionAttemptStarted(workerID string) {
	logInfo.Printf("Worker connection attempt started: workerId=%s", workerID)
}

func noteWorkerConnectionAttemptSuccess(workerID string, latencyMs int64) {
	rtRuntimeWorkerID.Store(workerID)
	rtLastSuccessfulWorkerID.Store(workerID)
	rtWorkerFailoverActive.Store(0)
	rtWorkerFailoverReason.Store("")
	logInfo.Printf("Worker connection attempt success: workerId=%s, latencyMs=%d", workerID, latencyMs)
}

func noteWorkerConnectionAttemptFailed(workerID, reason string) {
	rtLastFailedWorkerID.Store(workerID)
	rtWorkerFailoverReason.Store(reason)
	rtWorkerFailoverAttemptCnt.Add(1)
	logWarn.Printf("Worker connection attempt failed: workerId=%s, reason=%s", workerID, reason)
}

func noteWorkerFailoverNextCandidate(workerID string) {
	logInfo.Printf("Worker failover next candidate: workerId=%s", workerID)
}

func noteWorkerFailoverSkippedWorker(workerID, reason string) {
	logInfo.Printf("Worker failover skipped worker: workerId=%s, reason=%s", workerID, reason)
}

func noteWorkerFailoverExhausted(reason string, attempts int) {
	rtWorkerFailoverActive.Store(0)
	rtWorkerFailoverReason.Store(reason)
	rtWorkerFailoverAttemptCnt.Store(int32(attempts))
	logWarn.Printf("Worker failover exhausted: reason=%s, attempts=%d", reason, attempts)
}

func noteWorkerFailoverFinished(successWorkerID string, attempts int) {
	rtWorkerFailoverActive.Store(0)
	if successWorkerID != "" {
		rtRuntimeWorkerID.Store(successWorkerID)
		rtLastSuccessfulWorkerID.Store(successWorkerID)
	}
	rtWorkerFailoverAttemptCnt.Store(int32(attempts))
	logInfo.Printf("Worker failover finished: successWorkerId=%s, attempts=%d", successWorkerID, attempts)
}

func appendWorkerFailoverStatusFields(parts []string) []string {
	return append(parts,
		"selected_worker_id="+escapeStatusField(workerFailoverString(rtSelectedWorkerID)),
		"runtime_worker_id="+escapeStatusField(workerFailoverString(rtRuntimeWorkerID)),
		"runtime_worker_domain="+escapeStatusField(workerFailoverString(rtRuntimeWorkerDomain)),
		"last_successful_worker_id="+escapeStatusField(workerFailoverString(rtLastSuccessfulWorkerID)),
		"last_failed_worker_id="+escapeStatusField(workerFailoverString(rtLastFailedWorkerID)),
		"worker_failover_reason="+escapeStatusField(workerFailoverString(rtWorkerFailoverReason)),
		fmt.Sprintf("worker_failover_attempt_count=%d", rtWorkerFailoverAttemptCnt.Load()),
		fmt.Sprintf("worker_failover_active=%d", rtWorkerFailoverActive.Load()),
		fmt.Sprintf("worker_failover_skipped_backoff=%d", rtWorkerFailoverSkipped.Load()),
		"worker_selection_strategy="+escapeStatusField(workerFailoverString(rtWorkerSelectionStrategy)),
		"worker_selection_reason="+escapeStatusField(workerFailoverString(rtWorkerSelectionReason)),
		fmt.Sprintf("worker_candidate_count=%d", rtWorkerCandidateCount.Load()),
	)
}

func workerFailoverString(v atomic.Value) string {
	if x := v.Load(); x != nil {
		if s, ok := x.(string); ok {
			return s
		}
	}
	return ""
}

func classifyWorkerConnectFailure(err error) string {
	if err == nil {
		return "worker_runtime_failure"
	}
	if _, ok := workerCircuitError(err); ok {
		return "all_workers_circuit_open"
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "timeout"), strings.Contains(msg, "deadline"):
		return "worker_connect_timeout"
	case strings.Contains(msg, "dns"), strings.Contains(msg, "no such host"):
		return "worker_dns_failed"
	case strings.Contains(msg, "tls"), strings.Contains(msg, "certificate"):
		return "worker_tls_failed"
	case strings.Contains(msg, "websocket"), strings.Contains(msg, "ws "):
		return "worker_websocket_failed"
	default:
		return "worker_runtime_failure"
	}
}

func dialWorkerCandidate(domain, path, logPrefix string) (*RawWebSocket, error) {
	ws, err := wsConnect(domain, domain, path, 10)
	if err == nil {
		return ws, nil
	}
	resolved, resolveErr := resolvePreferredIPs(domain, 10)
	if resolveErr != nil {
		return nil, err
	}
	lastErr := err
	for _, ip := range resolved.Preferred() {
		ws, lastErr = wsConnect(ip, domain, path, 10)
		if lastErr == nil {
			return ws, nil
		}
		logDomainConnectFailure(logPrefix, domain, ip, lastErr)
	}
	return nil, lastErr
}

func tryWorkerFailoverConnect(
	settings runtimeSettings,
	label string,
	dc int,
	isMedia bool,
	workerDst string,
	sessionID string,
) (*RawWebSocket, workerFailoverCandidate, int, string, bool) {
	failover := settings.Worker.Failover
	candidates, stickyIndex := workerCandidatesForSession(failover, settings.Worker.Domain, sessionID)
	if len(candidates) == 0 {
		return nil, workerFailoverCandidate{}, 0, "no_enabled_worker", false
	}
	maxAttempts := failover.maxAttemptsFor(len(candidates))
	noteWorkerFailoverAttemptStarted(failover.SelectedID, len(candidates))
	logInfo.Printf(
		"Worker session selection session_id=%s strategy=%s candidate_count=%d sticky_index=%d primary_worker_id=%s primary_worker_host=%s",
		sessionID,
		mtProtoStatusField(failover.SelectionStrategy),
		len(candidates),
		stickyIndex,
		candidates[0].ID,
		candidates[0].Domain,
	)

	mTag := mediaTag(isMedia)
	dstIP := strings.TrimSpace(workerDst)
	if dstIP == "" {
		dstIP = telegramDCTargetIP(dc, "")
	}
	path := buildWorkerWSPath(dc, dstIP, isMedia, sessionID)
	prefix := fmt.Sprintf("[%s] session_id=%s DC%d%s cfworker", label, sessionID, dc, mTag)
	selectionReason := strings.TrimSpace(failover.SelectionReason)
	if selectionReason == "" {
		selectionReason = strings.TrimSpace(failover.SelectionStrategy)
	}
	if selectionReason == "" {
		selectionReason = "failover"
	}
	logInfo.Printf("opening_worker_ws_after_first_packet=true session_id=%s", sessionID)

	var lastReason string
	attempts := 0
	for i := 0; i < maxAttempts; i++ {
		candidate := candidates[i]
		if i > 0 {
			stats.workerEndpointPoolMisses.Add(1)
			noteWorkerFailoverNextCandidate(candidate.ID)
		}
		logInfo.Printf("Worker endpoint selected: workerId=%s reason=%s", candidate.ID, selectionReason)
		noteWorkerFailoverCandidateSelected(candidate.ID, candidate.ID, candidate.Domain)
		noteWorkerConnectionAttemptStarted(candidate.ID)

		logDebug.Printf("[%s] DC%d%s cfworker hostname dial start host=%s workerId=%s",
			label, dc, mTag, candidate.Domain, candidate.ID)
		poolKey := WorkerPoolKey{
			DC:           dc,
			WorkerDomain: candidate.Domain,
			Dst:          dstIP,
			Media:        false,
		}
		ws := workerPool.GetForSession(poolKey)
		var err error
		if ws != nil {
			logInfo.Printf("[%s] session_id=%s DC%d%s Worker WS preconnect hit host=%s workerId=%s worker_dst=%s",
				label, sessionID, dc, mTag, candidate.Domain, candidate.ID, dstIP)
		} else {
			logDebug.Printf("[%s] session_id=%s DC%d%s Worker WS preconnect miss host=%s workerId=%s worker_dst=%s",
				label, sessionID, dc, mTag, candidate.Domain, candidate.ID, dstIP)
			ws, err = dialWorkerCandidate(candidate.Domain, path, prefix)
		}

		if ws != nil {
			rtRuntimeWorkerDomain.Store(candidate.Domain)
			rtCurrentWorkerDomain.Store(candidate.Domain)
			attempts = i + 1
			stats.workerEndpointPoolHits.Add(1)
			noteWorkerFailoverFinished(candidate.ID, attempts)
			return ws, candidate, attempts, "", true
		}

		reason := classifyWorkerConnectFailure(err)
		lastReason = reason
		attempts = i + 1
		noteWorkerConnectionAttemptFailed(candidate.ID, reason)
	}

	finalReason := "all_workers_failed"
	if lastReason != "" {
		finalReason = lastReason
	}
	noteWorkerFailoverExhausted(finalReason, attempts)
	return nil, workerFailoverCandidate{}, attempts, finalReason, false
}
