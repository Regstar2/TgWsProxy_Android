package main

import (
	"hash/fnv"
	"strings"
	"time"
)

const workerSelectionStrategyRoundRobin = "ROUND_ROBIN"

// workerCandidatesForSession keeps configured ordering for all selection
// strategies except ROUND_ROBIN. For ROUND_ROBIN, a stable hash of the opaque
// session id rotates the candidate list once at session establishment. The
// selected Worker therefore stays sticky for the whole session while different
// sessions are spread across the configured Worker pool. Workers with an open
// runtime circuit are filtered after ordering so quota-exhausted endpoints do
// not receive repeated network connection attempts.
func workerCandidatesForSession(
	settings workerFailoverSettings,
	fallbackDomain string,
	sessionID string,
) ([]workerFailoverCandidate, int) {
	candidates := settings.effectiveCandidates(fallbackDomain)
	start := 0
	if len(candidates) > 1 &&
		strings.EqualFold(strings.TrimSpace(settings.SelectionStrategy), workerSelectionStrategyRoundRobin) {
		start = workerStickyCandidateIndex(sessionID, len(candidates))
		if start != 0 {
			rotated := make([]workerFailoverCandidate, 0, len(candidates))
			rotated = append(rotated, candidates[start:]...)
			rotated = append(rotated, candidates[:start]...)
			candidates = rotated
		}
	}

	now := time.Now()
	available := filterWorkerCandidatesByCircuit(candidates, now)
	if len(available) > 0 || len(candidates) == 0 {
		return available, start
	}

	// Keep a single blocked candidate as a local diagnostic probe. The chunk
	// relay dialer rejects it immediately from the in-memory circuit state, so
	// no request reaches Cloudflare. This lets the MTProto connector report
	// all_workers_circuit_open instead of the misleading worker_not_configured.
	summary := summarizeWorkerCandidateCircuits(candidates, now)
	if logWarn != nil {
		logWarn.Printf(
			"Worker candidates unavailable reason=all_workers_circuit_open configured_workers=%d available_workers=%d quota_exhausted_workers=%d temporary_failed_workers=%d other_blocked_workers=%d next_retry_at=%s",
			summary.Configured,
			summary.Available,
			summary.QuotaExhausted,
			summary.TemporaryFailed,
			summary.OtherBlocked,
			summary.nextRetryField(),
		)
	}
	return candidates[:1], start
}

func workerStickyCandidateIndex(sessionID string, candidateCount int) int {
	if candidateCount <= 1 {
		return 0
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return 0
	}

	h := fnv.New64a()
	_, _ = h.Write([]byte(sessionID))
	return int(h.Sum64() % uint64(candidateCount))
}
