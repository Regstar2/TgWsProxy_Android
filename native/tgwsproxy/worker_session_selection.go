package main

import (
	"hash/fnv"
	"strings"
)

const workerSelectionStrategyRoundRobin = "ROUND_ROBIN"

// workerCandidatesForSession keeps configured ordering for all selection
// strategies except ROUND_ROBIN. For ROUND_ROBIN, a stable hash of the opaque
// session id rotates the candidate list once at session establishment. The
// selected Worker therefore stays sticky for the whole session while different
// sessions are spread across the configured Worker pool.
func workerCandidatesForSession(
	settings workerFailoverSettings,
	fallbackDomain string,
	sessionID string,
) ([]workerFailoverCandidate, int) {
	candidates := settings.effectiveCandidates(fallbackDomain)
	if len(candidates) <= 1 ||
		!strings.EqualFold(strings.TrimSpace(settings.SelectionStrategy), workerSelectionStrategyRoundRobin) {
		return candidates, 0
	}

	start := workerStickyCandidateIndex(sessionID, len(candidates))
	if start == 0 {
		return candidates, 0
	}

	rotated := make([]workerFailoverCandidate, 0, len(candidates))
	rotated = append(rotated, candidates[start:]...)
	rotated = append(rotated, candidates[:start]...)
	return rotated, start
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
