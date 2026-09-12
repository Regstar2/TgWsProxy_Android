package main

import (
	"net/http"
	"testing"
	"time"
)

func TestWorkerCircuitBreakerFiltersQuotaExhaustedCandidate(t *testing.T) {
	resetWorkerCircuitForTests()
	t.Cleanup(resetWorkerCircuitForTests)

	now := time.Now()
	markWorkerQuotaExhausted("first.workers.dev", now.Add(time.Hour))
	candidates := []workerFailoverCandidate{
		{ID: "first", Domain: "first.workers.dev"},
		{ID: "second", Domain: "second.workers.dev"},
	}

	got := filterWorkerCandidatesByCircuit(candidates, now)
	if len(got) != 1 || got[0].ID != "second" {
		t.Fatalf("candidates=%+v want only second", got)
	}
}

func TestWorkerCircuitBreakerRecoversAfterCooldown(t *testing.T) {
	resetWorkerCircuitForTests()
	t.Cleanup(resetWorkerCircuitForTests)

	now := time.Now()
	markWorkerCircuit("first.workers.dev", workerCircuitReasonTemporaryFailed, now.Add(-time.Second))
	if _, blocked := activeWorkerCircuit("first.workers.dev", now); blocked {
		t.Fatal("expired circuit remained blocked")
	}
}

func TestWorkerCandidatesForSessionSkipsCircuitOpenPrimary(t *testing.T) {
	resetWorkerCircuitForTests()
	t.Cleanup(resetWorkerCircuitForTests)

	settings := workerFailoverSettings{
		Enabled:           true,
		SelectionStrategy: workerSelectionStrategyRoundRobin,
		Candidates: []workerFailoverCandidate{
			{ID: "first", Domain: "first.workers.dev"},
			{ID: "second", Domain: "second.workers.dev"},
		},
	}

	ordered, _ := workerCandidatesForSession(settings, "", "session-a")
	if len(ordered) != 2 {
		t.Fatalf("initial candidates=%+v", ordered)
	}
	blockedDomain := ordered[0].Domain
	markWorkerQuotaExhausted(blockedDomain, time.Now().Add(time.Hour))

	got, _ := workerCandidatesForSession(settings, "", "session-a")
	if len(got) != 1 || got[0].Domain == blockedDomain {
		t.Fatalf("blocked domain %s was not removed: %+v", blockedDomain, got)
	}
}

func TestChunkRelayCircuitErrorMarksQuotaUntilReset(t *testing.T) {
	resetWorkerCircuitForTests()
	t.Cleanup(resetWorkerCircuitForTests)

	now := time.Date(2026, 9, 12, 18, 0, 0, 0, time.UTC)
	reset := now.Add(6 * time.Hour)
	headers := make(http.Header)
	headers.Set(chunkRelayWorkerStateHeader, chunkRelayQuotaExhaustedState)
	headers.Set(chunkRelayQuotaResetHeader, reset.Format(time.RFC3339))

	err := chunkRelayCircuitErrorForResponse("quota.workers.dev", http.StatusServiceUnavailable, headers, now)
	if err == nil {
		t.Fatal("expected quota circuit error")
	}
	circuitErr, ok := workerCircuitError(err)
	if !ok {
		t.Fatalf("error type=%T want workerCircuitOpenError", err)
	}
	if circuitErr.Reason != workerCircuitReasonQuotaExhausted || !circuitErr.Until.Equal(reset) {
		t.Fatalf("circuit=%+v", circuitErr)
	}
	state, blocked := activeWorkerCircuit("quota.workers.dev", now)
	if !blocked || state.Reason != workerCircuitReasonQuotaExhausted || !state.Until.Equal(reset) {
		t.Fatalf("state=%+v blocked=%t", state, blocked)
	}
}
