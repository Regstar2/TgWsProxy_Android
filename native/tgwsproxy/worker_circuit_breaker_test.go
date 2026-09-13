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

func TestWorkerCandidatesForSessionKeepsLocalProbeWhenAllCircuitsOpen(t *testing.T) {
	resetWorkerCircuitForTests()
	t.Cleanup(resetWorkerCircuitForTests)

	now := time.Now()
	settings := workerFailoverSettings{
		Enabled:           true,
		SelectionStrategy: workerSelectionStrategyRoundRobin,
		Candidates: []workerFailoverCandidate{
			{ID: "first", Domain: "first.workers.dev"},
			{ID: "second", Domain: "second.workers.dev"},
		},
	}
	markWorkerQuotaExhausted("first.workers.dev", now.Add(time.Hour))
	markWorkerCircuit("second.workers.dev", workerCircuitReasonTemporaryFailed, now.Add(5*time.Minute))

	got, _ := workerCandidatesForSession(settings, "", "session-b")
	if len(got) != 1 {
		t.Fatalf("candidates=%+v want one local diagnostic probe", got)
	}
	if _, blocked := activeWorkerCircuit(got[0].Domain, time.Now()); !blocked {
		t.Fatalf("diagnostic probe domain=%s must remain circuit-open", got[0].Domain)
	}
}

func TestWorkerCircuitSummaryCountsReasonsAndNextRetry(t *testing.T) {
	resetWorkerCircuitForTests()
	t.Cleanup(resetWorkerCircuitForTests)

	now := time.Date(2026, 9, 12, 18, 0, 0, 0, time.UTC)
	quotaReset := now.Add(6 * time.Hour)
	temporaryReset := now.Add(45 * time.Second)
	markWorkerQuotaExhausted("quota.workers.dev", quotaReset)
	markWorkerCircuit("temporary.workers.dev", workerCircuitReasonTemporaryFailed, temporaryReset)

	summary := summarizeWorkerCandidateCircuits([]workerFailoverCandidate{
		{ID: "quota", Domain: "quota.workers.dev"},
		{ID: "temporary", Domain: "temporary.workers.dev"},
		{ID: "healthy", Domain: "healthy.workers.dev"},
	}, now)
	if summary.Configured != 3 || summary.Available != 1 || summary.QuotaExhausted != 1 || summary.TemporaryFailed != 1 || summary.OtherBlocked != 0 {
		t.Fatalf("summary=%+v", summary)
	}
	if !summary.NextRetry.Equal(temporaryReset) {
		t.Fatalf("next retry=%s want=%s", summary.NextRetry, temporaryReset)
	}
	if got := summary.nextRetryField(); got != temporaryReset.UTC().Format(time.RFC3339) {
		t.Fatalf("next retry field=%s", got)
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
