package main

import "testing"

func TestWorkerCandidatesForSessionPreservesNonRoundRobinOrder(t *testing.T) {
	settings := workerFailoverSettings{
		Enabled:           true,
		SelectionStrategy: "FAILOVER",
		Candidates: []workerFailoverCandidate{
			{ID: "first", Domain: "first.workers.dev"},
			{ID: "second", Domain: "second.workers.dev"},
		},
	}

	got, start := workerCandidatesForSession(settings, "", "session-a")
	if start != 0 {
		t.Fatalf("start=%d want=0", start)
	}
	if len(got) != 2 || got[0].ID != "first" || got[1].ID != "second" {
		t.Fatalf("candidates=%+v", got)
	}
}

func TestWorkerCandidatesForSessionRoundRobinIsStickyAndDistributed(t *testing.T) {
	settings := workerFailoverSettings{
		Enabled:           true,
		SelectionStrategy: workerSelectionStrategyRoundRobin,
		Candidates: []workerFailoverCandidate{
			{ID: "first", Domain: "first.workers.dev"},
			{ID: "second", Domain: "second.workers.dev"},
		},
	}

	firstA, indexA := workerCandidatesForSession(settings, "", "session-a")
	secondA, secondIndexA := workerCandidatesForSession(settings, "", "session-a")
	firstB, indexB := workerCandidatesForSession(settings, "", "session-b")

	if indexA != secondIndexA || firstA[0] != secondA[0] {
		t.Fatalf("same session was not sticky: first=%+v/%d second=%+v/%d", firstA, indexA, secondA, secondIndexA)
	}
	if firstA[0].ID == firstB[0].ID || indexA == indexB {
		t.Fatalf("expected the two deterministic sessions to use different primaries: a=%+v/%d b=%+v/%d", firstA, indexA, firstB, indexB)
	}
	if settings.Candidates[0].ID != "first" || settings.Candidates[1].ID != "second" {
		t.Fatalf("configured candidate order was mutated: %+v", settings.Candidates)
	}
}

func TestWorkerCandidatesForSessionRoundRobinFallbackWrapsAfterPrimary(t *testing.T) {
	settings := workerFailoverSettings{
		Enabled:           true,
		SelectionStrategy: workerSelectionStrategyRoundRobin,
		Candidates: []workerFailoverCandidate{
			{ID: "first", Domain: "first.workers.dev"},
			{ID: "second", Domain: "second.workers.dev"},
			{ID: "third", Domain: "third.workers.dev"},
		},
	}

	got, start := workerCandidatesForSession(settings, "", "alpha")
	if len(got) != 3 {
		t.Fatalf("len=%d want=3", len(got))
	}
	want := append([]workerFailoverCandidate{}, settings.Candidates[start:]...)
	want = append(want, settings.Candidates[:start]...)
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("candidate[%d]=%+v want=%+v start=%d", i, got[i], want[i], start)
		}
	}
}

func TestWorkerStickyCandidateIndexEmptySessionPreservesPrimary(t *testing.T) {
	if got := workerStickyCandidateIndex("", 2); got != 0 {
		t.Fatalf("index=%d want=0", got)
	}
}
