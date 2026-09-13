package main

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	workerCircuitReasonQuotaExhausted  = "quota_exhausted"
	workerCircuitReasonTemporaryFailed = "temporary_failure"
	workerTemporaryFailureCooldown     = 45 * time.Second
)

type workerCircuitState struct {
	Reason string
	Until  time.Time
}

type workerCircuitSummary struct {
	Configured       int
	Available        int
	QuotaExhausted   int
	TemporaryFailed  int
	OtherBlocked     int
	NextRetry        time.Time
}

func (s workerCircuitSummary) nextRetryField() string {
	if s.NextRetry.IsZero() {
		return "unknown"
	}
	return s.NextRetry.UTC().Format(time.RFC3339)
}

type workerCircuitOpenError struct {
	Domain string
	Reason string
	Until  time.Time
	Status int
}

func (e *workerCircuitOpenError) Error() string {
	if e == nil {
		return "worker circuit open"
	}
	if e.Until.IsZero() {
		return fmt.Sprintf("worker circuit open: domain=%s reason=%s status=%d", e.Domain, e.Reason, e.Status)
	}
	return fmt.Sprintf("worker circuit open: domain=%s reason=%s until=%s status=%d", e.Domain, e.Reason, e.Until.UTC().Format(time.RFC3339), e.Status)
}

var workerCircuitRuntime = struct {
	sync.Mutex
	states map[string]workerCircuitState
}{states: make(map[string]workerCircuitState)}

func workerCircuitKey(domain string) string {
	return strings.ToLower(strings.TrimSpace(NormalizeWorkerDomain(domain)))
}

func activeWorkerCircuit(domain string, now time.Time) (workerCircuitState, bool) {
	key := workerCircuitKey(domain)
	if key == "" {
		return workerCircuitState{}, false
	}
	workerCircuitRuntime.Lock()
	defer workerCircuitRuntime.Unlock()
	state, ok := workerCircuitRuntime.states[key]
	if !ok {
		return workerCircuitState{}, false
	}
	if !state.Until.IsZero() && !now.Before(state.Until) {
		delete(workerCircuitRuntime.states, key)
		return workerCircuitState{}, false
	}
	return state, true
}

func markWorkerCircuit(domain, reason string, until time.Time) {
	key := workerCircuitKey(domain)
	if key == "" {
		return
	}
	workerCircuitRuntime.Lock()
	prev, hadPrev := workerCircuitRuntime.states[key]
	if hadPrev && prev.Until.After(until) {
		until = prev.Until
		if prev.Reason == workerCircuitReasonQuotaExhausted {
			reason = prev.Reason
		}
	}
	workerCircuitRuntime.states[key] = workerCircuitState{Reason: reason, Until: until}
	workerCircuitRuntime.Unlock()
	if logWarn != nil && (!hadPrev || prev.Reason != reason || !prev.Until.Equal(until)) {
		logWarn.Printf("Worker circuit opened: domain=%s reason=%s until=%s", key, reason, until.UTC().Format(time.RFC3339))
	}
}

func clearWorkerCircuit(domain string) {
	key := workerCircuitKey(domain)
	if key == "" {
		return
	}
	workerCircuitRuntime.Lock()
	_, existed := workerCircuitRuntime.states[key]
	delete(workerCircuitRuntime.states, key)
	workerCircuitRuntime.Unlock()
	if existed && logInfo != nil {
		logInfo.Printf("Worker circuit recovered: domain=%s", key)
	}
}

func markWorkerQuotaExhausted(domain string, until time.Time) {
	if until.IsZero() {
		until = nextWorkerQuotaReset(time.Now())
	}
	markWorkerCircuit(domain, workerCircuitReasonQuotaExhausted, until)
}

func markWorkerTemporaryFailure(domain string, now time.Time) time.Time {
	until := now.Add(workerTemporaryFailureCooldown)
	markWorkerCircuit(domain, workerCircuitReasonTemporaryFailed, until)
	return until
}

func nextWorkerQuotaReset(now time.Time) time.Time {
	u := now.UTC()
	return time.Date(u.Year(), u.Month(), u.Day()+1, 0, 1, 0, 0, time.UTC)
}

func workerCircuitError(err error) (*workerCircuitOpenError, bool) {
	var target *workerCircuitOpenError
	if errors.As(err, &target) {
		return target, true
	}
	return nil, false
}

func summarizeWorkerCandidateCircuits(candidates []workerFailoverCandidate, now time.Time) workerCircuitSummary {
	summary := workerCircuitSummary{Configured: len(candidates)}
	for _, candidate := range candidates {
		state, blocked := activeWorkerCircuit(candidate.Domain, now)
		if !blocked {
			summary.Available++
			continue
		}
		switch state.Reason {
		case workerCircuitReasonQuotaExhausted:
			summary.QuotaExhausted++
		case workerCircuitReasonTemporaryFailed:
			summary.TemporaryFailed++
		default:
			summary.OtherBlocked++
		}
		if !state.Until.IsZero() && (summary.NextRetry.IsZero() || state.Until.Before(summary.NextRetry)) {
			summary.NextRetry = state.Until
		}
	}
	return summary
}

func filterWorkerCandidatesByCircuit(candidates []workerFailoverCandidate, now time.Time) []workerFailoverCandidate {
	if len(candidates) == 0 {
		return candidates
	}
	out := make([]workerFailoverCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if _, blocked := activeWorkerCircuit(candidate.Domain, now); blocked {
			continue
		}
		out = append(out, candidate)
	}
	return out
}

func resetWorkerCircuitForTests() {
	workerCircuitRuntime.Lock()
	workerCircuitRuntime.states = make(map[string]workerCircuitState)
	workerCircuitRuntime.Unlock()
}
