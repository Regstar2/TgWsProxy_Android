package main

import (
	"context"
	"errors"
	"testing"

	"tg-ws-proxy/mtproxyfrontend"
	"tg-ws-proxy/tgwsroute"
)

func TestMtProtoWorkerConnectorUsesSessionStickyRoundRobinPrimary(t *testing.T) {
	failover := workerFailoverSettings{
		Enabled:           true,
		MaxAttempts:       2,
		SelectionStrategy: workerSelectionStrategyRoundRobin,
		Candidates: []workerFailoverCandidate{
			{ID: "first", Domain: "first.workers.dev"},
			{ID: "second", Domain: "second.workers.dev"},
		},
	}
	withRuntimeSettings(t, func(settings runtimeSettings) runtimeSettings {
		settings.Worker.Enabled = true
		settings.Worker.Domain = "first.workers.dev"
		settings.Worker.Failover = failover
		settings.Worker.DestinationMode = tgwsroute.WorkerDestinationPreserveOriginalDst
		settings.MtProtoWorkerPreconnect = false
		return settings
	})

	relayInit := buildTestInitWithSignedDC(t, 2)
	request := mtproxyfrontend.OutboundRequest{
		DCID:      2,
		Transport: mtproxyfrontend.TransportAbridged,
		RelayInit: relayInit,
	}
	sessionID := mtproxyfrontend.SessionIDForRequest(request)
	expected, _ := workerCandidatesForSession(failover, "first.workers.dev", sessionID)
	if len(expected) != 2 {
		t.Fatalf("expected candidates=%d want=2", len(expected))
	}

	var attempts []string
	connector := &mtProtoWorkerConnector{
		dial: func(domain, _, _ string) (mtProtoFrameSocket, error) {
			attempts = append(attempts, domain)
			return &fakeMtProtoFrameSocket{}, nil
		},
	}
	conn, result := connector.Connect(context.Background(), request)
	if result.Err != nil {
		t.Fatalf("connect: %v", result.Err)
	}
	defer conn.Close()

	if len(attempts) != 1 || attempts[0] != expected[0].Domain {
		t.Fatalf("dial attempts=%v want primary=%s session_id=%s", attempts, expected[0].Domain, sessionID)
	}
}

func TestMtProtoWorkerConnectorRoundRobinFailoverWrapsAfterStickyPrimary(t *testing.T) {
	failover := workerFailoverSettings{
		Enabled:           true,
		MaxAttempts:       2,
		SelectionStrategy: workerSelectionStrategyRoundRobin,
		Candidates: []workerFailoverCandidate{
			{ID: "first", Domain: "first.workers.dev"},
			{ID: "second", Domain: "second.workers.dev"},
		},
	}
	withRuntimeSettings(t, func(settings runtimeSettings) runtimeSettings {
		settings.Worker.Enabled = true
		settings.Worker.Domain = "first.workers.dev"
		settings.Worker.Failover = failover
		settings.Worker.DestinationMode = tgwsroute.WorkerDestinationPreserveOriginalDst
		settings.MtProtoWorkerPreconnect = false
		return settings
	})

	request := mtproxyfrontend.OutboundRequest{
		DCID:      2,
		Transport: mtproxyfrontend.TransportAbridged,
		RelayInit: buildTestInitWithSignedDC(t, 2),
	}
	sessionID := mtproxyfrontend.SessionIDForRequest(request)
	expected, _ := workerCandidatesForSession(failover, "first.workers.dev", sessionID)
	if len(expected) != 2 {
		t.Fatalf("expected candidates=%d want=2", len(expected))
	}

	var attempts []string
	connector := &mtProtoWorkerConnector{
		dial: func(domain, _, _ string) (mtProtoFrameSocket, error) {
			attempts = append(attempts, domain)
			if len(attempts) == 1 {
				return nil, errors.New("primary unavailable")
			}
			return &fakeMtProtoFrameSocket{}, nil
		},
	}
	conn, result := connector.Connect(context.Background(), request)
	if result.Err != nil {
		t.Fatalf("connect: %v", result.Err)
	}
	defer conn.Close()

	if len(attempts) != 2 || attempts[0] != expected[0].Domain || attempts[1] != expected[1].Domain {
		t.Fatalf("dial attempts=%v want=%s,%s", attempts, expected[0].Domain, expected[1].Domain)
	}
}
