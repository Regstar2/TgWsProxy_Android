package tgwsroute

import (
	"strings"
	"testing"
)

func TestResolveCfWorkerDestination_PreserveOriginalDst(t *testing.T) {
	plan := ResolveCfWorkerDestination(WorkerDestinationInput{
		WorkerDomain:  "example.workers.dev",
		DCID:          2,
		IsMedia:       false,
		DCOk:          true,
		ParsedDstHost: "149.154.167.41",
		Mode:          WorkerDestinationPreserveOriginalDst,
		DcIPMap:       map[int]string{2: "149.154.167.220"},
	})
	if !plan.OK {
		t.Fatalf("expected ok, got %s", plan.FailReason)
	}
	if plan.WorkerDst != "149.154.167.41" {
		t.Fatalf("worker_dst=%q", plan.WorkerDst)
	}
	if plan.ConfiguredDestinationMode != WorkerDestinationPreserveOriginalDst {
		t.Fatalf("configured_destination_mode=%q", plan.ConfiguredDestinationMode)
	}
	if plan.EffectiveDestinationMode != WorkerDestinationPreserveOriginalDst {
		t.Fatalf("effective_destination_mode=%q", plan.EffectiveDestinationMode)
	}
}

func TestResolveCfWorkerDestination_FlowsealDCMap(t *testing.T) {
	plan := ResolveCfWorkerDestination(WorkerDestinationInput{
		WorkerDomain:  "example.workers.dev",
		DCID:          4,
		IsMedia:       false,
		DCOk:          true,
		ParsedDstHost: "149.154.167.91",
		Mode:          WorkerDestinationFlowsealDCMap,
		DcIPMap:       map[int]string{4: "149.154.167.220"},
	})
	if !plan.OK {
		t.Fatalf("expected ok, got %s", plan.FailReason)
	}
	if plan.WorkerDst != "149.154.167.220" {
		t.Fatalf("worker_dst=%q", plan.WorkerDst)
	}
	if plan.WorkerDstSource != WorkerDstSourceFlowsealDCMap {
		t.Fatalf("worker_dst_source=%q", plan.WorkerDstSource)
	}
	if plan.PreserveOriginalDst {
		t.Fatal("preserve_original_dst should be false")
	}
}

func TestResolveCfWorkerDestination_ExperimentalForceRequiresExplicitFlag(t *testing.T) {
	plan := ResolveCfWorkerDestination(WorkerDestinationInput{
		WorkerDomain:  "example.workers.dev",
		DCID:          2,
		IsMedia:       true,
		DCOk:          true,
		ParsedDstHost: "149.154.167.151",
		Mode:          WorkerDestinationFlowsealMediaDC4Fix,
		DcIPMap:       map[int]string{4: "149.154.167.220"},
		MediaFix: FlowsealMediaFixConfig{
			Enabled: false,
			DC:      4,
			IP:      "149.154.167.220",
		},
	})
	if plan.FlowsealMediaFixApplied {
		t.Fatal("media fix must not apply without explicit enabled flag")
	}
	if plan.EffectiveDestinationMode != WorkerDestinationPreserveOriginalDst {
		t.Fatalf("effective_destination_mode=%q want preserve original", plan.EffectiveDestinationMode)
	}
	if plan.WorkerDst != "149.154.167.151" {
		t.Fatalf("worker_dst=%q want preserve original", plan.WorkerDst)
	}
	if plan.MediaAudit.MediaFixSkipReason != "media_fix_disabled" {
		t.Fatalf("media_fix_skip_reason=%q", plan.MediaAudit.MediaFixSkipReason)
	}
}

func TestResolveCfWorkerDestination_ExperimentalMediaOverridePreservesLogicalDC(t *testing.T) {
	plan := ResolveCfWorkerDestination(WorkerDestinationInput{
		WorkerDomain:  "example.workers.dev",
		DCID:          2,
		IsMedia:       true,
		DCOk:          true,
		ParsedDstHost: "149.154.167.151",
		Mode:          WorkerDestinationFlowsealMediaDC4Fix,
		DcIPMap:       map[int]string{4: "149.154.167.220"},
		MediaFix: FlowsealMediaFixConfig{
			Enabled: true,
			DC:      4,
			IP:      "149.154.167.220",
		},
	})
	if !plan.OK {
		t.Fatalf("expected ok, got %s", plan.FailReason)
	}
	if !plan.FlowsealMediaFixApplied {
		t.Fatal("expected flowseal media fix")
	}
	if plan.EffectiveDestinationMode != WorkerDestinationExperimentalForceMediaDC4 {
		t.Fatalf("effective_destination_mode=%q", plan.EffectiveDestinationMode)
	}
	if plan.EffectiveDC != 2 || !plan.EffectiveIsMedia {
		t.Fatalf("effective dc/media = %d/%t", plan.EffectiveDC, plan.EffectiveIsMedia)
	}
	if plan.WorkerDst != "149.154.167.220" {
		t.Fatalf("worker_dst=%q", plan.WorkerDst)
	}
	if plan.WorkerDstSource != WorkerDstSourceFlowsealMediaDC4Fix {
		t.Fatalf("worker_dst_source=%q", plan.WorkerDstSource)
	}
	if !strings.Contains(plan.WorkerURL, "media=1") {
		t.Fatalf("workerURL=%s", plan.WorkerURL)
	}
	if !strings.Contains(plan.WorkerURL, "dc=2") {
		t.Fatalf("workerURL=%s", plan.WorkerURL)
	}
}

func TestResolveCfWorkerDestination_ExperimentalForceMediaDC4PreservesCoreDC2(t *testing.T) {
	plan := ResolveCfWorkerDestination(WorkerDestinationInput{
		WorkerDomain:  "example.workers.dev",
		DCID:          2,
		IsMedia:       false,
		DCOk:          true,
		ParsedDstHost: "2001:67c:4e8:f002::a",
		Mode:          WorkerDestinationFlowsealMediaDC4Fix,
		MediaFix: FlowsealMediaFixConfig{
			Enabled: true,
			DC:      4,
			IP:      "149.154.167.220",
		},
	})
	if !plan.OK {
		t.Fatalf("expected ok, got %s", plan.FailReason)
	}
	if plan.FlowsealMediaFixApplied {
		t.Fatal("core DC2 should not use flowseal media fix")
	}
	if plan.EffectiveDC != 2 || plan.EffectiveIsMedia {
		t.Fatalf("effective dc/media = %d/%t", plan.EffectiveDC, plan.EffectiveIsMedia)
	}
	if plan.WorkerDst != "149.154.167.51" {
		t.Fatalf("worker_dst=%q", plan.WorkerDst)
	}
	if !strings.Contains(plan.WorkerURL, "media=0") {
		t.Fatalf("workerURL=%s", plan.WorkerURL)
	}
}

func TestResolveCfWorkerDestination_ExperimentalForceMediaDC4UsesFallbackDCWhenUnknown(t *testing.T) {
	plan := ResolveCfWorkerDestination(WorkerDestinationInput{
		WorkerDomain:  "example.workers.dev",
		DCID:          0,
		IsMedia:       true,
		DCOk:          false,
		ParsedDstHost: "149.154.167.151",
		Mode:          WorkerDestinationFlowsealMediaDC4Fix,
		MediaFix: FlowsealMediaFixConfig{
			Enabled: true,
			DC:      4,
			IP:      "149.154.167.220",
		},
	})
	if !plan.OK {
		t.Fatalf("expected ok, got %s", plan.FailReason)
	}
	if !plan.FlowsealMediaFixApplied {
		t.Fatal("expected flowseal media fix")
	}
	if plan.EffectiveDC != 4 || !plan.EffectiveIsMedia {
		t.Fatalf("effective dc/media = %d/%t", plan.EffectiveDC, plan.EffectiveIsMedia)
	}
}

func TestIsUnknownTelegramIPv6WithoutDCMapping(t *testing.T) {
	if !IsUnknownTelegramIPv6WithoutDCMapping("2a0a:f280:203:a:5000::100") {
		t.Fatal("expected unknown telegram IPv6 without DC mapping")
	}
	if IsUnknownTelegramIPv6WithoutDCMapping("2001:67c:4e8:f002::a") {
		t.Fatal("known telegram IPv6 should have DC mapping")
	}
	if IsUnknownTelegramIPv6WithoutDCMapping("149.154.167.41") {
		t.Fatal("IPv4 should not match IPv6-only helper")
	}
}

func TestIsUnknownTelegramWithoutDCMapping_KnownCoreIP(t *testing.T) {
	if IsUnknownTelegramWithoutDCMapping("149.154.167.41") {
		t.Fatal("known core IP should have DC mapping")
	}
}

func TestAuditMediaClassification_CoreDC2NotEligibleForFlowsealRedirect(t *testing.T) {
	audit := AuditMediaClassification(WorkerDestinationInput{
		DCID:          2,
		IsMedia:       false,
		DCOk:          true,
		ParsedDstHost: "149.154.167.41",
		Mode:          WorkerDestinationExperimentalForceMediaDC4,
		MediaFix: FlowsealMediaFixConfig{
			Enabled: true,
			DC:      4,
			IP:      "149.154.167.220",
		},
	}, WorkerDestinationExperimentalForceMediaDC4, FlowsealMediaFixConfig{Enabled: true, DC: 4, IP: "149.154.167.220"})
	if audit.IsMedia {
		t.Fatal("core DC2 should not be classified as media")
	}
	if audit.TelegramClass != "core_dc" {
		t.Fatalf("telegram_class=%q", audit.TelegramClass)
	}
	if audit.MediaFixEligible {
		t.Fatal("core DC2 should not be eligible for flowseal redirect")
	}
	if audit.MediaFixSkipReason != "not_media_or_cdn" {
		t.Fatalf("media_fix_skip_reason=%q", audit.MediaFixSkipReason)
	}
}

func TestParseWorkerDestinationMode_LegacyAliases(t *testing.T) {
	if got := ParseWorkerDestinationMode("flowseal_media_dc4_fix"); got != WorkerDestinationExperimentalForceMediaDC4 {
		t.Fatalf("legacy alias=%q", got)
	}
	if got := ParseWorkerDestinationMode("experimental_force_media_dc4"); got != WorkerDestinationExperimentalForceMediaDC4 {
		t.Fatalf("new alias=%q", got)
	}
}
