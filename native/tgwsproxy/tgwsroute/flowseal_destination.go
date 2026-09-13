package tgwsroute

import (
	"net/netip"
	"strings"
)

const (
	WorkerDestinationPreserveOriginalDst       = "PRESERVE_ORIGINAL_DST"
	WorkerDestinationFlowsealDCMap             = "FLOWSEAL_DC_MAP"
	WorkerDestinationIPv6ToDCIPv4              = "IPV6_TO_DC_IPV4"
	WorkerDestinationExperimentalForceMediaDC4 = "EXPERIMENTAL_FORCE_MEDIA_DC4"
	// WorkerDestinationFlowsealMediaDC4Fix is a legacy wire label kept for parsing and logs.
	WorkerDestinationFlowsealMediaDC4Fix = "FLOWSEAL_MEDIA_DC4_FIX"

	WorkerDstSourceFlowsealDCMap       = "flowseal_dc_map"
	WorkerDstSourceFlowsealMediaDC4Fix = "flowseal_media_dc4_fix"

	FailTelegramIPv6UnknownDCNoMapping = "telegram_ipv6_unknown_dc_no_mapping"

	DefaultFlowsealMediaFixDC = 4
	DefaultFlowsealMediaFixIP = "149.154.167.220"
)

type FlowsealMediaFixConfig struct {
	Enabled bool
	DC      int
	IP      string
}

func (c FlowsealMediaFixConfig) normalized() FlowsealMediaFixConfig {
	out := c
	if out.DC <= 0 {
		out.DC = DefaultFlowsealMediaFixDC
	}
	out.IP = strings.TrimSpace(out.IP)
	if out.IP == "" {
		out.IP = DefaultFlowsealMediaFixIP
	}
	return out
}

type WorkerDestinationInput struct {
	WorkerDomain  string
	DCID          int
	IsMedia       bool
	DCOk          bool
	ParsedDstHost string
	Mode          string
	DcIPMap       map[int]string
	MediaFix      FlowsealMediaFixConfig
}

type WorkerDestinationPlan struct {
	CfWorkerTransparentResolve
	ConfiguredDestinationMode string
	EffectiveDestinationMode  string
	OriginalParsedDst         string
	EffectiveDC               int
	EffectiveIsMedia          bool
	FlowsealMediaFixApplied   bool
	MediaAudit                MediaClassificationAudit
}

// MediaClassificationAudit captures per-route media classification for diagnostics.
type MediaClassificationAudit struct {
	IsMedia            bool
	MediaReason        string
	TelegramClass      string
	MediaFixEligible   bool
	MediaFixSkipReason string
}

// DestinationMode returns the configured destination mode wire label.
func (p WorkerDestinationPlan) DestinationMode() string {
	return p.ConfiguredDestinationMode
}

func ParseWorkerDestinationMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "flowseal_dc_map", "dc_map":
		return WorkerDestinationFlowsealDCMap
	case "flowseal_media_dc4_fix", "media_dc4_fix", "flowseal_media_fix",
		"experimental_force_media_dc4", "force_media_dc4":
		return WorkerDestinationExperimentalForceMediaDC4
	default:
		return WorkerDestinationPreserveOriginalDst
	}
}

func IsExperimentalForceMediaDC4Mode(mode string) bool {
	return ParseWorkerDestinationMode(mode) == WorkerDestinationExperimentalForceMediaDC4
}

func IsMediaOrCdnRoute(isMedia, dcOk bool, parsedDstHost string) bool {
	if isMedia {
		return true
	}
	if info, ok := LookupTelegramDC(parsedDstHost); ok && info.Media {
		return true
	}
	if !dcOk && IsTelegramLikeIP(parsedDstHost) {
		return true
	}
	return false
}

func ResolveCfWorkerDestination(input WorkerDestinationInput) WorkerDestinationPlan {
	mode := ParseWorkerDestinationMode(input.Mode)
	mediaFix := input.MediaFix.normalized()
	original := strings.TrimSpace(input.ParsedDstHost)
	plan := WorkerDestinationPlan{
		CfWorkerTransparentResolve: ResolveCfWorkerTransparentWorker(
			input.WorkerDomain, input.DCID, input.IsMedia, input.DCOk, original,
		),
		ConfiguredDestinationMode: mode,
		OriginalParsedDst:         original,
		EffectiveDC:               input.DCID,
		EffectiveIsMedia:          input.IsMedia,
		MediaAudit:                AuditMediaClassification(input, mode, mediaFix),
	}

	if !plan.OK {
		plan.EffectiveDestinationMode = effectiveDestinationModeForResolve(plan)
		return plan
	}

	applyMediaFix := IsExperimentalForceMediaDC4Mode(mode) &&
		mediaFix.Enabled &&
		IsMediaOrCdnRoute(input.IsMedia, input.DCOk, original)

	if applyMediaFix {
		plan.FlowsealMediaFixApplied = true
		// Keep Telegram's logical DC/media identity intact. The Flowseal-style
		// media fix is a physical destination override only: a DC2 media
		// handshake must stay DC2 media even when TCP is sent to 149.154.167.220.
		// mediaFix.DC remains only as a fallback for synthetic/unknown routes
		// that do not carry a valid logical DC.
		plan.EffectiveDC = input.DCID
		if plan.EffectiveDC <= 0 {
			plan.EffectiveDC = mediaFix.DC
		}
		plan.EffectiveIsMedia = true
		plan.WorkerDst = mediaFix.IP
		plan.WorkerDstSource = WorkerDstSourceFlowsealMediaDC4Fix
		plan.PreserveOriginalDst = false
		plan.WorkerURL = BuildWorkerWSURL(input.WorkerDomain, plan.EffectiveDC, plan.WorkerDst, plan.EffectiveIsMedia, "")
		plan.OK = !WorkerURLContainsRawIPv6Dst(plan.WorkerURL)
		if !plan.OK {
			plan.FailReason = FailWorkerIPv6RawDstBlocked
		}
		plan.EffectiveDestinationMode = WorkerDestinationExperimentalForceMediaDC4
		return plan
	}

	switch mode {
	case WorkerDestinationExperimentalForceMediaDC4:
		// Experimental force requires explicit mediaFix.Enabled; preserve-original otherwise.
	case WorkerDestinationFlowsealDCMap:
		if !input.DCOk || input.DCID <= 0 {
			plan.EffectiveDestinationMode = effectiveDestinationModeForResolve(plan)
			return plan
		}
		mappedIP := configuredDCIp(input.DcIPMap, input.DCID)
		if mappedIP == "" {
			if canonical, ok := WorkerCanonicalIPv4ForDC(input.DCID); ok {
				mappedIP = canonical
			}
		}
		if mappedIP == "" {
			plan.EffectiveDestinationMode = effectiveDestinationModeForResolve(plan)
			return plan
		}
		plan.WorkerDst = mappedIP
		plan.WorkerDstSource = WorkerDstSourceFlowsealDCMap
		plan.PreserveOriginalDst = false
		plan.WorkerURL = BuildWorkerWSURL(input.WorkerDomain, input.DCID, plan.WorkerDst, input.IsMedia, "")
		plan.OK = !WorkerURLContainsRawIPv6Dst(plan.WorkerURL)
		if !plan.OK {
			plan.FailReason = FailWorkerIPv6RawDstBlocked
		}
		plan.EffectiveDestinationMode = WorkerDestinationFlowsealDCMap
		return plan
	default:
		// PRESERVE_ORIGINAL_DST: base resolve already preserves parsed IPv4 dst.
	}

	plan.EffectiveDestinationMode = effectiveDestinationModeForResolve(plan)
	return plan
}

func effectiveDestinationModeForResolve(plan WorkerDestinationPlan) string {
	if plan.FlowsealMediaFixApplied {
		return WorkerDestinationExperimentalForceMediaDC4
	}
	if plan.WorkerDstSource == WorkerDstSourceFlowsealDCMap {
		return WorkerDestinationFlowsealDCMap
	}
	if plan.WorkerDstSource == WorkerDstSourceIPv6ToDCIPv4 {
		return WorkerDestinationIPv6ToDCIPv4
	}
	return WorkerDestinationPreserveOriginalDst
}

func AuditMediaClassification(input WorkerDestinationInput, mode string, mediaFix FlowsealMediaFixConfig) MediaClassificationAudit {
	isMedia := input.IsMedia
	mediaReason := "not_media"
	if input.IsMedia {
		mediaReason = "init_session_is_media"
	} else if info, ok := LookupTelegramDC(input.ParsedDstHost); ok && info.Media {
		isMedia = true
		mediaReason = "telegram_dc_media_ip"
	} else if !input.DCOk && IsTelegramLikeIP(input.ParsedDstHost) {
		isMedia = true
		mediaReason = "telegram_like_unknown_dc"
	}

	telegramClass := classifyTelegramRoute(input)
	eligible := IsExperimentalForceMediaDC4Mode(mode) &&
		mediaFix.Enabled &&
		IsMediaOrCdnRoute(input.IsMedia, input.DCOk, input.ParsedDstHost)

	skipReason := ""
	if !eligible {
		switch {
		case !IsExperimentalForceMediaDC4Mode(mode):
			skipReason = "mode_not_experimental"
		case !mediaFix.Enabled:
			skipReason = "media_fix_disabled"
		case !IsMediaOrCdnRoute(input.IsMedia, input.DCOk, input.ParsedDstHost):
			skipReason = "not_media_or_cdn"
		default:
			skipReason = "unknown"
		}
	}

	return MediaClassificationAudit{
		IsMedia:            isMedia,
		MediaReason:        mediaReason,
		TelegramClass:      telegramClass,
		MediaFixEligible:   eligible,
		MediaFixSkipReason: skipReason,
	}
}

func classifyTelegramRoute(input WorkerDestinationInput) string {
	if info, ok := LookupTelegramDC(input.ParsedDstHost); ok {
		if info.Media {
			return "media_dc"
		}
		return "core_dc"
	}
	if IsTelegramLikeIP(input.ParsedDstHost) {
		return "cdn"
	}
	return "unknown"
}

func configuredDCIp(dcIPMap map[int]string, dc int) string {
	if dcIPMap != nil {
		if ip := strings.TrimSpace(dcIPMap[dc]); ip != "" {
			return ip
		}
	}
	if canonical, ok := WorkerCanonicalIPv4ForDC(dc); ok {
		return canonical
	}
	return ""
}

// IsUnknownTelegramWithoutDCMapping reports Telegram-like destinations with no reliable DC entry.
func IsUnknownTelegramWithoutDCMapping(dst string) bool {
	if info, ok := LookupTelegramDC(dst); ok && info.DC > 0 {
		return false
	}
	return IsTelegramLikeIP(dst)
}

func IsUnknownTelegramIPv6WithoutDCMapping(dst string) bool {
	addr, ok := parseAddr(dst)
	if !ok || !addr.Is6() {
		return false
	}
	return IsUnknownTelegramWithoutDCMapping(dst)
}

func MediaFixTargetIP(mediaFix FlowsealMediaFixConfig) string {
	ip := strings.TrimSpace(mediaFix.normalized().IP)
	if ip == "" {
		return DefaultFlowsealMediaFixIP
	}
	if _, err := netip.ParseAddr(ip); err != nil {
		return DefaultFlowsealMediaFixIP
	}
	return ip
}
