# Changelog

All notable user-facing changes are listed here. Detailed notes for older releases: [docs/releases/](docs/releases/).

## Unreleased - Worker transport investigation (#29, PR #39)

- Worker WebSocket close/cancellation no longer waits behind a blocked write; stream deadlines reach the socket and frame writes have a 45-second ceiling.
- Removed the unsuccessful 65535 + 1 fragment probe and restored one complete outbound WebSocket message. Removed shared 16-KiB segmentation that also changed packet-oriented routes.
- Worker source now connects TCP lazily, orders asynchronous message conversion with writes, bounds its queue and cancels pending writes on close.
- Added correlated Worker message/write logs, a deployed revision header and Linux/Android TCP delivery counters during stalls.
- Native rebuild inputs now include all Go subpackages and module files, preventing stale native libraries after frontend changes.
- Added real local TCP/TLS bulk regression tests, deployed-handler queue tests and a PowerShell build/install/log capture script. Android/Cloudflare/Telegram media acceptance remains pending.

## 1.10.13 - 2026-08-26
- MTProto WebSocket receive path now reassembles fragmented/continuation messages instead of dropping continuation frames.
- Added 16 MiB protection for individual WebSocket frames and accumulated fragmented messages before payload allocation/delivery.
- Failed/rejected MTProto WebSocket receive paths close the underlying connection.
- Worker preconnect remains disabled by default. When explicitly enabled, a session pool miss no longer launches a duplicate background refill while the foreground Worker dial is already in progress; hits still replenish the pool.
- Worker failover is regression-tested to stop after the first successful candidate.
- Worker ordering for equal creation timestamps preserves stable persistence/insertion order instead of using random UUID order.
- Reviewed relevant Flowseal runtime changes through `b2a8074`; Android-specific route keys, cooldown/watchdog/fake-TLS/diagnostics behavior are retained and disabled upstream 429 logic is not ported.
- Added a dedicated **Feedback** settings section backed by GitHub Issue Forms, with optional safe app/device context and no embedded GitHub write credential.
- Added a dedicated **Updates** section that checks only the official GitHub Releases feed asynchronously, applies SemVer stable/prerelease policy, shows bounded/cleaned release notes, and opens only the validated official release page.
- Update checks do not silently download/install APKs, add install permissions, or touch proxy runtime state.
- Public PR CI now runs on GitHub-hosted Windows; Project Sync uses GitHub-hosted Ubuntu; the persistent self-hosted runner is reserved for owner-controlled release/signing.
- Added signed release packaging verification and SHA-256 artifact generation through `scripts/release.ps1`.
- Added release-readiness auditing for version metadata, RU/EN resource parity, obsolete repository links, tracked private/local files, signing material, logs, environment files, and common credential signatures.
- Public README/docs now describe source `1.10.13` accurately and use the canonical `Regstar2/tg-ws-proxy-android` repository URL.
- Added `docs/research/flowseal-upstream-2026-08-24.md` with port/no-port decisions and `docs/releases/RELEASE_NOTES_v1.10.13.md` for the final release body.

## 1.10.12

- MTProto Proxy is now the default local frontend on shared port `1443`; SOCKS5/WS stays available as a compatibility mode.
- MTProto routing uses the existing route backend, including `cf_proxy_ws`. The `MTProto -> Cloudflare Proxy` path was manually verified on mobile network and Wi-Fi.
- Fake TLS support for MTProto links: `dd<secret>` by default, `ee<secret><domain_hex>` when a masking domain is configured, plus optional masking-domain passthrough for probes.
- Flowseal-inspired runtime hardening: direct IP cooldown, WS pool age rotation, listener watchdog, test DC support, and CF domain refresh quality gate.
- Main screen cleanup: no Worker Pool button, no expandable metrics details, and no Worker Pool line in the compact status card.
- Runtime log collection and persistent file logs are off by default; they can be enabled manually for diagnostics.
- README refreshed for publication, screenshots replaced, and local runtime artifacts/logs removed from the working tree.
- Known issue: Worker Pool is still slow and should be treated as a diagnostics/development path, not the default route.

## 1.9.8

- **DC2 Worker destination scoring:** per-destination runtime score keyed by `dc + worker_dst + destination_mode` tracks bidirectional vs zero-down sessions, heavy zero-down upload volume, and a 5-minute penalty window after `ws_up_bytes >= 64 KiB` with no downstream data.
- **DC2 candidate pool:** `149.154.167.41`, `149.154.167.50`, `149.154.167.51`; selection prefers higher-scored candidates, demotes `.50` by default unless DC2 IP is manually overridden, and can switch away from a penalized preserve-original destination.
- **IPv6→DC2 IPv4 mapping** no longer always picks `149.154.167.51`; uses the same scoring among DC2 candidates.
- **Early zero-down retry (DC2):** if the first 64 KiB upload gets no downstream packets, the worker session closes once and retries on another DC2 candidate (single retry, no loop).
- Logs: `dc_candidate_selected`, `dc_candidate_score`, `dc_candidate_penalty`, `zero_down_detected`, `heavy_zero_down_detected`, `worker_dst_penalized`, `retry_with_alternative_dst`, `original_ipv6_dst`, `selected_dc2_candidate`.
- Cloudflare Worker script unchanged; worker endpoint pool and disabled idle WS preconnect unchanged.

## 1.9.7

- **Worker data-plane correlation:** each `cf_worker_ws` session gets a monotonic `session_id` (`sid` query param on `/apiws`); same id in Android runtime logs and Cloudflare Worker `console.log` for end-to-end tracing.
- Logs: `configured_destination_mode` vs `effective_destination_mode` (no misleading `EXPERIMENTAL_FORCE_MEDIA_DC4` when media fix was skipped), `media_classification` audit (`media_reason`, `telegram_class`, `media_fix_eligible`, `media_fix_skip_reason`), `worker_session_result` (`bidirectional` / `zero_down` / `no_payload`).
- Proxy status destination metrics extended per profile: `original_parsed_dst`, `worker_dst`, `mapped_dc`, `is_media`, `flowseal_media_fix_applied`, `sessions_bidirectional`, `sessions_zero_down`, `up_bytes_total`, `down_bytes_total`.
- Cloudflare Worker script: reads `sid` and includes it in all relay log points.

## 1.9.6

- **Safe Flowseal media fix scope:** default `socks5_transparent` destination mode is `PRESERVE_ORIGINAL_DST`; original SOCKS5 destination is kept for media/CDN unless you explicitly enable experimental media DC4 fix.
- Destination modes: `PRESERVE_ORIGINAL_DST`, `FLOWSEAL_DC_MAP`, `EXPERIMENTAL_FORCE_MEDIA_DC4` (legacy pref `flowseal_media_dc4_fix`); fix applies only with `@flowseal_media_fix_enabled=1` on classified media routes.
- Flowseal DC→IP preset sets `FLOWSEAL_DC_MAP` only (no automatic media DC4 rewrite).
- Unknown Telegram IPv6/CDN without DC mapping: `route_decision=blocked_or_failed` (`telegram_ipv6_unknown_dc_no_mapping`); no silent DC4 bootstrap; TCP passthrough only when policy allows `tcp_fallback`.
- Logs: `experimental_flowseal_dc4_force=true`, `warning=transparent_dst_rewrite_may_break_media`.
- Proxy status A/B metrics: `destination_mode_stats` per mode (sessions, zero_down, avg_duration_ms, close_reason, media_fix_applied).

## 1.9.5

- **Flowseal DC/IP override mode:** `cf_worker_ws` destination modes — `PRESERVE_ORIGINAL_DST`, `FLOWSEAL_DC_MAP`, `FLOWSEAL_MEDIA_DC4_FIX` (media/CDN routes use configurable DC4 IP, default `149.154.167.220`).
- Unknown Telegram-like CDN destinations bootstrap to Worker instead of TCP passthrough when media DC4 fix is enabled.
- Worker Pool settings: destination mode selector and Flowseal media fix DC/IP fields; runtime tokens `@worker_destination_mode`, `@flowseal_media_fix_*`.
- Logs: `destination_mode`, `original_parsed_dst`, `flowseal_media_fix_applied`, `worker_dst_source=flowseal_media_dc4_fix`.

## 1.9.4

- **Worker Endpoint Pool without idle WS preconnect:** Worker Pool still selects/failovers between worker endpoints (manual, priority, failover, round-robin, lowest latency). Pre-opened idle `/apiws` WebSocket pool is disabled for `cf_worker_ws` / `socks5_transparent` — each SOCKS5 CONNECT reads `first_packet`, selects endpoint, opens a fresh WebSocket, sends `first_packet`, then marks route success.
- Runtime logs: `Worker endpoint selected`, `Worker WS preconnect disabled reason=cf_worker_ws_requires_first_payload`, `opening_worker_ws_after_first_packet=true`, `first_packet_sent_to_worker=true`, `route_success_after_first_write=true`.
- Metrics split: `worker_endpoint_pool_hits/misses` (endpoint selection/failover) vs `worker_ws_preconnect_*` (disabled for cf_worker_ws).
- Worker Pool UI polish: structured screen with summary, strategy, runtime state, and readable worker cards.
- Selected vs runtime worker markers, health states, empty/warning/invalid config states, and improved add/edit/delete flows.
- Diagnostics shows Worker Pool summary with link to Worker Pool settings.

## 1.9.3

- Worker selection strategies: Manual, Priority, Failover (default), Round-robin, and Lowest latency (cached health check data).
- Strategy is saved in Worker Pool settings; ordered candidates feed existing failover for new connections only.
- Runtime Route Truth, Diagnostics, and Diagnostic Report show active strategy, candidate order, and selection reason.

## 1.9.2

- Worker failover: when the selected worker fails, the app tries other enabled workers for new connections (does not change user-selected worker).
- Runtime Route Truth shows selected worker vs runtime worker, failover reason, and attempt count.
- Diagnostic report and Diagnostics screen include Worker Failover summary; persistent logs record failover attempt chain.

## 1.9.1

- Worker health check: per-worker and check-all diagnostics (DNS, TCP, TLS, WebSocket handshake, latency, timeout).
- Worker Pool UI shows state, latency, last checked, failures, and last error; health check does not change selected worker or active route.
- Diagnostic report and Diagnostics screen include Worker Pool health summary; persistent logs record health check events.

## 1.9.0

- Worker Pool foundation: multiple Worker endpoints with add/edit/delete, enable/disable, and manual selection.
- Safe migration from legacy single Worker domain to first pool entry on upgrade.
- Worker WebSocket route uses selected Worker when pool is enabled; legacy single Worker when disabled.
- Runtime route truth and diagnostic report include Worker Pool summary with masked URLs.
- Route probe Worker WebSocket checks selected Worker only (no multi-worker probe yet).

## 1.8.4

- Diagnostic report: copy/share from Route diagnostics screen (runtime route, probe results, config summary, recent logs).
- Report and persistent logs sanitization (tokens, secrets, Authorization, URL query params).
- Persistent log rotation tuned to 1 MB per file with up to 3 archived files (existing enable/export/clear in Settings → App).

## 1.8.3

- Route diagnostics screen with probe result cards, step details, and read-only runtime route block.
- Entry from main screen and Settings; uses `RouteDiagnosticsRepository` (no duplicate network checks in UI).

## 1.8.2

- Route Probe Core: structured route diagnostics (`RouteProbeRunner`, step-based DNS/TCP/TLS/HTTP/WebSocket checks).
- `RouteDiagnosticsRepository` and metrics-card **Check routes** button; results in UI and logcat only (no routing changes).
- Unit tests for probe status aggregation.

## 1.8.1

- Runtime route truth: UI separates configured mode, selected route, active route, last success/failure, and fallback reason.
- Go runtime exports structured route state via `GetProxyStatus` and logs route lifecycle events (`Route selected`, `Route connect started`, `Fallback activated`, etc.).
- Connection metrics card shows a compact route status block (strings in resources, EN/RU).

## 1.8.0

- Repository cleanup: Go runtime moved to `native/tgwsproxy/`.
- Documentation layout: `docs/architecture/`, `docs/development/`, `docs/testing/`, `docs/releases/`, `docs/assets/screenshots/`.
- README updated (structure, build, screenshot paths).
- `.gitignore` hardened for `artifacts/`, `runtime-logs/`, logs, secrets, keystores.
- Added `docs/development/repository-structure.md`, `docs/testing/manual-checklist-1.8.md`.
- Removed default Cloudflare domain `pclead.co.uk` (manual domain empty by default; built-in/cached pool).
- No intentional runtime or routing behavior changes for the v1.8.0 tag itself (1.7.9.x fixes included).

## 1.7.9.3

- Fixed route status showing generic “websocket” instead of actual route kind.
- Separated route kind from transport type in runtime/UI stats.
- Current route uses active sessions for the current policy generation; stale direct/worker events no longer override UI.
- Improved diagnostics logs (`routeKind`, `transportType`, `policyGeneration`).

## 1.7.9.2

- Route policy tokens from Android are applied in Go runtime; disabled routes (especially worker) are no longer selected.
- Policy acts as absolute filter on adaptive selection and fallbacks.

## 1.7.9.1

- Safer default route policies for mobile/Wi‑Fi; migration for users who did not customize policies.
- “Recommended” preset in route policy UI.

Earlier versions: see [docs/releases/](docs/releases/).

