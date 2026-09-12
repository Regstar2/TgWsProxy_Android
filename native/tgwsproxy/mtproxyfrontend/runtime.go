package mtproxyfrontend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Status string

const (
	StatusDisabled            Status = "DISABLED"
	StatusStarting            Status = "STARTING"
	StatusListeningLocalOnly  Status = "LISTENING_LOCAL_ONLY"
	StatusListeningRouteReady Status = "LISTENING_ROUTE_READY"
	StatusFailedPortInUse     Status = "FAILED_PORT_IN_USE"
	StatusFailedInvalidSecret Status = "FAILED_INVALID_SECRET"
	StatusFailedRuntimeError  Status = "FAILED_RUNTIME_ERROR"
	StatusStopped             Status = "STOPPED"
)

const OutboundUnsupported = "OUTBOUND_UNSUPPORTED"

var (
	ErrAlreadyRunning = errors.New("mtproto frontend already running")
	ErrInvalidSecret  = errors.New("invalid mtproto secret")
	ErrInvalidAddress = errors.New("invalid mtproto listen address")
	ErrNotRunning     = errors.New("mtproto frontend is not running")
)

type Config struct {
	Host                      string
	Port                      int
	Secret                    string
	FakeTLSDomain             string
	FakeTLSMaskingPassthrough bool
	ForceTestDC               bool
	Verbose                   bool
}

type StartResult struct {
	Status Status
	Err    error
}

type RuntimeStatus struct {
	Status                    Status
	Host                      string
	Port                      int
	SecretFingerprint         string
	FakeTLSEnabled            bool
	FakeTLSMaskingPassthrough bool
	FakeTLSAccepted           int64
	FakeTLSRejected           int64
	FakeTLSRedirected         int64
	FakeTLSProbe              int64
	FakeTLSPassthrough        int64
	FakeTLSLastError          string
	ForceTestDC               bool
	OutboundStatus            string
	SelectedBackend           string
	ActualBackend             string
	FallbackUsed              bool
	RouteReason               string
	ActiveConnections         int64
	TotalConnections          int64
	LastError                 string
}

type Logger interface {
	Printf(format string, args ...any)
}

type Runtime struct {
	mu        sync.Mutex
	listener  net.Listener
	cancel    context.CancelFunc
	logger    Logger
	connector OutboundConnector

	status                    Status
	host                      string
	port                      int
	secretFingerprint         string
	fakeTLSEnabled            bool
	fakeTLSMaskingPassthrough bool
	fakeTLSLastError          string
	forceTestDC               bool
	outboundStatus            string
	selectedBackend           string
	actualBackend             string
	fallbackUsed              bool
	routeReason               string
	lastError                 string
	activeConnections         atomic.Int64
	totalConnections          atomic.Int64
	fakeTLSAccepted           atomic.Int64
	fakeTLSRejected           atomic.Int64
	fakeTLSRedirected         atomic.Int64
	fakeTLSProbe              atomic.Int64
	fakeTLSPassthrough        atomic.Int64
}

func NewRuntime(logger Logger, connectors ...OutboundConnector) *Runtime {
	runtime := &Runtime{
		logger:         logger,
		status:         StatusDisabled,
		outboundStatus: OutboundUnsupported,
	}
	if len(connectors) > 0 {
		runtime.connector = connectors[0]
	}
	return runtime
}

func ValidateConfig(config Config) error {
	if strings.TrimSpace(config.Host) == "" || config.Port < 1 || config.Port > 65535 {
		return ErrInvalidAddress
	}
	if !IsRawSecret(config.Secret) {
		return ErrInvalidSecret
	}
	if strings.TrimSpace(config.FakeTLSDomain) != "" &&
		NormalizeFakeTLSDomain(config.FakeTLSDomain) == "" {
		return ErrInvalidAddress
	}
	return nil
}

func IsRawSecret(secret string) bool {
	normalized := strings.ToLower(strings.TrimSpace(secret))
	if len(normalized) != 32 {
		return false
	}
	_, err := hex.DecodeString(normalized)
	return err == nil
}

func SecretFingerprint(secret string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(secret))))
	return hex.EncodeToString(sum[:])[:12]
}

func (r *Runtime) Start(config Config) StartResult {
	rawFakeTLSDomain := strings.TrimSpace(config.FakeTLSDomain)
	normalizedFakeTLSDomain := NormalizeFakeTLSDomain(config.FakeTLSDomain)
	normalized := Config{
		Host:                      strings.TrimSpace(config.Host),
		Port:                      config.Port,
		Secret:                    strings.ToLower(strings.TrimSpace(config.Secret)),
		FakeTLSDomain:             normalizedFakeTLSDomain,
		FakeTLSMaskingPassthrough: config.FakeTLSMaskingPassthrough && normalizedFakeTLSDomain != "",
		ForceTestDC:               config.ForceTestDC,
		Verbose:                   config.Verbose,
	}
	if rawFakeTLSDomain != "" && normalizedFakeTLSDomain == "" {
		r.setFailure(StatusFailedRuntimeError, ErrInvalidAddress)
		return StartResult{Status: r.Status().Status, Err: ErrInvalidAddress}
	}
	if err := ValidateConfig(normalized); err != nil {
		r.setFailure(statusForValidationError(err), err)
		return StartResult{Status: r.Status().Status, Err: err}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.cancel != nil {
		return StartResult{Status: r.status, Err: ErrAlreadyRunning}
	}

	r.status = StatusStarting
	r.host = normalized.Host
	r.port = normalized.Port
	r.secretFingerprint = SecretFingerprint(normalized.Secret)
	r.fakeTLSEnabled = normalized.FakeTLSDomain != ""
	r.fakeTLSMaskingPassthrough = normalized.FakeTLSMaskingPassthrough
	r.forceTestDC = normalized.ForceTestDC
	r.lastError = ""
	r.fakeTLSLastError = ""
	r.actualBackend = ""
	r.fallbackUsed = false
	r.routeReason = "awaiting_connection"
	r.activeConnections.Store(0)
	r.totalConnections.Store(0)
	r.fakeTLSAccepted.Store(0)
	r.fakeTLSRejected.Store(0)
	r.fakeTLSRedirected.Store(0)
	r.fakeTLSProbe.Store(0)
	r.fakeTLSPassthrough.Store(0)
	r.outboundStatus = OutboundUnsupported
	r.selectedBackend = ""
	if r.connector != nil {
		capability := r.connector.Capability()
		r.outboundStatus = capability.Status
		r.selectedBackend = capability.SelectedBackend
	}

	addr := net.JoinHostPort(normalized.Host, strconv.Itoa(normalized.Port))
	r.log("MTProto frontend starting host=%s port=%d secret_fingerprint=%s fake_tls=%t masking_passthrough=%t force_test_dc=%t outbound=%s selected_backend=%s",
		normalized.Host, normalized.Port, r.secretFingerprint, r.fakeTLSEnabled, r.fakeTLSMaskingPassthrough, r.forceTestDC,
		r.outboundStatus, emptyStatusField(r.selectedBackend))

	ctx, cancel := context.WithCancel(context.Background())
	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		cancel()
		r.status = StatusFailedPortInUse
		r.lastError = err.Error()
		r.log("MTProto frontend failed: %s host=%s port=%d", r.status, normalized.Host, normalized.Port)
		return StartResult{Status: r.status, Err: fmt.Errorf("listen on %s: %w", addr, err)}
	}

	r.listener = listener
	r.cancel = cancel
	if r.connector == nil {
		r.status = StatusListeningLocalOnly
	} else {
		r.status = StatusListeningRouteReady
	}
	r.log("MTProto frontend listening on %s:%d status=%s outbound=%s selected_backend=%s",
		normalized.Host, normalized.Port, r.status, r.outboundStatus, emptyStatusField(r.selectedBackend))

	go r.acceptLoop(ctx, listener, normalized)
	return StartResult{Status: r.status}
}

func (r *Runtime) Stop() error {
	r.mu.Lock()
	cancel := r.cancel
	listener := r.listener
	if cancel == nil {
		r.mu.Unlock()
		return ErrNotRunning
	}
	r.cancel = nil
	r.listener = nil
	r.status = StatusStopped
	r.lastError = ""
	r.mu.Unlock()

	cancel()
	if listener != nil {
		_ = listener.Close()
	}
	r.log("MTProto frontend stopped")
	return nil
}

func (r *Runtime) Status() RuntimeStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	return RuntimeStatus{
		Status:                    r.status,
		Host:                      r.host,
		Port:                      r.port,
		SecretFingerprint:         r.secretFingerprint,
		FakeTLSEnabled:            r.fakeTLSEnabled,
		FakeTLSMaskingPassthrough: r.fakeTLSMaskingPassthrough,
		FakeTLSAccepted:           r.fakeTLSAccepted.Load(),
		FakeTLSRejected:           r.fakeTLSRejected.Load(),
		FakeTLSRedirected:         r.fakeTLSRedirected.Load(),
		FakeTLSProbe:              r.fakeTLSProbe.Load(),
		FakeTLSPassthrough:        r.fakeTLSPassthrough.Load(),
		FakeTLSLastError:          r.fakeTLSLastError,
		ForceTestDC:               r.forceTestDC,
		OutboundStatus:            r.outboundStatus,
		SelectedBackend:           r.selectedBackend,
		ActualBackend:             r.actualBackend,
		FallbackUsed:              r.fallbackUsed,
		RouteReason:               r.routeReason,
		ActiveConnections:         r.activeConnections.Load(),
		TotalConnections:          r.totalConnections.Load(),
		LastError:                 r.lastError,
	}
}

func (r *Runtime) StatusString() string {
	status := r.Status()
	return fmt.Sprintf(
		"status=%s;host=%s;port=%d;outbound=%s;selected_backend=%s;actual_backend=%s;fallback_used=%t;route_reason=%s;active=%d;total=%d;last_error=%s;secret_fingerprint=%s;fake_tls=%t;masking_passthrough=%t;fake_tls_accepted=%d;fake_tls_rejected=%d;fake_tls_redirected=%d;fake_tls_probe=%d;fake_tls_passthrough=%d;fake_tls_last_error=%s;force_test_dc=%t",
		status.Status,
		status.Host,
		status.Port,
		status.OutboundStatus,
		emptyStatusField(status.SelectedBackend),
		emptyStatusField(status.ActualBackend),
		status.FallbackUsed,
		emptyStatusField(status.RouteReason),
		status.ActiveConnections,
		status.TotalConnections,
		sanitizeStatusField(status.LastError),
		status.SecretFingerprint,
		status.FakeTLSEnabled,
		status.FakeTLSMaskingPassthrough,
		status.FakeTLSAccepted,
		status.FakeTLSRejected,
		status.FakeTLSRedirected,
		status.FakeTLSProbe,
		status.FakeTLSPassthrough,
		sanitizeStatusField(status.FakeTLSLastError),
		status.ForceTestDC,
	)
}

func (r *Runtime) acceptLoop(ctx context.Context, listener net.Listener, config Config) {
	current := listener
	addr := net.JoinHostPort(config.Host, strconv.Itoa(config.Port))
	for {
		conn, err := current.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue
				}
				next, restartErr := r.restartListener(ctx, current, addr)
				if restartErr != nil {
					r.setFailure(StatusFailedRuntimeError, restartErr)
					r.log("MTProto frontend failed: %s error=%s", StatusFailedRuntimeError, restartErr)
					return
				}
				current = next
				continue
			}
		}
		r.totalConnections.Add(1)
		r.activeConnections.Add(1)
		go r.handleConnection(ctx, conn, config)
	}
}

func (r *Runtime) restartListener(
	ctx context.Context,
	current net.Listener,
	addr string,
) (net.Listener, error) {
	r.log("MTProto listener watchdog restarting socket addr=%s", addr)
	_ = current.Close()
	for {
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}

		lc := net.ListenConfig{}
		next, err := lc.Listen(ctx, "tcp", addr)
		if err != nil {
			r.log("MTProto listener watchdog restart failed addr=%s error=%v", addr, err)
			continue
		}

		r.mu.Lock()
		r.listener = next
		if r.connector == nil {
			r.status = StatusListeningLocalOnly
		} else {
			r.status = StatusListeningRouteReady
		}
		r.lastError = ""
		r.mu.Unlock()
		r.log("MTProto listener watchdog restored socket addr=%s", addr)
		return next, nil
	}
}

func (r *Runtime) handleConnection(ctx context.Context, conn net.Conn, config Config) {
	defer r.activeConnections.Add(-1)
	defer conn.Close()
	remote := conn.RemoteAddr()
	if r.connector == nil {
		r.log("MTProto frontend accepted local connection remote=%s outbound=%s", remote, OutboundUnsupported)
		r.log("MTProto frontend closing local connection remote=%s reason=%s", remote, OutboundUnsupported)
		return
	}

	request, clientConn, err := prepareOutboundRequest(
		conn,
		config.Secret,
		config.FakeTLSDomain,
		config.FakeTLSMaskingPassthrough,
		config.ForceTestDC,
	)
	if err != nil {
		var passthrough *FakeTLSPassthroughRequest
		if errors.As(err, &passthrough) {
			r.noteFakeTLSPassthrough()
			r.updateRouteTruth(OutboundResult{
				SelectedBackend: r.connector.Capability().SelectedBackend,
				Reason:          "fake_tls_masking_passthrough",
			})
			r.log("MTProto fake TLS probe passthrough remote=%s domain=%s bytes=%d",
				remote, passthrough.Domain, len(passthrough.InitialData))
			if passErr := proxyFakeTLSPassthrough(ctx, conn, passthrough.Domain, passthrough.InitialData); passErr != nil {
				r.noteFakeTLSPassthroughFailure(passErr)
				r.updateRouteTruth(OutboundResult{
					SelectedBackend: r.connector.Capability().SelectedBackend,
					Reason:          "fake_tls_masking_passthrough_failed",
					Err:             passErr,
				})
				r.log("MTProto fake TLS masking passthrough failed remote=%s domain=%s error=%v",
					remote, passthrough.Domain, passErr)
			}
			return
		}
		if config.FakeTLSDomain != "" {
			switch {
			case errors.Is(err, ErrFakeTLSExpected):
				r.noteFakeTLSRedirect(err)
			case errors.Is(err, ErrFakeTLSVerification):
				r.noteFakeTLSRejected(err)
			default:
				r.noteFakeTLSError(err)
			}
		}
		r.updateRouteTruth(OutboundResult{
			SelectedBackend: r.connector.Capability().SelectedBackend,
			Reason:          "handshake_failed",
			Err:             err,
		})
		r.log("MTProto route rejected remote=%s reason=handshake_failed error=%v", remote, err)
		return
	}
	if config.FakeTLSDomain != "" {
		r.noteFakeTLSAccepted()
	}

	sessionID := SessionIDForRequest(request)
	r.log("MTProto route request session_id=%s remote=%s signed_dc=%d dc=%d media=%t test_dc=%t transport=%s selected_backend=%s",
		sessionID, remote, request.SignedDC, request.DCID, request.IsMedia, request.IsTestDC, request.Transport, r.connector.Capability().SelectedBackend)

	outbound, result := r.connector.Connect(ctx, request)
	r.updateRouteTruth(result)
	if result.Err != nil || outbound == nil {
		r.log("MTProto route failed frontend=MTProto session_id=%s signed_dc=%d dc=%d media=%t worker_dst=none selected_backend=%s actual_backend=%s fallback_used=%t reason=%s error=%v",
			sessionID, request.SignedDC, request.DCID, request.IsMedia,
			emptyStatusField(result.SelectedBackend), emptyStatusField(result.ActualBackend),
			result.FallbackUsed, emptyStatusField(result.Reason), result.Err)
		return
	}
	defer outbound.Close()

	diagnostics := RouteDiagnosticsFor(outbound, request)
	if diagnostics.SessionID != "" {
		sessionID = diagnostics.SessionID
	}
	workerDst := emptyStatusField(diagnostics.WorkerDst)

	r.log("MTProto route connected frontend=MTProto session_id=%s signed_dc=%d dc=%d media=%t worker_dst=%s selected_backend=%s actual_backend=%s fallback_used=%t reason=%s",
		sessionID, request.SignedDC, request.DCID, request.IsMedia, workerDst,
		result.SelectedBackend, result.ActualBackend, result.FallbackUsed, result.Reason)
	bridge := bridgeTransformedStreams(ctx, clientConn, outbound, request.ClientToRelay, request.RelayToClient)
	bridgeError := "none"
	if bridge.Err != nil {
		bridgeError = sanitizeStatusField(bridge.Err.Error())
	}
	r.log(
		"MTProto bridge closed frontend=MTProto session_id=%s signed_dc=%d dc=%d media=%t worker_dst=%s selected_backend=%s actual_backend=%s fallback_used=%t first_terminator=%s close_reason=%s up_bytes=%d down_bytes=%d up_chunks=%d down_chunks=%d duration_ms=%d error=%s",
		sessionID,
		request.SignedDC,
		request.DCID,
		request.IsMedia,
		workerDst,
		result.SelectedBackend,
		result.ActualBackend,
		result.FallbackUsed,
		bridge.FirstTerminator,
		bridge.CloseReason,
		bridge.UpBytes,
		bridge.DownBytes,
		bridge.UpChunks,
		bridge.DownChunks,
		bridge.Duration.Milliseconds(),
		bridgeError,
	)
	r.updateRouteTruth(OutboundResult{
		SelectedBackend: result.SelectedBackend,
		ActualBackend:   result.ActualBackend,
		FallbackUsed:    result.FallbackUsed,
		Reason:          "connection_closed",
	})
}

func (r *Runtime) updateRouteTruth(result OutboundResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.selectedBackend = result.SelectedBackend
	r.actualBackend = result.ActualBackend
	r.fallbackUsed = result.FallbackUsed
	r.routeReason = result.Reason
	if result.Err != nil {
		r.lastError = result.Err.Error()
	} else {
		r.lastError = ""
	}
}

func (r *Runtime) noteFakeTLSAccepted() {
	r.fakeTLSAccepted.Add(1)
}

func (r *Runtime) noteFakeTLSRedirect(err error) {
	r.fakeTLSRedirected.Add(1)
	r.fakeTLSProbe.Add(1)
	r.setFakeTLSError(err)
}

func (r *Runtime) noteFakeTLSRejected(err error) {
	r.fakeTLSRejected.Add(1)
	r.fakeTLSProbe.Add(1)
	r.setFakeTLSError(err)
}

func (r *Runtime) noteFakeTLSPassthrough() {
	r.fakeTLSPassthrough.Add(1)
	r.fakeTLSProbe.Add(1)
}

func (r *Runtime) noteFakeTLSPassthroughFailure(err error) {
	r.fakeTLSRejected.Add(1)
	r.setFakeTLSError(err)
}

func (r *Runtime) noteFakeTLSError(err error) {
	r.fakeTLSRejected.Add(1)
	r.setFakeTLSError(err)
}

func (r *Runtime) setFakeTLSError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		r.fakeTLSLastError = err.Error()
		return
	}
	r.fakeTLSLastError = ""
}

func (r *Runtime) setFailure(status Status, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status = status
	if err != nil {
		r.lastError = err.Error()
	}
}

func (r *Runtime) log(format string, args ...any) {
	if r.logger != nil {
		r.logger.Printf(format, args...)
	}
}

func statusForValidationError(err error) Status {
	if errors.Is(err, ErrInvalidSecret) {
		return StatusFailedInvalidSecret
	}
	return StatusFailedRuntimeError
}

func sanitizeStatusField(value string) string {
	return strings.NewReplacer(";", ",", "\n", " ", "\r", " ").Replace(value)
}

func emptyStatusField(value string) string {
	value = sanitizeStatusField(strings.TrimSpace(value))
	if value == "" {
		return "none"
	}
	return value
}

type bridgeDirectionResult struct {
	Direction string
	Bytes     int64
	Chunks    int64
	Reason    string
	Err       error
}

type bridgeResult struct {
	UpBytes         int64
	DownBytes       int64
	UpChunks        int64
	DownChunks      int64
	FirstTerminator string
	CloseReason     string
	Err             error
	Duration        time.Duration
}

func bridgeTransformedStreams(
	ctx context.Context,
	client net.Conn,
	outbound net.Conn,
	upTransform StreamTransform,
	downTransform StreamTransform,
) bridgeResult {
	started := time.Now()
	bridgeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan bridgeDirectionResult, 2)
	go func() {
		<-bridgeCtx.Done()
		_ = client.Close()
		_ = outbound.Close()
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	go copyTransformed(&wg, bridgeCtx, results, "client_to_upstream", outbound, client, upTransform)
	go copyTransformed(&wg, bridgeCtx, results, "upstream_to_client", client, outbound, downTransform)

	first := <-results
	cancel()
	wg.Wait()
	second := <-results

	result := bridgeResult{
		FirstTerminator: first.Direction,
		CloseReason:     first.Reason,
		Err:             first.Err,
		Duration:        time.Since(started),
	}
	applyBridgeDirectionResult(&result, first)
	applyBridgeDirectionResult(&result, second)
	return result
}

func applyBridgeDirectionResult(result *bridgeResult, direction bridgeDirectionResult) {
	if direction.Direction == "client_to_upstream" {
		result.UpBytes = direction.Bytes
		result.UpChunks = direction.Chunks
		return
	}
	result.DownBytes = direction.Bytes
	result.DownChunks = direction.Chunks
}

func copyTransformed(
	wg *sync.WaitGroup,
	ctx context.Context,
	results chan<- bridgeDirectionResult,
	direction string,
	dst io.Writer,
	src io.Reader,
	transform StreamTransform,
) {
	defer wg.Done()
	outcome := bridgeDirectionResult{Direction: direction}
	defer func() { results <- outcome }()

	buf := make([]byte, 64*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			chunk := append([]byte(nil), buf[:n]...)
			if transform != nil {
				chunk = transform(chunk)
			}
			if writeErr := writeFull(dst, chunk); writeErr != nil {
				if ctx.Err() != nil {
					outcome.Reason = "cancelled"
				} else {
					outcome.Reason = "write_error"
					outcome.Err = writeErr
				}
				return
			}
			outcome.Bytes += int64(n)
			outcome.Chunks++
		}
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				outcome.Reason = "eof"
			case ctx.Err() != nil:
				outcome.Reason = "cancelled"
			default:
				outcome.Reason = "read_error"
				outcome.Err = err
			}
			return
		}
	}
}

func writeFull(dst io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := dst.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
