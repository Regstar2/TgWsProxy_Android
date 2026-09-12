package main

/*
#cgo android LDFLAGS: -llog
#include <stdlib.h>
#include <signal.h>
#ifdef __ANDROID__
#include <android/log.h>
#endif

static void androidLogProxy(char *msg) {
#ifdef __ANDROID__
    __android_log_print(ANDROID_LOG_INFO, "TgWsProxy", "%s", msg);
#endif
}
*/
import "C"

import (
	"bufio"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/netip"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"tg-ws-proxy/mtproxyfrontend"
	"tg-ws-proxy/tgwsroute"
)

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const (
	defaultPort           = 1081
	tcpNodelay            = true
	defaultRecvBuf        = 256 * 1024
	defaultSendBuf        = 256 * 1024
	defaultPoolSz         = 4
	defaultCfProxyDomain  = ""
	wsPoolMaxAge          = 60.0
	wsPoolRotationCheck   = 5.0
	wsBridgeIdle          = 120.0
	bridgeSlowOpThreshold = 750 * time.Millisecond

	dcFailCooldown       = 30.0
	ipFailCooldown       = 3600.0
	wsFailTimeout        = 2.0
	poolMaintainInterval = 15
)

var (
	recvBuf    = defaultRecvBuf
	sendBuf    = defaultSendBuf
	poolSize   = defaultPoolSz
	logVerbose = false
)

// ---------------------------------------------------------------------------
// Logger
// ---------------------------------------------------------------------------

var (
	logInfo  *log.Logger
	logWarn  *log.Logger
	logError *log.Logger
	logDebug *log.Logger
)

type androidLogWriter struct{}

func (w androidLogWriter) Write(p []byte) (n int, err error) {
	_, _ = os.Stderr.Write(p)
	cs := C.CString(string(p))
	C.androidLogProxy(cs)
	C.free(unsafe.Pointer(cs))
	return len(p), nil
}

func initLogging(verbose bool) {
	flags := log.Ltime
	out := androidLogWriter{}
	logInfo = log.New(out, "INFO  ", flags)
	logWarn = log.New(out, "WARN  ", flags)
	logError = log.New(out, "ERROR ", flags)
	if verbose {
		logDebug = log.New(out, "DEBUG ", flags)
	} else {
		logDebug = log.New(io.Discard, "", 0)
	}
}

var dcOverrides = map[int]int{
	203: 2,
}

var dcDefaultIP = map[int]string{
	1:   "149.154.175.50",
	2:   "149.154.167.51",
	3:   "149.154.175.100",
	4:   "149.154.167.91",
	5:   "149.154.171.5",
	203: "91.105.192.100",
}

var dcTestIP = map[int]string{
	1: "149.154.175.10",
	2: "149.154.167.40",
	3: "149.154.175.117",
}

var validProtos = map[uint32]bool{
	0xEFEFEFEF: true,
	0xEEEEEEEE: true,
	0xDDDDDDDD: true,
}

// ---------------------------------------------------------------------------
// Global state
// ---------------------------------------------------------------------------

var (
	dcOpt   map[int]string
	dcOptMu sync.RWMutex

	cfCfg   = cfProxyConfig{Domain: defaultCfProxyDomain}
	cfCfgMu sync.RWMutex

	wsBlackMu   sync.RWMutex
	wsBlacklist = make(map[[2]int]bool)

	dcFailMu    sync.RWMutex
	dcFailUntil = make(map[[2]int]float64)

	ipFailMu    sync.RWMutex
	ipFailUntil = make(map[string]float64)

	zero64 = make([]byte, 64)
)

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

type Stats struct {
	connectionsTotal            atomic.Int64
	connectionsWs               atomic.Int64
	connectionsCfProxy          atomic.Int64
	connectionsCfWorker         atomic.Int64
	connectionsTcpFallback      atomic.Int64
	connectionsHttpReject       atomic.Int64
	connectionsPassthrough      atomic.Int64
	wsErrors                    atomic.Int64
	bytesUp                     atomic.Int64
	bytesDown                   atomic.Int64
	poolHits                    atomic.Int64
	poolMisses                  atomic.Int64
	workerEndpointPoolHits      atomic.Int64
	workerEndpointPoolMisses    atomic.Int64
	workerWsPreconnectHits      atomic.Int64
	workerWsPreconnectMisses    atomic.Int64
	workerWsPreconnectRefillErr atomic.Int64
	cfPoolHits                  atomic.Int64
	cfPoolMisses                atomic.Int64
	cfPoolRefillErrors          atomic.Int64
}

func (s *Stats) Summary() string {
	ph := s.poolHits.Load()
	pm := s.poolMisses.Load()
	return fmt.Sprintf(
		"total=%d ws=%d cf=%d worker=%d tcp_fb=%d http_skip=%d pass=%d err=%d pool=%d/%d up=%s down=%s",
		s.connectionsTotal.Load(),
		s.connectionsWs.Load(),
		s.connectionsCfProxy.Load(),
		s.connectionsCfWorker.Load(),
		s.connectionsTcpFallback.Load(),
		s.connectionsHttpReject.Load(),
		s.connectionsPassthrough.Load(),
		s.wsErrors.Load(),
		ph, ph+pm,
		humanBytes(s.bytesUp.Load()),
		humanBytes(s.bytesDown.Load()),
	)
}

func (s *Stats) Reset() {
	s.connectionsTotal.Store(0)
	s.connectionsWs.Store(0)
	s.connectionsCfProxy.Store(0)
	s.connectionsCfWorker.Store(0)
	s.connectionsTcpFallback.Store(0)
	s.connectionsHttpReject.Store(0)
	s.connectionsPassthrough.Store(0)
	s.wsErrors.Store(0)
	s.bytesUp.Store(0)
	s.bytesDown.Store(0)
	s.poolHits.Store(0)
	s.poolMisses.Store(0)
	s.workerEndpointPoolHits.Store(0)
	s.workerEndpointPoolMisses.Store(0)
	s.workerWsPreconnectHits.Store(0)
	s.workerWsPreconnectMisses.Store(0)
	s.workerWsPreconnectRefillErr.Store(0)
	s.cfPoolHits.Store(0)
	s.cfPoolMisses.Store(0)
	s.cfPoolRefillErrors.Store(0)
}

var stats Stats

func humanBytes(n int64) string {
	abs := n
	if abs < 0 {
		abs = -abs
	}
	units := []string{"B", "KB", "MB", "GB", "TB"}
	f := float64(n)
	for i, u := range units {
		if math.Abs(f) < 1024 || i == len(units)-1 {
			return fmt.Sprintf("%.1f%s", f, u)
		}
		f /= 1024
	}
	return fmt.Sprintf("%.1f%s", f, "TB")
}

// ---------------------------------------------------------------------------
// Socket helpers
// ---------------------------------------------------------------------------

func setSockOpts(conn net.Conn) {
	tc, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	if tcpNodelay {
		_ = tc.SetNoDelay(true)
	}
	raw, err := tc.SyscallConn()
	if err != nil {
		return
	}
	_ = raw.Control(func(fd uintptr) {
		_ = setSockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, recvBuf)
		_ = setSockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, sendBuf)
	})
}

func classifyConnError(err error) string {
	if err == nil {
		return "ok"
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return "dns_resolution_failure"
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return "tcp_dial_timeout"
	}
	return "network_error"
}

func joinAddr(host string, port int) string {
	return net.JoinHostPort(host, strconv.Itoa(port))
}

type wsStageError struct {
	Stage string
	Err   error
}

func (e *wsStageError) Error() string {
	if e == nil || e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

func (e *wsStageError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type resolvedIPs struct {
	IPv4 []string
	IPv6 []string
}

type cfProxyConfig struct {
	Enabled  bool
	Priority bool
	Only     bool
	Domain   string
}

func (r resolvedIPs) Preferred() []string {
	if len(r.IPv4) > 0 {
		return r.IPv4
	}
	return r.IPv6
}

func (r resolvedIPs) All() []string {
	out := make([]string, 0, len(r.IPv4)+len(r.IPv6))
	out = append(out, r.IPv4...)
	out = append(out, r.IPv6...)
	return out
}

func (r resolvedIPs) String() string {
	parts := make([]string, 0, 2)
	if len(r.IPv4) > 0 {
		parts = append(parts, "ipv4="+strings.Join(r.IPv4, ","))
	}
	if len(r.IPv6) > 0 {
		parts = append(parts, "ipv6="+strings.Join(r.IPv6, ","))
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, " ")
}

func normalizeCfProxyConfig(cfg cfProxyConfig) cfProxyConfig {
	if normalized, ok := tgwsroute.NormalizeCFDomain(cfg.Domain); ok {
		cfg.Domain = normalized
	} else {
		cfg.Domain = ""
	}
	if cfg.Only {
		cfg.Enabled = true
		cfg.Priority = true
	}
	return cfg
}

func setCfProxyConfig(cfg cfProxyConfig) {
	cfg = normalizeCfProxyConfig(cfg)
	cfCfgMu.Lock()
	cfCfg = cfg
	cfCfgMu.Unlock()

	settings := getRuntimeSettings()
	settings.CF = cfg
	if settings.Mode == "" {
		settings.Mode = legacyModeFromCF(cfg)
	}
	setRuntimeSettings(settings)
}

func getCfProxyConfig() cfProxyConfig {
	cfCfgMu.RLock()
	defer cfCfgMu.RUnlock()
	return cfCfg
}

func parseBoolValue(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func shouldTryDomainDial(err error) bool {
	var stageErr *wsStageError
	if !errors.As(err, &stageErr) || stageErr.Stage != "tcp_dial" {
		return false
	}
	switch classifyConnError(stageErr.Err) {
	case "tcp_dial_timeout", "network_error", "dns_resolution_failure":
		return true
	default:
		return false
	}
}

func directWSErrorTimedOut(err error) bool {
	if err == nil {
		return false
	}
	var stageErr *wsStageError
	if errors.As(err, &stageErr) {
		return classifyConnError(stageErr.Err) == "tcp_dial_timeout"
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return true
	}
	return false
}

func directIPCooldownKey(target string) string {
	return strings.TrimSpace(target)
}

func directIPCoolingDown(target string, now float64) bool {
	key := directIPCooldownKey(target)
	if key == "" {
		return false
	}
	ipFailMu.RLock()
	until := ipFailUntil[key]
	ipFailMu.RUnlock()
	return now < until
}

func markDirectIPTimeout(target string, now float64) {
	key := directIPCooldownKey(target)
	if key == "" {
		return
	}
	ipFailMu.Lock()
	ipFailUntil[key] = now + ipFailCooldown
	ipFailMu.Unlock()
}

func markDirectIPSuccess(target string) {
	key := directIPCooldownKey(target)
	if key == "" {
		return
	}
	ipFailMu.Lock()
	delete(ipFailUntil, key)
	ipFailMu.Unlock()
}

func clearDirectIPCooldowns() {
	ipFailMu.Lock()
	ipFailUntil = make(map[string]float64)
	ipFailMu.Unlock()
}

func directIPCooldownFallbackAvailable(settings runtimeSettings) bool {
	if settings.PolicyPresent {
		if !settings.AllowFallback {
			return false
		}
		return (settings.AllowWorker && settings.workerRouteAvailable()) ||
			(settings.AllowCFProxy && settings.CF.Enabled) ||
			settings.AllowTCP
	}
	return settings.Mode != modeDirectOnly
}

func resolvePreferredIPs(domain string, timeout float64) (resolvedIPs, error) {
	resolveTimeout := timeout
	if resolveTimeout <= 0 {
		resolveTimeout = 5
	}
	if resolveTimeout > 5 {
		resolveTimeout = 5
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(resolveTimeout*float64(time.Second)))
	defer cancel()

	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, domain)
	if err != nil {
		return resolvedIPs{}, err
	}

	out := resolvedIPs{}
	seen4 := make(map[string]bool)
	seen6 := make(map[string]bool)
	for _, addr := range addrs {
		if ip4 := addr.IP.To4(); ip4 != nil {
			s := ip4.String()
			if !seen4[s] {
				out.IPv4 = append(out.IPv4, s)
				seen4[s] = true
			}
			continue
		}
		if ip16 := addr.IP.To16(); ip16 != nil {
			s := ip16.String()
			if !seen6[s] {
				out.IPv6 = append(out.IPv6, s)
				seen6[s] = true
			}
		}
	}
	return out, nil
}

func logDomainConnectFailure(prefix, domain, target string, connErr error) {
	if connErr == nil {
		return
	}

	if wsErr, ok := connErr.(*WsHandshakeError); ok {
		if wsErr.IsRedirect() {
			logWarn.Printf("%s got %d from %s -> %s",
				prefix, wsErr.StatusCode, domain, wsErr.Location)
			return
		}
		logWarn.Printf("%s websocket upgrade failure for %s via %s: %s",
			prefix, domain, target, wsErr.StatusLine)
		return
	}

	var stageErr *wsStageError
	if errors.As(connErr, &stageErr) {
		switch stageErr.Stage {
		case "tcp_dial":
			logWarn.Printf("%s TCP dial failure for %s via %s (%s): %v",
				prefix, domain, target, classifyConnError(stageErr.Err), stageErr.Err)
		case "tls_handshake":
			logWarn.Printf("%s TLS handshake failure for %s via %s: %v",
				prefix, domain, target, stageErr.Err)
		case "ws_upgrade_write":
			logWarn.Printf("%s websocket upgrade write failure for %s via %s: %v",
				prefix, domain, target, stageErr.Err)
		case "ws_upgrade_read":
			logWarn.Printf("%s websocket upgrade read failure for %s via %s: %v",
				prefix, domain, target, stageErr.Err)
		default:
			logWarn.Printf("%s WS connect failed for %s via %s: %v",
				prefix, domain, target, connErr)
		}
		return
	}

	if errStr := connErr.Error(); strings.Contains(errStr, "certificate") ||
		strings.Contains(errStr, "hostname") {
		logWarn.Printf("%s SSL error for %s via %s: %v",
			prefix, domain, target, connErr)
		return
	}

	logWarn.Printf("%s WS connect failed for %s via %s: %v",
		prefix, domain, target, connErr)
}

func cfProxyHost(dc int, domain string) string {
	return fmt.Sprintf("kws%d.%s", dc, domain)
}

func fallbackTarget(dc int, dst string) string {
	if ip, ok := dcDefaultIP[dc]; ok && ip != "" {
		return ip
	}
	return dst
}

func runFallbackChain(ctx context.Context, client net.Conn, session *initSession, label string, dst string, port int, fallbackReason string) bool {
	settings := getRuntimeSettings()
	routes := adaptiveRoutesForMode(settings.Mode, settings, true, session.dc, session.isMedia)
	if len(routes) > 0 && strings.TrimSpace(fallbackReason) != "" {
		noteFallbackActivated(routes[0], fallbackReason)
	}
	return runRouteChain(ctx, client, session, label, dst, port, routes)
}

func wsConnectViaResolvedDomain(domain, path string, timeout float64) (*RawWebSocket, string, resolvedIPs, error) {
	resolved, err := resolvePreferredIPs(domain, timeout)
	if err != nil {
		return nil, "", resolved, &wsStageError{Stage: "tcp_dial", Err: err}
	}

	candidates := resolved.Preferred()
	if len(candidates) == 0 {
		return nil, "", resolved, &wsStageError{Stage: "tcp_dial", Err: fmt.Errorf("no resolved IPs for %s", domain)}
	}

	var lastErr error
	for _, ip := range candidates {
		ws, connErr := wsConnect(ip, domain, path, timeout)
		if connErr == nil {
			return ws, ip, resolved, nil
		}
		lastErr = connErr
		var wsErr *WsHandshakeError
		if errors.As(connErr, &wsErr) && wsErr.IsRedirect() {
			return nil, ip, resolved, connErr
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("domain dial failed for %s", domain)
	}
	return nil, candidates[0], resolved, lastErr
}

// ---------------------------------------------------------------------------
// XOR mask — optimized 8-byte processing
// ---------------------------------------------------------------------------

func xorMask(data, mask []byte) []byte {
	n := len(data)
	if n == 0 {
		return data
	}

	result := make([]byte, n)

	// Build 8-byte mask
	mask8 := uint64(mask[0]) | uint64(mask[1])<<8 |
		uint64(mask[2])<<16 | uint64(mask[3])<<24 |
		uint64(mask[0])<<32 | uint64(mask[1])<<40 |
		uint64(mask[2])<<48 | uint64(mask[3])<<56

	i := 0
	// Process 8 bytes at a time
	for ; i+8 <= n; i += 8 {
		v := binary.LittleEndian.Uint64(data[i:])
		binary.LittleEndian.PutUint64(result[i:], v^mask8)
	}
	// Process remaining bytes
	for ; i < n; i++ {
		result[i] = data[i] ^ mask[i&3]
	}
	return result
}

// xorMaskInPlace modifies data in place
func xorMaskInPlace(data, mask []byte) {
	n := len(data)
	if n == 0 {
		return
	}

	mask8 := uint64(mask[0]) | uint64(mask[1])<<8 |
		uint64(mask[2])<<16 | uint64(mask[3])<<24 |
		uint64(mask[0])<<32 | uint64(mask[1])<<40 |
		uint64(mask[2])<<48 | uint64(mask[3])<<56

	i := 0
	for ; i+8 <= n; i += 8 {
		v := binary.LittleEndian.Uint64(data[i:])
		binary.LittleEndian.PutUint64(data[i:], v^mask8)
	}
	for ; i < n; i++ {
		data[i] ^= mask[i&3]
	}
}

// ---------------------------------------------------------------------------
// WsHandshakeError
// ---------------------------------------------------------------------------

type WsHandshakeError struct {
	StatusCode int
	StatusLine string
	Headers    map[string]string
	Location   string
}

func (e *WsHandshakeError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, e.StatusLine)
}

func (e *WsHandshakeError) IsRedirect() bool {
	switch e.StatusCode {
	case 301, 302, 303, 307, 308:
		return true
	}
	return false
}

func (e *WsHandshakeError) IsRateLimited() bool {
	return e != nil && e.StatusCode == 429
}

// ---------------------------------------------------------------------------
// RawWebSocket
// ---------------------------------------------------------------------------

const (
	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xA
)

type RawWebSocket struct {
	conn      net.Conn
	bufReader *bufio.Reader
	writeMu   sync.Mutex
	closed    atomic.Bool
}

func wsConnect(ip, domain, path string, timeout float64) (*RawWebSocket, error) {
	if path == "" {
		path = "/apiws"
	}
	if timeout <= 0 {
		timeout = 10.0
	}

	dialTimeout := timeout
	if dialTimeout > 10.0 {
		dialTimeout = 10.0
	}

	dialer := &net.Dialer{
		Timeout: time.Duration(dialTimeout * float64(time.Second)),
	}

	tlsCfg := &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         domain,
	}

	rawConn, err := dialer.Dial("tcp", joinAddr(ip, 443))
	if err != nil {
		return nil, &wsStageError{Stage: "tcp_dial", Err: err}
	}
	setSockOpts(rawConn)

	tlsConn := tls.Client(rawConn, tlsCfg)
	_ = tlsConn.SetDeadline(time.Now().Add(time.Duration(timeout * float64(time.Second))))
	if err := tlsConn.Handshake(); err != nil {
		_ = tlsConn.Close()
		return nil, &wsStageError{Stage: "tls_handshake", Err: err}
	}
	_ = tlsConn.SetDeadline(time.Time{})

	wsKeyBytes := make([]byte, 16)
	_, _ = rand.Read(wsKeyBytes)
	wsKey := base64.StdEncoding.EncodeToString(wsKeyBytes)

	req := fmt.Sprintf(
		"GET %s HTTP/1.1\r\n"+
			"Host: %s\r\n"+
			"Upgrade: websocket\r\n"+
			"Connection: Upgrade\r\n"+
			"Sec-WebSocket-Key: %s\r\n"+
			"Sec-WebSocket-Version: 13\r\n"+
			"Sec-WebSocket-Protocol: binary\r\n"+
			"User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) "+
			"AppleWebKit/537.36 (KHTML, like Gecko) "+
			"Chrome/131.0.0.0 Safari/537.36\r\n"+
			"\r\n",
		path, domain, wsKey,
	)

	_ = tlsConn.SetWriteDeadline(time.Now().Add(time.Duration(timeout * float64(time.Second))))
	_, err = tlsConn.Write([]byte(req))
	if err != nil {
		_ = tlsConn.Close()
		return nil, &wsStageError{Stage: "ws_upgrade_write", Err: err}
	}
	_ = tlsConn.SetWriteDeadline(time.Time{})

	// Use buffered reader for efficient header parsing
	bufReader := bufio.NewReaderSize(tlsConn, 4096)

	_ = tlsConn.SetReadDeadline(time.Now().Add(time.Duration(timeout * float64(time.Second))))

	var responseLines []string
	for {
		line, err := bufReader.ReadString('\n')
		if err != nil {
			_ = tlsConn.Close()
			return nil, &wsStageError{Stage: "ws_upgrade_read", Err: err}
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		responseLines = append(responseLines, line)
		if len(responseLines) > 100 {
			_ = tlsConn.Close()
			return nil, &wsStageError{Stage: "ws_upgrade_read", Err: fmt.Errorf("too many HTTP headers")}
		}
	}
	_ = tlsConn.SetReadDeadline(time.Time{})

	if len(responseLines) == 0 {
		_ = tlsConn.Close()
		return nil, &WsHandshakeError{StatusCode: 0, StatusLine: "empty response"}
	}

	firstLine := responseLines[0]
	parts := strings.SplitN(firstLine, " ", 3)
	statusCode := 0
	if len(parts) >= 2 {
		statusCode, _ = strconv.Atoi(parts[1])
	}

	if statusCode == 101 {
		ws := &RawWebSocket{
			conn:      tlsConn,
			bufReader: bufReader,
		}
		return ws, nil
	}

	headers := make(map[string]string)
	for _, hl := range responseLines[1:] {
		idx := strings.IndexByte(hl, ':')
		if idx >= 0 {
			k := strings.TrimSpace(strings.ToLower(hl[:idx]))
			v := strings.TrimSpace(hl[idx+1:])
			headers[k] = v
		}
	}
	_ = tlsConn.Close()
	return nil, &WsHandshakeError{
		StatusCode: statusCode,
		StatusLine: firstLine,
		Headers:    headers,
		Location:   headers["location"],
	}
}

func (ws *RawWebSocket) Send(data []byte) error {
	if ws.closed.Load() {
		return fmt.Errorf("WebSocket closed")
	}
	frame := ws.buildFrame(opBinary, data, true)
	ws.writeMu.Lock()
	_, err := ws.conn.Write(frame)
	ws.writeMu.Unlock()
	return err
}

func (ws *RawWebSocket) SendBatch(parts [][]byte) error {
	if ws.closed.Load() {
		return fmt.Errorf("WebSocket closed")
	}
	ws.writeMu.Lock()
	defer ws.writeMu.Unlock()
	for _, part := range parts {
		frame := ws.buildFrame(opBinary, part, true)
		if _, err := ws.conn.Write(frame); err != nil {
			return err
		}
	}
	return nil
}

func (ws *RawWebSocket) SendPing() error {
	if ws.closed.Load() {
		return fmt.Errorf("WebSocket closed")
	}
	frame := ws.buildFrame(opPing, nil, true)
	ws.writeMu.Lock()
	_, err := ws.conn.Write(frame)
	ws.writeMu.Unlock()
	return err
}

func (ws *RawWebSocket) Recv() ([]byte, error) {
	for !ws.closed.Load() {
		opcode, payload, err := ws.readFrame()
		if err != nil {
			ws.closed.Store(true)
			return nil, err
		}

		switch opcode {
		case opClose:
			ws.closed.Store(true)
			closePayload := payload
			if len(closePayload) > 2 {
				closePayload = closePayload[:2]
			}
			reply := ws.buildFrame(opClose, closePayload, true)
			ws.writeMu.Lock()
			_, _ = ws.conn.Write(reply)
			ws.writeMu.Unlock()
			return nil, io.EOF

		case opPing:
			pong := ws.buildFrame(opPong, payload, true)
			ws.writeMu.Lock()
			_, _ = ws.conn.Write(pong)
			ws.writeMu.Unlock()
			continue

		case opPong:
			continue

		case opText, opBinary:
			return payload, nil

		default:
			continue
		}
	}
	return nil, io.EOF
}

func (ws *RawWebSocket) Close() {
	if ws.closed.Swap(true) {
		return
	}
	frame := ws.buildFrame(opClose, nil, true)
	ws.writeMu.Lock()
	_, _ = ws.conn.Write(frame)
	ws.writeMu.Unlock()
	_ = ws.conn.Close()
}

// SetReadDeadline exposes deadline control for the bridge
func (ws *RawWebSocket) SetReadDeadline(t time.Time) error {
	return ws.conn.SetReadDeadline(t)
}

// buildFrame creates a WebSocket frame with minimal allocations
func (ws *RawWebSocket) buildFrame(opcode int, data []byte, mask bool) []byte {
	length := len(data)
	fb := byte(0x80 | opcode)

	// Calculate total size
	headerSize := 2
	if mask {
		headerSize += 4
	}
	if length >= 126 && length < 65536 {
		headerSize += 2
	} else if length >= 65536 {
		headerSize += 8
	}

	totalSize := headerSize + length
	result := make([]byte, totalSize)
	pos := 0

	result[pos] = fb
	pos++

	var maskKey [4]byte
	if mask {
		_, _ = rand.Read(maskKey[:])
	}

	if length < 126 {
		lb := byte(length)
		if mask {
			lb |= 0x80
		}
		result[pos] = lb
		pos++
	} else if length < 65536 {
		lb := byte(126)
		if mask {
			lb |= 0x80
		}
		result[pos] = lb
		pos++
		binary.BigEndian.PutUint16(result[pos:], uint16(length))
		pos += 2
	} else {
		lb := byte(127)
		if mask {
			lb |= 0x80
		}
		result[pos] = lb
		pos++
		binary.BigEndian.PutUint64(result[pos:], uint64(length))
		pos += 8
	}

	if mask {
		copy(result[pos:], maskKey[:])
		pos += 4
		// XOR directly into result buffer
		payloadStart := pos
		copy(result[payloadStart:], data)
		xorMaskInPlace(result[payloadStart:payloadStart+length], maskKey[:])
	} else {
		copy(result[pos:], data)
	}

	return result
}

func (ws *RawWebSocket) readFrame() (int, []byte, error) {
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(ws.bufReader, hdr); err != nil {
		return 0, nil, err
	}

	opcode := int(hdr[0] & 0x0F)
	length := uint64(hdr[1] & 0x7F)

	if length == 126 {
		buf := make([]byte, 2)
		if _, err := io.ReadFull(ws.bufReader, buf); err != nil {
			return 0, nil, err
		}
		length = uint64(binary.BigEndian.Uint16(buf))
	} else if length == 127 {
		buf := make([]byte, 8)
		if _, err := io.ReadFull(ws.bufReader, buf); err != nil {
			return 0, nil, err
		}
		length = binary.BigEndian.Uint64(buf)
	}

	hasMask := (hdr[1] & 0x80) != 0
	var maskKey []byte
	if hasMask {
		maskKey = make([]byte, 4)
		if _, err := io.ReadFull(ws.bufReader, maskKey); err != nil {
			return 0, nil, err
		}
	}

	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(ws.bufReader, payload); err != nil {
			return 0, nil, err
		}
	}

	if hasMask {
		xorMaskInPlace(payload, maskKey)
	}

	return opcode, payload, nil
}

// ---------------------------------------------------------------------------
// Crypto helpers: DC extraction & patching
// ---------------------------------------------------------------------------

func newAESCTR(key, iv []byte) (cipher.Stream, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewCTR(block, iv), nil
}

func dcFromInit(data []byte) (dc int, isMedia bool, ok bool) {
	if len(data) < 64 {
		return 0, false, false
	}

	stream, err := newAESCTR(data[8:40], data[40:56])
	if err != nil {
		logDebug.Printf("DC extraction failed: %v", err)
		return 0, false, false
	}

	keystream := make([]byte, 64)
	stream.XORKeyStream(keystream, zero64)

	plain := make([]byte, 8)
	for i := 0; i < 8; i++ {
		plain[i] = data[56+i] ^ keystream[56+i]
	}

	proto := binary.LittleEndian.Uint32(plain[0:4])
	dcRaw := int16(binary.LittleEndian.Uint16(plain[4:6]))

	logDebug.Printf("dc_from_init: proto=0x%08X dc_raw=%d plain=%x", proto, dcRaw, plain)

	if !validProtos[proto] {
		return 0, false, false
	}

	dcAbs := int(dcRaw)
	if dcAbs < 0 {
		dcAbs = -dcAbs
	}
	media := dcRaw < 0

	if (dcAbs >= 1 && dcAbs <= 5) || dcAbs == 203 {
		return dcAbs, media, true
	}

	return 0, false, false
}

func patchInitDC(data []byte, dc int) []byte {
	if len(data) < 64 {
		return data
	}

	newDC := make([]byte, 2)
	binary.LittleEndian.PutUint16(newDC, uint16(int16(dc)))

	stream, err := newAESCTR(data[8:40], data[40:56])
	if err != nil {
		return data
	}

	ks := make([]byte, 64)
	stream.XORKeyStream(ks, zero64)

	patched := make([]byte, len(data))
	copy(patched, data)
	patched[60] = ks[60] ^ newDC[0]
	patched[61] = ks[61] ^ newDC[1]

	logDebug.Printf("init patched: dc_id -> %d", dc)
	return patched
}

// ---------------------------------------------------------------------------
// MsgSplitter
// ---------------------------------------------------------------------------

type MsgSplitter struct {
	stream    cipher.Stream
	proto     uint32
	cipherBuf []byte
	plainBuf  []byte
	disabled  bool
}

func newMsgSplitter(initData []byte) (*MsgSplitter, error) {
	if len(initData) < 64 {
		return nil, fmt.Errorf("init data too short")
	}
	proto, ok := initProtoFromObfsInit(initData)
	if !ok {
		return nil, fmt.Errorf("unsupported init protocol")
	}
	stream, err := newAESCTR(initData[8:40], initData[40:56])
	if err != nil {
		return nil, err
	}
	skip := make([]byte, 64)
	stream.XORKeyStream(skip, zero64)

	return &MsgSplitter{stream: stream, proto: proto}, nil
}

func (s *MsgSplitter) Split(chunk []byte) [][]byte {
	if len(chunk) == 0 {
		return nil
	}
	if s.disabled {
		return [][]byte{chunk}
	}

	plain := make([]byte, len(chunk))
	s.stream.XORKeyStream(plain, chunk)
	s.cipherBuf = append(s.cipherBuf, chunk...)
	s.plainBuf = append(s.plainBuf, plain...)

	parts := make([][]byte, 0)
	offset := 0
	bufLen := len(s.cipherBuf)
	for offset < bufLen {
		packetLen, complete, disable := s.nextPacketLen(offset, bufLen-offset)
		if !complete {
			break
		}
		if disable {
			parts = append(parts, append([]byte(nil), s.cipherBuf[offset:]...))
			offset = bufLen
			s.disabled = true
			break
		}
		parts = append(parts, append([]byte(nil), s.cipherBuf[offset:offset+packetLen]...))
		offset += packetLen
	}

	if offset > 0 {
		s.cipherBuf = append(s.cipherBuf[:0], s.cipherBuf[offset:]...)
		s.plainBuf = append(s.plainBuf[:0], s.plainBuf[offset:]...)
	}
	return parts
}

func (s *MsgSplitter) Flush() [][]byte {
	if len(s.cipherBuf) == 0 {
		return nil
	}
	tail := append([]byte(nil), s.cipherBuf...)
	s.cipherBuf = s.cipherBuf[:0]
	s.plainBuf = s.plainBuf[:0]
	return [][]byte{tail}
}

func (s *MsgSplitter) nextPacketLen(offset int, avail int) (packetLen int, complete bool, disable bool) {
	if avail <= 0 {
		return 0, false, false
	}
	switch s.proto {
	case 0xEFEFEFEF:
		return s.nextAbridgedLen(offset, avail)
	case 0xEEEEEEEE, 0xDDDDDDDD:
		return s.nextIntermediateLen(offset, avail)
	default:
		return 0, true, true
	}
}

func (s *MsgSplitter) nextAbridgedLen(offset int, avail int) (int, bool, bool) {
	first := s.plainBuf[offset]
	headerLen := 1
	payloadLen := int(first&0x7f) * 4
	if first == 0x7f || first == 0xff {
		if avail < 4 {
			return 0, false, false
		}
		headerLen = 4
		payloadLen = int(uint32(s.plainBuf[offset+1])|uint32(s.plainBuf[offset+2])<<8|uint32(s.plainBuf[offset+3])<<16) * 4
	}
	if payloadLen <= 0 {
		return 0, true, true
	}
	packetLen := headerLen + payloadLen
	if avail < packetLen {
		return 0, false, false
	}
	return packetLen, true, false
}

func (s *MsgSplitter) nextIntermediateLen(offset int, avail int) (int, bool, bool) {
	if avail < 4 {
		return 0, false, false
	}
	payloadLen := int(binary.LittleEndian.Uint32(s.plainBuf[offset:offset+4]) & 0x7fffffff)
	if payloadLen <= 0 {
		return 0, true, true
	}
	packetLen := 4 + payloadLen
	if avail < packetLen {
		return 0, false, false
	}
	return packetLen, true, false
}

// ---------------------------------------------------------------------------
// WS domains
// ---------------------------------------------------------------------------

func wsDomains(dc int, isMedia *bool) []string {
	effectiveDC := dc
	if override, ok := dcOverrides[dc]; ok {
		effectiveDC = override
	}

	if isMedia == nil || *isMedia {
		return []string{
			fmt.Sprintf("kws%d-1.web.telegram.org", effectiveDC),
			fmt.Sprintf("kws%d.web.telegram.org", effectiveDC),
		}
	}
	return []string{
		fmt.Sprintf("kws%d.web.telegram.org", effectiveDC),
		fmt.Sprintf("kws%d-1.web.telegram.org", effectiveDC),
	}
}

// ---------------------------------------------------------------------------
// WsPool
// ---------------------------------------------------------------------------

type poolEntry struct {
	ws      *RawWebSocket
	created float64
}

type WsPool struct {
	mu        sync.Mutex
	idle      map[[2]int][]poolEntry
	refilling map[[2]int]bool
	rotating  map[[2]int]bool
}

func newWsPool() *WsPool {
	return &WsPool{
		idle:      make(map[[2]int][]poolEntry),
		refilling: make(map[[2]int]bool),
		rotating:  make(map[[2]int]bool),
	}
}

func isMediaInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func monoNow() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

func (p *WsPool) Get(dc int, isMedia bool, targetIP string, domains []string) *RawWebSocket {
	key := [2]int{dc, isMediaInt(isMedia)}
	now := monoNow()

	p.mu.Lock()
	defer p.mu.Unlock()

	bucket := p.idle[key]
	for len(bucket) > 0 {
		entry := bucket[0]
		bucket = bucket[1:]
		p.idle[key] = bucket

		age := now - entry.created
		if age > wsPoolMaxAge || entry.ws.closed.Load() {
			go entry.ws.Close()
			continue
		}

		stats.poolHits.Add(1)
		logDebug.Printf("WS pool hit for DC%d%s (age=%.1fs, left=%d)",
			dc, mediaTag(isMedia), age, len(bucket))
		p.scheduleRefillLocked(key, targetIP, domains)
		p.scheduleRotationLocked(key, targetIP, domains)
		return entry.ws
	}

	stats.poolMisses.Add(1)
	p.scheduleRefillLocked(key, targetIP, domains)
	return nil
}

// scheduleRefillLocked must be called with p.mu held
func (p *WsPool) scheduleRefillLocked(key [2]int, targetIP string, domains []string) {
	if poolSize <= 0 {
		return
	}
	if p.refilling[key] {
		return
	}
	p.refilling[key] = true
	go p.refill(key, targetIP, domains)
}

func (p *WsPool) refill(key [2]int, targetIP string, domains []string) {
	dc := key[0]
	isMedia := key[1] == 1

	defer func() {
		p.mu.Lock()
		delete(p.refilling, key)
		p.mu.Unlock()
	}()

	p.mu.Lock()
	bucket := p.idle[key]
	needed := poolSize - len(bucket)
	p.mu.Unlock()

	if needed <= 0 {
		return
	}

	type result struct {
		ws *RawWebSocket
	}

	ch := make(chan result, needed)
	for i := 0; i < needed; i++ {
		go func() {
			ws := connectOneWS(targetIP, domains)
			ch <- result{ws}
		}()
	}

	for i := 0; i < needed; i++ {
		r := <-ch
		if r.ws != nil {
			p.mu.Lock()
			p.idle[key] = append(p.idle[key], poolEntry{r.ws, monoNow()})
			p.scheduleRotationLocked(key, targetIP, domains)
			p.mu.Unlock()
		}
	}

	p.mu.Lock()
	logDebug.Printf("WS pool refilled DC%d%s: %d ready",
		dc, mediaTag(isMedia), len(p.idle[key]))
	p.mu.Unlock()
}

// scheduleRotationLocked must be called with p.mu held.
func (p *WsPool) scheduleRotationLocked(key [2]int, targetIP string, domains []string) {
	if poolSize <= 0 || p.rotating[key] || len(p.idle[key]) == 0 {
		return
	}
	p.rotating[key] = true
	go p.rotate(key, targetIP, append([]string(nil), domains...))
}

func (p *WsPool) rotate(key [2]int, targetIP string, domains []string) {
	dc := key[0]
	isMedia := key[1] == 1
	defer func() {
		p.mu.Lock()
		delete(p.rotating, key)
		p.mu.Unlock()
	}()

	for {
		delay := wsPoolRotationCheck
		p.mu.Lock()
		bucket := p.idle[key]
		if len(bucket) == 0 {
			p.mu.Unlock()
			return
		}
		now := monoNow()
		for _, entry := range bucket {
			until := entry.created + wsPoolMaxAge - now
			if until < delay {
				delay = until
			}
		}
		p.mu.Unlock()

		if delay < 0 {
			delay = 0
		}
		time.Sleep(time.Duration(delay * float64(time.Second)))

		now = monoNow()
		var expired []*RawWebSocket
		p.mu.Lock()
		bucket = p.idle[key]
		fresh := bucket[:0]
		for _, entry := range bucket {
			if now-entry.created >= wsPoolMaxAge || entry.ws.closed.Load() {
				expired = append(expired, entry.ws)
			} else {
				fresh = append(fresh, entry)
			}
		}
		p.idle[key] = fresh
		ready := len(fresh)
		if len(expired) > 0 {
			p.scheduleRefillLocked(key, targetIP, domains)
		}
		empty := ready == 0
		p.mu.Unlock()

		for _, ws := range expired {
			go ws.Close()
		}
		if len(expired) > 0 {
			logDebug.Printf("WS pool rotated DC%d%s: %d stale, %d ready",
				dc, mediaTag(isMedia), len(expired), ready)
		}
		if empty {
			return
		}
	}
}

func connectOneWS(targetIP string, domains []string) *RawWebSocket {
	for _, domain := range domains {
		ws, err := wsConnect(targetIP, domain, "/apiws", 8)
		if err != nil {
			if shouldTryDomainDial(err) {
				logDebug.Printf("WS pool connect retry via DNS for %s after pinned endpoint %s",
					domain, targetIP)
				ws, usedIP, resolved, fallbackErr := wsConnectViaResolvedDomain(domain, "/apiws", 8)
				if fallbackErr == nil {
					logDebug.Printf("WS pool connect via DNS succeeded for %s -> %s (%s)",
						domain, usedIP, resolved.String())
					return ws
				}
				logDebug.Printf("WS pool connect via DNS failed for %s after pinned endpoint %s (%s): %v",
					domain, targetIP, resolved.String(), fallbackErr)
				err = fallbackErr
			}
			if wsErr, ok := err.(*WsHandshakeError); ok && wsErr.IsRedirect() {
				continue
			}
			return nil
		}
		return ws
	}
	return nil
}

func (p *WsPool) Warmup(dcOptMap map[int]string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for dc, targetIP := range dcOptMap {
		if targetIP == "" {
			continue
		}
		for _, isMedia := range []bool{false, true} {
			domains := wsDomains(dc, &isMedia)
			key := [2]int{dc, isMediaInt(isMedia)}
			p.scheduleRefillLocked(key, targetIP, domains)
		}
	}
	logInfo.Printf("WS pool warmup started for %d DC(s)", len(dcOptMap))
}

func (p *WsPool) Maintain(ctx context.Context, dcOptMap map[int]string) {
	ticker := time.NewTicker(poolMaintainInterval * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.maintainOnce(dcOptMap)
		}
	}
}

func (p *WsPool) maintainOnce(dcOptMap map[int]string) {
	now := monoNow()

	p.mu.Lock()
	for key, bucket := range p.idle {
		var fresh []poolEntry
		for _, e := range bucket {
			age := now - e.created
			if age > wsPoolMaxAge || e.ws.closed.Load() {
				go e.ws.Close()
			} else {
				// Send ping to keep connection alive
				go func(ws *RawWebSocket) {
					if err := ws.SendPing(); err != nil {
						ws.Close()
					}
				}(e.ws)
				fresh = append(fresh, e)
			}
		}
		p.idle[key] = fresh
	}
	p.mu.Unlock()

	// Refill all known DCs
	p.mu.Lock()
	for dc, targetIP := range dcOptMap {
		if targetIP == "" {
			continue
		}
		for _, isMedia := range []bool{false, true} {
			domains := wsDomains(dc, &isMedia)
			key := [2]int{dc, isMediaInt(isMedia)}
			p.scheduleRefillLocked(key, targetIP, domains)
		}
	}
	p.mu.Unlock()
}

func (p *WsPool) IdleCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	count := 0
	for _, bucket := range p.idle {
		count += len(bucket)
	}
	return count
}

func (p *WsPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for key, bucket := range p.idle {
		for _, e := range bucket {
			go e.ws.Close()
		}
		delete(p.idle, key)
	}
	p.refilling = make(map[[2]int]bool)
	p.rotating = make(map[[2]int]bool)
}

var wsPool = newWsPool()

// ---------------------------------------------------------------------------
// WorkerWsPool — idle /apiws preconnect pool for cf_worker_ws.
// ---------------------------------------------------------------------------

var workerWsPreconnectEnabled = false

const workerWsPreconnectMaxPerKey = 1

func workerWsPreconnectActive() bool {
	return workerWsPreconnectEnabled || getRuntimeSettings().MtProtoWorkerPreconnect
}

func workerWsPreconnectActiveForSettings(settings runtimeSettings) bool {
	return workerWsPreconnectEnabled || settings.MtProtoWorkerPreconnect
}

func workerWsPreconnectTargetSize() int {
	if poolSize <= 0 {
		return 0
	}
	if poolSize < workerWsPreconnectMaxPerKey {
		return poolSize
	}
	return workerWsPreconnectMaxPerKey
}

type WorkerPoolKey struct {
	DC           int
	WorkerDomain string
	Dst          string
	Media        bool
}

func workerWarmupDomains(settings runtimeSettings) []string {
	candidates := settings.Worker.Failover.effectiveCandidates(settings.Worker.Domain)
	if len(candidates) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(candidates))
	out := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		domain := NormalizeWorkerDomain(candidate.Domain)
		if domain == "" {
			continue
		}
		if _, ok := seen[domain]; ok {
			continue
		}
		seen[domain] = struct{}{}
		out = append(out, domain)
	}
	return out
}

// workerWarmupKeys derives preconnect keys through the same
// WorkerDestinationPlan used by foreground MTProto Worker sessions.
// Media is intentionally not warmed up: media/CDN sessions use a fresh dial
// unless an exact media key was populated explicitly in the future.
func workerWarmupKeys(settings runtimeSettings, workerDomains []string) []WorkerPoolKey {
	seen := make(map[WorkerPoolKey]struct{})
	out := make([]WorkerPoolKey, 0, len(workerDomains)*6)
	for _, workerDomain := range workerDomains {
		workerDomain = NormalizeWorkerDomain(workerDomain)
		if workerDomain == "" {
			continue
		}
		for _, dc := range []int{1, 2, 3, 4, 5, 203} {
			destination := buildMtProtoWorkerDestinationPlan(workerDomain, dc, false, settings)
			target := strings.TrimSpace(destination.WorkerDst)
			if !destination.OK || destination.EffectiveDC <= 0 || target == "" || destination.EffectiveIsMedia {
				continue
			}
			key := WorkerPoolKey{
				DC:           destination.EffectiveDC,
				WorkerDomain: workerDomain,
				Dst:          target,
				Media:        false,
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, key)
		}
	}
	return out
}

type workerPoolDialer interface {
	DialWorker(key WorkerPoolKey) (*RawWebSocket, error)
}

type defaultWorkerPoolDialer struct{}

func (defaultWorkerPoolDialer) DialWorker(key WorkerPoolKey) (*RawWebSocket, error) {
	path := buildWorkerWSPath(key.DC, key.Dst, key.Media, "")
	ws, err := wsConnect(key.WorkerDomain, key.WorkerDomain, path, 10)
	if err == nil {
		return ws, nil
	}

	resolved, resolveErr := resolvePreferredIPs(key.WorkerDomain, 10)
	if resolveErr != nil {
		return nil, err
	}
	lastErr := err
	for _, ip := range resolved.Preferred() {
		ws, lastErr = wsConnect(ip, key.WorkerDomain, path, 10)
		if lastErr == nil {
			return ws, nil
		}
	}
	return nil, lastErr
}

const workerWsReuseProbeTimeout = 2 * time.Millisecond

// workerWebSocketReusable rejects preconnected sockets that are locally
// closed, already have unexpected upstream data buffered, or whose peer has
// already closed the connection. A read timeout means the idle socket is still
// quiet and can be handed to a foreground MTProto session.
func workerWebSocketReusable(ws *RawWebSocket) bool {
	if ws == nil || ws.closed.Load() || ws.conn == nil || ws.bufReader == nil {
		return false
	}
	if ws.bufReader.Buffered() > 0 {
		return false
	}

	_ = ws.conn.SetReadDeadline(time.Now().Add(workerWsReuseProbeTimeout))
	_, err := ws.bufReader.Peek(1)
	_ = ws.conn.SetReadDeadline(time.Time{})
	if err == nil {
		return false
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

type WorkerWsPool struct {
	mu        sync.Mutex
	idle      map[WorkerPoolKey][]poolEntry
	refilling map[WorkerPoolKey]bool
	dialer    workerPoolDialer
	reusable  func(*RawWebSocket) bool
	now       func() float64
	maxAge    float64
}

func newWorkerWsPool(dialer workerPoolDialer) *WorkerWsPool {
	if dialer == nil {
		dialer = defaultWorkerPoolDialer{}
	}
	return &WorkerWsPool{
		idle:      make(map[WorkerPoolKey][]poolEntry),
		refilling: make(map[WorkerPoolKey]bool),
		dialer:    dialer,
		reusable:  workerWebSocketReusable,
		now:       monoNow,
		maxAge:    100.0,
	}
}

func (p *WorkerWsPool) Get(key WorkerPoolKey) *RawWebSocket {
	if !workerWsPreconnectActive() || workerWsPreconnectTargetSize() <= 0 || key.WorkerDomain == "" || key.Dst == "" {
		return nil
	}

	now := p.now()
	p.mu.Lock()
	defer p.mu.Unlock()

	bucket := p.idle[key]
	for len(bucket) > 0 {
		entry := bucket[0]
		bucket = bucket[1:]
		p.idle[key] = bucket

		age := now - entry.created
		if age > p.maxAge || !p.reusable(entry.ws) {
			go entry.ws.Close()
			continue
		}

		stats.workerWsPreconnectHits.Add(1)
		p.scheduleRefillLocked(key)
		return entry.ws
	}

	stats.workerWsPreconnectMisses.Add(1)
	p.scheduleRefillLocked(key)
	return nil
}

func (p *WorkerWsPool) scheduleRefillLocked(key WorkerPoolKey) {
	if !workerWsPreconnectActive() || workerWsPreconnectTargetSize() <= 0 || p.refilling[key] {
		return
	}
	p.refilling[key] = true
	go p.refill(key)
}

func (p *WorkerWsPool) refill(key WorkerPoolKey) {
	defer func() {
		p.mu.Lock()
		delete(p.refilling, key)
		p.mu.Unlock()
	}()

	p.mu.Lock()
	needed := workerWsPreconnectTargetSize() - len(p.idle[key])
	p.mu.Unlock()
	if needed <= 0 {
		return
	}

	for i := 0; i < needed; i++ {
		ws, err := p.dialer.DialWorker(key)
		if err != nil || ws == nil {
			stats.workerWsPreconnectRefillErr.Add(1)
			continue
		}
		p.mu.Lock()
		p.idle[key] = append(p.idle[key], poolEntry{ws: ws, created: p.now()})
		p.mu.Unlock()
	}
}

func (p *WorkerWsPool) Warmup(settings runtimeSettings, workerDomains []string) {
	if !workerWsPreconnectActive() || workerWsPreconnectTargetSize() <= 0 || len(workerDomains) == 0 {
		return
	}
	keys := workerWarmupKeys(settings, workerDomains)
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, key := range keys {
		p.scheduleRefillLocked(key)
	}
	if len(keys) > 0 && logInfo != nil {
		logInfo.Printf("Worker pool warmup scheduled for %d effective non-media target(s) across %d worker(s)", len(keys), len(workerDomains))
	}
}

func (p *WorkerWsPool) Maintain(ctx context.Context, settings runtimeSettings, workerDomains []string) {
	ticker := time.NewTicker(poolMaintainInterval * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.maintainOnce(settings, workerDomains)
		}
	}
}

func (p *WorkerWsPool) maintainOnce(settings runtimeSettings, workerDomains []string) {
	now := p.now()
	p.mu.Lock()
	for key, bucket := range p.idle {
		var fresh []poolEntry
		for _, entry := range bucket {
			if now-entry.created > p.maxAge || !p.reusable(entry.ws) {
				go entry.ws.Close()
				continue
			}
			fresh = append(fresh, entry)
		}
		p.idle[key] = fresh
	}
	p.mu.Unlock()
	p.Warmup(settings, workerDomains)
}

func (p *WorkerWsPool) IdleCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	count := 0
	for _, bucket := range p.idle {
		count += len(bucket)
	}
	return count
}

func (p *WorkerWsPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for key, bucket := range p.idle {
		for _, entry := range bucket {
			go entry.ws.Close()
		}
		delete(p.idle, key)
	}
	for key := range p.refilling {
		delete(p.refilling, key)
	}
}

var workerPool = newWorkerWsPool(nil)

// ---------------------------------------------------------------------------
// Helper tags
// ---------------------------------------------------------------------------

func mediaTag(isMedia bool) string {
	if isMedia {
		return "m"
	}
	return ""
}

// ---------------------------------------------------------------------------
// HTTP detection
// ---------------------------------------------------------------------------

func isHTTPTransport(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	return string(data[:4]) == "POST" ||
		string(data[:3]) == "GET" ||
		string(data[:4]) == "HEAD" ||
		string(data[:7]) == "OPTIONS"
}

// ---------------------------------------------------------------------------
// SOCKS5 reply
// ---------------------------------------------------------------------------

var socks5Replies = map[byte][]byte{
	0x00: {0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0},
	0x05: {0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0},
	0x07: {0x05, 0x07, 0x00, 0x01, 0, 0, 0, 0, 0, 0},
	0x08: {0x05, 0x08, 0x00, 0x01, 0, 0, 0, 0, 0, 0},
}

func socks5Reply(status byte) []byte {
	if r, ok := socks5Replies[status]; ok {
		return r
	}
	return []byte{0x05, status, 0x00, 0x01, 0, 0, 0, 0, 0, 0}
}

// ---------------------------------------------------------------------------
// Bridging: TCP <-> WebSocket
// ---------------------------------------------------------------------------

type bridgeCloseSummary struct {
	Reason      string
	Primary     bool
	UpBytes     int64
	DownBytes   int64
	UpPackets   int64
	DownPackets int64
}

type bridgeWSMeta struct {
	route              routeKind
	workerHost         string
	sessionID          string
	configuredDestMode string
	effectiveDestMode  string
	originalParsedDst  string
	workerDst          string
	mappedDC           int
	isMedia            bool
	mediaFixApplied    bool
	upTransform        func([]byte) []byte
	downTransform      func([]byte) []byte
}

type bridgeCloseTracker struct {
	mu      sync.Mutex
	reason  string
	primary bool
}

func (s bridgeCloseSummary) String() string {
	if s.Reason == "" {
		return "unknown"
	}
	return s.Reason
}

func (t *bridgeCloseTracker) Record(reason string, primary bool) bool {
	if reason == "" {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.reason != "" {
		return false
	}
	t.reason = reason
	t.primary = primary
	return true
}

func (t *bridgeCloseTracker) Summary() bridgeCloseSummary {
	t.mu.Lock()
	defer t.mu.Unlock()
	return bridgeCloseSummary{Reason: t.reason, Primary: t.primary}
}

func formatBridgeCloseReason(prefix string, err error) string {
	if err == nil {
		return prefix
	}
	if errors.Is(err, io.EOF) {
		return prefix + ": EOF"
	}
	return fmt.Sprintf("%s: %v", prefix, err)
}

func bridgeWS(ctx context.Context, conn net.Conn, ws *RawWebSocket,
	label string, dc int, dst string, port int, isMedia bool,
	splitter *MsgSplitter, meta bridgeWSMeta) bridgeCloseSummary {

	dcTag := fmt.Sprintf("DC%d%s", dc, mediaTag(isMedia))
	dstTag := joinAddr(dst, port)
	sessionIDForLog := strings.TrimSpace(meta.sessionID)
	if sessionIDForLog == "" {
		sessionIDForLog = "-"
	}
	routeLabelForLog := string(meta.route)
	if routeLabelForLog == "" {
		routeLabelForLog = string(routeDirectWS)
	}

	var upBytes, downBytes, upPkts, downPkts int64
	startTime := time.Now()

	ctx2, cancel := context.WithCancel(ctx)
	closeTracker := &bridgeCloseTracker{}

	// Critical: close connections when context is cancelled
	// This unblocks the Read() calls in goroutines
	go func() {
		<-ctx2.Done()
		if err := ctx.Err(); err != nil {
			closeTracker.Record(formatBridgeCloseReason("context_cancel", err), true)
		}
		_ = conn.Close()
		ws.Close()
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	// tcp -> ws
	go func() {
		defer wg.Done()
		defer cancel()
		buf := make([]byte, 65536)
		firstPacket := true
		for {
			readStart := time.Now()
			n, err := conn.Read(buf)
			readWait := time.Since(readStart)
			if n > 0 {
				chunk := buf[:n]
				stats.bytesUp.Add(int64(n))
				upBytes += int64(n)
				upPkts++
				if readWait >= bridgeSlowOpThreshold {
					logInfo.Printf("[%s] session_id=%s bridge_slow_op route=%s dir=client->ws op=client_read_wait duration_ms=%d bytes=%d packet=%d dst=%s",
						label, sessionIDForLog, routeLabelForLog, readWait.Milliseconds(), n, upPkts, dstTag)
				}
				if meta.upTransform != nil {
					chunk = meta.upTransform(chunk)
				}

				var sendErr error
				partsCount := 1
				sendStart := time.Now()
				if splitter != nil {
					parts := splitter.Split(chunk)
					partsCount = len(parts)
					if len(parts) == 0 {
						firstPacket = false
						continue
					} else if len(parts) > 1 {
						logDebug.Printf("[%s] session_id=%s ws_split parts=%d bytes=%d route=%s",
							label, strings.TrimSpace(meta.sessionID), len(parts), n, meta.route)
						sendErr = ws.SendBatch(parts)
					} else {
						sendErr = ws.Send(parts[0])
					}
				} else {
					sendErr = ws.Send(chunk)
				}
				sendDuration := time.Since(sendStart)
				if sendDuration >= bridgeSlowOpThreshold {
					logWarn.Printf("[%s] session_id=%s bridge_slow_op route=%s dir=client->ws op=ws_send duration_ms=%d bytes=%d parts=%d packet=%d dst=%s",
						label, sessionIDForLog, routeLabelForLog, sendDuration.Milliseconds(), n, partsCount, upPkts, dstTag)
				}
				if sendErr != nil {
					primary := closeTracker.Record(formatBridgeCloseReason("client_to_ws_write", sendErr), true)
					if firstPacket && primary {
						logWarn.Printf("[%s] %s bridge first-exit primary dir=client->ws stage=write dst=%s up=%s/%d down=%s/%d err=%v",
							label, dcTag, dstTag, humanBytes(upBytes), upPkts, humanBytes(downBytes), downPkts, sendErr)
					} else if firstPacket {
						logDebug.Printf("[%s] %s bridge secondary dir=client->ws stage=write dst=%s err=%v",
							label, dcTag, dstTag, sendErr)
					}
					return
				}
				firstPacket = false
			}
			if err != nil {
				if splitter != nil && errors.Is(err, io.EOF) {
					tail := splitter.Flush()
					if len(tail) > 0 {
						if flushErr := ws.SendBatch(tail); flushErr != nil {
							primary := closeTracker.Record(formatBridgeCloseReason("client_to_ws_flush", flushErr), true)
							if primary {
								logWarn.Printf("[%s] %s bridge first-exit primary dir=client->ws stage=flush dst=%s up=%s/%d down=%s/%d err=%v",
									label, dcTag, dstTag, humanBytes(upBytes), upPkts, humanBytes(downBytes), downPkts, flushErr)
							}
							return
						}
					}
				}
				primary := closeTracker.Record(formatBridgeCloseReason("client_read", err), true)
				if primary {
					logInfo.Printf("[%s] %s bridge first-exit primary dir=client->ws stage=read dst=%s up=%s/%d down=%s/%d err=%v",
						label, dcTag, dstTag, humanBytes(upBytes), upPkts, humanBytes(downBytes), downPkts, err)
				}
				return
			}
		}
	}()

	// ws -> tcp
	go func() {
		defer wg.Done()
		defer cancel()
		firstPacket := true
		for {
			recvStart := time.Now()
			data, err := ws.Recv()
			recvWait := time.Since(recvStart)
			if err != nil || data == nil {
				if err != nil {
					primary := closeTracker.Record(formatBridgeCloseReason("ws_read", err), true)
					if err != nil && firstPacket && primary {
						logWarn.Printf("[%s] %s bridge first-exit primary dir=ws->client stage=read dst=%s up=%s/%d down=%s/%d err=%v",
							label, dcTag, dstTag, humanBytes(upBytes), upPkts, humanBytes(downBytes), downPkts, err)
					} else if err != nil && firstPacket {
						logDebug.Printf("[%s] %s bridge secondary dir=ws->client stage=read dst=%s err=%v",
							label, dcTag, dstTag, err)
					}
				} else {
					closeTracker.Record("ws_read: nil_frame", true)
				}
				return
			}
			n := len(data)
			stats.bytesDown.Add(int64(n))
			downBytes += int64(n)
			downPkts++
			if meta.downTransform != nil {
				data = meta.downTransform(data)
			}
			if recvWait >= bridgeSlowOpThreshold {
				logInfo.Printf("[%s] session_id=%s bridge_slow_op route=%s dir=ws->client op=ws_read_wait duration_ms=%d bytes=%d packet=%d dst=%s",
					label, sessionIDForLog, routeLabelForLog, recvWait.Milliseconds(), n, downPkts, dstTag)
			}
			writeStart := time.Now()
			if _, err := conn.Write(data); err != nil {
				primary := closeTracker.Record(formatBridgeCloseReason("ws_to_client_write", err), true)
				if firstPacket && primary {
					logWarn.Printf("[%s] %s bridge first-exit primary dir=ws->client stage=write dst=%s up=%s/%d down=%s/%d err=%v",
						label, dcTag, dstTag, humanBytes(upBytes), upPkts, humanBytes(downBytes), downPkts, err)
				} else if firstPacket {
					logDebug.Printf("[%s] %s bridge secondary dir=ws->client stage=write dst=%s err=%v",
						label, dcTag, dstTag, err)
				}
				return
			}
			writeDuration := time.Since(writeStart)
			if writeDuration >= bridgeSlowOpThreshold {
				logWarn.Printf("[%s] session_id=%s bridge_slow_op route=%s dir=ws->client op=client_write duration_ms=%d bytes=%d packet=%d dst=%s",
					label, sessionIDForLog, routeLabelForLog, writeDuration.Milliseconds(), n, downPkts, dstTag)
			}
			firstPacket = false
		}
	}()

	wg.Wait()

	tracked := closeTracker.Summary()
	summary := bridgeCloseSummary{
		Reason:      tracked.Reason,
		Primary:     tracked.Primary,
		UpBytes:     upBytes,
		DownBytes:   downBytes,
		UpPackets:   upPkts,
		DownPackets: downPkts,
	}
	if summary.Reason == "" {
		summary.Reason = "unknown"
	}
	durationMs := time.Since(startTime).Milliseconds()
	routeLabel := string(meta.route)
	if routeLabel == "" {
		routeLabel = string(routeDirectWS)
	}
	workerHost := meta.workerHost
	if workerHost == "" {
		workerHost = "-"
	}
	sessionID := strings.TrimSpace(meta.sessionID)
	if sessionID == "" {
		sessionID = "-"
	}
	result := workerSessionResult(upBytes, downBytes)
	logInfo.Printf("[%s] session_id=%s %s (%s) WS session closed route=%s worker_host=%s worker_session_result=%s ws_up_bytes=%d ws_up_packets=%d ws_down_bytes=%d ws_down_packets=%d duration_ms=%d close_reason=%s (^%s/%d pkts v%s/%d pkts)",
		label, sessionID, dcTag, dstTag,
		routeLabel, workerHost, result,
		upBytes, upPkts, downBytes, downPkts,
		durationMs, summary.String(),
		humanBytes(upBytes), upPkts,
		humanBytes(downBytes), downPkts)
	if meta.effectiveDestMode != "" {
		noteWorkerDestinationSession(workerDestinationSessionNote{
			effectiveDestinationMode: meta.effectiveDestMode,
			originalParsedDst:        meta.originalParsedDst,
			workerDst:                meta.workerDst,
			mappedDC:                 meta.mappedDC,
			isMedia:                  meta.isMedia,
			mediaFixApplied:          meta.mediaFixApplied,
			upBytes:                  upBytes,
			downBytes:                downBytes,
			durationMs:               durationMs,
			closeReason:              summary.String(),
		})
		noteWorkerDstSessionOutcome(workerDstSessionOutcome{
			DC:              meta.mappedDC,
			WorkerDst:       meta.workerDst,
			DestinationMode: meta.effectiveDestMode,
			SessionID:       sessionID,
			Route:           meta.route,
			IsMedia:         meta.isMedia,
			UpBytes:         upBytes,
			DownBytes:       downBytes,
			DurationMs:      durationMs,
		})
	}
	return summary
}

// ---------------------------------------------------------------------------
// Bridging: TCP <-> TCP (fallback)
// ---------------------------------------------------------------------------

func bridgeTCP(ctx context.Context, client, remote net.Conn,
	label string, dc int, dst string, port int, isMedia bool) {

	ctx2, cancel := context.WithCancel(ctx)

	// Close connections when context cancelled
	go func() {
		<-ctx2.Done()
		_ = client.Close()
		_ = remote.Close()
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	forward := func(src, dstW net.Conn, isUp bool) {
		defer wg.Done()
		defer cancel()
		buf := make([]byte, 65536)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				if isUp {
					stats.bytesUp.Add(int64(n))
				} else {
					stats.bytesDown.Add(int64(n))
				}
				if _, werr := dstW.Write(buf[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}

	go forward(client, remote, true)
	go forward(remote, client, false)

	wg.Wait()
}

// ---------------------------------------------------------------------------
// TCP fallback
// ---------------------------------------------------------------------------

func tcpFallback(ctx context.Context, client net.Conn, dst string, port int,
	init []byte, label string, dc int, isMedia bool) bool {

	dialer := &net.Dialer{Timeout: 10 * time.Second}
	remote, err := dialer.DialContext(ctx, "tcp", joinAddr(dst, port))
	if err != nil {
		logWarn.Printf("[%s] TCP fallback connect to %s failed (%s): %v",
			label, joinAddr(dst, port), classifyConnError(err), err)
		return false
	}

	stats.connectionsTcpFallback.Add(1)
	if _, err := remote.Write(init); err != nil {
		logWarn.Printf("[%s] TCP fallback first write failed for %s: %v",
			label, joinAddr(dst, port), err)
		_ = remote.Close()
		return false
	}
	bridgeTCP(ctx, client, remote, label, dc, dst, port, isMedia)
	return true
}

// ---------------------------------------------------------------------------
// Pipe (non-Telegram passthrough)
// ---------------------------------------------------------------------------

func pipe(ctx context.Context, src, dst net.Conn, done chan<- struct{}) {
	defer func() { done <- struct{}{} }()
	buf := make([]byte, 65536)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// ---------------------------------------------------------------------------
// SOCKS5 client handler
// ---------------------------------------------------------------------------

func readExactly(conn net.Conn, n int, timeout time.Duration) ([]byte, error) {
	if timeout > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(timeout))
		defer func() { _ = conn.SetReadDeadline(time.Time{}) }()
	}
	buf := make([]byte, n)
	_, err := io.ReadFull(conn, buf)
	return buf, err
}

func handleClient(ctx context.Context, conn net.Conn) {
	stats.connectionsTotal.Add(1)
	peer := conn.RemoteAddr().String()
	label := peer

	setSockOpts(conn)

	defer conn.Close()

	// -- SOCKS5 greeting --
	hdr, err := readExactly(conn, 2, 10*time.Second)
	if err != nil {
		logDebug.Printf("[%s] read greeting failed: %v", label, err)
		return
	}
	if hdr[0] != 5 {
		logDebug.Printf("[%s] not SOCKS5 (ver=%d)", label, hdr[0])
		return
	}
	nmethods := int(hdr[1])
	if _, err := readExactly(conn, nmethods, 10*time.Second); err != nil {
		return
	}
	if _, err := conn.Write([]byte{0x05, 0x00}); err != nil {
		return
	}

	// -- SOCKS5 CONNECT request --
	req, err := readExactly(conn, 4, 10*time.Second)
	if err != nil {
		return
	}
	cmd := req[1]
	atyp := req[3]

	if cmd != 1 {
		_, _ = conn.Write(socks5Reply(0x07))
		return
	}

	var dst string
	var rawDst string
	switch atyp {
	case 1: // IPv4
		raw, err := readExactly(conn, 4, 10*time.Second)
		if err != nil {
			return
		}
		rawDst = fmt.Sprintf("%x", raw)
		dst = net.IP(raw).String()
	case 3: // domain
		dlenBuf, err := readExactly(conn, 1, 10*time.Second)
		if err != nil {
			return
		}
		domBytes, err := readExactly(conn, int(dlenBuf[0]), 10*time.Second)
		if err != nil {
			return
		}
		rawDst = string(domBytes)
		dst = string(domBytes)
	case 4: // IPv6
		raw, err := readExactly(conn, 16, 10*time.Second)
		if err != nil {
			return
		}
		rawDst = fmt.Sprintf("%x", raw)
		dst = net.IP(raw).String()
	default:
		_, _ = conn.Write(socks5Reply(0x08))
		return
	}

	portBuf, err := readExactly(conn, 2, 10*time.Second)
	if err != nil {
		return
	}
	port := int(binary.BigEndian.Uint16(portBuf))

	workerSessionID := newWorkerSessionID()

	logDebug.Printf("[%s] session_id=%s SOCKS5 CONNECT raw_atyp=0x%02x raw_dst=%s parsed_dst=%s",
		label, workerSessionID, atyp, rawDst, joinAddr(dst, port))

	settings := getRuntimeSettings()
	cfg := settings.CF
	effectiveMode := settings.Mode
	if cfg.Only {
		effectiveMode = modeCFOnly
	}
	telegramLike := isTelegramLikeIP(dst)

	if atyp == 4 {
		logInfo.Printf("[%s] IPv6 destination accepted into pipeline: %s",
			label, joinAddr(dst, port))
		if telegramLike {
			logInfo.Printf("[%s] Telegram IPv6 destination accepted into routing pipeline: %s",
				label, joinAddr(dst, port))
		}
	}

	// -- Non-Telegram IP -> direct passthrough --
	if !telegramLike {
		stats.connectionsPassthrough.Add(1)
		workerAllowed := !settings.PolicyPresent || settings.AllowWorker
		logDebug.Printf("[%s] route_decision=passthrough reason=non_telegram_ip parsed_dst=%s raw_dst=%s mapped_dc=none worker_available=%t worker_allowed=%t worker_skip_reason=destination_not_telegram mode=%s policy_generation=%d",
			label, joinAddr(dst, port), rawDst, settings.workerRouteAvailable(), workerAllowed, effectiveMode, settings.PolicyGen)
		logDebug.Printf("[%s] passthrough raw_dst=%s parsed_dst=%s mapped_dc=none",
			label, rawDst, joinAddr(dst, port))

		dialer := &net.Dialer{Timeout: 10 * time.Second}
		remote, err := dialer.DialContext(ctx, "tcp", joinAddr(dst, port))
		if err != nil {
			logWarn.Printf("[%s] passthrough connect to %s failed (%s): %T: %v",
				label, joinAddr(dst, port), classifyConnError(err), err, err)
			_, _ = conn.Write(socks5Reply(0x05))
			return
		}

		_, _ = conn.Write(socks5Reply(0x00))

		ctx2, cancel := context.WithCancel(ctx)
		defer cancel()

		// Close connections when context done
		go func() {
			<-ctx2.Done()
			_ = conn.Close()
			_ = remote.Close()
		}()

		done := make(chan struct{}, 2)
		go pipe(ctx2, conn, remote, done)
		go pipe(ctx2, remote, conn, done)
		<-done
		cancel()
		<-done
		_ = remote.Close()
		return
	}

	// -- Telegram DC: accept SOCKS, read init --
	_, _ = conn.Write(socks5Reply(0x00))

	init, err := readExactly(conn, 64, 15*time.Second)
	if err != nil {
		logDebug.Printf("[%s] client disconnected before init: %v", label, err)
		return
	}

	// HTTP transport -> reject
	if isHTTPTransport(init) {
		stats.connectionsHttpReject.Add(1)
		logDebug.Printf("[%s] HTTP transport to %s:%d (rejected)", label, dst, port)
		return
	}

	// -- Resolve DC from init (read-only); route-specific init patching happens later --
	session := newInitSession(init, dst, port, rawDst)
	session.workerSessionID = workerSessionID
	dc := session.dc
	isMedia := session.isMedia
	dcOk := session.dcOk
	var isMediaPtr *bool
	if dcOk {
		isMediaPtr = &isMedia
	}

	dcOptMu.RLock()
	_, dcConfigured := dcOpt[dc]
	dcOptMu.RUnlock()

	if !dcOk {
		if tgwsroute.IsUnknownTelegramWithoutDCMapping(dst) {
			reason := tgwsroute.FailTelegramIPv6UnknownDCNoMapping
			if addr, err := netip.ParseAddr(dst); err == nil && addr.Is4() {
				reason = "telegram_unknown_cdn_no_mapping"
			}
			logWarn.Printf("[%s] route_decision=blocked_or_failed reason=%s parsed_dst=%s raw_dst=%s",
				label, reason, joinAddr(dst, port), rawDst)
			tcpAllowed := !settings.PolicyPresent || settings.AllowTCP
			if blocksDirectPassthrough(effectiveMode, dst, false) || !tcpAllowed {
				_ = conn.Close()
				return
			}
			logInfo.Printf("[%s] unknown DC for %s -> TCP passthrough (tcp_fallback allowed)", label, joinAddr(dst, port))
			tcpFallback(ctx, conn, dst, port, session.original, label, 0, false)
			return
		}
		workerAllowed := !settings.PolicyPresent || settings.AllowWorker
		workerSkipReason := "dc_unresolved"
		if !settings.workerRouteAvailable() {
			workerSkipReason = "worker_route_unavailable"
		} else if !workerAllowed {
			workerSkipReason = "worker_disabled_by_policy"
		}
		logDebug.Printf("[%s] route_decision=passthrough reason=unknown_dc parsed_dst=%s raw_dst=%s mapped_dc=none worker_available=%t worker_allowed=%t worker_skip_reason=%s mode=%s policy_generation=%d blocks_passthrough=%t",
			label, joinAddr(dst, port), rawDst, settings.workerRouteAvailable(), workerAllowed, workerSkipReason, effectiveMode, settings.PolicyGen, blocksDirectPassthrough(effectiveMode, dst, false))
		logDebug.Printf("[%s] raw_dst=%s parsed_dst=%s mapped_dc=none configured=%t -> TCP passthrough",
			label, rawDst, joinAddr(dst, port), dcConfigured)
		logInfo.Printf("[%s] unknown DC for %s -> TCP passthrough", label, joinAddr(dst, port))
		if blocksDirectPassthrough(effectiveMode, dst, false) {
			if atyp == 4 {
				logWarn.Printf("[%s] telegram IPv6 destination is not mapped, mode=%s blocks direct passthrough dst=%s",
					label, effectiveMode, joinAddr(dst, port))
			} else {
				logWarn.Printf("[%s] telegram destination is not mapped, mode=%s blocks direct passthrough dst=%s",
					label, effectiveMode, joinAddr(dst, port))
			}
			_ = conn.Close()
			return
		}
		tcpFallback(ctx, conn, dst, port, session.original, label, dc, isMedia)
		return
	}

	dcKey := [2]int{dc, isMediaInt(isMedia)}
	now := monoNow()

	mTag := ""
	if isMediaPtr == nil {
		mTag = " media?"
	} else if *isMediaPtr {
		mTag = " media"
	}

	if !dcConfigured {
		logDebug.Printf("[%s] raw_dst=%s parsed_dst=%s mapped_dc=%d configured=false -> fallback",
			label, rawDst, joinAddr(dst, port), dc)
		logInfo.Printf("[%s] DC%d%s not in config -> fallback",
			label, dc, mTag)
		runFallbackChain(ctx, conn, session, label, dst, port, "dc_not_configured")
		return
	}

	// Absolute policy guard: if direct_ws is disabled, never attempt direct WS path.
	if settings.PolicyPresent && !settings.AllowDirect {
		logInfo.Printf("[%s] DC%d%s direct_ws disabled by policy -> fallback chain",
			label, dc, mTag)
		runFallbackChain(ctx, conn, session, label, dst, port, "direct_disabled_by_policy")
		return
	}

	// -- WS blacklist check --
	wsBlackMu.RLock()
	blacklisted := wsBlacklist[dcKey]
	wsBlackMu.RUnlock()

	if blacklisted {
		logInfo.Printf("[%s] DC%d%s fallback reason=ws_blacklisted -> fallback chain",
			label, dc, mTag)
		runFallbackChain(ctx, conn, session, label, dst, port, "ws_blacklisted")
		return
	}

	if settings.Mode == modeCFOnly || settings.Mode == modeWorkerOnly || cfg.Only {
		logInfo.Printf("[%s] DC%d%s fallback reason=restricted_mode mode=%s -> route chain",
			label, dc, mTag, settings.Mode)
		runFallbackChain(ctx, conn, session, label, dst, port, "restricted_mode")
		return
	}

	if pre := primaryRoutesBeforeDirectWS(settings.Mode, settings); len(pre) > 0 {
		logInfo.Printf("[%s] DC%d%s trying primary routes before direct WS mode=%s",
			label, dc, mTag, settings.Mode)
		if runRouteChain(ctx, conn, session, label, dst, port, pre) {
			return
		}
		noteFallbackActivated(routeDirectWS, "primary_routes_exhausted")
	}

	if shouldSkipDirectWSAdaptive(dcKey, settings.Mode) {
		logInfo.Printf("[%s] DC%d%s skipping direct WS (mode=%s cooldown/active)",
			label, dc, mTag, settings.Mode)
		runFallbackChain(ctx, conn, session, label, dst, port, "direct_ws_cooldown")
		return
	}

	// -- Try WebSocket --
	noteRouteSelected(routeDirectWS)
	noteRouteConnectStarted(routeDirectWS)

	dcFailMu.RLock()
	failUntil := dcFailUntil[dcKey]
	dcFailMu.RUnlock()

	wsTimeout := 10.0
	if now < failUntil {
		wsTimeout = wsFailTimeout
	}

	isMediaForDomains := isMedia
	domains := wsDomains(dc, &isMediaForDomains)

	dcOptMu.RLock()
	target := dcOpt[dc]
	dcOptMu.RUnlock()

	if directIPCoolingDown(target, now) && directIPCooldownFallbackAvailable(settings) {
		logInfo.Printf("[%s] DC%d%s direct_ws target=%s on IP cooldown for %ds -> fallback chain",
			label, dc, mTag, target, int(ipFailCooldown))
		runFallbackChain(ctx, conn, session, label, dst, port, "direct_ip_cooldown")
		return
	}

	logDebug.Printf("[%s] raw_dst=%s parsed_dst=%s mapped_dc=%d is_media=%t selected_endpoint=%s ws_domains=%s",
		label, rawDst, joinAddr(dst, port), dc, isMedia, target, strings.Join(domains, ","))

	var ws *RawWebSocket
	wsFailedRedirect := false
	wsTimedOut := false
	allRedirects := true

	ws = wsPool.Get(dc, isMedia, target, domains)
	if ws != nil {
		logInfo.Printf("[%s] DC%d%s (%s:%d) -> pool hit via %s",
			label, dc, mTag, dst, port, target)
	} else {
		for _, domain := range domains {
			url := fmt.Sprintf("wss://%s/apiws", domain)
			logInfo.Printf("[%s] DC%d%s (%s:%d) -> %s via %s",
				label, dc, mTag, dst, port, url, target)

			var connErr error
			attemptTarget := target
			ws, connErr = wsConnect(target, domain, "/apiws", wsTimeout)
			if connErr != nil && shouldTryDomainDial(connErr) {
				logInfo.Printf("[%s] DC%d%s retry via DNS for %s after pinned endpoint %s",
					label, dc, mTag, domain, target)
				var resolved resolvedIPs
				ws, attemptTarget, resolved, connErr = wsConnectViaResolvedDomain(domain, "/apiws", wsTimeout)
				if connErr == nil {
					logInfo.Printf("[%s] DC%d%s domain-dial success for %s via %s (%s)",
						label, dc, mTag, domain, attemptTarget, resolved.String())
				} else {
					logWarn.Printf("[%s] DC%d%s domain-dial failed for %s after pinned endpoint %s (%s): %v",
						label, dc, mTag, domain, target, resolved.String(), connErr)
				}
			}
			if connErr == nil {
				allRedirects = false
				markDirectIPSuccess(target)
				break
			}

			stats.wsErrors.Add(1)
			if directWSErrorTimedOut(connErr) {
				wsTimedOut = true
			}

			if wsErr, ok := connErr.(*WsHandshakeError); ok {
				if wsErr.IsRedirect() {
					wsFailedRedirect = true
					logWarn.Printf("[%s] DC%d%s got %d from %s -> %s",
						label, dc, mTag, wsErr.StatusCode, domain,
						wsErr.Location)
					continue
				}
				allRedirects = false
				logWarn.Printf("[%s] DC%d%s websocket upgrade failure for %s via %s: %s",
					label, dc, mTag, domain, target, wsErr.StatusLine)
			} else {
				allRedirects = false
				var stageErr *wsStageError
				if errors.As(connErr, &stageErr) {
					switch stageErr.Stage {
					case "tcp_dial":
						logWarn.Printf("[%s] DC%d%s TCP dial failure for %s via %s (%s): %v",
							label, dc, mTag, domain, attemptTarget, classifyConnError(stageErr.Err), stageErr.Err)
					case "tls_handshake":
						logWarn.Printf("[%s] DC%d%s TLS handshake failure for %s via %s: %v",
							label, dc, mTag, domain, attemptTarget, stageErr.Err)
					case "ws_upgrade_write":
						logWarn.Printf("[%s] DC%d%s websocket upgrade write failure for %s via %s: %v",
							label, dc, mTag, domain, attemptTarget, stageErr.Err)
					case "ws_upgrade_read":
						logWarn.Printf("[%s] DC%d%s websocket upgrade read failure for %s via %s: %v",
							label, dc, mTag, domain, attemptTarget, stageErr.Err)
					default:
						logWarn.Printf("[%s] DC%d%s WS connect failed for %s via %s: %v",
							label, dc, mTag, domain, attemptTarget, connErr)
					}
				} else if errStr := connErr.Error(); strings.Contains(errStr, "certificate") ||
					strings.Contains(errStr, "hostname") {
					logWarn.Printf("[%s] DC%d%s SSL error for %s via %s: %v",
						label, dc, mTag, domain, attemptTarget, connErr)
				} else {
					logWarn.Printf("[%s] DC%d%s WS connect failed for %s via %s: %v",
						label, dc, mTag, domain, attemptTarget, connErr)
				}
			}
		}
	}

	// -- WS failed -> fallback --
	if ws == nil {
		if wsTimedOut {
			markDirectIPTimeout(target, now)
			logInfo.Printf("[%s] DC%d%s WS connect to %s timed out, IP cooldown for %ds",
				label, dc, mTag, target, int(ipFailCooldown))
		}
		if wsFailedRedirect && allRedirects {
			wsBlackMu.Lock()
			wsBlacklist[dcKey] = true
			wsBlackMu.Unlock()
			logWarn.Printf("[%s] DC%d%s blacklisted for WS (all 302)",
				label, dc, mTag)
		} else if wsFailedRedirect {
			dcFailMu.Lock()
			dcFailUntil[dcKey] = now + dcFailCooldown
			dcFailMu.Unlock()
			logInfo.Printf("[%s] DC%d%s fallback reason=ws_redirect_mixed cooldown_reason=redirect_after_partial_failure for %ds",
				label, dc, mTag, int(dcFailCooldown))
		} else {
			dcFailMu.Lock()
			dcFailUntil[dcKey] = now + dcFailCooldown
			dcFailMu.Unlock()
			logInfo.Printf("[%s] DC%d%s WS cooldown for %ds (reason=ws_connect_failure)",
				label, dc, mTag, int(dcFailCooldown))
		}

		if settings.Mode == modeAuto || settings.Mode == modeDirectWithFallback {
			reason := "WS_TIMEOUT"
			if wsFailedRedirect {
				reason = "WS_302"
			}
			recordAdaptiveFailure(routeDirectWS, dc, isMedia, reason, 0)
		}
		noteRouteConnectFailed(routeDirectWS, "ws_unavailable")
		logInfo.Printf("[%s] DC%d%s fallback reason=ws_unavailable -> fallback chain",
			label, dc, mTag)
		runFallbackChain(ctx, conn, session, label, dst, port, "ws_unavailable")
		return
	}

	// -- WS success --
	dcFailMu.Lock()
	delete(dcFailUntil, dcKey)
	dcFailMu.Unlock()

	stats.connectionsWs.Add(1)
	noteRouteConnectSucceeded(routeDirectWS)
	noteActiveRoute(routeDirectWS)

	// Send init packet
	routeInit, splitter := session.prepareForRoute(routeDirectWS)
	if err := ws.Send(routeInit); err != nil {
		logWarn.Printf("[%s] DC%d%s first client->ws write failed for %s: %v",
			label, dc, mTag, joinAddr(dst, port), err)
		logDebug.Printf("[%s] reconnecting via TCP fallback (WS broken): %v", label, err)
		ws.Close()
		fallbackInit, _ := session.prepareForRoute(routeTCPFallback)
		tcpFallback(ctx, conn, dst, port, fallbackInit, label, dc, isMedia)
		return
	}

	// Bidirectional bridge
	summary := bridgeWS(ctx, conn, ws, label, dc, dst, port, isMedia, splitter, bridgeWSMeta{route: routeDirectWS})
	recordAdaptiveSessionSuccess(routeDirectWS, dc, isMedia, 0, summary.String(), settings)
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

func listenProxyTCP(ctx context.Context, addr string) (net.Listener, error) {
	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}

	if tcpL, ok := listener.(*net.TCPListener); ok {
		raw, err := tcpL.SyscallConn()
		if err == nil {
			_ = raw.Control(func(fd uintptr) {
				_ = setSockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
			})
		}
	}
	return listener, nil
}

func runProxy(ctx context.Context, host string, port int, dcOptMap map[int]string) error {
	dcOptMu.Lock()
	dcOpt = dcOptMap
	dcOptMu.Unlock()

	addr := joinAddr(host, port)
	listener, err := listenProxyTCP(ctx, addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	srvCtx, srvCancel := context.WithCancel(ctx)
	defer srvCancel()

	logInfo.Println(strings.Repeat("=", 60))
	logInfo.Println("  Telegram WS Bridge Proxy (Go)")
	logInfo.Printf("  Listening on   %s:%d", host, port)
	logInfo.Println("  Target DC IPs:")
	for dc, ip := range dcOptMap {
		logInfo.Printf("    DC%d: %s", dc, ip)
	}
	logRuntimeRouteConfig()
	logInfo.Println(strings.Repeat("=", 60))
	logInfo.Printf("  Configure Telegram Desktop:")
	logInfo.Printf("    SOCKS5 proxy -> %s:%d  (no user/pass)", host, port)
	logInfo.Println(strings.Repeat("=", 60))

	// Stats logger
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-srvCtx.Done():
				return
			case <-ticker.C:
				wsBlackMu.RLock()
				var blParts []string
				for k := range wsBlacklist {
					m := ""
					if k[1] == 1 {
						m = "m"
					}
					blParts = append(blParts, fmt.Sprintf("DC%d%s", k[0], m))
				}
				wsBlackMu.RUnlock()
				bl := "none"
				if len(blParts) > 0 {
					bl = strings.Join(blParts, ", ")
				}
				idleCount := wsPool.IdleCount()
				logInfo.Printf("stats: %s idle=%d | ws_bl: %s", stats.Summary(), idleCount, bl)
			}
		}
	}()

	settings := getRuntimeSettings()
	skipWarmup := settings.Mode == modeCFOnly || settings.Mode == modeWorkerOnly ||
		settings.Mode == modeCFFirst || settings.Mode == modeWorkerFirst ||
		settings.CF.Only || settings.CF.Priority
	if skipWarmup {
		logInfo.Printf("  WS pool warmup skipped (mode=%s)", settings.Mode)
	} else {
		// Warmup WS pool
		wsPool.Warmup(dcOptMap)

		// Periodic pool maintenance
		go wsPool.Maintain(srvCtx, dcOptMap)
	}
	workerDomains := workerWarmupDomains(settings)
	workerPreconnectActive := workerWsPreconnectActiveForSettings(settings)
	if settings.workerRouteAvailable() && workerPreconnectActive && len(workerDomains) > 0 {
		workerPool.Warmup(settings, workerDomains)
		go workerPool.Maintain(srvCtx, settings, workerDomains)
		logInfo.Printf("  Worker WS preconnect enabled domains=%d", len(workerDomains))
	} else {
		logInfo.Printf("  Worker WS preconnect skipped enabled=%t route_available=%t domains=%d",
			workerPreconnectActive, settings.workerRouteAvailable(), len(workerDomains))
	}

	// Track active connections for graceful shutdown
	var activeConns sync.WaitGroup
	var listenerMu sync.Mutex
	currentListener := listener

	// Accept loop
	go func() {
		current := listener
		for {
			conn, err := current.Accept()
			if err != nil {
				select {
				case <-srvCtx.Done():
					return
				default:
					if ne, ok := err.(net.Error); ok && ne.Timeout() {
						continue
					}
					logWarn.Printf("listener watchdog restarting socket after accept error: %v", err)
					_ = current.Close()
					for {
						timer := time.NewTimer(time.Second)
						select {
						case <-srvCtx.Done():
							timer.Stop()
							return
						case <-timer.C:
						}
						next, listenErr := listenProxyTCP(srvCtx, addr)
						if listenErr != nil {
							logError.Printf("listener watchdog restart failed on %s: %v", addr, listenErr)
							continue
						}
						listenerMu.Lock()
						currentListener = next
						listenerMu.Unlock()
						current = next
						logInfo.Printf("listener watchdog restored socket on %s", addr)
						break
					}
					continue
				}
			}
			activeConns.Add(1)
			go func() {
				defer activeConns.Done()
				handleClient(srvCtx, conn)
			}()
		}
	}()

	// Wait for context cancellation
	<-srvCtx.Done()
	logInfo.Println("Shutting down proxy server...")
	listenerMu.Lock()
	_ = currentListener.Close()
	listenerMu.Unlock()

	// Wait for active connections with timeout
	done := make(chan struct{})
	go func() {
		activeConns.Wait()
		close(done)
	}()

	select {
	case <-done:
		logInfo.Println("All connections closed gracefully")
	case <-time.After(30 * time.Second):
		logWarn.Println("Graceful shutdown timed out after 30s")
	}

	// Close pool connections
	wsPool.CloseAll()
	workerPool.CloseAll()
	clearDirectIPCooldowns()

	logInfo.Printf("Final stats: %s", stats.Summary())
	return nil
}

// ---------------------------------------------------------------------------
// Parse DC:IP list / CIDR pool
// ---------------------------------------------------------------------------

func parseCIDRPool(cidrsStr string) (map[int]string, error) {
	result := make(map[int]string)

	pairs := strings.Split(cidrsStr, ",")
	for _, pair := range pairs {
		parts := strings.Split(pair, ":")
		if len(parts) == 2 {
			dcRaw := strings.TrimSpace(parts[0])
			ipRaw := strings.TrimSpace(parts[1])

			dc, err := strconv.Atoi(dcRaw)
			if err == nil && ipRaw != "" {
				result[dc] = ipRaw
			}
		}
	}

	return result, nil
}

func parseRuntimeConfig(raw string) (map[int]string, runtimeSettings, error) {
	dcMap := make(map[int]string)
	cfg := cfProxyConfig{Domain: defaultCfProxyDomain, Enabled: true}
	settings := runtimeSettings{
		Mode: "",
		CF:   cfg,
		Worker: workerConfig{
			Enabled:         false,
			DestinationMode: tgwsroute.WorkerDestinationPreserveOriginalDst,
			MediaFix: flowsealMediaFixConfig{
				DC: tgwsroute.DefaultFlowsealMediaFixDC,
				IP: tgwsroute.DefaultFlowsealMediaFixIP,
			},
		},
	}

	for _, token := range strings.Split(raw, ",") {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}

		if strings.HasPrefix(token, "@") {
			kv := strings.SplitN(strings.TrimPrefix(token, "@"), "=", 2)
			if len(kv) != 2 {
				continue
			}
			key := strings.ToLower(strings.TrimSpace(kv[0]))
			val := strings.TrimSpace(kv[1])
			switch key {
			case "connection_mode":
				if mode, ok := parseConnectionMode(val); ok {
					settings.Mode = mode
				}
			case "cfproxy":
				cfg.Enabled = parseBoolValue(val)
			case "cfproxy_priority":
				cfg.Priority = parseBoolValue(val)
			case "cfproxy_only":
				cfg.Only = parseBoolValue(val)
			case "cfproxy_domain":
				if val != "" {
					if normalized, ok := tgwsroute.NormalizeCFDomain(val); ok {
						cfg.Domain = normalized
						settings.CFManualDomains = []string{normalized}
					}
				}
			case "cf_manual_domains":
				settings.CFManualDomains = parseCFDomains(val)
				if len(settings.CFManualDomains) > 0 {
					cfg.Domain = settings.CFManualDomains[0]
				}
			case "cf_use_manual_domain":
				if parseBoolValue(val) && cfg.Domain != "" {
					if len(settings.CFManualDomains) == 0 {
						settings.CFManualDomains = []string{cfg.Domain}
					}
				}
			case "cf_cached_domains":
				if domains, ok := parseCachedCFDomains(val); ok {
					settings.CFCachedUpstream = domains
				}
			case "worker_enabled":
				settings.Worker.Enabled = parseBoolValue(val)
			case "worker_domain":
				settings.Worker.Domain = NormalizeWorkerDomain(val)
				if settings.Worker.Domain != "" && (!settings.PolicyPresent || settings.AllowWorker) {
					settings.Worker.Enabled = true
				}
			case "worker_failover_enabled":
				settings.Worker.Failover.Enabled = parseBoolValue(val)
			case "worker_selected_id":
				settings.Worker.Failover.SelectedID = strings.TrimSpace(val)
			case "worker_failover_max_attempts":
				if n, err := strconv.Atoi(strings.TrimSpace(val)); err == nil && n > 0 {
					settings.Worker.Failover.MaxAttempts = n
				}
			case "worker_failover_candidates":
				settings.Worker.Failover.Candidates = parseWorkerFailoverCandidates(val)
			case "worker_failover_skipped_backoff":
				if n, err := strconv.Atoi(strings.TrimSpace(val)); err == nil && n >= 0 {
					settings.Worker.Failover.SkippedBackoff = n
				}
			case "worker_selection_strategy":
				settings.Worker.Failover.SelectionStrategy = strings.TrimSpace(val)
			case "worker_selection_reason":
				settings.Worker.Failover.SelectionReason = strings.TrimSpace(val)
			case "worker_candidate_count":
				if n, err := strconv.Atoi(strings.TrimSpace(val)); err == nil && n >= 0 {
					settings.Worker.Failover.CandidateCount = n
				}
			case "worker_round_robin_cursor":
				settings.Worker.Failover.RoundRobinCursor = strings.TrimSpace(val)
			case "worker_destination_mode":
				settings.Worker.DestinationMode = strings.TrimSpace(val)
			case "flowseal_media_fix_enabled":
				settings.Worker.MediaFix.Enabled = parseBoolValue(val)
			case "flowseal_media_fix_dc":
				if n, err := strconv.Atoi(strings.TrimSpace(val)); err == nil && n > 0 {
					settings.Worker.MediaFix.DC = n
				}
			case "flowseal_media_fix_ip":
				settings.Worker.MediaFix.IP = strings.TrimSpace(val)
			case "network_profile_id":
				settings.NetworkProfileID = val
			case "network_profile_type":
				settings.NetworkProfileType = val
			case "network_profile_label":
				settings.NetworkProfileLabel = val
			case "adaptive_route_stats":
				settings.AdaptiveRouteStats = val
			case "auto_strategy":
				settings.AutoStrategy = val
			case "mtproto_fake_tls_domain", "fake_tls_domain":
				settings.MtProtoFakeTLSDomain = mtproxyfrontend.NormalizeFakeTLSDomain(val)
			case "mtproto_masking_passthrough", "fake_tls_masking_passthrough":
				settings.MtProtoMaskingPassthrough = parseBoolValue(val)
			case "mtproto_worker_preconnect":
				settings.MtProtoWorkerPreconnect = parseBoolValue(val)
			case "force_test_dc":
				settings.ForceTestDC = parseBoolValue(val)
			case "route_direct_ws":
				settings.PolicyPresent = true
				settings.AllowDirect = parseBoolValue(val)
			case "route_worker_ws":
				settings.PolicyPresent = true
				settings.AllowWorker = parseBoolValue(val)
			case "route_cf_proxy_ws":
				settings.PolicyPresent = true
				settings.AllowCFProxy = parseBoolValue(val)
			case "route_tcp_fallback":
				settings.PolicyPresent = true
				settings.AllowTCP = parseBoolValue(val)
			case "preferred_route":
				settings.PolicyPresent = true
				settings.Preferred = parsePreferredRoute(val)
			case "route_fallback":
				settings.PolicyPresent = true
				settings.AllowFallback = parseBoolValue(val)
			}
			continue
		}

		parsed, err := parseCIDRPool(token)
		if err != nil {
			return nil, settings, err
		}
		for dc, ip := range parsed {
			dcMap[dc] = ip
		}
	}

	cfg = normalizeCfProxyConfig(cfg)
	settings.CF = cfg
	if settings.Mode == "" {
		settings.Mode = legacyModeFromCF(cfg)
	}
	if settings.Worker.Domain != "" && (!settings.PolicyPresent || settings.AllowWorker) {
		settings.Worker.Enabled = true
	}

	if settings.Worker.MediaFix.DC <= 0 {
		settings.Worker.MediaFix.DC = tgwsroute.DefaultFlowsealMediaFixDC
	}
	if strings.TrimSpace(settings.Worker.MediaFix.IP) == "" {
		settings.Worker.MediaFix.IP = tgwsroute.DefaultFlowsealMediaFixIP
	}

	// Enforce policy as absolute filter when present.
	if settings.PolicyPresent {
		if !settings.AllowWorker {
			settings.Worker.Enabled = false
		}
		if !settings.AllowCFProxy {
			settings.CF.Enabled = false
			settings.CF.Priority = false
			settings.CF.Only = false
		}
	}
	return dcMap, settings, nil
}

func parseCFDomains(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	return tgwsroute.NormalizeCFDomains(strings.Split(raw, "|"))
}

const minCachedCFDomains = 3

func parseCachedCFDomains(raw string) ([]string, bool) {
	domains := tgwsroute.NormalizeCachedUpstreamCFDomains(strings.Split(raw, "|"))
	if len(domains) > 0 && len(domains) < minCachedCFDomains {
		if logWarn != nil {
			logWarn.Printf("CF cached upstream quality gate rejected count=%d required=%d",
				len(domains), minCachedCFDomains)
		}
		return nil, false
	}
	return domains, true
}

// ---------------------------------------------------------------------------
// CGO exports for Android .so
// ---------------------------------------------------------------------------

var (
	globalCtx    context.Context
	globalCancel context.CancelFunc
	globalMu     sync.Mutex

	mtProtoRuntime              = mtproxyfrontend.NewRuntime(mtProtoLogWriter{}, newMtProtoRouteConnector())
	mtProtoWorkerPreconnectMu   sync.Mutex
	mtProtoWorkerPreconnectStop context.CancelFunc
)

type mtProtoLogWriter struct{}

func (mtProtoLogWriter) Printf(format string, args ...any) {
	if logInfo != nil {
		logInfo.Printf(format, args...)
	}
}

func startMtProtoWorkerPreconnect(dcOptMap map[int]string, settings runtimeSettings) {
	workerDomains := workerWarmupDomains(settings)
	active := workerWsPreconnectActiveForSettings(settings)
	if !settings.workerRouteAvailable() || !active || len(workerDomains) == 0 {
		if logInfo != nil {
			logInfo.Printf("MTProto Worker WS preconnect skipped enabled=%t route_available=%t domains=%d",
				active, settings.workerRouteAvailable(), len(workerDomains))
		}
		stopMtProtoWorkerPreconnect(false)
		return
	}

	mtProtoWorkerPreconnectMu.Lock()
	if mtProtoWorkerPreconnectStop != nil {
		mtProtoWorkerPreconnectStop()
		mtProtoWorkerPreconnectStop = nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	mtProtoWorkerPreconnectStop = cancel
	mtProtoWorkerPreconnectMu.Unlock()

	workerPool.Warmup(settings, workerDomains)
	go workerPool.Maintain(ctx, settings, workerDomains)
	if logInfo != nil {
		logInfo.Printf("MTProto Worker WS preconnect enabled domains=%d", len(workerDomains))
	}
}

func stopMtProtoWorkerPreconnect(closePool bool) {
	mtProtoWorkerPreconnectMu.Lock()
	cancel := mtProtoWorkerPreconnectStop
	mtProtoWorkerPreconnectStop = nil
	mtProtoWorkerPreconnectMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if closePool {
		workerPool.CloseAll()
	}
}

//export StartProxy
func StartProxy(cHost *C.char, port C.int, cDcIps *C.char, verbose C.int) C.int {
	globalMu.Lock()
	defer globalMu.Unlock()

	if globalCancel != nil {
		return -1 // Already running
	}

	host := C.GoString(cHost)
	goPort := int(port)
	dcIpsStr := C.GoString(cDcIps)
	isVerbose := int(verbose) != 0

	initLogging(isVerbose)

	dcOptMap, settings, err := parseRuntimeConfig(dcIpsStr)
	if err != nil {
		logError.Printf("parseRuntimeConfig: %v", err)
		return -2
	}
	setRuntimeSettings(settings)
	setCfProxyConfig(settings.CF)

	globalCtx, globalCancel = context.WithCancel(context.Background())

	go func() {
		if err := runProxy(globalCtx, host, goPort, dcOptMap); err != nil {
			logError.Printf("runProxy error: %v", err)
		}
	}()

	return 0
}

//export StopProxy
func StopProxy() C.int {
	globalMu.Lock()
	defer globalMu.Unlock()

	if globalCancel == nil {
		return -1
	}

	globalCancel()
	globalCancel = nil
	globalCtx = nil

	// Reset state
	stats.Reset()

	wsBlackMu.Lock()
	wsBlacklist = make(map[[2]int]bool)
	wsBlackMu.Unlock()

	dcFailMu.Lock()
	dcFailUntil = make(map[[2]int]float64)
	dcFailMu.Unlock()

	wsPool.CloseAll()
	workerPool.CloseAll()
	resetWorkerRouteCooldowns()
	clearDirectIPCooldowns()
	resetProxyRouteDisplayState()

	return 0
}

//export StartMtProtoProxy
func StartMtProtoProxy(cHost *C.char, port C.int, cSecret *C.char, cRuntimeConfig *C.char, verbose C.int) C.int {
	isVerbose := int(verbose) != 0
	initLogging(isVerbose)

	dcOptMap, settings, err := parseRuntimeConfig(C.GoString(cRuntimeConfig))
	if err != nil {
		logError.Printf("MTProto parseRuntimeConfig: %v", err)
		return -4
	}
	setRuntimeSettings(settings)
	setCfProxyConfig(settings.CF)
	dcOptMu.Lock()
	dcOpt = dcOptMap
	dcOptMu.Unlock()

	result := mtProtoRuntime.Start(mtproxyfrontend.Config{
		Host:                      C.GoString(cHost),
		Port:                      int(port),
		Secret:                    C.GoString(cSecret),
		FakeTLSDomain:             settings.MtProtoFakeTLSDomain,
		FakeTLSMaskingPassthrough: settings.MtProtoMaskingPassthrough,
		ForceTestDC:               settings.ForceTestDC,
		Verbose:                   isVerbose,
	})
	if result.Err == nil {
		startMtProtoWorkerPreconnect(dcOptMap, settings)
		return 0
	}
	switch result.Status {
	case mtproxyfrontend.StatusFailedInvalidSecret:
		return -2
	case mtproxyfrontend.StatusFailedPortInUse:
		return -3
	default:
		return -4
	}
}

//export StopMtProtoProxy
func StopMtProtoProxy() C.int {
	if err := mtProtoRuntime.Stop(); err != nil {
		return -1
	}
	stopMtProtoWorkerPreconnect(true)
	return 0
}

//export GetMtProtoProxyStatus
func GetMtProtoProxyStatus() *C.char {
	return C.CString(mtProtoRuntime.StatusString())
}

//export SetPoolSize
func SetPoolSize(size C.int) {
	n := int(size)
	if n < 0 {
		n = 0
	}
	if n > 16 {
		n = 16
	}
	poolSize = n
	if logInfo != nil {
		logInfo.Printf("Pool size set to %d", n)
	}
}

//export GetStats
func GetStats() *C.char {
	s := stats.Summary()
	return C.CString(s)
}

//export ResetCFDomainCooldowns
func ResetCFDomainCooldowns() {
	cfPool.ResetCooldowns()
}

//export SetCachedCFDomains
func SetCachedCFDomains(cDomains *C.char) {
	domains, ok := parseCachedCFDomains(C.GoString(cDomains))
	if !ok {
		return
	}
	settings := getRuntimeSettings()
	settings.CFCachedUpstream = domains
	setRuntimeSettings(settings)
}

//export SetManualCFDomains
func SetManualCFDomains(cDomains *C.char) {
	domains := parseCFDomains(C.GoString(cDomains))
	settings := getRuntimeSettings()
	settings.CFManualDomains = domains
	if len(domains) > 0 {
		settings.CF.Domain = domains[0]
	}
	setRuntimeSettings(settings)
}

//export GetAdaptiveRouteStats
func GetAdaptiveRouteStats() *C.char {
	return C.CString(exportAdaptiveRouteStats())
}

//export ResetAdaptiveRouteStats
func ResetAdaptiveRouteStats(all C.int) {
	resetAdaptiveRouteStats(int(all) != 0, "")
}

//export ResetAdaptiveNetworkRouteStats
func ResetAdaptiveNetworkRouteStats(cProfileID *C.char) {
	resetAdaptiveRouteStats(false, C.GoString(cProfileID))
}

//export FreeString
func FreeString(p *C.char) {
	C.free(unsafe.Pointer(p))
}

// ---------------------------------------------------------------------------
// Standalone main
// ---------------------------------------------------------------------------

func main() {
	runtime.LockOSThread()

	initLogging(false)

	dcOptMap := map[int]string{
		2: "149.154.167.220",
		4: "149.154.167.220",
	}
	setCfProxyConfig(cfProxyConfig{Enabled: false, Priority: false, Only: false, Domain: defaultCfProxyDomain})

	host := "127.0.0.1"
	port := defaultPort

	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--port":
			if i+1 < len(args) {
				i++
				p, err := strconv.Atoi(args[i])
				if err == nil {
					port = p
				}
			}
		case "--host":
			if i+1 < len(args) {
				i++
				host = args[i]
			}
		case "-v", "--verbose":
			initLogging(true)
		case "--dc-ip":
			if i+1 < len(args) {
				i++
				entry := args[i]
				parsed, settings, err := parseRuntimeConfig(entry)
				if err != nil {
					logError.Printf("%v", err)
					os.Exit(1)
				}
				if strings.Contains(entry, "@") {
					setRuntimeSettings(settings)
					setCfProxyConfig(settings.CF)
				}
				for k, v := range parsed {
					dcOptMap[k] = v
				}
			}
		case "--cfproxy":
			cfg := getCfProxyConfig()
			cfg.Enabled = true
			setCfProxyConfig(cfg)
		case "--cfproxy-priority":
			cfg := getCfProxyConfig()
			cfg.Enabled = true
			cfg.Priority = true
			setCfProxyConfig(cfg)
		case "--cfproxy-only":
			cfg := getCfProxyConfig()
			cfg.Enabled = true
			cfg.Priority = true
			cfg.Only = true
			setCfProxyConfig(cfg)
		case "--cfproxy-domain":
			if i+1 < len(args) {
				i++
				cfg := getCfProxyConfig()
				cfg.Domain = args[i]
				setCfProxyConfig(cfg)
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logInfo.Printf("Received signal %v, shutting down...", sig)
		cancel()
	}()

	if err := runProxy(ctx, host, port, dcOptMap); err != nil {
		logError.Printf("Fatal: %v", err)
		os.Exit(1)
	}
}
