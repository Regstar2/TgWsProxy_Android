package main

import (
	"bufio"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"tg-ws-proxy/tgwsroute"
)

type nopConn struct{}

func (nopConn) Read(_ []byte) (int, error)       { return 0, nil }
func (nopConn) Write(p []byte) (int, error)      { return len(p), nil }
func (nopConn) Close() error                     { return nil }
func (nopConn) LocalAddr() net.Addr              { return dummyAddr("local") }
func (nopConn) RemoteAddr() net.Addr             { return dummyAddr("remote") }
func (nopConn) SetDeadline(time.Time) error      { return nil }
func (nopConn) SetReadDeadline(time.Time) error  { return nil }
func (nopConn) SetWriteDeadline(time.Time) error { return nil }

type dummyAddr string

func (a dummyAddr) Network() string { return string(a) }
func (a dummyAddr) String() string  { return string(a) }

type fakeIdleTimeoutError struct{}

func (fakeIdleTimeoutError) Error() string   { return "idle timeout" }
func (fakeIdleTimeoutError) Timeout() bool   { return true }
func (fakeIdleTimeoutError) Temporary() bool { return true }

type fakeIdleConn struct{ nopConn }

func (fakeIdleConn) Read(_ []byte) (int, error) { return 0, fakeIdleTimeoutError{} }

type fakeWorkerDialer struct {
	count atomic.Int64
	fail  bool
}

func (d *fakeWorkerDialer) DialWorker(_ WorkerPoolKey) (*RawWebSocket, error) {
	d.count.Add(1)
	if d.fail {
		return nil, errFakeDial
	}
	return newFakeWebSocket(), nil
}

var errFakeDial = &net.DNSError{Err: "fake dial failed", Name: "worker.test"}

func newFakeWebSocket() *RawWebSocket {
	conn := fakeIdleConn{}
	return &RawWebSocket{
		conn:      conn,
		bufReader: bufio.NewReader(conn),
	}
}

func withPoolSize(t *testing.T, size int) {
	t.Helper()
	previous := poolSize
	poolSize = size
	t.Cleanup(func() { poolSize = previous })
}

func testWorkerKey() WorkerPoolKey {
	return WorkerPoolKey{DC: 2, WorkerDomain: "worker.example", Dst: "149.154.167.51", Media: false}
}

func withWorkerWsPreconnect(t *testing.T, enabled bool) {
	t.Helper()
	previous := workerWsPreconnectEnabled
	workerWsPreconnectEnabled = enabled
	t.Cleanup(func() { workerWsPreconnectEnabled = previous })
}

func TestWorkerPoolGetMissSchedulesRefill(t *testing.T) {
	withWorkerWsPreconnect(t, true)
	withPoolSize(t, 1)
	stats.Reset()
	dialer := &fakeWorkerDialer{}
	pool := newWorkerWsPool(dialer)
	t.Cleanup(func() {
		waitWorkerPoolRefills(t, pool)
		pool.CloseAll()
	})

	if got := pool.Get(testWorkerKey()); got != nil {
		t.Fatal("first get should miss")
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if dialer.count.Load() > 0 && pool.IdleCount() > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("refill was not scheduled, dialed=%d idle=%d", dialer.count.Load(), pool.IdleCount())
}

func TestWorkerPoolHitReusesIdleConnection(t *testing.T) {
	withWorkerWsPreconnect(t, true)
	withPoolSize(t, 1)
	stats.Reset()
	pool := newWorkerWsPool(&fakeWorkerDialer{})
	t.Cleanup(func() {
		waitWorkerPoolRefills(t, pool)
		pool.CloseAll()
	})
	key := testWorkerKey()
	ws := newFakeWebSocket()
	pool.idle[key] = []poolEntry{{ws: ws, created: pool.now()}}

	if got := pool.Get(key); got != ws {
		t.Fatal("expected idle websocket to be reused")
	}
	if stats.workerWsPreconnectHits.Load() != 1 {
		t.Fatalf("worker ws preconnect hit counter = %d, want 1", stats.workerWsPreconnectHits.Load())
	}
}

func TestWorkerPoolDropsExpiredConnection(t *testing.T) {
	withWorkerWsPreconnect(t, true)
	withPoolSize(t, 1)
	stats.Reset()
	pool := newWorkerWsPool(&fakeWorkerDialer{fail: true})
	t.Cleanup(func() {
		waitWorkerPoolRefills(t, pool)
		pool.CloseAll()
	})
	pool.maxAge = 1
	now := 10.0
	pool.now = func() float64 { return now }
	key := testWorkerKey()
	expired := newFakeWebSocket()
	expired.closed.Store(true)
	pool.idle[key] = []poolEntry{{ws: expired, created: 0}}

	if got := pool.Get(key); got != nil {
		t.Fatal("expired websocket should not be returned")
	}
	if stats.workerWsPreconnectHits.Load() != 0 {
		t.Fatalf("worker ws preconnect hit counter = %d, want 0", stats.workerWsPreconnectHits.Load())
	}
}

func TestWorkerPoolResetClearsIdle(t *testing.T) {
	pool := newWorkerWsPool(&fakeWorkerDialer{})
	t.Cleanup(func() {
		waitWorkerPoolRefills(t, pool)
		pool.CloseAll()
	})
	key := testWorkerKey()
	pool.idle[key] = []poolEntry{{ws: newFakeWebSocket(), created: pool.now()}}

	pool.CloseAll()

	if got := pool.IdleCount(); got != 0 {
		t.Fatalf("idle count = %d, want 0", got)
	}
}

func TestWorkerPoolDoesNotRefillWhenPoolSizeZero(t *testing.T) {
	withPoolSize(t, 0)
	dialer := &fakeWorkerDialer{}
	pool := newWorkerWsPool(dialer)
	t.Cleanup(func() {
		waitWorkerPoolRefills(t, pool)
		pool.CloseAll()
	})

	if got := pool.Get(testWorkerKey()); got != nil {
		t.Fatal("pool disabled should return nil")
	}
	time.Sleep(30 * time.Millisecond)
	if dialer.count.Load() != 0 {
		t.Fatalf("dial count = %d, want 0", dialer.count.Load())
	}
}

func TestWorkerPoolCapsPreconnectPerKey(t *testing.T) {
	withWorkerWsPreconnect(t, true)
	withPoolSize(t, 8)
	dialer := &fakeWorkerDialer{}
	pool := newWorkerWsPool(dialer)
	t.Cleanup(func() {
		waitWorkerPoolRefills(t, pool)
		pool.CloseAll()
	})

	pool.refill(testWorkerKey())

	if got := pool.IdleCount(); got != workerWsPreconnectMaxPerKey {
		t.Fatalf("idle count = %d, want %d", got, workerWsPreconnectMaxPerKey)
	}
	if got := dialer.count.Load(); got != workerWsPreconnectMaxPerKey {
		t.Fatalf("dial count = %d, want %d", got, workerWsPreconnectMaxPerKey)
	}
}

func TestWorkerWsPreconnectDisabledByDefault(t *testing.T) {
	if workerWsPreconnectEnabled {
		t.Fatal("worker ws preconnect should be disabled by default for cf_worker_ws")
	}
	withPoolSize(t, 1)
	stats.Reset()
	pool := newWorkerWsPool(&fakeWorkerDialer{})
	t.Cleanup(func() {
		waitWorkerPoolRefills(t, pool)
		pool.CloseAll()
	})
	key := testWorkerKey()
	pool.idle[key] = []poolEntry{{ws: newFakeWebSocket(), created: pool.now()}}

	if got := pool.Get(key); got != nil {
		t.Fatal("disabled preconnect pool must not return idle websocket")
	}
	if stats.workerWsPreconnectHits.Load() != 0 {
		t.Fatalf("hits = %d, want 0", stats.workerWsPreconnectHits.Load())
	}
}

func TestWorkerPoolDoesNotWarmupWhenWorkerRouteDisabled(t *testing.T) {
	settings := runtimeSettings{
		Mode: modeCFOnly,
		Worker: workerConfig{
			Enabled: true,
			Domain:  "worker.example",
		},
		CF: cfProxyConfig{Enabled: true, Only: true},
	}

	if settings.workerRouteAvailable() {
		t.Fatal("worker route should be unavailable for strict CF-only mode")
	}
}


func TestWorkerWebSocketReusableDetectsPeerClose(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	ws := &RawWebSocket{
		conn:      client,
		bufReader: bufio.NewReader(client),
	}

	if !workerWebSocketReusable(ws) {
		t.Fatal("quiet connected Worker socket should be reusable")
	}
	_ = server.Close()
	if workerWebSocketReusable(ws) {
		t.Fatal("peer-closed Worker socket must not be reused")
	}
}

func TestWorkerWarmupKeysUseEffectiveDestinationAndExcludeMedia(t *testing.T) {
	withMtProtoWorkerDCMap(t, map[int]string{2: "149.154.167.220"})
	settings := runtimeSettings{
		Worker: workerConfig{
			Enabled:         true,
			Domain:          "worker.example",
			DestinationMode: tgwsroute.WorkerDestinationFlowsealDCMap,
		},
	}
	keys := workerWarmupKeys(settings, []string{"worker.example"})
	foundDC2 := false
	for _, key := range keys {
		if key.Media {
			t.Fatalf("media key must not be warmed up: %+v", key)
		}
		if key.DC == 2 && key.Dst == "149.154.167.220" {
			foundDC2 = true
		}
	}
	if !foundDC2 {
		t.Fatalf("effective DC2 destination not warmed up: %+v", keys)
	}
}

// Refill goroutines read global test settings. Join them before cleanup
// restores those settings, otherwise the next test races with the old pool.
func waitWorkerPoolRefills(t *testing.T, pool *WorkerWsPool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		pool.mu.Lock()
		active := len(pool.refilling)
		pool.mu.Unlock()
		if active == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("Worker pool refill did not finish before test cleanup")
		}
		time.Sleep(time.Millisecond)
	}
}
