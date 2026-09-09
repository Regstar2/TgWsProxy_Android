package main

import (
	"testing"
	"time"
)

func TestWorkerPoolSessionMissDoesNotScheduleDuplicateRefill(t *testing.T) {
	withWorkerWsPreconnect(t, true)
	withPoolSize(t, 1)
	stats.Reset()
	dialer := &fakeWorkerDialer{}
	pool := newWorkerWsPool(dialer)

	if got := pool.GetForSession(testWorkerKey()); got != nil {
		t.Fatal("first session get should miss")
	}

	time.Sleep(40 * time.Millisecond)
	if got := dialer.count.Load(); got != 0 {
		t.Fatalf("session miss scheduled %d duplicate background dial(s), want 0", got)
	}
	if got := stats.workerWsPreconnectMisses.Load(); got != 1 {
		t.Fatalf("misses=%d want=1", got)
	}
}

func TestWorkerPoolSessionHitSchedulesReplacementRefill(t *testing.T) {
	withWorkerWsPreconnect(t, true)
	withPoolSize(t, 1)
	stats.Reset()
	dialer := &fakeWorkerDialer{}
	pool := newWorkerWsPool(dialer)
	key := testWorkerKey()
	pooled := newFakeWebSocket()
	pool.idle[key] = []poolEntry{{ws: pooled, created: pool.now()}}

	if got := pool.GetForSession(key); got != pooled {
		t.Fatal("expected existing preconnected websocket")
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if dialer.count.Load() > 0 && pool.IdleCount() > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("replacement refill was not scheduled after hit, dialed=%d idle=%d", dialer.count.Load(), pool.IdleCount())
}


func TestWorkerPoolSessionDoesNotCrossEffectiveMediaOrDestination(t *testing.T) {
	withWorkerWsPreconnect(t, true)
	withPoolSize(t, 1)
	stats.Reset()
	pool := newWorkerWsPool(&fakeWorkerDialer{})
	normalKey := testWorkerKey()
	pooled := newFakeWebSocket()
	pool.idle[normalKey] = []poolEntry{{ws: pooled, created: pool.now()}}

	mediaKey := normalKey
	mediaKey.Media = true
	if got := pool.GetForSession(mediaKey); got != nil {
		t.Fatal("non-media pooled socket must not be reused for media")
	}

	otherDst := normalKey
	otherDst.Dst = "149.154.167.220"
	if got := pool.GetForSession(otherDst); got != nil {
		t.Fatal("pooled socket must not be reused for another effective destination")
	}

	if got := pool.IdleCount(); got != 1 {
		t.Fatalf("unrelated pooled entry was consumed, idle=%d", got)
	}
}
