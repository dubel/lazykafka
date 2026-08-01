package kafka

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newTestDialer creates a bootstrapFallbackDialer with an injectable lookup
// function for unit testing the DNS cache and host-rewrite logic.
// Uses a very short timeout so tests that trigger net.DialTimeout on
// unreachable addresses don't stall.
func newTestDialer(bootstrapHost string, timeout time.Duration, lookup func(string) ([]string, error)) *bootstrapFallbackDialer {
	return &bootstrapFallbackDialer{
		bootstrapHost: bootstrapHost,
		timeout:       timeout,
		lookupCache:   make(map[string]bool),
		lookupFunc:    lookup,
	}
}

// TestFallbackDialer_CachesLookupResult verifies that after the first DNS
// lookup the result is cached and subsequent Dial calls for the same host
// do not invoke the lookup function again.  This is the core fix for the
// Alpine musl DNS penalty: without the cache every Sarama connection paid
// a ~5 s timeout.
func TestFallbackDialer_CachesLookupResult(t *testing.T) {
	var lookupCount int64

	d := newTestDialer("127.0.0.1", 50*time.Millisecond, func(host string) ([]string, error) {
		atomic.AddInt64(&lookupCount, 1)
		return nil, fmt.Errorf("no such host: %s", host)
	})

	// Dial 5 times sequentially with the same unresolvable host.
	// The actual TCP connection will fail – we only care about lookup calls.
	for i := 0; i < 5; i++ {
		_, _ = d.Dial("tcp", "kafka-0.fake.local:9092")
	}

	if count := atomic.LoadInt64(&lookupCount); count != 1 {
		t.Errorf("expected lookupFunc to be called exactly once (cached), got %d calls", count)
	}

	// Verify the cache entry was stored correctly.
	d.cacheMutex.RLock()
	resolvable, cached := d.lookupCache["kafka-0.fake.local"]
	d.cacheMutex.RUnlock()

	if !cached {
		t.Error("expected host to be present in lookup cache after first dial")
	}
	if resolvable {
		t.Error("expected host to be cached as unresolvable")
	}
}

// TestFallbackDialer_CachesResolvableHost verifies that a host that resolves
// successfully is also cached (as resolvable) and the lookup is not repeated.
func TestFallbackDialer_CachesResolvableHost(t *testing.T) {
	var lookupCount int64

	d := newTestDialer("127.0.0.1", 50*time.Millisecond, func(host string) ([]string, error) {
		atomic.AddInt64(&lookupCount, 1)
		return []string{"10.0.0.1"}, nil
	})

	for i := 0; i < 5; i++ {
		_, _ = d.Dial("tcp", "resolvable-broker:9092")
	}

	if count := atomic.LoadInt64(&lookupCount); count != 1 {
		t.Errorf("expected lookupFunc to be called exactly once for resolvable host, got %d", count)
	}

	d.cacheMutex.RLock()
	resolvable, cached := d.lookupCache["resolvable-broker"]
	d.cacheMutex.RUnlock()

	if !cached {
		t.Error("expected resolvable host to be present in cache")
	}
	if !resolvable {
		t.Error("expected host to be cached as resolvable")
	}
}

// TestFallbackDialer_CachesConcurrently fires many goroutines through the
// dialer simultaneously.  Run with -race to prove the RWMutex-guarded cache
// is free of data races.
//
// The current cache design uses a read-lock check before performing the
// lookup.  When all goroutines start simultaneously, many will see
// cached=false and proceed to do the lookup in parallel.  This is by design:
// the cache is an optimisation for *subsequent* calls (like Sarama opening
// multiple connections over a session), not a strict single-flight guarantee.
//
// We verify two things:
//  1. No data races (run with -race).
//  2. After the burst, the cache is populated so a follow-up call is free.
func TestFallbackDialer_CachesConcurrently(t *testing.T) {
	var lookupCount int64

	d := newTestDialer("127.0.0.1", 50*time.Millisecond, func(host string) ([]string, error) {
		atomic.AddInt64(&lookupCount, 1)
		return nil, fmt.Errorf("no such host: %s", host)
	})

	const goroutines = 50
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = d.Dial("tcp", "kafka-0.fake.local:9092")
		}()
	}
	wg.Wait()

	count := atomic.LoadInt64(&lookupCount)
	t.Logf("%d concurrent dials resulted in %d DNS lookups", goroutines, count)

	// After the burst, the cache must be populated so subsequent calls are free.
	atomic.StoreInt64(&lookupCount, 0)
	_, _ = d.Dial("tcp", "kafka-0.fake.local:9092")
	if postCount := atomic.LoadInt64(&lookupCount); postCount != 0 {
		t.Error("expected zero lookups after cache was populated by burst, got", postCount)
	}

	// Verify cache state is correct.
	d.cacheMutex.RLock()
	resolvable, cached := d.lookupCache["kafka-0.fake.local"]
	d.cacheMutex.RUnlock()
	if !cached || resolvable {
		t.Errorf("expected host cached as unresolvable, got cached=%v resolvable=%v", cached, resolvable)
	}
}

// TestFallbackDialer_ResolvableHostNotRewritten verifies that when DNS
// resolves a hostname successfully, the dialer connects to the original
// address rather than rewriting it to the bootstrap IP.
func TestFallbackDialer_ResolvableHostNotRewritten(t *testing.T) {
	// Start a local TCP listener to prove we connect to the right place.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer ln.Close()
	_, port, _ := net.SplitHostPort(ln.Addr().String())

	// Bootstrap points to a black-hole IP – if the dialer incorrectly
	// rewrites, the connection will fail or time out.
	d := newTestDialer("192.0.2.1", 2*time.Second, func(host string) ([]string, error) {
		if host == "localhost" {
			return []string{"127.0.0.1"}, nil
		}
		return nil, fmt.Errorf("no such host: %s", host)
	})

	// Accept one connection in the background.
	accepted := make(chan struct{})
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			conn.Close()
		}
		close(accepted)
	}()

	conn, err := d.Dial("tcp", net.JoinHostPort("localhost", port))
	if err != nil {
		t.Fatalf("expected connection to succeed (host is resolvable), got: %v", err)
	}
	conn.Close()
	<-accepted

	// Cache must show localhost as resolvable.
	d.cacheMutex.RLock()
	resolvable := d.lookupCache["localhost"]
	d.cacheMutex.RUnlock()
	if !resolvable {
		t.Error("expected localhost to be cached as resolvable")
	}
}

// TestFallbackDialer_UnresolvableHostRewritten verifies that when DNS
// cannot resolve the broker hostname, the dialer rewrites the address to
// the bootstrap IP so the connection still succeeds.
func TestFallbackDialer_UnresolvableHostRewritten(t *testing.T) {
	// Start a local TCP listener – this is the bootstrap target.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer ln.Close()
	_, port, _ := net.SplitHostPort(ln.Addr().String())

	d := newTestDialer("127.0.0.1", 2*time.Second, func(host string) ([]string, error) {
		return nil, fmt.Errorf("no such host: %s", host)
	})

	// Accept one connection in the background.
	accepted := make(chan struct{})
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			conn.Close()
		}
		close(accepted)
	}()

	// Dial an unresolvable host – the dialer should rewrite to 127.0.0.1.
	conn, err := d.Dial("tcp", net.JoinHostPort("kafka-0.internal", port))
	if err != nil {
		t.Fatalf("expected connection to succeed (host should be rewritten to bootstrap), got: %v", err)
	}
	conn.Close()
	<-accepted
}

// TestFallbackDialer_IPAddressNotLookedUp verifies that when the broker
// address is already an IP (not a hostname), the dialer skips DNS lookup
// entirely and the cache stays empty.
func TestFallbackDialer_IPAddressNotLookedUp(t *testing.T) {
	var lookupCount int64

	d := newTestDialer("127.0.0.1", 50*time.Millisecond, func(host string) ([]string, error) {
		atomic.AddInt64(&lookupCount, 1)
		return nil, fmt.Errorf("should not be called for IP addresses")
	})

	// Dial with a raw IP address – should bypass DNS completely.
	_, _ = d.Dial("tcp", "10.0.0.1:9092")

	if count := atomic.LoadInt64(&lookupCount); count != 0 {
		t.Errorf("expected no DNS lookups for IP address, got %d", count)
	}

	// Cache should remain empty since IPs bypass the lookup path.
	d.cacheMutex.RLock()
	cacheLen := len(d.lookupCache)
	d.cacheMutex.RUnlock()
	if cacheLen != 0 {
		t.Errorf("expected empty cache after IP dial, got %d entries", cacheLen)
	}
}

// TestFallbackDialer_MultipleDifferentHosts verifies that the cache
// correctly tracks multiple different hostnames independently.
func TestFallbackDialer_MultipleDifferentHosts(t *testing.T) {
	var lookupCount int64

	d := newTestDialer("127.0.0.1", 50*time.Millisecond, func(host string) ([]string, error) {
		atomic.AddInt64(&lookupCount, 1)
		if host == "broker-ok" {
			return []string{"10.0.0.1"}, nil
		}
		return nil, fmt.Errorf("no such host: %s", host)
	})

	// Dial two different hosts, each multiple times (sequentially, so cache works).
	for i := 0; i < 3; i++ {
		_, _ = d.Dial("tcp", "broker-ok:9092")
		_, _ = d.Dial("tcp", "broker-bad:9092")
	}

	// Exactly 2 lookups: one per unique hostname.
	if count := atomic.LoadInt64(&lookupCount); count != 2 {
		t.Errorf("expected 2 lookups (one per unique host), got %d", count)
	}

	d.cacheMutex.RLock()
	defer d.cacheMutex.RUnlock()

	if !d.lookupCache["broker-ok"] {
		t.Error("expected broker-ok to be cached as resolvable")
	}
	if d.lookupCache["broker-bad"] {
		t.Error("expected broker-bad to be cached as unresolvable")
	}
}
