package observability

import (
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsStateSnapshot(t *testing.T) {
	state := new(MetricsState)

	state.ObserveLookup(LookupL1Hit)
	state.ObserveLookup(LookupL2Hit)
	state.ObserveLookup(LookupMiss)
	state.ObserveL1Miss()
	errorClass := L2ErrorTransport
	state.ObserveL2(OperationGet, 2*time.Millisecond, errorClass)
	state.ObserveBatch(OperationGetMany, 3)
	state.SetL1Usage(12, 1024)
	state.SetL1Limits(64<<20, 4<<20)
	state.ObserveL1Eviction(2, 128)
	state.SetDegraded(true)
	state.SetDegraded(true)
	state.SetDegraded(false)
	state.ObserveRecoveryAttempt()
	state.ObserveRecoveryResult(true)
	state.ObserveRecoveryResult(false)
	state.SetInvalidationReady(true)
	state.ObserveInvalidation(InvalidationPublished, InvalidationKey, InvalidationSuccess)
	state.SetPendingInvalidations(4)
	state.SetPendingFlush(true)
	state.ObservePostCommitError()
	state.ObserveL1MutationError()
	state.ObserveL1FlushFallback()

	snapshot := state.Snapshot()
	if snapshot.Lookup != [LookupResultCount]uint64{1, 1, 1} {
		t.Fatalf("lookup = %#v", snapshot.Lookup)
	}
	if snapshot.L1Misses != 1 {
		t.Fatalf("L1 misses = %d, want 1", snapshot.L1Misses)
	}
	if snapshot.L2[OperationGet].Calls != 1 || snapshot.L2[OperationGet].Errors[L2ErrorTransport] != 1 {
		t.Fatalf("get L2 snapshot = %#v", snapshot.L2[OperationGet])
	}
	if snapshot.L2[OperationGetMany].BatchCalls != 1 || snapshot.L2[OperationGetMany].BatchItems != 3 {
		t.Fatalf("get_many batch snapshot = %#v", snapshot.L2[OperationGetMany])
	}
	if snapshot.L1Entries != 12 || snapshot.L1Bytes != 1024 || snapshot.L1Evictions != 2 || snapshot.L1EvictedBytes != 128 {
		t.Fatalf("L1 snapshot = entries=%d bytes=%d evictions=%d evicted_bytes=%d", snapshot.L1Entries, snapshot.L1Bytes, snapshot.L1Evictions, snapshot.L1EvictedBytes)
	}
	if snapshot.Degraded || snapshot.DegradedTransitions != 2 {
		t.Fatalf("health snapshot = degraded=%t transitions=%d", snapshot.Degraded, snapshot.DegradedTransitions)
	}
	if snapshot.RecoveryAttempts != 1 || snapshot.RecoverySuccesses != 1 || snapshot.RecoveryFailures != 1 {
		t.Fatalf("recovery snapshot = attempts=%d successes=%d failures=%d", snapshot.RecoveryAttempts, snapshot.RecoverySuccesses, snapshot.RecoveryFailures)
	}
	if !snapshot.InvalidationReady || snapshot.Invalidation[InvalidationPublished][InvalidationKey][InvalidationSuccess] != 1 {
		t.Fatalf("invalidation snapshot = %#v", snapshot.Invalidation)
	}
	if snapshot.PendingInvalidations != 4 || !snapshot.PendingFlush || snapshot.PostCommitErrors != 1 || snapshot.L1MutationErrors != 1 || snapshot.L1FlushFallbacks != 1 {
		t.Fatalf("pending/error snapshot = %#v", snapshot)
	}
}

func TestMetricsStateConcurrentUpdates(t *testing.T) {
	state := new(MetricsState)
	const goroutines = 8
	const iterations = 1000

	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				state.ObserveLookup(LookupL1Hit)
				state.ObserveL2(OperationGet, time.Microsecond, L2ErrorNone)
			}
		}()
	}
	wg.Wait()

	snapshot := state.Snapshot()
	want := uint64(goroutines * iterations)
	if snapshot.Lookup[LookupL1Hit] != want {
		t.Fatalf("L1 hits = %d, want %d", snapshot.Lookup[LookupL1Hit], want)
	}
	if snapshot.L2[OperationGet].Calls != want {
		t.Fatalf("L2 gets = %d, want %d", snapshot.L2[OperationGet].Calls, want)
	}
}

func TestCollectorGather(t *testing.T) {
	state := new(MetricsState)
	state.ObserveLookup(LookupL1Hit)
	state.ObserveL2(OperationGet, 2*time.Millisecond, L2ErrorNone)

	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(NewCollectorWithL1(state, "0.1.0", func() L1Snapshot {
		return L1Snapshot{Entries: 2, Bytes: 128, MaxBytes: 1024, MaxItemBytes: 512}
	})); err != nil {
		t.Fatalf("register collector: %v", err)
	}

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	seen := make(map[string]bool, len(families))
	for _, family := range families {
		seen[family.GetName()] = true
	}
	for _, name := range []string{
		"franken_cache_build_info",
		"franken_cache_lookup_total",
		"franken_cache_l2_operations_total",
		"franken_cache_l2_duration_seconds",
		"franken_cache_invalidation_ready",
	} {
		if !seen[name] {
			t.Errorf("metric family %q was not gathered", name)
		}
	}
}
