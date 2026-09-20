package observability

import (
	"sync/atomic"
	"time"
)

// Operation identifies a cache operation for metrics. The values are fixed so
// the collector can expose bounded labels without constructing strings on the
// cache hot path.
type Operation uint8

const (
	OperationGet Operation = iota
	OperationGetMany
	OperationSet
	OperationSetMany
	OperationAdd
	OperationForever
	OperationForget
	OperationTouch
	OperationFlush
	OperationIncrement
	OperationDecrement
	OperationRecoveryProbe
	OperationCount
)

func (o Operation) String() string {
	switch o {
	case OperationGet:
		return "get"
	case OperationGetMany:
		return "get_many"
	case OperationSet:
		return "set"
	case OperationSetMany:
		return "set_many"
	case OperationAdd:
		return "add"
	case OperationForever:
		return "forever"
	case OperationForget:
		return "forget"
	case OperationTouch:
		return "touch"
	case OperationFlush:
		return "flush"
	case OperationIncrement:
		return "increment"
	case OperationDecrement:
		return "decrement"
	case OperationRecoveryProbe:
		return "recovery_probe"
	default:
		return "unknown"
	}
}

// LookupResult identifies the authoritative result of a read.
type LookupResult uint8

const (
	LookupL1Hit LookupResult = iota
	LookupL2Hit
	LookupMiss
	LookupResultCount
)

func (r LookupResult) String() string {
	switch r {
	case LookupL1Hit:
		return "l1_hit"
	case LookupL2Hit:
		return "l2_hit"
	case LookupMiss:
		return "miss"
	default:
		return "unknown"
	}
}

// L2ErrorClass is deliberately small and stable. Raw Redis errors must never
// become Prometheus label values.
type L2ErrorClass uint8

const L2ErrorNone L2ErrorClass = ^L2ErrorClass(0)

const (
	L2ErrorTransport L2ErrorClass = iota
	L2ErrorCommand
	L2ErrorPostCommit
	L2ErrorInternal
	L2ErrorClassCount
)

func (c L2ErrorClass) String() string {
	switch c {
	case L2ErrorTransport:
		return "transport"
	case L2ErrorCommand:
		return "command"
	case L2ErrorPostCommit:
		return "post_commit"
	case L2ErrorInternal:
		return "internal"
	default:
		return "unknown"
	}
}

type InvalidationDirection uint8

const (
	InvalidationPublished InvalidationDirection = iota
	InvalidationReceived
	InvalidationDirectionCount
)

func (d InvalidationDirection) String() string {
	switch d {
	case InvalidationPublished:
		return "published"
	case InvalidationReceived:
		return "received"
	default:
		return "unknown"
	}
}

type InvalidationType uint8

const (
	InvalidationKey InvalidationType = iota
	InvalidationFlush
	InvalidationTypeCount
)

func (t InvalidationType) String() string {
	switch t {
	case InvalidationKey:
		return "key"
	case InvalidationFlush:
		return "flush"
	default:
		return "unknown"
	}
}

type InvalidationResult uint8

const (
	InvalidationSuccess InvalidationResult = iota
	InvalidationError
	InvalidationResultCount
)

func (r InvalidationResult) String() string {
	switch r {
	case InvalidationSuccess:
		return "success"
	case InvalidationError:
		return "error"
	default:
		return "unknown"
	}
}

type L1FlushFallbackResult uint8

const (
	L1FlushFallbackSuccess L1FlushFallbackResult = iota
	L1FlushFallbackFailure
	L1FlushFallbackResultCount
)

func (r L1FlushFallbackResult) String() string {
	switch r {
	case L1FlushFallbackSuccess:
		return "success"
	case L1FlushFallbackFailure:
		return "failure"
	default:
		return "unknown"
	}
}

const L2LatencyBucketCount = 16

var l2LatencyBucketUpperBounds = [...]time.Duration{
	100 * time.Microsecond,
	500 * time.Microsecond,
	time.Millisecond,
	2500 * time.Microsecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	25 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	time.Second,
	2500 * time.Millisecond,
	5 * time.Second,
	10 * time.Second,
}

// L2LatencyBucketUpperBounds returns the fixed histogram boundaries used by
// the collector. The last bucket is the implicit +Inf bucket.
func L2LatencyBucketUpperBounds() [L2LatencyBucketCount - 1]time.Duration {
	return l2LatencyBucketUpperBounds
}

type l2OperationState struct {
	calls           atomic.Uint64
	errors          [L2ErrorClassCount]atomic.Uint64
	latencyBuckets  [L2LatencyBucketCount]atomic.Uint64
	latencySumNanos atomic.Uint64
	batchCalls      atomic.Uint64
	batchItems      atomic.Uint64
}

// MetricsState contains only fixed-size atomic state. It has no dependency on
// Prometheus and can therefore be shared by the cache and a later collector.
type MetricsState struct {
	lookup   [LookupResultCount]atomic.Uint64
	l1Misses atomic.Uint64
	l2       [OperationCount]l2OperationState

	l1Entries      atomic.Int64
	l1Bytes        atomic.Int64
	l1MaxBytes     atomic.Int64
	l1MaxItemBytes atomic.Int64
	l1Evictions    atomic.Uint64
	l1EvictedBytes atomic.Uint64

	degraded            atomic.Uint32
	degradedTransitions atomic.Uint64
	recoveryAttempts    atomic.Uint64
	recoverySuccesses   atomic.Uint64
	recoveryFailures    atomic.Uint64

	invalidationReady    atomic.Uint32
	invalidation         [InvalidationDirectionCount][InvalidationTypeCount][InvalidationResultCount]atomic.Uint64
	pendingInvalidations atomic.Uint64
	pendingFlush         atomic.Uint32

	postCommitErrors atomic.Uint64
	l1MutationErrors atomic.Uint64
	l1FlushFallbacks [L1FlushFallbackResultCount]atomic.Uint64
}

type L2OperationSnapshot struct {
	Calls           uint64
	Errors          [L2ErrorClassCount]uint64
	LatencyBuckets  [L2LatencyBucketCount]uint64
	LatencySumNanos uint64
	BatchCalls      uint64
	BatchItems      uint64
}

type Snapshot struct {
	Lookup   [LookupResultCount]uint64
	L1Misses uint64
	L2       [OperationCount]L2OperationSnapshot

	L1Entries      int64
	L1Bytes        int64
	L1MaxBytes     int64
	L1MaxItemBytes int64
	L1Evictions    uint64
	L1EvictedBytes uint64

	Degraded            bool
	DegradedTransitions uint64
	RecoveryAttempts    uint64
	RecoverySuccesses   uint64
	RecoveryFailures    uint64

	InvalidationReady    bool
	Invalidation         [InvalidationDirectionCount][InvalidationTypeCount][InvalidationResultCount]uint64
	PendingInvalidations uint64
	PendingFlush         bool

	PostCommitErrors uint64
	L1MutationErrors uint64
	L1FlushFallbacks [L1FlushFallbackResultCount]uint64
}

func (m *MetricsState) ObserveLookup(result LookupResult) {
	if m == nil || result >= LookupResultCount {
		return
	}
	m.lookup[result].Add(1)
}

func (m *MetricsState) ObserveL1Miss() {
	if m != nil {
		m.l1Misses.Add(1)
	}
}

// ObserveL2 records one L2 call. elapsed is measured by the caller around the
// backend call and errorClass is one of the fixed L2ErrorClass values.
func (m *MetricsState) ObserveL2(operation Operation, elapsed time.Duration, errorClass L2ErrorClass) {
	if m == nil || operation >= OperationCount {
		return
	}

	state := &m.l2[operation]
	state.calls.Add(1)
	if elapsed < 0 {
		elapsed = 0
	}
	state.latencySumNanos.Add(uint64(elapsed))
	bucket := L2LatencyBucketCount - 1
	for i, upperBound := range l2LatencyBucketUpperBounds {
		if elapsed <= upperBound {
			bucket = i
			break
		}
	}
	state.latencyBuckets[bucket].Add(1)

	if errorClass < L2ErrorClassCount {
		state.errors[errorClass].Add(1)
	}
}

func (m *MetricsState) ObserveBatch(operation Operation, items uint64) {
	if m == nil || operation >= OperationCount {
		return
	}
	m.l2[operation].batchCalls.Add(1)
	m.l2[operation].batchItems.Add(items)
}

func (m *MetricsState) SetL1Usage(entries, bytes int64) {
	if m == nil {
		return
	}
	m.l1Entries.Store(entries)
	m.l1Bytes.Store(bytes)
}

func (m *MetricsState) SetL1Limits(maxBytes, maxItemBytes int64) {
	if m == nil {
		return
	}
	m.l1MaxBytes.Store(maxBytes)
	m.l1MaxItemBytes.Store(maxItemBytes)
}

func (m *MetricsState) ObserveL1Eviction(entries uint64, bytes int64) {
	if m == nil {
		return
	}
	m.l1Evictions.Add(entries)
	if bytes > 0 {
		m.l1EvictedBytes.Add(uint64(bytes))
	}
}

func (m *MetricsState) SetDegraded(value bool) {
	if m == nil {
		return
	}
	wanted := uint32(0)
	if value {
		wanted = 1
	}
	if m.degraded.Swap(wanted) != wanted {
		m.degradedTransitions.Add(1)
	}
}

func (m *MetricsState) ObserveRecoveryAttempt() {
	if m != nil {
		m.recoveryAttempts.Add(1)
	}
}

func (m *MetricsState) ObserveRecoveryResult(success bool) {
	if m == nil {
		return
	}
	if success {
		m.recoverySuccesses.Add(1)
		return
	}
	m.recoveryFailures.Add(1)
}

func (m *MetricsState) SetInvalidationReady(value bool) {
	if m == nil {
		return
	}
	if value {
		m.invalidationReady.Store(1)
		return
	}
	m.invalidationReady.Store(0)
}

func (m *MetricsState) ObserveInvalidation(direction InvalidationDirection, typ InvalidationType, result InvalidationResult) {
	if m == nil || direction >= InvalidationDirectionCount || typ >= InvalidationTypeCount || result >= InvalidationResultCount {
		return
	}
	m.invalidation[direction][typ][result].Add(1)
}

func (m *MetricsState) SetPendingInvalidations(value uint64) {
	if m != nil {
		m.pendingInvalidations.Store(value)
	}
}

func (m *MetricsState) SetPendingFlush(value bool) {
	if m == nil {
		return
	}
	if value {
		m.pendingFlush.Store(1)
		return
	}
	m.pendingFlush.Store(0)
}

func (m *MetricsState) ObservePostCommitError() {
	if m != nil {
		m.postCommitErrors.Add(1)
	}
}

func (m *MetricsState) ObserveL1MutationError() {
	if m != nil {
		m.l1MutationErrors.Add(1)
	}
}

func (m *MetricsState) ObserveL1FlushFallback(result L1FlushFallbackResult) {
	if m == nil || result >= L1FlushFallbackResultCount {
		return
	}
	m.l1FlushFallbacks[result].Add(1)
}

func (m *MetricsState) Snapshot() Snapshot {
	var snapshot Snapshot
	if m == nil {
		return snapshot
	}

	for i := range m.lookup {
		snapshot.Lookup[i] = m.lookup[i].Load()
	}
	snapshot.L1Misses = m.l1Misses.Load()
	for i := range m.l2 {
		state := &m.l2[i]
		snapshot.L2[i] = L2OperationSnapshot{
			Calls:           state.calls.Load(),
			LatencySumNanos: state.latencySumNanos.Load(),
			BatchCalls:      state.batchCalls.Load(),
			BatchItems:      state.batchItems.Load(),
		}
		for j := range state.errors {
			snapshot.L2[i].Errors[j] = state.errors[j].Load()
		}
		for j := range state.latencyBuckets {
			snapshot.L2[i].LatencyBuckets[j] = state.latencyBuckets[j].Load()
		}
	}

	snapshot.L1Entries = m.l1Entries.Load()
	snapshot.L1Bytes = m.l1Bytes.Load()
	snapshot.L1MaxBytes = m.l1MaxBytes.Load()
	snapshot.L1MaxItemBytes = m.l1MaxItemBytes.Load()
	snapshot.L1Evictions = m.l1Evictions.Load()
	snapshot.L1EvictedBytes = m.l1EvictedBytes.Load()
	snapshot.Degraded = m.degraded.Load() != 0
	snapshot.DegradedTransitions = m.degradedTransitions.Load()
	snapshot.RecoveryAttempts = m.recoveryAttempts.Load()
	snapshot.RecoverySuccesses = m.recoverySuccesses.Load()
	snapshot.RecoveryFailures = m.recoveryFailures.Load()
	snapshot.InvalidationReady = m.invalidationReady.Load() != 0
	for i := range m.invalidation {
		for j := range m.invalidation[i] {
			for k := range m.invalidation[i][j] {
				snapshot.Invalidation[i][j][k] = m.invalidation[i][j][k].Load()
			}
		}
	}
	snapshot.PendingInvalidations = m.pendingInvalidations.Load()
	snapshot.PendingFlush = m.pendingFlush.Load() != 0
	snapshot.PostCommitErrors = m.postCommitErrors.Load()
	snapshot.L1MutationErrors = m.l1MutationErrors.Load()
	for i := range m.l1FlushFallbacks {
		snapshot.L1FlushFallbacks[i] = m.l1FlushFallbacks[i].Load()
	}
	return snapshot
}
