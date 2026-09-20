package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Collector exposes a MetricsState snapshot to a Prometheus registry. It does
// not mutate the state and allocates only while Prometheus is collecting.
type Collector struct {
	state                *MetricsState
	version              string
	l1Stats              func() L1Snapshot
	buildInfo            *prometheus.Desc
	lookup               *prometheus.Desc
	l1Misses             *prometheus.Desc
	l2Calls              *prometheus.Desc
	l2Errors             *prometheus.Desc
	l2Duration           *prometheus.Desc
	batchCalls           *prometheus.Desc
	batchItems           *prometheus.Desc
	l1Entries            *prometheus.Desc
	l1Bytes              *prometheus.Desc
	l1MaxBytes           *prometheus.Desc
	l1MaxItem            *prometheus.Desc
	l1Evictions          *prometheus.Desc
	l1EvictedBytes       *prometheus.Desc
	degraded             *prometheus.Desc
	degradedTransitions  *prometheus.Desc
	recoveryAttempts     *prometheus.Desc
	recoverySuccesses    *prometheus.Desc
	recoveryFailures     *prometheus.Desc
	invalidation         *prometheus.Desc
	invalidationReady    *prometheus.Desc
	pendingInvalidations *prometheus.Desc
	pendingFlush         *prometheus.Desc
	postCommitErrors     *prometheus.Desc
	l1MutationErrors     *prometheus.Desc
	l1FlushFallbacks     *prometheus.Desc
}

func NewCollector(state *MetricsState, version string) *Collector {
	return newCollector(state, version, nil)
}

// L1Snapshot is supplied by the concrete memory backend at scrape time. The
// callback keeps MetricsState independent from the MemoryCache package while
// preserving lock-free cache operations.
type L1Snapshot struct {
	Entries      int64
	Bytes        int64
	MaxBytes     int64
	MaxItemBytes int64
}

func NewCollectorWithL1(state *MetricsState, version string, l1Stats func() L1Snapshot) *Collector {
	return newCollector(state, version, l1Stats)
}

func newCollector(state *MetricsState, version string, l1Stats func() L1Snapshot) *Collector {
	const namespace = "franken_cache"

	return &Collector{
		state:   state,
		version: version,
		l1Stats: l1Stats,
		buildInfo: prometheus.NewDesc(
			namespace+"_build_info",
			"Build information for FrankenPHP Tiered Cache.",
			[]string{"version"}, nil,
		),
		lookup: prometheus.NewDesc(
			namespace+"_lookup_total",
			"Cache lookups by authoritative result.",
			[]string{"result"}, nil,
		),
		l1Misses: prometheus.NewDesc(
			namespace+"_l1_misses_total",
			"L1 cache misses.", nil, nil,
		),
		l2Calls: prometheus.NewDesc(
			namespace+"_l2_operations_total",
			"L2 operations.", []string{"operation"}, nil,
		),
		l2Errors: prometheus.NewDesc(
			namespace+"_l2_errors_total",
			"L2 operation errors by bounded error class.", []string{"operation", "error_class"}, nil,
		),
		l2Duration: prometheus.NewDesc(
			namespace+"_l2_duration_seconds",
			"L2 operation latency.", []string{"operation"}, nil,
		),
		batchCalls: prometheus.NewDesc(
			namespace+"_batch_calls_total",
			"Batch operation calls.", []string{"operation"}, nil,
		),
		batchItems: prometheus.NewDesc(
			namespace+"_batch_items_total",
			"Items processed by batch operations.", []string{"operation"}, nil,
		),
		l1Entries: prometheus.NewDesc(
			namespace+"_l1_entries",
			"Current number of entries in L1.", nil, nil,
		),
		l1Bytes: prometheus.NewDesc(
			namespace+"_l1_bytes",
			"Current bytes accounted in L1.", nil, nil,
		),
		l1MaxBytes: prometheus.NewDesc(
			namespace+"_l1_max_bytes",
			"Configured maximum L1 bytes.", nil, nil,
		),
		l1MaxItem: prometheus.NewDesc(
			namespace+"_l1_max_item_bytes",
			"Configured maximum L1 item bytes.", nil, nil,
		),
		l1Evictions: prometheus.NewDesc(
			namespace+"_l1_evictions_total",
			"L1 live entries evicted due to memory pressure.", nil, nil,
		),
		l1EvictedBytes: prometheus.NewDesc(
			namespace+"_l1_evicted_bytes_total",
			"L1 bytes evicted due to memory pressure.", nil, nil,
		),
		degraded: prometheus.NewDesc(
			namespace+"_degraded",
			"Whether the local TieredCache is degraded.", nil, nil,
		),
		degradedTransitions: prometheus.NewDesc(
			namespace+"_degraded_transitions_total",
			"TieredCache health state transitions.", nil, nil,
		),
		recoveryAttempts: prometheus.NewDesc(
			namespace+"_recovery_attempts_total",
			"TieredCache recovery attempts.", nil, nil,
		),
		recoverySuccesses: prometheus.NewDesc(
			namespace+"_recovery_success_total",
			"Successful TieredCache recoveries.", nil, nil,
		),
		recoveryFailures: prometheus.NewDesc(
			namespace+"_recovery_failure_total",
			"Failed TieredCache recovery attempts.", nil, nil,
		),
		invalidation: prometheus.NewDesc(
			namespace+"_invalidation_total",
			"Redis Pub/Sub invalidation events.", []string{"direction", "type", "result"}, nil,
		),
		invalidationReady: prometheus.NewDesc(
			namespace+"_invalidation_ready",
			"Whether the Pub/Sub subscription is currently ready.", nil, nil,
		),
		pendingInvalidations: prometheus.NewDesc(
			namespace+"_pending_invalidations",
			"Number of pending invalidation keys awaiting retry.", nil, nil,
		),
		pendingFlush: prometheus.NewDesc(
			namespace+"_pending_flush",
			"Whether a flush invalidation is awaiting retry.", nil, nil,
		),
		postCommitErrors: prometheus.NewDesc(
			namespace+"_post_commit_errors_total",
			"Errors reported after an authoritative Redis mutation committed.", nil, nil,
		),
		l1MutationErrors: prometheus.NewDesc(
			namespace+"_l1_mutation_errors_total",
			"L1 mutation errors.", nil, nil,
		),
		l1FlushFallbacks: prometheus.NewDesc(
			namespace+"_l1_flush_fallback_total",
			"L1 flush fallback attempts after mutation errors.", nil, nil,
		),
	}
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{
		c.buildInfo,
		c.lookup,
		c.l1Misses,
		c.l2Calls,
		c.l2Errors,
		c.l2Duration,
		c.batchCalls,
		c.batchItems,
		c.l1Entries,
		c.l1Bytes,
		c.l1MaxBytes,
		c.l1MaxItem,
		c.l1Evictions,
		c.l1EvictedBytes,
		c.degraded,
		c.degradedTransitions,
		c.recoveryAttempts,
		c.recoverySuccesses,
		c.recoveryFailures,
		c.invalidation,
		c.invalidationReady,
		c.pendingInvalidations,
		c.pendingFlush,
		c.postCommitErrors,
		c.l1MutationErrors,
		c.l1FlushFallbacks,
	} {
		ch <- desc
	}
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	snapshot := c.state.Snapshot()
	if c.l1Stats != nil {
		l1 := c.l1Stats()
		snapshot.L1Entries = l1.Entries
		snapshot.L1Bytes = l1.Bytes
		snapshot.L1MaxBytes = l1.MaxBytes
		snapshot.L1MaxItemBytes = l1.MaxItemBytes
	}

	ch <- prometheus.MustNewConstMetric(c.buildInfo, prometheus.GaugeValue, 1, c.buildVersion())
	for result := LookupResult(0); result < LookupResultCount; result++ {
		ch <- prometheus.MustNewConstMetric(c.lookup, prometheus.CounterValue, float64(snapshot.Lookup[result]), result.String())
	}
	ch <- prometheus.MustNewConstMetric(c.l1Misses, prometheus.CounterValue, float64(snapshot.L1Misses))

	for operation := Operation(0); operation < OperationCount; operation++ {
		l2 := snapshot.L2[operation]
		ch <- prometheus.MustNewConstMetric(c.l2Calls, prometheus.CounterValue, float64(l2.Calls), operation.String())
		for errorClass := L2ErrorClass(0); errorClass < L2ErrorClassCount; errorClass++ {
			ch <- prometheus.MustNewConstMetric(c.l2Errors, prometheus.CounterValue, float64(l2.Errors[errorClass]), operation.String(), errorClass.String())
		}
		ch <- prometheus.MustNewConstHistogram(
			c.l2Duration,
			l2.Calls,
			float64(l2.LatencySumNanos)/float64(time.Second),
			c.histogramBuckets(l2.LatencyBuckets),
			operation.String(),
		)
		if l2.BatchCalls != 0 || l2.BatchItems != 0 {
			ch <- prometheus.MustNewConstMetric(c.batchCalls, prometheus.CounterValue, float64(l2.BatchCalls), operation.String())
			ch <- prometheus.MustNewConstMetric(c.batchItems, prometheus.CounterValue, float64(l2.BatchItems), operation.String())
		}
	}

	ch <- prometheus.MustNewConstMetric(c.l1Entries, prometheus.GaugeValue, float64(snapshot.L1Entries))
	ch <- prometheus.MustNewConstMetric(c.l1Bytes, prometheus.GaugeValue, float64(snapshot.L1Bytes))
	ch <- prometheus.MustNewConstMetric(c.l1MaxBytes, prometheus.GaugeValue, float64(snapshot.L1MaxBytes))
	ch <- prometheus.MustNewConstMetric(c.l1MaxItem, prometheus.GaugeValue, float64(snapshot.L1MaxItemBytes))
	ch <- prometheus.MustNewConstMetric(c.l1Evictions, prometheus.CounterValue, float64(snapshot.L1Evictions))
	ch <- prometheus.MustNewConstMetric(c.l1EvictedBytes, prometheus.CounterValue, float64(snapshot.L1EvictedBytes))
	degraded := float64(0)
	if snapshot.Degraded {
		degraded = 1
	}
	ch <- prometheus.MustNewConstMetric(c.degraded, prometheus.GaugeValue, degraded)
	ch <- prometheus.MustNewConstMetric(c.degradedTransitions, prometheus.CounterValue, float64(snapshot.DegradedTransitions))
	ch <- prometheus.MustNewConstMetric(c.recoveryAttempts, prometheus.CounterValue, float64(snapshot.RecoveryAttempts))
	ch <- prometheus.MustNewConstMetric(c.recoverySuccesses, prometheus.CounterValue, float64(snapshot.RecoverySuccesses))
	ch <- prometheus.MustNewConstMetric(c.recoveryFailures, prometheus.CounterValue, float64(snapshot.RecoveryFailures))

	for direction := InvalidationDirection(0); direction < InvalidationDirectionCount; direction++ {
		for typ := InvalidationType(0); typ < InvalidationTypeCount; typ++ {
			for result := InvalidationResult(0); result < InvalidationResultCount; result++ {
				value := snapshot.Invalidation[direction][typ][result]
				ch <- prometheus.MustNewConstMetric(c.invalidation, prometheus.CounterValue, float64(value), direction.String(), typ.String(), result.String())
			}
		}
	}
	ready := float64(0)
	if snapshot.InvalidationReady {
		ready = 1
	}
	ch <- prometheus.MustNewConstMetric(c.invalidationReady, prometheus.GaugeValue, ready)
	ch <- prometheus.MustNewConstMetric(c.pendingInvalidations, prometheus.GaugeValue, float64(snapshot.PendingInvalidations))
	pendingFlush := float64(0)
	if snapshot.PendingFlush {
		pendingFlush = 1
	}
	ch <- prometheus.MustNewConstMetric(c.pendingFlush, prometheus.GaugeValue, pendingFlush)
	ch <- prometheus.MustNewConstMetric(c.postCommitErrors, prometheus.CounterValue, float64(snapshot.PostCommitErrors))
	ch <- prometheus.MustNewConstMetric(c.l1MutationErrors, prometheus.CounterValue, float64(snapshot.L1MutationErrors))
	ch <- prometheus.MustNewConstMetric(c.l1FlushFallbacks, prometheus.CounterValue, float64(snapshot.L1FlushFallbacks))
}

func (c *Collector) buildVersion() string {
	return c.version
}

func (c *Collector) histogramBuckets(counts [L2LatencyBucketCount]uint64) map[float64]uint64 {
	upperBounds := L2LatencyBucketUpperBounds()
	buckets := make(map[float64]uint64, len(upperBounds))
	var cumulative uint64
	for i, upperBound := range upperBounds {
		cumulative += counts[i]
		buckets[upperBound.Seconds()] = cumulative
	}
	return buckets
}
