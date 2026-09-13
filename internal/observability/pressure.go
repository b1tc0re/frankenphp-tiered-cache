package observability

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrInvalidInterval = errors.New("observability: interval must be greater than zero")
	ErrNilReporter     = errors.New("observability: report callback must not be nil")
)

// PressureSummary describes live-entry evictions accumulated during one
// reporting window.
type PressureSummary struct {
	EvictedEntries uint64
	EvictedBytes   int64
	Window         time.Duration
}

// PressureReporter aggregates pressure events and invokes report at most once
// per interval. Observe is intended for a fast diagnostic path: it only
// updates counters and never performs logging or formatting.
type PressureReporter struct {
	interval time.Duration
	report   func(PressureSummary)

	mu             sync.Mutex
	evictedEntries uint64
	evictedBytes   int64

	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// NewPressureReporter starts an interval-based pressure reporter.
func NewPressureReporter(interval time.Duration, report func(PressureSummary)) (*PressureReporter, error) {
	if interval <= 0 {
		return nil, ErrInvalidInterval
	}
	if report == nil {
		return nil, ErrNilReporter
	}

	reporter := &PressureReporter{
		interval: interval,
		report:   report,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go reporter.run()

	return reporter, nil
}

// Observe records one live-entry eviction. Negative byte counts are ignored
// because they cannot represent a valid eviction size.
func (r *PressureReporter) Observe(evictedBytes int64) {
	if evictedBytes < 0 {
		return
	}

	r.mu.Lock()
	r.evictedEntries++
	r.evictedBytes += evictedBytes
	r.mu.Unlock()
}

// Close stops the reporter and flushes events accumulated since the last
// reporting window. It is safe to call Close more than once.
func (r *PressureReporter) Close() {
	r.closeOnce.Do(func() {
		close(r.stop)
		<-r.done
	})
}

func (r *PressureReporter) run() {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	defer close(r.done)

	for {
		select {
		case <-ticker.C:
			r.flush()
		case <-r.stop:
			r.flush()
			return
		}
	}
}

func (r *PressureReporter) flush() {
	r.mu.Lock()
	entries := r.evictedEntries
	bytes := r.evictedBytes
	r.evictedEntries = 0
	r.evictedBytes = 0
	r.mu.Unlock()

	if entries == 0 {
		return
	}

	r.report(PressureSummary{
		EvictedEntries: entries,
		EvictedBytes:   bytes,
		Window:         r.interval,
	})
}
