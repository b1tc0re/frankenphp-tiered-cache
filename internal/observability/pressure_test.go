package observability

import (
	"sync"
	"testing"
	"time"
)

func TestPressureReporterAggregatesEvents(t *testing.T) {
	summaries := make(chan PressureSummary, 1)
	reporter := newTestPressureReporter(t, 10*time.Millisecond, func(summary PressureSummary) {
		summaries <- summary
	})

	reporter.Observe(1, 12)
	reporter.Observe(1, 30)

	select {
	case summary := <-summaries:
		if summary.EvictedEntries != 2 {
			t.Fatalf("evicted entries = %d, want 2", summary.EvictedEntries)
		}
		if summary.EvictedBytes != 42 {
			t.Fatalf("evicted bytes = %d, want 42", summary.EvictedBytes)
		}
		if summary.Window != 10*time.Millisecond {
			t.Fatalf("window = %s, want 10ms", summary.Window)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for pressure summary")
	}
}

func TestPressureReporterDoesNotReportEmptyWindows(t *testing.T) {
	summaries := make(chan PressureSummary, 1)
	newTestPressureReporter(t, 10*time.Millisecond, func(summary PressureSummary) {
		summaries <- summary
	})

	select {
	case summary := <-summaries:
		t.Fatalf("received unexpected summary: %+v", summary)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestPressureReporterConcurrentObserveAggregatesBatches(t *testing.T) {
	summaries := make(chan PressureSummary, 1)
	reporter := newTestPressureReporter(t, 10*time.Millisecond, func(summary PressureSummary) {
		summaries <- summary
	})

	const batches = 100
	const entriesPerBatch uint64 = 3
	const bytesPerBatch int64 = 21
	var wg sync.WaitGroup
	wg.Add(batches)
	for i := 0; i < batches; i++ {
		go func() {
			defer wg.Done()
			reporter.Observe(entriesPerBatch, bytesPerBatch)
		}()
	}
	wg.Wait()

	select {
	case summary := <-summaries:
		wantEntries := uint64(batches) * entriesPerBatch
		if summary.EvictedEntries != wantEntries {
			t.Fatalf("evicted entries = %d, want %d", summary.EvictedEntries, wantEntries)
		}
		wantBytes := int64(batches) * bytesPerBatch
		if summary.EvictedBytes != wantBytes {
			t.Fatalf("evicted bytes = %d, want %d", summary.EvictedBytes, wantBytes)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for concurrent pressure summary")
	}
}

func TestPressureReporterCloseFlushesPendingEvents(t *testing.T) {
	summaries := make(chan PressureSummary, 1)
	reporter, err := NewPressureReporter(time.Hour, func(summary PressureSummary) {
		summaries <- summary
	})
	if err != nil {
		t.Fatalf("NewPressureReporter() error = %v", err)
	}

	reporter.Observe(1, 99)
	reporter.Close()
	reporter.Close()

	select {
	case summary := <-summaries:
		if summary.EvictedEntries != 1 || summary.EvictedBytes != 99 {
			t.Fatalf("summary = %+v, want one event with 99 bytes", summary)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for close flush")
	}
}

func TestNewPressureReporterValidatesArguments(t *testing.T) {
	if reporter, err := NewPressureReporter(0, func(PressureSummary) {}); reporter != nil || err != ErrInvalidInterval {
		t.Fatalf("zero interval = reporter %v, error %v; want nil, ErrInvalidInterval", reporter, err)
	}
	if reporter, err := NewPressureReporter(time.Second, nil); reporter != nil || err != ErrNilReporter {
		t.Fatalf("nil report = reporter %v, error %v; want nil, ErrNilReporter", reporter, err)
	}
}

func newTestPressureReporter(t *testing.T, interval time.Duration, report func(PressureSummary)) *PressureReporter {
	t.Helper()

	reporter, err := NewPressureReporter(interval, report)
	if err != nil {
		t.Fatalf("NewPressureReporter() error = %v", err)
	}
	t.Cleanup(reporter.Close)
	return reporter
}
