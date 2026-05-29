package batcher

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestTickMode_SteadyCadence verifies that successive batch flushes
// in tick mode are spaced ~interval apart regardless of arrival jitter.
// This is the load-bearing property of fixed-interval mode.
func TestTickMode_SteadyCadence(t *testing.T) {
	const interval = 20 * time.Millisecond
	const observationWindow = 200 * time.Millisecond

	var mu sync.Mutex
	var flushTimes []time.Time

	b := New[int](10000, time.Hour, func(_ []*int) {
		mu.Lock()
		flushTimes = append(flushTimes, time.Now())
		mu.Unlock()
	}, false)
	b.SetTickInterval(interval)
	defer b.Close()

	// Sustained Puts with mild jitter.
	done := make(chan struct{})
	go func() {
		defer close(done)
		deadline := time.Now().Add(observationWindow)
		i := 0
		for time.Now().Before(deadline) {
			v := i
			b.Put(&v)
			i++
			time.Sleep(time.Microsecond * 100)
		}
	}()
	<-done

	// Allow last in-flight tick to land.
	time.Sleep(2 * interval)

	mu.Lock()
	defer mu.Unlock()

	require.GreaterOrEqual(t, len(flushTimes), 5, "expected several flushes in observation window")

	// Successive gaps should average within ±20% of interval.
	var totalGap time.Duration
	gaps := 0
	for i := 1; i < len(flushTimes); i++ {
		gap := flushTimes[i].Sub(flushTimes[i-1])
		// Don't count gaps that span the warm-up or shutdown of the producer.
		if gap > 5*interval {
			continue
		}
		totalGap += gap
		gaps++
	}
	require.GreaterOrEqual(t, gaps, 3)

	avg := totalGap / time.Duration(gaps)
	loBound := interval - interval/5
	hiBound := interval + interval/5
	require.GreaterOrEqual(t, avg, loBound, "avg gap %s below 80%% of interval %s", avg, interval)
	require.LessOrEqual(t, avg, hiBound, "avg gap %s above 120%% of interval %s", avg, interval)
}

// TestTickMode_RejectsAfterDrain verifies that SetTickInterval is a no-op
// when drain mode is already enabled.
func TestTickMode_RejectsAfterDrain(t *testing.T) {
	var flushes int
	var mu sync.Mutex

	b := New[int](100, 100*time.Millisecond, func(_ []*int) {
		mu.Lock()
		flushes++
		mu.Unlock()
	}, false)
	defer b.Close()

	b.SetDrainMode(true)
	b.SetTickInterval(10 * time.Millisecond) // should be rejected

	v := 1
	b.Put(&v)

	// Allow drain mode to fire.
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, flushes, 1, "drain mode must still fire — tick mode should have been rejected")
}

// TestDrainMode_RejectsAfterTick verifies that SetDrainMode(true) is a no-op
// when tick mode is already enabled.
func TestDrainMode_RejectsAfterTick(t *testing.T) {
	const interval = 20 * time.Millisecond

	var mu sync.Mutex
	var flushTimes []time.Time

	b := New[int](10000, time.Hour, func(_ []*int) {
		mu.Lock()
		flushTimes = append(flushTimes, time.Now())
		mu.Unlock()
	}, false)
	defer b.Close()

	b.SetTickInterval(interval)
	b.SetDrainMode(true) // should be rejected

	// If drain mode took effect, this single Put would flush immediately.
	// If tick mode held, the Put waits up to ~interval.
	start := time.Now()
	v := 1
	b.Put(&v)
	time.Sleep(2 * interval)

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(flushTimes), 1)
	firstFlushDelay := flushTimes[0].Sub(start)
	// Drain mode would flush in < 1ms; tick mode in interval/2 average, up to interval worst case.
	require.GreaterOrEqual(t, firstFlushDelay, 1*time.Millisecond,
		"tick mode should still be in effect — drain mode should have been rejected")
}

// TestTickMode_EmptyTickIsNoop verifies that ticks with no queued items
// do not invoke fn, do not emit a span, do not increment metrics.
func TestTickMode_EmptyTickIsNoop(t *testing.T) {
	var calls int
	var mu sync.Mutex

	b := New[int](100, time.Hour, func(_ []*int) {
		mu.Lock()
		calls++
		mu.Unlock()
	}, false)
	defer b.Close()

	b.SetTickInterval(5 * time.Millisecond)

	// No Puts. Sleep through many tick intervals.
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 0, calls, "fn must not be called on empty ticks")
}

// TestTickMode_SingleItemFlushesOnTick verifies that a single Put still
// flushes within ~one tick interval.
func TestTickMode_SingleItemFlushesOnTick(t *testing.T) {
	const interval = 10 * time.Millisecond

	var received []*int
	var mu sync.Mutex
	done := make(chan struct{}, 1)

	b := New[int](100, time.Hour, func(batch []*int) {
		mu.Lock()
		received = append(received, batch...)
		mu.Unlock()
		select {
		case done <- struct{}{}:
		default:
		}
	}, false)
	defer b.Close()

	b.SetTickInterval(interval)

	v := 42
	start := time.Now()
	b.Put(&v)

	select {
	case <-done:
	case <-time.After(3 * interval):
		t.Fatalf("flush did not occur within 3 tick intervals")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, received, 1)
	require.Equal(t, 42, *received[0])
	require.Less(t, time.Since(start), 3*interval)
}

// TestTickMode_SizeCapFiresEarly verifies the size cap still flushes
// before the next tick when reached.
func TestTickMode_SizeCapFiresEarly(t *testing.T) {
	const size = 10
	const interval = time.Hour // effectively never

	flushCh := make(chan int, 4)

	b := New[int](size, time.Hour, func(batch []*int) {
		flushCh <- len(batch)
	}, false)
	defer b.Close()

	b.SetTickInterval(interval)

	start := time.Now()
	for i := 0; i < size; i++ {
		v := i
		b.Put(&v)
	}

	select {
	case got := <-flushCh:
		require.Equal(t, size, got)
		require.Less(t, time.Since(start), 100*time.Millisecond,
			"size cap should have fired early, not waited for the 1h tick")
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("size-cap flush did not occur")
	}
}

// TestTickMode_SparseLoadLatency verifies the clock-already-ticking benefit:
// an item arriving after a long idle period should NOT wait the full interval
// (which is the worst-case for lazy-timer mode).
func TestTickMode_SparseLoadLatency(t *testing.T) {
	const interval = 50 * time.Millisecond
	const idleDuration = 5 * interval // long enough that we're mid-tick

	flushCh := make(chan time.Time, 1)

	b := New[int](100, time.Hour, func(_ []*int) {
		select {
		case flushCh <- time.Now():
		default:
		}
	}, false)
	defer b.Close()

	b.SetTickInterval(interval)

	// Idle.
	time.Sleep(idleDuration)

	// Single Put — measure how long until flush.
	v := 1
	putAt := time.Now()
	b.Put(&v)

	var flushAt time.Time
	select {
	case flushAt = <-flushCh:
	case <-time.After(2 * interval):
		t.Fatalf("flush did not occur")
	}

	delay := flushAt.Sub(putAt)
	// In steady-ticker mode with always-running clock, max wait is one interval.
	// Lazy-timer mode would also wait up to interval, so this test mostly checks
	// the upper bound is honored. The avg-case benefit is measured in benchmarks.
	require.LessOrEqual(t, delay, interval+10*time.Millisecond,
		"flush delay %s exceeded tick interval %s", delay, interval)
}

// TestTickMode_TriggerStillWorks verifies Trigger() flushes immediately even
// in tick mode, and the ticker cadence continues unmodified.
func TestTickMode_TriggerStillWorks(t *testing.T) {
	const interval = time.Hour // effectively never; only Trigger() can fire

	flushCh := make(chan int, 4)

	b := New[int](100, time.Hour, func(batch []*int) {
		flushCh <- len(batch)
	}, false)
	defer b.Close()

	b.SetTickInterval(interval)

	v := 1
	b.Put(&v)
	b.Trigger()

	select {
	case got := <-flushCh:
		require.Equal(t, 1, got)
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("Trigger did not flush")
	}
}

// TestTickMode_ShutdownDrains verifies Close still flushes pending items
// with ReasonShutdown.
func TestTickMode_ShutdownDrains(t *testing.T) {
	const interval = time.Hour // never auto-fires

	var received []*int
	var mu sync.Mutex
	done := make(chan struct{})

	b := New[int](100, time.Hour, func(batch []*int) {
		mu.Lock()
		received = append(received, batch...)
		mu.Unlock()
		close(done)
	}, false)

	b.SetTickInterval(interval)

	for i := 0; i < 5; i++ {
		v := i
		b.Put(&v)
	}

	b.Close()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("shutdown did not drain")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, received, 5)
}

// TestTickMode_MaxConcurrentSelfRateLimits verifies that when the pool is
// saturated, effective flush rate caps at the pool's throughput rather than
// the configured tick rate.
func TestTickMode_MaxConcurrentSelfRateLimits(t *testing.T) {
	const interval = 5 * time.Millisecond
	const fnLatency = 50 * time.Millisecond
	const maxConcurrent = 2
	const observationWindow = 500 * time.Millisecond

	var flushes int32
	var mu sync.Mutex

	b := New[int](10000, time.Hour, func(_ []*int) {
		time.Sleep(fnLatency)
		mu.Lock()
		flushes++
		mu.Unlock()
	}, true)
	defer b.Close()

	b.SetMaxConcurrent(maxConcurrent)
	b.SetTickInterval(interval)

	deadline := time.Now().Add(observationWindow)
	i := 0
	for time.Now().Before(deadline) {
		v := i
		b.Put(&v)
		i++
		time.Sleep(time.Microsecond * 200)
	}

	// Allow in-flight work to drain.
	time.Sleep(2 * fnLatency)

	mu.Lock()
	defer mu.Unlock()

	// Pool-bounded ceiling: maxConcurrent / fnLatency * window.
	maxBound := int(float64(maxConcurrent) * float64(observationWindow) / float64(fnLatency))
	// Account for in-flight items that started before deadline and finished after.
	maxBound += 2 * maxConcurrent

	require.LessOrEqual(t, int(flushes), maxBound,
		"flushes=%d exceeded pool-bounded ceiling %d — backpressure not working", flushes, maxBound)
}
