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
