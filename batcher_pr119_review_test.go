package batcher

import (
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// runShutdownRaceScenario drives the shutdown-race exit path shared by the two
// tests below: it primes the full SetMaxConcurrent pool with a batch that
// blocks in fn, enqueues an oversized PutBatch that blocks mid-split on the
// busy pool, then calls Close() concurrently so flushBatch takes its
// shutdown-race (exit=true) branch. The batcher is fully created AND closed
// within this call. onBatch is invoked for every non-prime batch handed to fn.
// The prime batch carries negative values; group items are 0..groupN-1.
func runShutdownRaceScenario(t *testing.T, size, poolN, groupN int, onBatch func(batch []*int)) {
	t.Helper()

	primeStarted := make(chan struct{})
	releasePrime := make(chan struct{})
	var primeOnce sync.Once

	b := New[int](size, time.Hour, func(batch []*int) {
		prime := false
		for _, v := range batch {
			if *v < 0 {
				prime = true
			}
		}
		if prime {
			primeOnce.Do(func() { close(primeStarted) })
			<-releasePrime
			return
		}
		onBatch(batch)
	}, true)
	b.SetMaxConcurrent(poolN)

	// Prime a full batch so every pool worker is busy in fn.
	for i := 0; i < size; i++ {
		v := -(i + 1)
		b.Put(&v)
	}
	<-primeStarted

	// Oversized group: the worker pulls it, appends the first chunk, and blocks
	// sending that chunk to the busy pool.
	items := make([]*int, groupN)
	for i := range items {
		v := i
		items[i] = &v
	}
	b.PutBatch(items)
	time.Sleep(100 * time.Millisecond) // let the worker reach the blocked send

	closed := make(chan struct{})
	go func() {
		b.Close()
		close(closed)
	}()
	time.Sleep(50 * time.Millisecond) // let Close close done so flushBatch exits
	close(releasePrime)               // let the prime finish so shutdown can complete

	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close() did not return")
	}
}

// TestPutBatch_ShutdownRaceDeliversAllItemsAndHonorsSize covers the P0 where a
// shutdown racing a full SetMaxConcurrent pool mid-split caused appendItems to
// drop the unconsumed tail of an oversized PutBatch (Close() then returned
// "success" having silently lost items), and the shutdown drain handed fn a
// batch larger than the configured size. Every accepted item must still reach
// fn, and no batch may exceed the configured size on any path.
func TestPutBatch_ShutdownRaceDeliversAllItemsAndHonorsSize(t *testing.T) {
	const size = 2

	var mu sync.Mutex
	var delivered []int
	maxBatch := 0

	runShutdownRaceScenario(t, size, 1, 6, func(batch []*int) {
		mu.Lock()
		if len(batch) > maxBatch {
			maxBatch = len(batch)
		}
		for _, v := range batch {
			delivered = append(delivered, *v)
		}
		mu.Unlock()
	})

	mu.Lock()
	defer mu.Unlock()
	sort.Ints(delivered)
	require.Equal(t, []int{0, 1, 2, 3, 4, 5}, delivered,
		"every PutBatch item must be delivered even when shutdown races a full pool mid-split")
	require.LessOrEqual(t, maxBatch, size,
		"no batch may exceed the configured size, including on the shutdown drain path")
}

// TestSetMaxConcurrent_NoWorkerLeakOnShutdownRace covers the P0 goroutine leak:
// when flushBatch took the exit=true shutdown-race branch the worker returned
// without closing workCh, so the persistent pool goroutines blocked forever on
// range workCh. After Close returns, goroutine count must settle back.
func TestSetMaxConcurrent_NoWorkerLeakOnShutdownRace(t *testing.T) {
	before := runtime.NumGoroutine()

	runShutdownRaceScenario(t, 2, 1, 6, func(_ []*int) {})

	// Poll with a plain loop rather than require.Eventually: Eventually runs its
	// condition on a testify-spawned goroutine, which would itself inflate the
	// live goroutine count and defeat the comparison.
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	require.LessOrEqual(t, runtime.NumGoroutine(), before,
		"persistent pool workers must exit after Close (workCh closed on every worker exit)")
}

// TestGreedyAccumulate_DoesNotStarveTriggerUnderLoad covers the P0 where the
// greedy-accumulate inner drain loop never returned to the outer select while
// the channel stayed non-empty, starving Trigger()/timers/Close() for seconds
// under sustained producer load.
func TestGreedyAccumulate_DoesNotStarveTriggerUnderLoad(t *testing.T) {
	// size=1 plus a per-flush cost slower than the producers keeps the channel
	// continuously non-empty, so the buggy inner loop never hits its empty
	// (default) arm and never returns to the outer select.
	b := New[int](1, time.Hour, func(_ []*int) {
		time.Sleep(time.Millisecond)
	}, false, WithGreedyAccumulate(true))

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				b.Put(&v)
			}
		}()
	}
	time.Sleep(100 * time.Millisecond) // ramp the load

	triggered := make(chan struct{})
	go func() {
		b.Trigger()
		close(triggered)
	}()

	select {
	case <-triggered:
	case <-time.After(2 * time.Second):
		close(stop)
		wg.Wait()
		b.Close()
		t.Fatal("Trigger() starved by the greedy-accumulate inner loop under sustained load")
	}

	close(stop)
	wg.Wait()
	b.Close()
}

// TestDrainMode_DoesNotStarveTriggerUnderLoad is the drain-mode counterpart of
// the greedy starvation test: the drain inner loop must also yield to the outer
// select promptly under sustained load.
func TestDrainMode_DoesNotStarveTriggerUnderLoad(t *testing.T) {
	b := New[int](1, time.Hour, func(_ []*int) {
		time.Sleep(time.Millisecond)
	}, false)
	b.SetDrainMode(true)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				b.Put(&v)
			}
		}()
	}
	time.Sleep(100 * time.Millisecond)

	triggered := make(chan struct{})
	go func() {
		b.Trigger()
		close(triggered)
	}()

	select {
	case <-triggered:
	case <-time.After(2 * time.Second):
		close(stop)
		wg.Wait()
		b.Close()
		t.Fatal("Trigger() starved by the drain-mode inner loop under sustained load")
	}

	close(stop)
	wg.Wait()
	b.Close()
}

// TestPutBatch_SnapshotsCallerSlice covers the P1 where PutBatch retained the
// caller's slice and read it asynchronously on the worker goroutine. A caller
// that reuses the slice after the call must not corrupt the enqueued batch.
//
// The worker is held busy in fn until after the caller overwrites the slice, so
// the read/write ordering that exposes the bug is deterministic.
func TestPutBatch_SnapshotsCallerSlice(t *testing.T) {
	const size = 10

	gate := make(chan struct{})
	var gateOnce sync.Once
	primeStarted := make(chan struct{})

	var mu sync.Mutex
	var got []int

	b := New[int](size, time.Hour, func(batch []*int) {
		// First (prime) call blocks so the worker cannot process the PutBatch
		// envelope until the caller has overwritten the slice.
		if len(batch) == size {
			gateOnce.Do(func() { close(primeStarted) })
			<-gate
			return
		}
		mu.Lock()
		for _, v := range batch {
			got = append(got, *v)
		}
		mu.Unlock()
	}, false)

	// Prime a full batch to block the worker in fn.
	for i := 0; i < size; i++ {
		v := 1000 + i
		b.Put(&v)
	}
	<-primeStarted

	// Enqueue a sub-size group, then immediately reuse (overwrite) the slice.
	items := make([]*int, 5)
	for i := range items {
		v := i
		items[i] = &v
	}
	b.PutBatch(items)
	for i := range items {
		v := 9000 + i
		items[i] = &v // caller reuses its buffer right after the call
	}

	close(gate) // release the worker; it now processes the PutBatch envelope
	time.Sleep(50 * time.Millisecond)
	b.Trigger()
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	sort.Ints(got)
	require.Equal(t, []int{0, 1, 2, 3, 4}, got,
		"PutBatch must snapshot the caller's slice; reusing it after the call must not corrupt the batch")

	b.Close()
}

// TestNew_PanicsOnNonPositiveSize covers the P2 where size<=0 livelocked
// appendItems and hung Close(). Construction must reject it outright.
func TestNew_PanicsOnNonPositiveSize(t *testing.T) {
	require.Panics(t, func() {
		New[int](0, time.Hour, func(_ []*int) {}, false)
	}, "New with size 0 must panic rather than build a batcher that livelocks")
	require.Panics(t, func() {
		New[int](-1, time.Hour, func(_ []*int) {}, false)
	}, "New with negative size must panic")
}

// TestNewWithPool_PanicsOnNonPositiveSize is the pooled-constructor counterpart.
func TestNewWithPool_PanicsOnNonPositiveSize(t *testing.T) {
	require.Panics(t, func() {
		NewWithPool[int](0, time.Hour, func(_ []*int) {}, false)
	}, "NewWithPool with size 0 must panic")
	require.Panics(t, func() {
		NewWithPool[int](-1, time.Hour, func(_ []*int) {}, false)
	}, "NewWithPool with negative size must panic")
}

// TestModeSetters_MutualExclusionUnderConcurrency covers the P2 TOCTOU: drain
// mode and greedy accumulate are mutually exclusive, and concurrent setters
// must never leave both enabled.
func TestModeSetters_MutualExclusionUnderConcurrency(t *testing.T) {
	for iter := 0; iter < 500; iter++ {
		b := New[int](10, time.Hour, func(_ []*int) {}, false)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); b.SetDrainMode(true) }()
		go func() { defer wg.Done(); b.SetGreedyAccumulate(true) }()
		wg.Wait()

		require.False(t, b.drainMode.Load() && b.greedyAccumulate.Load(),
			"drain mode and greedy accumulate must never both be enabled (iteration %d)", iter)

		b.Close()
	}
}
