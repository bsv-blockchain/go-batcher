package completion

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

// BenchmarkOldPerItemProtocol simulates the pre-Group completion protocol
// this package replaces: one goroutine, one buffered channel, and one timer
// per item, fanned out via an errgroup and joined with g.Wait.
func BenchmarkOldPerItemProtocol(b *testing.B) {
	const itemsPerCall = 8

	for b.Loop() {
		var g errgroup.Group

		for i := 0; i < itemsPerCall; i++ {
			g.Go(func() error {
				errCh := make(chan error, 1)
				timer := time.NewTimer(time.Second)
				defer timer.Stop()

				// Simulate the dispatcher completing the item.
				go func() { errCh <- nil }()

				select {
				case err := <-errCh:
					return err
				case <-timer.C:
					return errors.New("timeout")
				}
			})
		}

		_ = g.Wait()
	}
}

// BenchmarkGroupCompletionProtocol measures the same itemsPerCall items
// completed via one shared Group and a single Wait — no per-item goroutine,
// channel, or timer.
func BenchmarkGroupCompletionProtocol(b *testing.B) {
	const itemsPerCall = 8

	for b.Loop() {
		g := NewGroup(itemsPerCall)

		// Simulate the dispatcher completing every item from one goroutine,
		// as a batch dispatcher does after a single BatchOperate call.
		go func() {
			for i := 0; i < itemsPerCall; i++ {
				g.Done()
			}
		}()

		_ = g.Wait(context.Background(), time.Second)
	}
}

// BenchmarkGroupCompletionProtocol_ConcurrentDone measures the case where
// each item is completed from its own goroutine (worst case for the Group —
// still just one atomic decrement each, no channel/timer).
func BenchmarkGroupCompletionProtocol_ConcurrentDone(b *testing.B) {
	const itemsPerCall = 8

	for b.Loop() {
		g := NewGroup(itemsPerCall)

		var wg sync.WaitGroup
		for i := 0; i < itemsPerCall; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				g.Done()
			}()
		}

		_ = g.Wait(context.Background(), time.Second)
		wg.Wait()
	}
}
