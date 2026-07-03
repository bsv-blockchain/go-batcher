package completion

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewGroup_ZeroItemsStartsClosed(t *testing.T) {
	g := NewGroup(0)

	select {
	case <-g.C():
	default:
		t.Fatal("expected done channel to start closed for n=0")
	}

	require.NoError(t, g.Wait(context.Background(), time.Second))
}

func TestNewGroup_NegativeCountPanics(t *testing.T) {
	require.Panics(t, func() {
		NewGroup(-1)
	})
}

func TestGroup_DoneClosesAfterAllComplete(t *testing.T) {
	g := NewGroup(3)

	select {
	case <-g.C():
		t.Fatal("done should not be closed yet")
	default:
	}

	g.Done()
	g.Done()

	select {
	case <-g.C():
		t.Fatal("done should not be closed until all items complete")
	default:
	}

	g.Done()

	select {
	case <-g.C():
	default:
		t.Fatal("done should be closed after all items complete")
	}
}

func TestGroup_DoneCalledTooManyTimesPanics(t *testing.T) {
	g := NewGroup(1)
	g.Done()

	require.Panics(t, func() {
		g.Done()
	})
}

func TestGroup_WaitReturnsNilOnCompletion(t *testing.T) {
	g := NewGroup(1)

	go func() {
		time.Sleep(10 * time.Millisecond)
		g.Done()
	}()

	err := g.Wait(context.Background(), time.Second)
	require.NoError(t, err)
}

func TestGroup_WaitReturnsErrorOnContextCancel(t *testing.T) {
	g := NewGroup(1)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	err := g.Wait(ctx, time.Second)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)

	// Item never completed; a late Done must not panic or block.
	g.Done()
}

func TestGroup_WaitReturnsErrorOnTimeout(t *testing.T) {
	g := NewGroup(1)

	err := g.Wait(context.Background(), 10*time.Millisecond)
	require.Error(t, err)

	// Item never completed; a late Done must not panic or block.
	g.Done()
}

func TestGroup_WaitWithZeroTimeoutBlocksUntilDone(t *testing.T) {
	g := NewGroup(1)

	go func() {
		time.Sleep(10 * time.Millisecond)
		g.Done()
	}()

	// timeout <= 0 means no timer arm; only ctx/done can unblock this.
	err := g.Wait(context.Background(), 0)
	require.NoError(t, err)
}

func TestGroup_WaitWithZeroTimeoutRespectsContext(t *testing.T) {
	g := NewGroup(1)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	err := g.Wait(ctx, 0)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)

	g.Done()
}

func TestGroup_ConcurrentDoneIsRace_Free(t *testing.T) {
	const n = 1000
	g := NewGroup(n)

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.Done()
		}()
	}
	wg.Wait()

	err := g.Wait(context.Background(), time.Second)
	require.NoError(t, err)
}

func TestGroup_ManyGroupsManyItemsNoRace(t *testing.T) {
	const groups = 50
	const itemsPerGroup = 200

	var wg sync.WaitGroup
	for i := 0; i < groups; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g := NewGroup(itemsPerGroup)
			var innerWg sync.WaitGroup
			for j := 0; j < itemsPerGroup; j++ {
				innerWg.Add(1)
				go func() {
					defer innerWg.Done()
					g.Done()
				}()
			}
			innerWg.Wait()
			require.NoError(t, g.Wait(context.Background(), time.Second))
		}()
	}
	wg.Wait()
}
