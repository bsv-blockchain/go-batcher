// Package main demonstrates practical usage patterns for the go-batcher library.
//
// This example showcases a real-world scenario where we batch user events for
// efficient processing and storage. The example simulates collecting user
// activity events and periodically saving them to a database in batches,
// which is much more efficient than individual inserts.
package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bsv-blockchain/go-batcher"
)

// UserEvent represents a user activity event in our system
type UserEvent struct {
	UserID    string
	EventType string
	Timestamp time.Time
	Metadata  map[string]string
}

// EventProcessor simulates a database or analytics service
type EventProcessor struct {
	mu              sync.Mutex
	processedCount  int64
	processedEvents []*UserEvent
}

// ProcessBatch simulates batch processing of events (e.g., database bulk insert)
func (ep *EventProcessor) ProcessBatch(events []*UserEvent) {
	ep.mu.Lock()
	defer ep.mu.Unlock()

	// Simulate processing time
	processingTime := time.Duration(len(events)*10) * time.Millisecond
	time.Sleep(processingTime)

	// Track processed events
	ep.processedEvents = append(ep.processedEvents, events...)
	atomic.AddInt64(&ep.processedCount, int64(len(events)))

	log.Printf("📦 Processed batch of %d events (total: %d)", len(events), atomic.LoadInt64(&ep.processedCount))
}

// cryptoRandInt generates a secure random integer up to maxValue
func cryptoRandInt(maxValue int) int {
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(maxValue)))
	return int(n.Int64())
}

// generateRandomEvent creates a random user event for demonstration
func generateRandomEvent() *UserEvent {
	eventTypes := []string{"page_view", "button_click", "form_submit", "api_call", "login"}
	userIDs := []string{"user001", "user002", "user003", "user004", "user005"}

	return &UserEvent{
		UserID:    userIDs[cryptoRandInt(len(userIDs))],
		EventType: eventTypes[cryptoRandInt(len(eventTypes))],
		Timestamp: time.Now(),
		Metadata: map[string]string{
			"session_id": fmt.Sprintf("session_%d", cryptoRandInt(1000)),
			"ip_address": fmt.Sprintf("192.168.1.%d", cryptoRandInt(255)),
		},
	}
}

// startEventGenerator creates an event generator that runs at the specified interval
func startEventGenerator(ctx context.Context, wg *sync.WaitGroup, batcher *batcher.Batcher[UserEvent], interval time.Duration, label string) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				event := generateRandomEvent()
				batcher.Put(event)
				log.Printf("➡️  %s event: %s by %s", label, event.EventType, event.UserID)
			case <-ctx.Done():
				return
			}
		}
	}()
}

// startBurstGenerator creates a burst event generator
func startBurstGenerator(ctx context.Context, wg *sync.WaitGroup, batcher *batcher.Batcher[UserEvent]) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Generate a burst of 10-20 events
				burstSize := 10 + cryptoRandInt(11)
				log.Printf("💥 Generating burst of %d events", burstSize)

				for i := 0; i < burstSize; i++ {
					event := generateRandomEvent()
					batcher.Put(event)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

// startStatsMonitor creates a statistics monitor
func startStatsMonitor(ctx context.Context, wg *sync.WaitGroup, processor *EventProcessor) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				count := atomic.LoadInt64(&processor.processedCount)
				log.Printf("📊 Statistics: Total events processed: %d", count)
			case <-ctx.Done():
				return
			}
		}
	}()
}

func main() {
	log.Println("🚀 Starting User Event Batching Example")

	// Create our event processor
	processor := &EventProcessor{}

	// Create a batcher with:
	// - Batch size of 50 events
	// - 2-second timeout for incomplete batches
	// - Background processing enabled
	eventBatcher := batcher.New[UserEvent](
		50,                     // Process when we have 50 events
		2*time.Second,          // Or after 2 seconds
		processor.ProcessBatch, // Our batch processing function
		true,                   // Process batches in the background
	)

	// Set up a graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Simulate event generation from multiple sources
	var wg sync.WaitGroup

	// Start event generators
	startEventGenerator(ctx, &wg, eventBatcher, 100*time.Millisecond, "High-freq")
	startEventGenerator(ctx, &wg, eventBatcher, 500*time.Millisecond, "Low-freq")
	startBurstGenerator(ctx, &wg, eventBatcher)
	startStatsMonitor(ctx, &wg, processor)

	// Wait for shutdown signal
	log.Println("✅ Event batching system is running. Press Ctrl+C to stop...")
	<-sigChan

	log.Println("🛑 Shutting down gracefully...")

	// Cancel context to stop event generation
	cancel()

	// Give generators time to stop
	time.Sleep(100 * time.Millisecond)

	// Trigger final batch processing for any remaining events
	log.Println("🔄 Processing remaining events...")
	eventBatcher.Trigger()

	// Wait a bit for the final batch to process
	time.Sleep(1 * time.Second)

	// Wait for all goroutines to finish
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("✅ All goroutines finished")
	case <-time.After(5 * time.Second):
		log.Println("⚠️  Timeout waiting for goroutines")
	}

	// Final statistics
	finalCount := atomic.LoadInt64(&processor.processedCount)
	log.Printf("📈 Final Report: Processed %d total events", finalCount)
	log.Println("👋 Goodbye!")
}
