// Package main demonstrates practical usage patterns for the go-batcher library.
//
// This example showcases a real-world scenario where we batch user events for
// efficient processing and storage. The example simulates collecting user
// activity events and periodically saving them to a database in batches,
// which is much more efficient than individual inserts.
//
// Key concepts demonstrated:
// - Creating a batcher with custom batch size and timeout settings
// - Processing items in batches using a callback function
// - Generating events from multiple sources at different rates
// - Handling burst traffic patterns
// - Graceful shutdown with processing of remaining items
// - Monitoring and statistics collection
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

// UserEvent represents a user activity event in our system.
// In a real application, this might represent:
// - User interactions (clicks, page views, form submissions)
// - API calls or system events
// - Analytics data points
// - Any time-series data that benefits from batch processing
type UserEvent struct {
	UserID    string            // Unique identifier for the user
	EventType string            // Type of event (e.g., "page_view", "button_click")
	Timestamp time.Time         // When the event occurred
	Metadata  map[string]string // Additional event-specific data
}

// EventProcessor simulates a database or analytics service.
// In a real application, this would be your actual data store or processing system.
type EventProcessor struct {
	mu              sync.Mutex   // Protects concurrent access to processedEvents
	processedCount  int64        // Total count of processed events (atomic)
	processedEvents []*UserEvent // Storage for demonstration purposes
}

// ProcessBatch is the callback function that the batcher calls with accumulated events.
// This is where you would implement your actual batch processing logic:
// - Database bulk inserts
// - API batch calls
// - File writes
// - Analytics pipeline processing
func (ep *EventProcessor) ProcessBatch(events []*UserEvent) {
	ep.mu.Lock()
	defer ep.mu.Unlock()

	// Simulate processing time (e.g., network latency, database write)
	// In reality, batch processing is usually much faster per-item than individual operations
	processingTime := time.Duration(len(events)*10) * time.Millisecond
	time.Sleep(processingTime)

	// Track processed events (in production, this would be your actual storage operation)
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

// startEventGenerator simulates a steady stream of events at a fixed interval.
// This represents typical application behavior where events arrive at a predictable rate.
// Examples in production:
// - Web server access logs
// - Periodic sensor readings
// - Regular API polling results
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
				// Put the event into the batcher - it will accumulate until batch conditions are met
				batcher.Put(event)
				log.Printf("➡️  %s event: %s by %s", label, event.EventType, event.UserID)
			case <-ctx.Done():
				return
			}
		}
	}()
}

// startBurstGenerator simulates burst traffic patterns.
// This represents scenarios where many events arrive simultaneously:
// - Flash sales or marketing campaigns
// - System startup where many components initialize at once
// - Scheduled batch jobs that generate many events
// - Traffic spikes from viral content
func startBurstGenerator(ctx context.Context, wg *sync.WaitGroup, batcher *batcher.Batcher[UserEvent]) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Generate a burst of 10-20 events all at once
				burstSize := 10 + cryptoRandInt(11)
				log.Printf("💥 Generating burst of %d events", burstSize)

				// The batcher handles this burst efficiently, avoiding individual processing
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

	// Create our event processor that will handle batches of events
	processor := &EventProcessor{}

	// Create a batcher with specific configuration:
	// - Batch size of 50: Accumulate up to 50 events before processing
	// - 2-second timeout: Process incomplete batches after 2 seconds to avoid delays
	// - Background processing: Handle batches asynchronously for better throughput
	//
	// These settings balance between:
	// - Efficiency (larger batches = fewer operations)
	// - Latency (timeout ensures events don't wait too long)
	// - Throughput (background processing doesn't block new events)
	eventBatcher := batcher.New[UserEvent](
		50,                     // Process when we have 50 events
		2*time.Second,          // Or after 2 seconds (whichever comes first)
		processor.ProcessBatch, // Our batch processing function
		true,                   // Process batches in the background (non-blocking)
	)

	// Set up a graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Simulate event generation from multiple sources
	// This demonstrates how the batcher can handle events from various sources
	// with different patterns, all feeding into the same batch processor
	var wg sync.WaitGroup

	// Start different types of event generators:
	// 1. High-frequency: Simulates active user sessions or real-time monitoring
	startEventGenerator(ctx, &wg, eventBatcher, 100*time.Millisecond, "High-freq")

	// 2. Low-frequency: Simulates periodic background tasks or less active components
	startEventGenerator(ctx, &wg, eventBatcher, 500*time.Millisecond, "Low-freq")

	// 3. Burst generator: Simulates traffic spikes or batch job outputs
	startBurstGenerator(ctx, &wg, eventBatcher)

	// 4. Statistics monitor: Shows the effectiveness of batching
	startStatsMonitor(ctx, &wg, processor)

	// Wait for shutdown signal
	log.Println("✅ Event batching system is running. Press Ctrl+C to stop...")
	<-sigChan

	log.Println("🛑 Shutting down gracefully...")

	// Step 1: Cancel context to signal all event generators to stop
	cancel()

	// Step 2: Give generators time to stop cleanly
	time.Sleep(100 * time.Millisecond)

	// Step 3: Process any remaining events that haven't reached batch size
	// This is crucial for data integrity - ensures no events are lost on shutdown
	log.Println("🔄 Processing remaining events...")
	eventBatcher.Trigger() // Forces immediate processing of partial batch

	// Step 4: Wait for the final batch to be processed
	// In production, you might want to make this configurable based on your processing time
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
