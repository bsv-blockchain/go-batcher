package batcher

import (
	"sync"
	"sync/atomic"
	"time"

	txmap "github.com/bsv-blockchain/go-tx-map"
)

// TimePartitionedMap is a time-based data structure for efficient expiration of entries.
//
// This map implementation divides time into fixed-size buckets and stores items in the
// appropriate bucket based on insertion time. This design enables efficient bulk deletion
// of expired entries by dropping entire buckets rather than checking individual items.
//
// The map is particularly useful for deduplication scenarios where items need to be
// remembered for a specific time window and then automatically forgotten.
//
// Type parameters:
// - K: The key type (must be comparable for map operations)
// - V: The value type (can be any type)
//
// Fields:
// - buckets: Thread-safe map of bucket IDs to bucket contents
// - bucketSize: Duration of each time bucket (e.g., 1 second, 1 minute)
// - maxBuckets: Maximum number of buckets to retain (defines retention window)
// - oldestBucket: Atomic counter tracking the oldest bucket ID
// - newestBucket: Atomic counter tracking the newest bucket ID
// - itemCount: Atomic counter of total items across all buckets
// - currentBucketID: Cached current bucket ID, updated periodically
// - zero: Zero value for type V (used for returns when key not found)
// - bucketsMu: Mutex protecting bucket creation and deletion operations
//
// Notes:
// - Thread-safe for concurrent access
// - Automatic cleanup of expired buckets via background goroutine
// - Global deduplication across all buckets
// - Efficient O(1) average case for Set/Get operations
type TimePartitionedMap[K comparable, V any] struct {
	buckets         *txmap.SyncedMap[int64, *txmap.SyncedMap[K, V]] // Map of timestamp buckets to key-value maps
	bucketSize      time.Duration                                   // Size of each time bucket (e.g., 1 minute)
	maxBuckets      int                                             // Maximum number of buckets to keep
	oldestBucket    atomic.Int64                                    // Timestamp of the oldest bucket
	newestBucket    atomic.Int64                                    // Timestamp of the newest bucket
	itemCount       atomic.Int64                                    // Total number of items across all buckets
	currentBucketID atomic.Int64                                    // Current bucket ID, updated periodically
	zero            V                                               // Zero value for V
	bucketsMu       sync.Mutex                                      // Mutex for buckets
}

// NewTimePartitionedMap creates a new time-partitioned map with the specified configuration.
//
// This function initializes a TimePartitionedMap that automatically manages time-based
// expiration of entries. It starts two background goroutines: one for updating the
// current bucket ID and another for cleaning-up expired buckets.
//
// Parameters:
//   - bucketSize: Duration of each time bucket. Smaller sizes provide finer granularity
//     but use more memory. Common values: 1 s, 10s, 1 m
//   - maxBuckets: Maximum number of buckets to retain. Total retention time = bucketSize * maxBuckets
//
// Returns:
// - *TimePartitionedMap[K, V]: A configured and running time-partitioned map
//
// Side Effects:
// - Starts a goroutine to update current bucket ID (runs every bucketSize/10 or 100ms minimum)
// - Starts a goroutine to clean up old buckets (runs every bucketSize/2)
// - Both goroutines run indefinitely
//
// Notes:
// - The update interval is set to 1/10th of bucket size for accuracy (minimum 100ms)
// - Cleanup runs at half the bucket size interval to ensure timely removal
// - Initial bucket ID is calculated from current time
// - The map starts empty with no buckets allocated
func NewTimePartitionedMap[K comparable, V any](bucketSize time.Duration, maxBuckets int) *TimePartitionedMap[K, V] {
	m := &TimePartitionedMap[K, V]{
		buckets:         txmap.NewSyncedMap[int64, *txmap.SyncedMap[K, V]](),
		bucketSize:      bucketSize,
		maxBuckets:      maxBuckets,
		oldestBucket:    atomic.Int64{},
		newestBucket:    atomic.Int64{},
		itemCount:       atomic.Int64{},
		currentBucketID: atomic.Int64{},
	}

	// Initialize the current bucket ID
	initialBucketID := time.Now().UnixNano() / int64(m.bucketSize)
	m.currentBucketID.Store(initialBucketID)

	// Start a goroutine to update the current bucket ID periodically
	// Use a ticker with a period that's a fraction of the bucket size to ensure accuracy
	updateInterval := m.bucketSize / 10
	if updateInterval < time.Millisecond*100 {
		updateInterval = time.Millisecond * 100 // Minimum update interval of 100 ms
	}

	go func() {
		ticker := time.NewTicker(updateInterval)
		defer ticker.Stop()

		for range ticker.C {
			m.currentBucketID.Store(time.Now().UnixNano() / int64(m.bucketSize))
		}
	}()

	go func() {
		ticker := time.NewTicker(m.bucketSize / 2)
		defer ticker.Stop()

		for range ticker.C {
			m.cleanupOldBuckets()
		}
	}()

	return m
}

// Get retrieves a value from the map by searching all active buckets.
//
// This method performs a linear search across all buckets to find the specified key.
// While this approach has O(n) complexity where n is the number of buckets, it ensures
// global deduplication and is acceptable given the typically small number of buckets.
//
// Parameters:
// - key: The key to search for across all buckets
//
// Returns:
// - V: The value associated with the key (or zero value if not found)
// - bool: True if the key was found, false otherwise
//
// Side Effects:
// - None (read-only operation)
//
// Notes:
// - Searches buckets in arbitrary order (map iteration order)
// - Returns the first matching key found (should be unique due to deduplication)
// - Thread-safe for concurrent access
// - Performance degrades linearly with number of buckets
func (m *TimePartitionedMap[K, V]) Get(key K) (V, bool) {
	for bucketID := range m.buckets.Range() {
		if bucket, exists := m.buckets.Get(bucketID); exists {
			if value, found := bucket.Get(key); found {
				return value, true
			}
		}
	}

	return m.zero, false
}

// Set adds a key-value pair to the map with global deduplication.
//
// This method first checks if the key exists anywhere in the map (global deduplication)
// and only adds it if it's not already present. The key is added to the current time
// bucket, creating the bucket if necessary.
//
// This function performs the following steps:
// - Checks all buckets for existing key (deduplication)
// - Loads the current bucket ID from cached value
// - Acquires lock to safely create/access bucket
// - Creates new bucket if needed and updates tracking
// - Adds the key-value pair to the appropriate bucket
// - Updates the global item count
//
// Parameters:
// - key: The key to add (must not already exist in any bucket)
// - value: The value to associate with the key
//
// Returns:
// - bool: True if the key was added, false if it already existed (duplicate)
//
// Side Effects:
// - May create a new bucket if current bucket doesn't exist
// - Increments the global item count
// - Updates oldest/newest bucket trackers
//
// Notes:
// - Global deduplication check has O(n*m) complexity (n buckets, m items per bucket)
// - Bucket creation is protected by mutex to prevent races
// - The entire add operation is atomic to prevent bucket deletion races
// - Returns false for duplicates without modifying the map
func (m *TimePartitionedMap[K, V]) Set(key K, value V) bool {
	// Global deduplication check: if key already exists in any bucket, do not proceed.
	// This check iterating over all buckets and can be resource-intensive.
	if _, exists := m.Get(key); exists {
		return false
	}

	var (
		bucket *txmap.SyncedMap[K, V]
		exists bool
	)

	bucketID := m.currentBucketID.Load()

	m.bucketsMu.Lock() // Lock before accessing or modifying m.buckets structure

	// Initialize bucket if it doesn't exist for the current bucketID
	if bucket, exists = m.buckets.Get(bucketID); !exists {
		bucket = txmap.NewSyncedMap[K, V]()
		m.buckets.Set(bucketID, bucket)

		// Update newest/oldest bucket trackers since a new bucket was added
		if m.newestBucket.Load() < bucketID {
			m.newestBucket.Store(bucketID)
		}

		if m.oldestBucket.Load() == 0 || m.oldestBucket.Load() > bucketID {
			m.oldestBucket.Store(bucketID)
		}
	}

	// Add item to the determined bucket and update total item count.
	// This is now done under the m.bucketsMu lock to prevent a race condition
	// where the bucket might be removed by cleanupOldBuckets between being
	// retrieved/created and the item being added to it.
	bucket.Set(key, value)
	m.itemCount.Add(1)

	m.bucketsMu.Unlock() // Unlock after all modifications to m.buckets and the specific bucket's content.

	return true
}

// Delete removes a key from the map across all buckets.
//
// This method searches all buckets for the specified key and removes it if found.
// If the deletion leaves a bucket empty, the bucket itself is also removed to
// conserve memory.
//
// This function performs the following steps:
// - Iterates through all buckets searching for the key
// - Removes the key from the bucket where it's found
// - Decrements the global item count
// - Removes empty buckets after deletion
// - Updates oldest bucket tracker if necessary
//
// Parameters:
// - key: The key to remove from the map
//
// Returns:
// - bool: True if the key was found and deleted, false if not found
//
// Side Effects:
// - Decrements the global item count if key is deleted
// - May remove empty buckets
// - May trigger recalculation of oldest bucket
//
// Notes:
// - Only deletes the first occurrence found (should be unique)
// - Bucket removal is important for memory efficiency
// - Thread-safe but not atomic with Set operations
// - Breaks after first deletion since keys are unique
func (m *TimePartitionedMap[K, V]) Delete(key K) bool {
	deleted := false

	// Check all buckets
	bucketMap := m.buckets.Range()
	for bucketID, bucket := range bucketMap {
		if _, found := bucket.Get(key); found {
			bucket.Delete(key)

			deleted = true

			m.itemCount.Add(-1)

			// If the bucket is empty, remove it
			if bucket.Length() == 0 {
				m.buckets.Delete(bucketID)
				// Recalculate the oldest bucket if needed
				if bucketID == m.oldestBucket.Load() {
					m.recalculateOldestBucket()
				}
			}

			break
		}
	}

	return deleted
}

// cleanupOldBuckets removes buckets that have exceeded the retention window.
//
// This method is called periodically by a background goroutine to remove buckets
// that are older than the configured retention period (bucketSize * maxBuckets).
// It ensures memory doesn't grow unbounded by removing expired data in bulk.
//
// This function performs the following steps:
// - Acquires exclusive lock to prevent concurrent modifications
// - Calculates the cutoff bucket ID based on retention window
// - Iterates through all buckets and removes expired ones
// - Updates the global item count by subtracting removed items
// - Updates the oldest bucket tracker if necessary
//
// Parameters:
// - None (operates on receiver fields)
//
// Returns:
// - Nothing
//
// Side Effects:
// - Removes expired buckets and all their contents
// - Updates global item count
// - May trigger recalculation of oldest/newest buckets
// - Holds exclusive lock during operation
//
// Notes:
// - Called every bucketSize/2 by background goroutine
// - Bulk deletion is efficient compared to per-item expiration
// - Lock ensures consistency during cleanup
// - Cutoff calculation uses negative duration arithmetic
func (m *TimePartitionedMap[K, V]) cleanupOldBuckets() {
	m.bucketsMu.Lock()
	defer m.bucketsMu.Unlock()

	// Clean up by time - remove buckets older than our window
	// Use the cached bucket ID by passing a zero time
	maxAgeBucketID := time.Now().Add(-m.bucketSize*time.Duration(m.maxBuckets)).UnixNano() / int64(m.bucketSize)

	for bucketID, bucket := range m.buckets.Range() {
		if bucketID <= maxAgeBucketID {
			m.itemCount.Add(int64(-1 * bucket.Length()))
			m.buckets.Delete(bucketID)
		}
	}

	// Update oldest bucket
	if m.oldestBucket.Load() <= maxAgeBucketID {
		m.oldestBucket.Store(maxAgeBucketID)
		m.recalculateOldestBucket()
	}
}

// recalculateOldestBucket recalculates the oldest and newest bucket IDs after deletions.
//
// This method is called when buckets are removed (either through Delete or cleanupOldBuckets)
// to ensure the oldest and newest bucket trackers remain accurate. It performs a full
// scan of remaining buckets to find the actual min/max bucket IDs.
//
// This function performs the following steps:
// - Checks if any buckets remain (early return if empty)
// - Initializes search with extreme values
// - Iterates through all remaining buckets
// - Tracks both minimum (oldest) and maximum (newest) bucket IDs
// - Updates the atomic trackers with found values
//
// Parameters:
// - None (operates on receiver fields)
//
// Returns:
// - Nothing
//
// Side Effects:
// - Updates oldestBucket and newestBucket atomic values
//
// Notes:
// - O(n) complexity where n is the number of buckets
// - Only called when buckets are deleted, not on every operation
// - Resets both trackers to 0 when map is empty
// - Uses max int64 as sentinel for finding minimum
func (m *TimePartitionedMap[K, V]) recalculateOldestBucket() {
	if m.buckets.Length() == 0 {
		m.oldestBucket.Store(0)
		m.newestBucket.Store(0)

		return
	}

	oldest := int64(1<<63 - 1) // Max int64
	newest := int64(0)

	for bucketID := range m.buckets.Range() {
		if bucketID < oldest {
			oldest = bucketID
		}

		if bucketID > newest {
			newest = bucketID
		}
	}

	m.oldestBucket.Store(oldest)
	m.newestBucket.Store(newest)
}

// Count returns the total number of items across all buckets.
//
// This method provides an O(1) count of all items in the map by returning
// the value of an atomic counter that's maintained during Set/Delete operations.
//
// Parameters:
// - None
//
// Returns:
// - int: Total number of items currently in the map
//
// Side Effects:
// - None (read-only operation)
//
// Notes:
// - Count is maintained atomically for accuracy
// - Includes items from all buckets, including those near expiration
// - More efficient than iterating through all buckets
// - Thread-safe for concurrent access
func (m *TimePartitionedMap[K, V]) Count() int {
	return int(m.itemCount.Load())
}

// BatcherWithDedup extends Batcher with automatic deduplication of items.
//
// This type provides all the functionality of the basic Batcher plus the ability
// to automatically filter out duplicate items within a configurable time window.
// It uses a TimePartitionedMap to efficiently track seen items and automatically
// forget them after the deduplication window expires.
//
// Type parameters:
// - T: The type of items to batch (must be comparable for deduplication)
//
// Fields:
// - Batcher[T]: Embedded base batcher providing core batching functionality
// - deduplicationWindow: Duration for which items are remembered for deduplication
// - deduplicationMap: Time-partitioned storage for tracking seen items
//
// Notes:
// - Items must be comparable (support == operator) for deduplication
// - Deduplication is global across all batches within the time window
// - Memory usage grows with unique items but is bounded by time window
// - Suitable for scenarios like event processing where duplicates must be filtered
type BatcherWithDedup[T comparable] struct { //nolint:revive // Name is clear and intentional
	Batcher[T]

	// Deduplication related fields
	deduplicationWindow time.Duration
	deduplicationMap    *TimePartitionedMap[T, struct{}]
}

// NewWithDeduplication creates a new Batcher with automatic deduplication support.
//
// This function initializes a BatcherWithDedup that filters out duplicate items
// within a 1-minute time window. Items are considered duplicates if they have
// the same value (using == comparison) and occur within the deduplication window.
//
// The deduplication is implemented using a TimePartitionedMap with 1-second bucket,
// providing efficient memory usage and automatic expiration of old entries.
//
// Parameters:
// - size: Maximum number of items per batch before automatic processing
// - timeout: Maximum duration to wait before processing an incomplete batch
// - fn: Callback function that processes each batch of unique items
// - background: If true, batch processing happens asynchronously
//
// Returns:
// - *BatcherWithDedup[T]: A configured batcher with deduplication enabled
//
// Side Effects:
// - Starts a background worker goroutine for batch processing
// - Starts additional goroutines for deduplication map maintenance
//
// Notes:
// - T must be comparable (support == operator) for deduplication to work
// - Deduplication window is fixed at 1 minute (not configurable)
// - Uses 1-second buckets for fine-grained expiration (61 buckets total)
// - Duplicates are silently dropped without notification
// - Memory usage scales with number of unique items in the time window
func NewWithDeduplication[T comparable](size int, timeout time.Duration, fn func(batch []*T), background bool) *BatcherWithDedup[T] {
	deduplicationWindow := time.Minute // 1-minute deduplication window

	b := &BatcherWithDedup[T]{
		Batcher: Batcher[T]{
			fn:         fn,
			size:       size,
			timeout:    timeout,
			batch:      make([]*T, 0, size),
			ch:         make(chan *T, size*64),
			triggerCh:  make(chan struct{}),
			background: background,
		},
		deduplicationWindow: deduplicationWindow,
		// Create a time-partitioned map with 1-second bucket and enough buckets to cover the deduplication window
		deduplicationMap: NewTimePartitionedMap[T, struct{}](time.Second, int(deduplicationWindow.Seconds())+1),
	}

	go b.worker()

	return b
}

// Put adds an item to the batch with automatic deduplication.
//
// This method extends the base Batcher's Put functionality by first checking
// if the item has been seen within the deduplication window. Only unique items
// are added to the batch for processing.
//
// This function performs the following steps:
// - Validates that the item is not nil
// - Attempts to add the item to the deduplication map
// - If successful (not a duplicate), forwards the item to the batcher
// - If unsuccessful (duplicate), silently drops the item
//
// Parameters:
// - item: Pointer to the item to be batched. Nil items are ignored
//
// Returns:
// - Nothing
//
// Side Effects:
// - Adds item to deduplication map if unique
// - Sends item to worker goroutine if not a duplicate
// - May trigger batch processing if batch becomes full
//
// Notes:
// - This method overrides the base Batcher.Put method
// - Nil items are silently ignored without error
// - Duplicates are silently dropped without notification
// - The variadic parameter from base Put is not supported
// - Deduplication is based on item value equality (==)
func (b *BatcherWithDedup[T]) Put(item *T) {
	if item == nil {
		return
	}

	// Set returns TRUE if the item was added, FALSE if it was a duplicate
	if b.deduplicationMap.Set(*item, struct{}{}) {
		// Add the item to the batch
		b.ch <- item
	}
}
