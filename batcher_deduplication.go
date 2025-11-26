package batcher

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"sync"
	"sync/atomic"
	"time"

	txmap "github.com/bsv-blockchain/go-tx-map"
)

// BloomFilter is a simple bloom filter implementation for fast negative lookups.
// This implementation uses a bit array and multiple hash functions to provide
// probabilistic set membership testing with no false negatives.
type BloomFilter struct {
	bits      []uint64
	size      uint64
	hashFuncs uint
	itemCount atomic.Uint64
	mu        sync.RWMutex
}

// NewBloomFilter creates a new bloom filter with the specified size and hash functions.
func NewBloomFilter(size uint64, hashFuncs uint) *BloomFilter {
	// Ensure size is a multiple of 64 for uint64 alignment
	alignedSize := (size + 63) / 64
	return &BloomFilter{
		bits:      make([]uint64, alignedSize),
		size:      alignedSize * 64,
		hashFuncs: hashFuncs,
	}
}

// Add adds a key to the bloom filter.
func (bf *BloomFilter) Add(key interface{}) {
	hashes := bf.hash(key)
	bf.mu.Lock()
	defer bf.mu.Unlock()
	for _, h := range hashes {
		wordIndex := h / 64
		bitIndex := h % 64
		bf.bits[wordIndex] |= 1 << bitIndex
	}
	bf.itemCount.Add(1)
}

// Test checks if a key might be in the bloom filter.
// Returns false if definitely not present, true if maybe present.
func (bf *BloomFilter) Test(key interface{}) bool {
	hashes := bf.hash(key)
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	for _, h := range hashes {
		wordIndex := h / 64
		bitIndex := h % 64
		if bf.bits[wordIndex]&(1<<bitIndex) == 0 {
			return false
		}
	}
	return true
}

// Reset clears the bloom filter.
func (bf *BloomFilter) Reset() {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	for i := range bf.bits {
		bf.bits[i] = 0
	}
	bf.itemCount.Store(0)
}

// hash generates multiple hash values for the given key.
// The hash.Hash.Write() method never returns an error for FNV hash, but we check defensively.
func (bf *BloomFilter) hash(key interface{}) []uint64 { //nolint:gocyclo,gocognit // Type switch for performance
	h := fnv.New64a()
	// Convert key to bytes for hashing - fast paths for common types
	switch k := key.(type) {
	case string:
		if _, err := h.Write([]byte(k)); err != nil {
			panic(fmt.Sprintf("unexpected hash.Write error: %v", err))
		}
	case int:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(k)) //nolint:gosec // Safe conversion for hashing
		if _, err := h.Write(buf[:]); err != nil {
			panic(fmt.Sprintf("unexpected hash.Write error: %v", err))
		}
	case int8:
		if _, err := h.Write([]byte{byte(k)}); err != nil {
			panic(fmt.Sprintf("unexpected hash.Write error: %v", err))
		}
	case int16:
		var buf [2]byte
		binary.BigEndian.PutUint16(buf[:], uint16(k)) //nolint:gosec // Safe conversion for hashing
		if _, err := h.Write(buf[:]); err != nil {
			panic(fmt.Sprintf("unexpected hash.Write error: %v", err))
		}
	case int32:
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], uint32(k)) //nolint:gosec // Safe conversion for hashing
		if _, err := h.Write(buf[:]); err != nil {
			panic(fmt.Sprintf("unexpected hash.Write error: %v", err))
		}
	case int64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(k)) //nolint:gosec // Safe conversion for hashing
		if _, err := h.Write(buf[:]); err != nil {
			panic(fmt.Sprintf("unexpected hash.Write error: %v", err))
		}
	case uint:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(k))
		if _, err := h.Write(buf[:]); err != nil {
			panic(fmt.Sprintf("unexpected hash.Write error: %v", err))
		}
	case uint8:
		if _, err := h.Write([]byte{k}); err != nil {
			panic(fmt.Sprintf("unexpected hash.Write error: %v", err))
		}
	case uint16:
		var buf [2]byte
		binary.BigEndian.PutUint16(buf[:], k)
		if _, err := h.Write(buf[:]); err != nil {
			panic(fmt.Sprintf("unexpected hash.Write error: %v", err))
		}
	case uint32:
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], k)
		if _, err := h.Write(buf[:]); err != nil {
			panic(fmt.Sprintf("unexpected hash.Write error: %v", err))
		}
	case uint64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], k)
		if _, err := h.Write(buf[:]); err != nil {
			panic(fmt.Sprintf("unexpected hash.Write error: %v", err))
		}
	case float32:
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], math.Float32bits(k))
		if _, err := h.Write(buf[:]); err != nil {
			panic(fmt.Sprintf("unexpected hash.Write error: %v", err))
		}
	case float64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], math.Float64bits(k))
		if _, err := h.Write(buf[:]); err != nil {
			panic(fmt.Sprintf("unexpected hash.Write error: %v", err))
		}
	case bool:
		if k {
			if _, err := h.Write([]byte{1}); err != nil {
				panic(fmt.Sprintf("unexpected hash.Write error: %v", err))
			}
		} else {
			if _, err := h.Write([]byte{0}); err != nil {
				panic(fmt.Sprintf("unexpected hash.Write error: %v", err))
			}
		}
	default:
		// For other types (structs, arrays, etc), use fmt.Fprintf for generic conversion
		if _, err := fmt.Fprintf(h, "%v", key); err != nil {
			panic(fmt.Sprintf("unexpected hash.Write error: %v", err))
		}
	}
	hash1 := h.Sum64()
	// Generate additional hashes using double hashing
	hashes := make([]uint64, bf.hashFuncs)
	hash2 := hash1 >> 32
	for i := uint(0); i < bf.hashFuncs; i++ {
		hashes[i] = (hash1 + uint64(i)*hash2) % bf.size
	}
	return hashes
}

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
	// Optimization fields
	bloomFilter      *BloomFilter
	bloomResetTicker *time.Ticker
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

	// Initialize bloom filter for fast negative lookups
	// Size based on expected items per bucket with low false positive rate
	bloomSize := uint64(maxBuckets) * 10000 * 10 //nolint:gosec // Controlled input with reasonable bounds
	m.bloomFilter = NewBloomFilter(bloomSize, 3) // 3 hash functions

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

	// Reset bloom filter periodically to handle expired items
	m.bloomResetTicker = time.NewTicker(bucketSize * time.Duration(maxBuckets))
	go func() {
		for range m.bloomResetTicker.C {
			m.bloomFilter.Reset()
			// Re-add all current items to bloom filter
			m.rebuildBloomFilter()
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
// rebuildBloomFilter reconstructs the bloom filter from current items.
func (m *TimePartitionedMap[K, V]) rebuildBloomFilter() {
	m.bucketsMu.Lock()
	defer m.bucketsMu.Unlock()
	for _, bucket := range m.buckets.Range() {
		for key := range bucket.Range() {
			m.bloomFilter.Add(key)
		}
	}
}

// Get retrieves a value from the map using bloom filter for fast negative lookups.
// Searches from newest to oldest bucket for better performance on recent items.
func (m *TimePartitionedMap[K, V]) Get(key K) (V, bool) { //nolint:gocognit // Complex due to bloom filter optimization
	// First check bloom filter for fast negative
	if !m.bloomFilter.Test(key) {
		return m.zero, false
	}

	// Search from newest to oldest bucket for recent items
	newestID := m.newestBucket.Load()
	oldestID := m.oldestBucket.Load()

	// If no buckets exist
	if newestID == 0 || oldestID == 0 {
		// Fall back to original search
		for bucketID := range m.buckets.Range() {
			if bucket, exists := m.buckets.Get(bucketID); exists {
				if value, found := bucket.Get(key); found {
					return value, true
				}
			}
		}
		return m.zero, false
	}

	// Search backwards from newest bucket
	for bucketID := newestID; bucketID >= oldestID; bucketID-- {
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
// Set adds a key-value pair using bloom filter for fast duplicate detection.
// Uses double-checked locking pattern for thread safety:
//  1. First check (outside lock): Bloom filter for fast negative lookups
//  2. Acquire lock
//  3. Second check (inside lock): Full verification to prevent TOCTOU race
//  4. Insert if still no duplicate
func (m *TimePartitionedMap[K, V]) Set(key K, value V) bool {
	// FIRST CHECK (outside lock): Bloom filter for fast rejection of duplicates
	if m.bloomFilter.Test(key) {
		if _, exists := m.Get(key); exists {
			return false
		}
	}

	var (
		bucket *txmap.SyncedMap[K, V]
		exists bool
	)

	bucketID := m.currentBucketID.Load()

	m.bucketsMu.Lock()

	// SECOND CHECK (inside lock): Prevent TOCTOU race condition
	// Another goroutine may have inserted the key between our first check and lock acquisition
	if m.keyExistsInBucketsLocked(key) {
		m.bucketsMu.Unlock()
		return false
	}

	// Safe to add - update bloom filter under lock for consistency
	m.bloomFilter.Add(key)

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
	bucket.Set(key, value)
	m.itemCount.Add(1)

	m.bucketsMu.Unlock()

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

	m.bucketsMu.Lock()
	defer m.bucketsMu.Unlock()

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

// keyExistsInBucketsLocked checks if a key exists in any bucket.
// MUST be called while holding bucketsMu lock.
func (m *TimePartitionedMap[K, V]) keyExistsInBucketsLocked(key K) bool {
	newestID := m.newestBucket.Load()
	oldestID := m.oldestBucket.Load()

	// Fall back to range iteration if bucket tracking not initialized
	if newestID == 0 || oldestID == 0 {
		return m.keyExistsInBucketsRange(key)
	}

	// Search backwards from newest bucket (most likely location for recent duplicates)
	for bucketID := newestID; bucketID >= oldestID; bucketID-- {
		if m.keyExistsInBucket(key, bucketID) {
			return true
		}
	}
	return false
}

// keyExistsInBucketsRange checks if key exists using range iteration.
// MUST be called while holding bucketsMu lock.
func (m *TimePartitionedMap[K, V]) keyExistsInBucketsRange(key K) bool {
	for _, bucket := range m.buckets.Range() {
		if _, found := bucket.Get(key); found {
			return true
		}
	}
	return false
}

// keyExistsInBucket checks if key exists in a specific bucket.
// MUST be called while holding bucketsMu lock.
func (m *TimePartitionedMap[K, V]) keyExistsInBucket(key K, bucketID int64) bool {
	bucket, exists := m.buckets.Get(bucketID)
	if !exists {
		return false
	}
	_, found := bucket.Get(key)
	return found
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

// Close stops the bloom filter reset ticker.
func (m *TimePartitionedMap[K, V]) Close() {
	if m.bloomResetTicker != nil {
		m.bloomResetTicker.Stop()
	}
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
			usePool:    false,
			done:       make(chan struct{}),
		},
		deduplicationWindow: deduplicationWindow,
		// Create an optimized time-partitioned map with bloom filter
		deduplicationMap: NewTimePartitionedMap[T, struct{}](time.Second, int(deduplicationWindow.Seconds())+1),
	}

	go b.worker()

	return b
}

// NewWithDeduplicationAndPool creates a BatcherWithDedup with slice pooling enabled.
func NewWithDeduplicationAndPool[T comparable](size int, timeout time.Duration, fn func(batch []*T), background bool) *BatcherWithDedup[T] {
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
			usePool:    true,
			done:       make(chan struct{}),
			pool: &sync.Pool{
				New: func() interface{} {
					slice := make([]*T, 0, size)
					return &slice
				},
			},
		},
		deduplicationWindow: deduplicationWindow,
		// Create an optimized time-partitioned map with bloom filter
		deduplicationMap: NewTimePartitionedMap[T, struct{}](time.Second, int(deduplicationWindow.Seconds())+1),
	}

	go b.worker()

	return b
}

// Close properly shuts down the batcher and deduplication map resources.
//
// This method performs a graceful shutdown by:
// 1. Stopping the background worker goroutine via the parent Batcher.Close()
// 2. Processing any remaining items in the queue
// 3. Closing the deduplication map and stopping its cleanup ticker
//
// It is safe to call Close() multiple times (subsequent calls have no effect).
//
// IMPORTANT: Do not call Put() after Close() has been called, as this will
// result in a panic due to sending on a closed channel.
func (b *BatcherWithDedup[T]) Close() {
	b.Batcher.Close()
	b.deduplicationMap.Close()
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
