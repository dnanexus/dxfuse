package dxfuse

import "sync"

type MemoryManager struct {
	mutex           sync.Mutex
	cond            *sync.Cond // Condition variable for waiting
	maxMemory       int64      // Maximum memory allowed (in bytes)
	usedMemory      int64      // Currently used memory (in bytes)
	prefetchWaiting int        // Number of prefetch threads waiting for memory
	uploadMemory    int64      // Memory allocated for uploads
	prefetchMemory  int64      // Memory allocated for prefetching
}

func NewMemoryManager(maxMemory int64) *MemoryManager {
	mm := &MemoryManager{
		maxMemory:  maxMemory,
		usedMemory: 0,
	}
	mm.cond = sync.NewCond(&mm.mutex)
	return mm
}

// Try to allocate memory. Dynamically rebalance priorities if needed.
func (mm *MemoryManager) Allocate(size int64, priority bool) bool {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	if !priority {
		mm.prefetchWaiting++
		defer func() { mm.prefetchWaiting-- }()
	}

	for mm.usedMemory+size > mm.maxMemory || (!priority && mm.prefetchWaiting > 0) {
		// Dynamic rebalancing: prioritize uploads if prefetching uses too much memory
		if mm.prefetchMemory > mm.uploadMemory && priority {
			mm.prefetchWaiting = 0 // Allow uploads to proceed
		}
		mm.cond.Wait()
	}

	mm.usedMemory += size
	if priority {
		mm.uploadMemory += size
	} else {
		mm.prefetchMemory += size
	}
	return true
}

// Release memory back to the pool and adjust usage tracking.
func (mm *MemoryManager) Release(size int64, priority bool) {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	mm.usedMemory -= size
	if priority {
		mm.uploadMemory -= size
	} else {
		mm.prefetchMemory -= size
	}

	if mm.usedMemory < 0 {
		mm.usedMemory = 0
	}
	mm.cond.Broadcast()
}

// Get the current memory usage.
func (mm *MemoryManager) GetUsedMemory() int64 {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()
	return mm.usedMemory
}
