package dxfuse

import "sync"

type MemoryManager struct {
	mutex           sync.Mutex // Lock for thread-safe updates to counters
	cond            *sync.Cond // Condition variable for waiting
	maxMemory       int64      // Maximum memory allowed (in bytes)
	minReadMemory   int64      // Minimum reserved memory for reads
	minWriteMemory  int64      // Minimum reserved memory for writes
	usedMemory      int64      // Currently used memory (in bytes)
	prefetchWaiting int        // Number of prefetch threads waiting for memory
	uploadMemory    int64      // Memory allocated for uploads
	prefetchMemory  int64      // Memory allocated for prefetching
}

func NewMemoryManager(maxMemory, minReadMemory, minWriteMemory int64) *MemoryManager {
	mm := &MemoryManager{
		maxMemory:      maxMemory,
		minReadMemory:  minReadMemory,
		minWriteMemory: minWriteMemory,
		usedMemory:     0,
	}
	mm.cond = sync.NewCond(&mm.mutex)
	return mm
}

// Separate functions for read and write buffer allocation
func (mm *MemoryManager) AllocateReadBuffer(size int64) []byte {
	if !mm.allocate(size, false) {
		return nil
	}
	return make([]byte, size)
}

func (mm *MemoryManager) AllocateWriteBuffer(size int64) []byte {
	if !mm.allocate(size, true) {
		return nil
	}
	return make([]byte, size)
}

// Separate functions for read and write buffer release
func (mm *MemoryManager) ReleaseReadBuffer(size int64) {
	mm.release(size, false)
}

func (mm *MemoryManager) ReleaseWriteBuffer(size int64) {
	mm.release(size, true)
}

// Add a helper function to allocate memory for buffers
func (mm *MemoryManager) AllocateBuffer(size int64) []byte {
	if !mm.allocate(size, true) {
		return nil
	}
	return make([]byte, size)
}

// Add a helper function to release memory for buffers
func (mm *MemoryManager) ReleaseBuffer(data []byte) {
	if data != nil {
		mm.release(int64(len(data)), false)
		// Set the buffer to nil to allow garbage collection
		data = nil
	}
}

// Internal helper functions for allocation and release
func (mm *MemoryManager) allocate(size int64, isWriteBuffer bool) bool {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	if !isWriteBuffer {
		mm.prefetchWaiting++
		defer func() { mm.prefetchWaiting-- }()
	}

	for mm.usedMemory+size > mm.maxMemory || (!isWriteBuffer && mm.prefetchWaiting > 0) {
		mm.cond.Wait()
	}

	mm.usedMemory += size
	if isWriteBuffer {
		mm.uploadMemory += size
	} else {
		mm.prefetchMemory += size
	}
	return true
}

func (mm *MemoryManager) release(size int64, isWriteBuffer bool) {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	mm.usedMemory -= size
	if isWriteBuffer {
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
