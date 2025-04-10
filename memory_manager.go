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

// Separate functions for read and write buffer allocation
func (mm *MemoryManager) AllocateReadBuffer(size int64, priority bool) bool {
	return mm.allocate(size, priority, false)
}

func (mm *MemoryManager) AllocateWriteBuffer(size int64, priority bool) bool {
	return mm.allocate(size, priority, true)
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
	if !mm.allocate(size, true, false) {
		return nil
	}
	return make([]byte, size)
}

// Add a helper function to release memory for buffers
func (mm *MemoryManager) ReleaseBuffer(data []byte) {
	if data != nil {
		mm.release(int64(len(data)), false)
	}
}

// Internal helper functions for allocation and release
func (mm *MemoryManager) allocate(size int64, priority bool, isWriteBuffer bool) bool {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	if !priority {
		mm.prefetchWaiting++
		defer func() { mm.prefetchWaiting-- }()
	}

	for mm.usedMemory+size > mm.maxMemory || (!priority && mm.prefetchWaiting > 0) {
		if isWriteBuffer || (mm.prefetchMemory > mm.uploadMemory && priority) {
			mm.prefetchWaiting = 0
		}
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
