package dxfuse

import (
	"sync"
)

type MemoryManager struct {
	mutex                   sync.Mutex // Lock for thread-safe updates to counters
	cond                    *sync.Cond // Condition variable for waiting
	maxMemory               int64      // Maximum memory allowed (in bytes)
	maxMemoryUsagePerModule int64      // Maximum memory (in bytes) a single module can use
	usedMemory              int64      // Currently used memory (in bytes)
	readsWaiting            int        // Number of prefetch threads waiting for memory
	writesWaiting           int        // Number of write threads waiting for memory
	writeMemory             int64      // Memory allocated for writes and uploads
	readMemory              int64      // Memory allocated for read cache and prefetch
}

func NewMemoryManager(maxMemory int64, maxMemoryUsagePerModule int64) *MemoryManager {
	mm := &MemoryManager{
		maxMemory:               maxMemory,
		maxMemoryUsagePerModule: maxMemoryUsagePerModule,
		usedMemory:              0,
	}
	mm.cond = sync.NewCond(&mm.mutex)
	return mm
}

func (pgs *MemoryManager) log(a string, args ...interface{}) {
	LogMsg("mem", a, args...)
}

// Separate functions for read and write buffer allocation
func (mm *MemoryManager) AllocateReadBuffer(size int64) []byte {
	return mm.allocate(size, false, false)
}

func (mm *MemoryManager) AllocateWriteBuffer(size int64) []byte {
	return mm.allocate(size, true, true)
}

func (mm *MemoryManager) AllocateReadBufferWait(size int64) []byte {
	return mm.allocate(size, false, true)
}

func (mm *MemoryManager) ReleaseReadBuffer(buf []byte) {
	mm.release(buf, false)
}

func (mm *MemoryManager) ReleaseWriteBuffer(buf []byte) {
	mm.release(buf, true)
}

// Internal helper functions for allocation and release
func (mm *MemoryManager) allocate(size int64, isWriteBuffer bool, waitIndefinitely bool) []byte {
	mm.mutex.Lock()
	if isWriteBuffer {
		mm.writesWaiting++
		mm.log("Allocating %d bytes for write buffer", size)
	} else {
		mm.readsWaiting++
		mm.log("Allocating %d bytes for read buffer", size)
	}
	mm.log("Memory stats before allocation: used=%d, write=%d, read=%d, readsWaiting=%d, writesWaiting=%d",
		mm.usedMemory, mm.writeMemory, mm.readMemory, mm.readsWaiting, mm.writesWaiting)

	defer func() {
		if isWriteBuffer {
			mm.writesWaiting--
		} else {
			mm.readsWaiting--
		}
		mm.mutex.Unlock()
	}()

	for mm.usedMemory+size > mm.maxMemory ||
		(isWriteBuffer && mm.writeMemory+size > mm.maxMemoryUsagePerModule) ||
		(!isWriteBuffer && mm.readMemory+size > mm.maxMemoryUsagePerModule) ||
		(!isWriteBuffer && mm.readsWaiting > 1) {
		if !waitIndefinitely {
			return nil
		}
		mm.cond.Wait()
	}

	mm.usedMemory += size
	if isWriteBuffer {
		mm.writeMemory += size
	} else {
		mm.readMemory += size
	}

	mm.log("Memory stats: used=%d, write=%d, read=%d, readsWaiting=%d, writesWaiting=%d",
		mm.usedMemory, mm.writeMemory, mm.readMemory, mm.readsWaiting, mm.writesWaiting)

	return make([]byte, size)
}

func (mm *MemoryManager) release(buf []byte, isWriteBuffer bool) {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()
	size := int64(len(buf))
	// Release the buffer
	buf = nil
	mm.usedMemory -= size
	if isWriteBuffer {
		mm.writeMemory -= size
	} else {
		mm.readMemory -= size
	}

	if mm.usedMemory < 0 {
		mm.usedMemory = 0
	}

	mm.log("Memory stats after release: used=%d, write=%d, read=%d, readsWaiting=%d",
		mm.usedMemory, mm.writeMemory, mm.readMemory, mm.readsWaiting)
	mm.cond.Broadcast()
}

// Get the current memory usage.
func (mm *MemoryManager) GetUsedMemory() int64 {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()
	return mm.usedMemory
}
