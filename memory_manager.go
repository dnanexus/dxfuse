package dxfuse

import (
	"context"
	"sync/atomic"

	"golang.org/x/sync/semaphore"
)

type MemoryManager struct {
	maxMemory               int64 // Maximum memory allowed (in bytes)
	maxMemoryUsagePerModule int64 // Maximum memory (in bytes) a single module can use
	verboseLevel            int

	// Budgets enforce limits; counters are best-effort telemetry.
	totalBudget *semaphore.Weighted
	readBudget  *semaphore.Weighted
	writeBudget *semaphore.Weighted

	usedMemory  atomic.Int64 // total reserved bytes
	readMemory  atomic.Int64 // reserved bytes for reads/prefetch
	writeMemory atomic.Int64 // reserved bytes for writes/uploads
}

func NewMemoryManager(verboseLevel int, maxMemory int64, maxMemoryUsagePerModule int64) *MemoryManager {
	if maxMemory < 0 {
		maxMemory = 0
	}
	if maxMemoryUsagePerModule < 0 {
		maxMemoryUsagePerModule = 0
	}
	if maxMemoryUsagePerModule > maxMemory {
		maxMemoryUsagePerModule = maxMemory
	}

	mm := &MemoryManager{
		maxMemory:               maxMemory,
		maxMemoryUsagePerModule: maxMemoryUsagePerModule,
		verboseLevel:            verboseLevel,
		totalBudget:             semaphore.NewWeighted(maxMemory),
		readBudget:              semaphore.NewWeighted(maxMemoryUsagePerModule),
		writeBudget:             semaphore.NewWeighted(maxMemoryUsagePerModule),
	}
	return mm
}

func (mm *MemoryManager) log(a string, args ...interface{}) {
	LogMsg("mem", a, args...)
}

func (mm *MemoryManager) debug(a string, args ...interface{}) {
	if mm.verboseLevel > 1 {
		LogMsg("mem", a, args...)
	}
}

// Separate functions for read and write buffer allocation
func (mm *MemoryManager) AllocateReadBuffer(size int64) []byte {
	return mm.allocate(context.Background(), size, false)
}

// TryAllocateReadBuffer attempts to reserve memory without blocking.
// Useful for best-effort background work (e.g. prefetch).
func (mm *MemoryManager) TryAllocateReadBuffer(size int64) []byte {
	return mm.tryAllocate(size, false)
}

func (mm *MemoryManager) AllocateWriteBuffer(size int64) []byte {
	return mm.allocate(context.Background(), size, true)
}

// TryAllocateWriteBuffer attempts to reserve write memory without blocking.
func (mm *MemoryManager) TryAllocateWriteBuffer(size int64) []byte {
	return mm.tryAllocate(size, true)
}

func (mm *MemoryManager) ReleaseReadBuffer(buf []byte) {
	mm.debug("Releasing read buffer of size %d", cap(buf))
	mm.release(buf, false)
}

func (mm *MemoryManager) ReleaseWriteBuffer(buf []byte) {
	mm.release(buf, true)
}

// Internal helper for allocation. Reserves bytes from both the total budget and the
// per-module (read/write) budget, then allocates the backing slice.
func (mm *MemoryManager) allocate(ctx context.Context, size int64, isWriteBuffer bool) []byte {
	if size <= 0 {
		return make([]byte, 0)
	}

	// Avoid deadlock on impossible requests: a semaphore Acquire(n) blocks forever
	// if n > capacity.
	if size > mm.maxMemory {
		return nil
	}
	if size > mm.maxMemoryUsagePerModule {
		return nil
	}

	mm.debug("Reserve memory: isWriteBuffer=%v size=%d used=%d write=%d read=%d",
		isWriteBuffer, size, mm.usedMemory.Load(), mm.writeMemory.Load(), mm.readMemory.Load())

	if err := mm.totalBudget.Acquire(ctx, size); err != nil {
		return nil
	}
	reservedTotal := true

	var budget *semaphore.Weighted
	if isWriteBuffer {
		budget = mm.writeBudget
	} else {
		budget = mm.readBudget
	}
	if err := budget.Acquire(ctx, size); err != nil {
		if reservedTotal {
			mm.totalBudget.Release(size)
		}
		return nil
	}

	mm.usedMemory.Add(size)
	if isWriteBuffer {
		mm.writeMemory.Add(size)
	} else {
		mm.readMemory.Add(size)
	}

	return make([]byte, size)
}

func (mm *MemoryManager) tryAllocate(size int64, isWriteBuffer bool) []byte {
	if size <= 0 {
		return make([]byte, 0)
	}
	if size > mm.maxMemory {
		return nil
	}
	if size > mm.maxMemoryUsagePerModule {
		return nil
	}

	if !mm.totalBudget.TryAcquire(size) {
		return nil
	}

	var budget *semaphore.Weighted
	if isWriteBuffer {
		budget = mm.writeBudget
	} else {
		budget = mm.readBudget
	}
	if !budget.TryAcquire(size) {
		mm.totalBudget.Release(size)
		return nil
	}

	mm.usedMemory.Add(size)
	if isWriteBuffer {
		mm.writeMemory.Add(size)
	} else {
		mm.readMemory.Add(size)
	}

	return make([]byte, size)
}

func (mm *MemoryManager) release(buf []byte, isWriteBuffer bool) {
	// Use capacity for accounting so callers can safely slice buffers
	// (e.g. uploading only the written prefix) without breaking memory tracking.
	size := int64(cap(buf))
	if size <= 0 {
		return
	}

	mm.usedMemory.Add(-size)
	if isWriteBuffer {
		mm.writeMemory.Add(-size)
		mm.writeBudget.Release(size)
	} else {
		mm.readMemory.Add(-size)
		mm.readBudget.Release(size)
	}
	mm.totalBudget.Release(size)

	mm.debug("Released buffer of size %d, usedMemory=%d, writeMemory=%d, readMemory=%d",
		size, mm.usedMemory.Load(), mm.writeMemory.Load(), mm.readMemory.Load())
}

func (mm *MemoryManager) GetUsedMemory() int64 {
	return mm.usedMemory.Load()
}

func (mm *MemoryManager) GetUsedReadMemory() int64 {
	return mm.readMemory.Load()
}

func (mm *MemoryManager) GetUsedWriteMemory() int64 {
	return mm.writeMemory.Load()
}
