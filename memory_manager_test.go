package dxfuse

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

func recvWithin[T any](t *testing.T, ch <-chan T, d time.Duration, msg string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(d):
		t.Fatalf("timed out after %s: %s", d, msg)
		var zero T
		return zero
	}
}

func assertNoRecvWithin[T any](t *testing.T, ch <-chan T, d time.Duration, msg string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("unexpected receive within %s: %s", d, msg)
	case <-time.After(d):
		return
	}
}

func TestMemoryManager_AllocateAndReleaseWriteBuffer(t *testing.T) {
	maxMemory := int64(64 * 1024 * 1024)       // 64 MiB
	maxModuleMemory := int64(32 * 1024 * 1024) // 32 MiB
	mm := NewMemoryManager(0, maxMemory, maxModuleMemory)

	// Allocate a 16 MiB write buffer
	buf := mm.AllocateWriteBuffer(16 * 1024 * 1024)
	if buf == nil {
		t.Fatalf("Failed to allocate write buffer")
	}

	if mm.GetUsedMemory() != 16*1024*1024 {
		t.Errorf("Expected used memory to be 16 MiB, got %d", mm.GetUsedMemory())
	}

	// Release the buffer
	mm.ReleaseWriteBuffer(buf)
	if mm.GetUsedMemory() != 0 {
		t.Errorf("Expected used memory to be 0 after release, got %d", mm.GetUsedMemory())
	}
}

func TestMemoryManager_ConcurrentAllocations(t *testing.T) {
	maxMemory := int64(64 * 1024 * 1024)       // 64 MiB
	maxModuleMemory := int64(32 * 1024 * 1024) // 32 MiB
	mm := NewMemoryManager(0, maxMemory, maxModuleMemory)

	var wg sync.WaitGroup
	numGoroutines := 10
	bufferSize := int64(4 * 1024 * 1024) // 4 MiB

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			buf := mm.AllocateWriteBuffer(bufferSize)
			if buf != nil {
				mm.ReleaseWriteBuffer(buf)
			}
		}()
	}

	wg.Wait()

	if mm.GetUsedMemory() != 0 {
		t.Errorf("Expected used memory to be 0 after all releases, got %d", mm.GetUsedMemory())
	}
}

func TestMemoryManager_ExceedMaxMemory(t *testing.T) {
	maxMemory := int64(32 * 1024 * 1024)       // 32 MiB
	maxModuleMemory := int64(16 * 1024 * 1024) // 16 MiB
	mm := NewMemoryManager(0, maxMemory, maxModuleMemory)

	// Try to allocate more than max memory
	buf := mm.AllocateWriteBuffer(64 * 1024 * 1024) // 64 MiB
	if buf != nil {
		t.Fatalf("Expected allocation to fail, but it succeeded")
	}

	if mm.GetUsedMemory() != 0 {
		t.Errorf("Expected used memory to be 0 after failed allocation, got %d", mm.GetUsedMemory())
	}
}

func TestMemoryManager_ParallelAllocationAndRelease(t *testing.T) {
	maxMemory := int64(14 * 1024 * 1024 * 1024) // 14 GiB
	maxModuleMemory := int64(64 * 1024 * 1024)  // 64 MiB
	mm := NewMemoryManager(0, maxMemory, maxModuleMemory)

	var wg sync.WaitGroup
	numGoroutines := 100
	bufferSize := int64(64 * 1024 * 1024) // 64 MiB

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			buf := mm.AllocateWriteBuffer(bufferSize)
			if buf != nil {
				// Simulate some work with the buffer
				time.Sleep(10 * time.Millisecond)
				mm.ReleaseWriteBuffer(buf)
			}
		}()
	}

	wg.Wait()

	if mm.GetUsedMemory() != 0 {
		t.Errorf("Expected used memory to be 0 after all releases, got %d", mm.GetUsedMemory())
	}
}

func TestMemoryManager_TryAllocateReadBuffer(t *testing.T) {
	mm := NewMemoryManager(0, 10, 10)

	buf1 := mm.TryAllocateReadBuffer(6)
	if buf1 == nil {
		t.Fatalf("expected first TryAllocateReadBuffer to succeed")
	}
	if mm.GetUsedMemory() != 6 {
		t.Fatalf("expected used memory 6, got %d", mm.GetUsedMemory())
	}

	buf2 := mm.TryAllocateReadBuffer(6)
	if buf2 != nil {
		t.Fatalf("expected second TryAllocateReadBuffer to fail")
	}
	if mm.GetUsedMemory() != 6 {
		t.Fatalf("expected used memory still 6 after failed try, got %d", mm.GetUsedMemory())
	}

	mm.ReleaseReadBuffer(buf1)
	if mm.GetUsedMemory() != 0 {
		t.Fatalf("expected used memory 0 after release, got %d", mm.GetUsedMemory())
	}

	buf3 := mm.TryAllocateReadBuffer(10)
	if buf3 == nil {
		t.Fatalf("expected TryAllocateReadBuffer to succeed after release")
	}
	mm.ReleaseReadBuffer(buf3)
}

func TestMemoryManager_ReleaseUsesCapNotLen(t *testing.T) {
	mm := NewMemoryManager(0, 100, 100)

	buf := mm.AllocateWriteBuffer(10)
	if buf == nil {
		t.Fatalf("expected allocation to succeed")
	}
	if mm.GetUsedMemory() != 10 {
		t.Fatalf("expected used memory 10, got %d", mm.GetUsedMemory())
	}

	// Two-index slicing keeps capacity; release should free the entire reservation.
	prefix := buf[:3]
	mm.ReleaseWriteBuffer(prefix)
	if mm.GetUsedMemory() != 0 {
		t.Fatalf("expected used memory 0 after releasing a sliced buffer, got %d", mm.GetUsedMemory())
	}
}

func TestMemoryManager_AllocateBlocksUntilRelease_TotalBudget(t *testing.T) {
	mm := NewMemoryManager(0, 10, 10)

	buf := mm.AllocateReadBuffer(10)
	if buf == nil {
		t.Fatalf("expected initial allocation to succeed")
	}

	allocated := make(chan []byte, 1)
	go func() {
		allocated <- mm.AllocateReadBuffer(1)
	}()

	assertNoRecvWithin(t, allocated, 50*time.Millisecond, "expected allocation to block when total budget exhausted")
	mm.ReleaseReadBuffer(buf)

	buf2 := recvWithin(t, allocated, 500*time.Millisecond, "expected allocation to proceed after release")
	if buf2 == nil {
		t.Fatalf("expected blocked allocation to succeed after release")
	}
	mm.ReleaseReadBuffer(buf2)
	if mm.GetUsedMemory() != 0 {
		t.Fatalf("expected used memory 0 at end, got %d", mm.GetUsedMemory())
	}
}

func TestMemoryManager_AllocateBlocksUntilRelease_ModuleBudgetRead(t *testing.T) {
	mm := NewMemoryManager(0, 20, 10)

	buf := mm.AllocateReadBuffer(10)
	if buf == nil {
		t.Fatalf("expected initial allocation to succeed")
	}
	if mm.GetUsedReadMemory() != 10 {
		t.Fatalf("expected used read memory 10, got %d", mm.GetUsedReadMemory())
	}
	if mm.GetUsedMemory() != 10 {
		t.Fatalf("expected used total memory 10, got %d", mm.GetUsedMemory())
	}

	allocated := make(chan []byte, 1)
	go func() {
		allocated <- mm.AllocateReadBuffer(1)
	}()

	assertNoRecvWithin(t, allocated, 50*time.Millisecond, "expected allocation to block when read module budget exhausted")
	mm.ReleaseReadBuffer(buf)

	buf2 := recvWithin(t, allocated, 500*time.Millisecond, "expected allocation to proceed after releasing read budget")
	if buf2 == nil {
		t.Fatalf("expected blocked allocation to succeed")
	}
	mm.ReleaseReadBuffer(buf2)
	if mm.GetUsedMemory() != 0 || mm.GetUsedReadMemory() != 0 {
		t.Fatalf("expected all counters 0 at end, got total=%d read=%d", mm.GetUsedMemory(), mm.GetUsedReadMemory())
	}
}

func TestMemoryManager_ReadAndWriteShareTotalButSeparateModuleBudgets(t *testing.T) {
	mm := NewMemoryManager(0, 20, 15)

	readBuf := mm.AllocateReadBuffer(10)
	if readBuf == nil {
		t.Fatalf("expected read allocation to succeed")
	}
	writeBuf := mm.AllocateWriteBuffer(10)
	if writeBuf == nil {
		t.Fatalf("expected write allocation to succeed")
	}
	if mm.GetUsedMemory() != 20 || mm.GetUsedReadMemory() != 10 || mm.GetUsedWriteMemory() != 10 {
		t.Fatalf("unexpected counters: total=%d read=%d write=%d", mm.GetUsedMemory(), mm.GetUsedReadMemory(), mm.GetUsedWriteMemory())
	}

	// Total exhausted; a further allocation (even in a different module) must block.
	allocated := make(chan []byte, 1)
	go func() {
		allocated <- mm.AllocateWriteBuffer(1)
	}()
	assertNoRecvWithin(t, allocated, 50*time.Millisecond, "expected allocation to block when total budget exhausted")

	mm.ReleaseReadBuffer(readBuf)
	buf2 := recvWithin(t, allocated, 500*time.Millisecond, "expected blocked allocation to proceed after releasing total budget")
	if buf2 == nil {
		t.Fatalf("expected blocked allocation to succeed")
	}

	mm.ReleaseWriteBuffer(buf2)
	mm.ReleaseWriteBuffer(writeBuf)
	if mm.GetUsedMemory() != 0 || mm.GetUsedReadMemory() != 0 || mm.GetUsedWriteMemory() != 0 {
		t.Fatalf("expected all counters 0 at end, got total=%d read=%d write=%d", mm.GetUsedMemory(), mm.GetUsedReadMemory(), mm.GetUsedWriteMemory())
	}
}

func TestMemoryManager_TryAllocateWriteBuffer_RespectsBudgets(t *testing.T) {
	mm := NewMemoryManager(0, 10, 6)

	buf := mm.TryAllocateWriteBuffer(6)
	if buf == nil {
		t.Fatalf("expected TryAllocateWriteBuffer(6) to succeed")
	}
	if mm.GetUsedMemory() != 6 || mm.GetUsedWriteMemory() != 6 {
		t.Fatalf("unexpected counters: total=%d write=%d", mm.GetUsedMemory(), mm.GetUsedWriteMemory())
	}

	buf2 := mm.TryAllocateWriteBuffer(1)
	if buf2 != nil {
		t.Fatalf("expected TryAllocateWriteBuffer(1) to fail due to write module budget")
	}

	mm.ReleaseWriteBuffer(buf)
	if mm.GetUsedMemory() != 0 || mm.GetUsedWriteMemory() != 0 {
		t.Fatalf("expected counters to return to 0, got total=%d write=%d", mm.GetUsedMemory(), mm.GetUsedWriteMemory())
	}
}

func TestMemoryManager_ZeroAndNegativeSizes(t *testing.T) {
	mm := NewMemoryManager(0, 10, 10)

	buf0 := mm.AllocateReadBuffer(0)
	if buf0 == nil {
		t.Fatalf("expected AllocateReadBuffer(0) to return empty slice, got nil")
	}
	if len(buf0) != 0 {
		t.Fatalf("expected empty slice for size 0, got len=%d", len(buf0))
	}
	if mm.GetUsedMemory() != 0 {
		t.Fatalf("expected counters unchanged for size 0, got total=%d", mm.GetUsedMemory())
	}

	bufNeg := mm.AllocateWriteBuffer(-1)
	if bufNeg == nil {
		t.Fatalf("expected AllocateWriteBuffer(-1) to return empty slice, got nil")
	}
	if len(bufNeg) != 0 {
		t.Fatalf("expected empty slice for negative size, got len=%d", len(bufNeg))
	}
	if mm.GetUsedMemory() != 0 {
		t.Fatalf("expected counters unchanged for negative size, got total=%d", mm.GetUsedMemory())
	}

	// Ensure releasing nil/empty doesn't crash and doesn't perturb counters.
	mm.ReleaseReadBuffer(nil)
	mm.ReleaseWriteBuffer([]byte{})
	if mm.GetUsedMemory() != 0 {
		t.Fatalf("expected counters still 0 after releasing nil/empty, got total=%d", mm.GetUsedMemory())
	}

	// Encourage goroutines/timers used by other tests to run promptly on busy builders.
	runtime.Gosched()
}
