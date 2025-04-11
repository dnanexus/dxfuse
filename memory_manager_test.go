package dxfuse

import (
	"sync"
	"testing"
)

func TestMemoryManager_AllocateAndReleaseWriteBuffer(t *testing.T) {
	maxMemory := int64(64 * 1024 * 1024)       // 64 MiB
	maxModuleMemory := int64(32 * 1024 * 1024) // 32 MiB
	mm := NewMemoryManager(maxMemory, maxModuleMemory)

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
	mm := NewMemoryManager(maxMemory, maxModuleMemory)

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
	mm := NewMemoryManager(maxMemory, maxModuleMemory)

	// Try to allocate more than max memory
	buf := mm.AllocateWriteBuffer(64 * 1024 * 1024) // 64 MiB
	if buf != nil {
		t.Fatalf("Expected allocation to fail, but it succeeded")
	}

	if mm.GetUsedMemory() != 0 {
		t.Errorf("Expected used memory to be 0 after failed allocation, got %d", mm.GetUsedMemory())
	}
}
