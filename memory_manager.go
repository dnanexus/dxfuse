package dxfuse

import (
	"runtime"
	"sync"
	"time"

	"github.com/shirou/gopsutil/mem"
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

	// Periodically log system and Go runtime memory usage
	go func() {
		for {
			// Use gopsutil to get system memory stats
			vmStat, err := mem.VirtualMemory()
			if err == nil {
				mm.log("System memory: total=%.2f MiB, free=%.2f MiB, used=%.2f MiB",
					float64(vmStat.Total)/1024/1024,
					float64(vmStat.Free)/1024/1024,
					float64(vmStat.Used)/1024/1024)
			} else {
				mm.log("Error fetching system memory stats: %v", err)
			}

			// Log Go runtime memory usage
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)
			mm.log("Go runtime memory: Alloc=%.2f MiB, Sys=%.2f MiB, HeapAlloc=%.2f MiB, HeapSys=%.2f MiB",
				float64(memStats.Alloc)/1024/1024,
				float64(memStats.Sys)/1024/1024,
				float64(memStats.HeapAlloc)/1024/1024,
				float64(memStats.HeapSys)/1024/1024)

			time.Sleep(5 * time.Second) // Log every 5 seconds
		}
	}()

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
	} else {
		mm.readsWaiting++
	}
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
		if !waitIndefinitely || size > mm.maxMemory {
			// If we can't wait indefinitely or the requested buffer size exceeds maxMemory, return nil
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

	// Log memory usage in MiB
	mm.log("Memory stats after allocate: used=%.2f MiB, write=%.2f MiB, read=%.2f MiB, readsWaiting=%d, writesWaiting=%d",
		float64(mm.usedMemory)/1024/1024,
		float64(mm.writeMemory)/1024/1024,
		float64(mm.readMemory)/1024/1024,
		mm.readsWaiting,
		mm.writesWaiting)

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

	// Log memory usage in MiB
	mm.log("Memory stats after release: used=%.2f MiB, write=%.2f MiB, read=%.2f MiB, readsWaiting=%d, writesWaiting=%d",
		float64(mm.usedMemory)/1024/1024,
		float64(mm.writeMemory)/1024/1024,
		float64(mm.readMemory)/1024/1024,
		mm.readsWaiting, mm.writesWaiting)

	// Log how long the garbage collection takes to execute
	// start := time.Now()
	// runtime.GC()
	// duration := time.Since(start)
	// mm.log("Garbage collection took %s", duration)

	mm.cond.Broadcast()
}

// Get the current memory usage.
func (mm *MemoryManager) GetUsedMemory() int64 {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()
	return mm.usedMemory
}

func (mm *MemoryManager) ResizeWriteBuffer(buf []byte, newSize int64) []byte {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	sizeDiff := newSize - int64(len(buf))
	if sizeDiff <= 0 {
		// Shrink the buffer first, then decrement memory usage
		buf = buf[:newSize]
		mm.usedMemory += sizeDiff // sizeDiff is negative, so this reduces usedMemory
		mm.writeMemory += sizeDiff
		mm.log("Shrinking write buffer to %d bytes", newSize)
		// Log memory usage in MiB
		mm.log("Memory stats after shrink: used=%.2f MiB, write=%.2f MiB, read=%.2f MiB, readsWaiting=%d, writesWaiting=%d",
			float64(mm.usedMemory)/1024/1024,
			float64(mm.writeMemory)/1024/1024,
			float64(mm.readMemory)/1024/1024,
			mm.readsWaiting,
			mm.writesWaiting)
		return buf
	}

	// Check if we have enough memory to resize
	for mm.usedMemory+sizeDiff > mm.maxMemory || mm.writeMemory+sizeDiff > mm.maxMemoryUsagePerModule {
		mm.cond.Wait()
	}

	// Update memory usage
	mm.usedMemory += sizeDiff
	mm.writeMemory += sizeDiff

	mm.log("Resizing write buffer to %d bytes", newSize)
	// Log memory usage in MiB
	mm.log("Memory stats after resize: used=%.2f MiB, write=%.2f MiB, read=%.2f MiB, readsWaiting=%d, writesWaiting=%d",
		float64(mm.usedMemory)/1024/1024,
		float64(mm.writeMemory)/1024/1024,
		float64(mm.readMemory)/1024/1024,
		mm.readsWaiting,
		mm.writesWaiting)

	// Create a new buffer with the new size and copy the old data
	newBuf := make([]byte, newSize)
	copy(newBuf, buf)
	return newBuf
}
