package dxfuse

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/mem"
)

type MemoryManager struct {
	mutex                   sync.Mutex
	maxMemory               int64         // Maximum memory allowed (in bytes)
	maxMemoryUsagePerModule int64         // Maximum memory (in bytes) a single module can use
	usedMemory              int64         // Currently used memory (in bytes)
	readsWaiting            int32         // Number of prefetch threads waiting for memory
	writesWaiting           int32         // Number of write threads waiting for memory
	writeMemory             int64         // Memory allocated for writes and uploads
	readMemory              int64         // Memory allocated for read cache and prefetch
	verboseLevel            int           // Verbose level for logging
	notifyChan              chan struct{} // Channel for notifying waiting goroutines
}

func NewMemoryManager(verboseLevel int, maxMemory int64, maxMemoryUsagePerModule int64) *MemoryManager {
	mm := &MemoryManager{
		maxMemory:               maxMemory,
		maxMemoryUsagePerModule: maxMemoryUsagePerModule,
		usedMemory:              0,
		verboseLevel:            verboseLevel,
		notifyChan:              make(chan struct{}, 1), // Buffered channel to avoid blocking
	}

	// Trigger garbage collection and log memory usage every 30 seconds
	go func() {
		i := 0
		for {
			runtime.GC()
			if verboseLevel > 1 {
				vmStat, err := mem.VirtualMemory()
				if err == nil {
					mm.debug("System memory: total=%.2f MiB, free=%.2f MiB, used=%.2f MiB",
						float64(vmStat.Total)/1024/1024,
						float64(vmStat.Free)/1024/1024,
						float64(vmStat.Used)/1024/1024)
				}

				// log MemoryManager usage
				mm.debug("MemoryManager: usedMemory=%.2f MiB, writeMemory=%.2f MiB, readMemory=%.2f MiB, readsWaiting=%d, writesWaiting=%d",
					float64(mm.GetUsedMemory())/1024/1024,
					float64(mm.GetUsedWriteMemory())/1024/1024,
					float64(mm.GetUsedReadMemory())/1024/1024,
					atomic.LoadInt32(&mm.readsWaiting),
					atomic.LoadInt32(&mm.writesWaiting))

				// Log Go runtime memory usage
				var memStats runtime.MemStats
				runtime.ReadMemStats(&memStats)
				mm.debug("Go runtime memory: Alloc=%.2f MiB, Sys=%.2f MiB, HeapAlloc=%.2f MiB, HeapSys=%.2f MiB",
					float64(memStats.Alloc)/1024/1024,
					float64(memStats.Sys)/1024/1024,
					float64(memStats.HeapAlloc)/1024/1024,
					float64(memStats.HeapSys)/1024/1024)
			}
			i++
			time.Sleep(30 * time.Second)
		}
	}()

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
	return mm.allocate(size, false, false)
}

func (mm *MemoryManager) AllocateWriteBuffer(size int64) []byte {
	return mm.allocate(size, true, true)
}

func (mm *MemoryManager) AllocateReadBufferWait(size int64) []byte {
	return mm.allocate(size, false, true)
}

func (mm *MemoryManager) ReleaseReadBuffer(buf []byte) {
	mm.debug("Releasing read buffer of size %d", len(buf))
	mm.release(buf, false)
}

func (mm *MemoryManager) ReleaseWriteBuffer(buf []byte) {
	mm.release(buf, true)
}

// Internal helper functions for allocation and release
func (mm *MemoryManager) allocate(size int64, isWriteBuffer bool, waitIndefinitely bool) []byte {
	mm.mutex.Lock()
	if isWriteBuffer {
		atomic.AddInt32(&mm.writesWaiting, 1)
	} else {
		atomic.AddInt32(&mm.readsWaiting, 1)
	}
	defer func() {
		if isWriteBuffer {
			atomic.AddInt32(&mm.writesWaiting, -1)
		} else {
			atomic.AddInt32(&mm.readsWaiting, -1)
		}
		mm.mutex.Unlock()
	}()

	for atomic.LoadInt64(&mm.usedMemory)+size > mm.maxMemory ||
		(isWriteBuffer && atomic.LoadInt64(&mm.writeMemory)+size > mm.maxMemoryUsagePerModule) ||
		(!isWriteBuffer && atomic.LoadInt64(&mm.readMemory)+size > mm.maxMemoryUsagePerModule) ||
		(!isWriteBuffer && atomic.LoadInt32(&mm.readsWaiting) > 1) {
		if !waitIndefinitely || size > mm.maxMemory {
			// If we can't wait indefinitely or the requested buffer size exceeds maxMemory, return nil
			mm.debug("Memory allocation failed: waitIndefinitely=%v, isWriteBuffer=%v size=%d, maxMemory=%d, usedMemory=%d, writeMemory=%d, readMemory=%d", waitIndefinitely, isWriteBuffer, size, mm.maxMemory, mm.GetUsedMemory(), mm.GetUsedWriteMemory(), mm.GetUsedReadMemory())
			return nil
		}

		// Wait for notification without holding the mutex
		mm.mutex.Unlock()
		<-mm.notifyChan
		mm.mutex.Lock()
	}

	atomic.AddInt64(&mm.usedMemory, size)
	if isWriteBuffer {
		atomic.AddInt64(&mm.writeMemory, size)
	} else {
		atomic.AddInt64(&mm.readMemory, size)
	}

	return make([]byte, size)
}

func (mm *MemoryManager) release(buf []byte, isWriteBuffer bool) {
	size := int64(len(buf))
	// Release the buffer
	buf = nil
	atomic.AddInt64(&mm.usedMemory, -size)
	mm.debug("Added used mem")
	if isWriteBuffer {
		atomic.AddInt64(&mm.writeMemory, -size)
	} else {
		atomic.AddInt64(&mm.readMemory, -size)
		mm.debug("Added read mem")
	}

	if atomic.LoadInt64(&mm.usedMemory) < 0 {
		atomic.StoreInt64(&mm.usedMemory, 0)
	}
	mm.debug("Released buffer of size %d, usedMemory=%d, writeMemory=%d, readMemory=%d", size, mm.GetUsedMemory(), mm.GetUsedWriteMemory(), mm.GetUsedReadMemory())

	mm.notify() // Notify waiting allocate()
}

func (mm *MemoryManager) notify() {
	select {
	case mm.notifyChan <- struct{}{}:
		// Notify a waiting goroutine
	default:
		// Do nothing if the channel is already full
	}
	mm.debug("Notified routines waiting for memory allocation")
}

func (mm *MemoryManager) GetUsedMemory() int64 {
	return atomic.LoadInt64(&mm.usedMemory)
}

func (mm *MemoryManager) GetUsedReadMemory() int64 {
	return atomic.LoadInt64(&mm.readMemory)
}

func (mm *MemoryManager) GetUsedWriteMemory() int64 {
	return atomic.LoadInt64(&mm.writeMemory)
}

func (mm *MemoryManager) TrimWriteBuffer(buf []byte) []byte {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()
	newSize := int64(len(buf))
	sizeDiff := newSize - int64(cap(buf))

	// Shrink the buffer first, then decrement memory usage
	buf = buf[:newSize]
	atomic.AddInt64(&mm.usedMemory, sizeDiff) // sizeDiff is negative, so this reduces usedMemory
	atomic.AddInt64(&mm.writeMemory, sizeDiff)
	mm.notify()
	return buf
}
