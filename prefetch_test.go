package dxfuse

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jacobsa/fuse/fuseops"
)

func TestCalcPrefetchHeuristics_NumPrefetchThreadsBounds(t *testing.T) {
	{
		h := calcPrefetchHeuristics(1, "", 1024*MiB)
		if h.numPrefetchThreads != 10 {
			t.Fatalf("expected 10 threads for 1 CPU, got %d", h.numPrefetchThreads)
		}
	}
	{
		h := calcPrefetchHeuristics(128, "", 1024*MiB)
		if h.numPrefetchThreads != maxNumPrefetchThreads {
			t.Fatalf("expected threads capped at %d, got %d", maxNumPrefetchThreads, h.numPrefetchThreads)
		}
	}
}

func TestCalcPrefetchHeuristics_PrefetchMaxIoSizeDependsOnJob(t *testing.T) {
	h1 := calcPrefetchHeuristics(4, "", 1024*MiB)
	if h1.prefetchMaxIoSize != 16*MiB {
		t.Fatalf("expected 16MiB outside jobs, got %d", h1.prefetchMaxIoSize)
	}

	h2 := calcPrefetchHeuristics(4, "job-123", 1024*MiB)
	if h2.prefetchMaxIoSize != 64*MiB {
		t.Fatalf("expected 64MiB in jobs, got %d", h2.prefetchMaxIoSize)
	}
}

func TestCalcPrefetchHeuristics_MinEntriesEnforcedEvenUnderLowMemory(t *testing.T) {
	// With tiny maxMemoryUsagePerModule, the raw computed value would be 0; we still
	// enforce a minimum table size.
	h := calcPrefetchHeuristics(2, "", 1*MiB)
	if h.maxNumEntriesInTable != minNumEntriesInTable {
		t.Fatalf("expected min entries %d, got %d", minNumEntriesInTable, h.maxNumEntriesInTable)
	}
	if h.maxNumChunksReadAhead < 1 {
		t.Fatalf("expected at least 1 read-ahead chunk, got %d", h.maxNumChunksReadAhead)
	}
	if h.ioQueueDepth < 1 {
		t.Fatalf("expected ioQueueDepth at least 1, got %d", h.ioQueueDepth)
	}
}

func TestCalcPrefetchHeuristics_EntriesLimitedByCPUs(t *testing.T) {
	// Very large memory would allow many entries, but we also cap by numCPUs*4.
	h := calcPrefetchHeuristics(2, "", 1<<50) // huge
	if h.maxNumEntriesInTable != 8 {
		t.Fatalf("expected entries capped to numCPUs*4=8, got %d", h.maxNumEntriesInTable)
	}
}

func TestPrefetch_AttemptCreateStreamEntryHeuristics(t *testing.T) {
	mm := NewMemoryManager(0, 1024*MiB, 1024*MiB)
	pgs := &PrefetchGlobalState{
		handlesInfo:          make(map[fuseops.HandleID]*PrefetchFileMetadata),
		nonSequentialHandles: make(map[fuseops.HandleID]bool),
		maxNumEntriesInTable: 1,
		memoryManager:        mm,
	}

	bigFile := File{Inode: 1, Id: "file-1", Size: minFileSize + 1}
	url := DxDownloadURL{URL: "http://example", Headers: map[string]string{}}

	if ok := pgs.attemptCreateStreamEntry(1, bigFile, url); !ok {
		t.Fatalf("expected first stream entry to be created")
	}
	if ok := pgs.attemptCreateStreamEntry(1, bigFile, url); ok {
		t.Fatalf("expected duplicate stream entry to be rejected")
	}

	// Table is full.
	if ok := pgs.attemptCreateStreamEntry(2, bigFile, url); ok {
		t.Fatalf("expected stream entry to be rejected when table full")
	}

	// File too small.
	pgs2 := &PrefetchGlobalState{
		handlesInfo:          make(map[fuseops.HandleID]*PrefetchFileMetadata),
		nonSequentialHandles: make(map[fuseops.HandleID]bool),
		maxNumEntriesInTable: 10,
		memoryManager:        mm,
	}
	smallFile := File{Inode: 2, Id: "file-2", Size: minFileSize - 1}
	if ok := pgs2.attemptCreateStreamEntry(3, smallFile, url); ok {
		t.Fatalf("expected small file to be rejected")
	}

	// Marked as non-sequential.
	pgs2.nonSequentialHandles[4] = true
	if ok := pgs2.attemptCreateStreamEntry(4, bigFile, url); ok {
		t.Fatalf("expected nonSequential handle to be rejected")
	}
}

func TestPrefetch_IsWorthItUsesTimeWindow(t *testing.T) {
	pgs := &PrefetchGlobalState{}

	now := time.Now()
	pfm := &PrefetchFileMetadata{lastIoTimestamp: now.Add(-maxDeltaTime - time.Second)}
	if pgs.isWorthIt(pfm, now) {
		t.Fatalf("expected isWorthIt=false when last IO is older than maxDeltaTime")
	}

	pfm2 := &PrefetchFileMetadata{lastIoTimestamp: now.Add(-maxDeltaTime + time.Second)}
	if !pgs.isWorthIt(pfm2, now) {
		t.Fatalf("expected isWorthIt=true when last IO is recent")
	}
}

func TestPrefetch_FirstAccessCreatesAlignedInitialCache(t *testing.T) {
	mm := NewMemoryManager(0, 1024*MiB, 1024*MiB)
	pgs := &PrefetchGlobalState{memoryManager: mm}
	pfm := &PrefetchFileMetadata{mutex: sync.Mutex{}}

	ofs := int64(12345)
	pgs.firstAccessToStream(pfm, ofs)

	if pfm.cache.prefetchIoSize != prefetchMinIoSize {
		t.Fatalf("expected prefetchIoSize=%d, got %d", prefetchMinIoSize, pfm.cache.prefetchIoSize)
	}
	if len(pfm.cache.iovecs) != 2 {
		t.Fatalf("expected 2 initial iovecs, got %d", len(pfm.cache.iovecs))
	}
	if pfm.cache.iovecs[0] == nil || pfm.cache.iovecs[1] == nil {
		t.Fatalf("expected non-nil iovecs")
	}
	start0 := pfm.cache.iovecs[0].startByte
	if start0%(4*KiB) != 0 {
		t.Fatalf("expected first iovec start to be 4KiB-aligned, got %d", start0)
	}
	if start0 > ofs {
		t.Fatalf("expected first iovec start <= ofs, got start=%d ofs=%d", start0, ofs)
	}
	if pfm.cache.iovecs[0].endByte != start0+prefetchMinIoSize-1 {
		t.Fatalf("unexpected first iovec end=%d", pfm.cache.iovecs[0].endByte)
	}
	if pfm.cache.iovecs[1].startByte != pfm.cache.iovecs[0].startByte+prefetchMinIoSize {
		t.Fatalf("unexpected second iovec start=%d", pfm.cache.iovecs[1].startByte)
	}
}

func TestPrefetch_AddIoReqToCache_MemoryPressureBecomesHole(t *testing.T) {
	mm := NewMemoryManager(0, 1024*MiB, 1024*MiB)
	pgs := &PrefetchGlobalState{memoryManager: mm}
	pfm := &PrefetchFileMetadata{mutex: sync.Mutex{}}

	iov := &Iovec{startByte: 0, endByte: 9, state: IOV_IN_FLIGHT, cond: sync.NewCond(&pfm.mutex)}
	pfm.cache = Cache{iovecs: []*Iovec{iov}}

	ioReq := IoReq{startByte: 0, endByte: 9}
	pgs.addIoReqToCache(pfm, ioReq, nil, errPrefetchNoMemory)

	if iov.state != IOV_HOLE {
		t.Fatalf("expected IOV_HOLE on memory pressure, got %d", iov.state)
	}
	if iov.data != nil {
		t.Fatalf("expected no data to be stored on memory pressure")
	}
}

func TestPrefetch_AddIoReqToCache_NonMemoryErrorReleasesBufferAndMarksErrored(t *testing.T) {
	mm := NewMemoryManager(0, 1024*MiB, 1024*MiB)
	pgs := &PrefetchGlobalState{memoryManager: mm}
	pfm := &PrefetchFileMetadata{mutex: sync.Mutex{}}

	buf := mm.AllocateReadBuffer(10)
	if buf == nil {
		t.Fatalf("expected buffer allocation")
	}

	iov := &Iovec{startByte: 0, endByte: 9, state: IOV_IN_FLIGHT, cond: sync.NewCond(&pfm.mutex)}
	pfm.cache = Cache{iovecs: []*Iovec{iov}}

	ioReq := IoReq{startByte: 0, endByte: 9}
	pgs.addIoReqToCache(pfm, ioReq, buf, errors.New("boom"))

	if iov.state != IOV_ERRORED {
		t.Fatalf("expected IOV_ERRORED on error, got %d", iov.state)
	}
	if mm.GetUsedReadMemory() != 0 {
		t.Fatalf("expected buffer to be released on error, usedRead=%d", mm.GetUsedReadMemory())
	}
}

func TestPrefetch_AddIoReqToCache_SuccessStoresDataAndMarksDone(t *testing.T) {
	mm := NewMemoryManager(0, 1024*MiB, 1024*MiB)
	pgs := &PrefetchGlobalState{memoryManager: mm}
	pfm := &PrefetchFileMetadata{mutex: sync.Mutex{}}

	data := mm.AllocateReadBuffer(10)
	if data == nil {
		t.Fatalf("expected data buffer allocation")
	}
	data[0] = 7

	iov := &Iovec{startByte: 0, endByte: 9, state: IOV_IN_FLIGHT, cond: sync.NewCond(&pfm.mutex)}
	pfm.cache = Cache{iovecs: []*Iovec{iov}}
	pfm.mw = MeasureWindow{timestamp: time.Now()}

	ioReq := IoReq{startByte: 0, endByte: 9}
	pgs.addIoReqToCache(pfm, ioReq, data, nil)

	if iov.state != IOV_DONE {
		t.Fatalf("expected IOV_DONE on success, got %d", iov.state)
	}
	if iov.data == nil || iov.data[0] != 7 {
		t.Fatalf("expected data to be stored in cache")
	}
	if pfm.mw.numBytesPrefetched != 10 {
		t.Fatalf("expected numBytesPrefetched=10, got %d", pfm.mw.numBytesPrefetched)
	}
	if pfm.mw.numPrefetchIOs != 1 {
		t.Fatalf("expected numPrefetchIOs=1, got %d", pfm.mw.numPrefetchIOs)
	}

	// Cleanup: resetPfm would release this in the real system.
	mm.ReleaseReadBuffer(iov.data)
	if mm.GetUsedReadMemory() != 0 {
		t.Fatalf("expected cleanup release to return usedRead=0")
	}
}

func TestPrefetch_AddIoReqToCache_DroppedIoReleasesData(t *testing.T) {
	mm := NewMemoryManager(0, 1024*MiB, 1024*MiB)
	pgs := &PrefetchGlobalState{memoryManager: mm}
	pfm := &PrefetchFileMetadata{mutex: sync.Mutex{}}

	// Cache is empty; addIoReqToCache will not find a matching placeholder.
	pfm.cache = Cache{iovecs: []*Iovec{}}

	data := mm.AllocateReadBuffer(10)
	if data == nil {
		t.Fatalf("expected data buffer allocation")
	}
	if mm.GetUsedReadMemory() != 10 {
		t.Fatalf("expected usedRead=10, got %d", mm.GetUsedReadMemory())
	}

	ioReq := IoReq{startByte: 0, endByte: 9}
	pgs.addIoReqToCache(pfm, ioReq, data, nil)

	if mm.GetUsedReadMemory() != 0 {
		t.Fatalf("expected data to be released when IO is dropped, usedRead=%d", mm.GetUsedReadMemory())
	}
}

func TestPrefetch_MoveCacheWindow_DoesNotBlockWhenQueueFull(t *testing.T) {
	mm := NewMemoryManager(0, 1024*MiB, 1024*MiB)
	pgs := &PrefetchGlobalState{
		verboseLevel: 2,
		ioQueue:      make(chan IoReq), // unbuffered, nobody receives
		prefetchMaxIoSize: 16 * MiB,
		memoryManager:     mm,
	}

	pfm := &PrefetchFileMetadata{mutex: sync.Mutex{}, size: 100 * MiB}
	// Seed cache with two iovecs so iovIndex=1 triggers read-ahead attempt.
	pfm.cache = Cache{
		prefetchIoSize: prefetchMinIoSize,
		maxNumIovecs:   2,
		iovecs: []*Iovec{
			{startByte: 0, endByte: prefetchMinIoSize - 1, ioSize: prefetchMinIoSize, state: IOV_DONE, data: nil, cond: sync.NewCond(&pfm.mutex)},
			{startByte: prefetchMinIoSize, endByte: 2*prefetchMinIoSize - 1, ioSize: prefetchMinIoSize, state: IOV_DONE, data: nil, cond: sync.NewCond(&pfm.mutex)},
		},
		startByte: 0,
		endByte:   2*prefetchMinIoSize - 1,
	}

	done := make(chan struct{})
	go func() {
		pfm.mutex.Lock()
		defer pfm.mutex.Unlock()
		pgs.moveCacheWindow(pfm, 1)
		close(done)
	}()

	select {
	case <-done:
		// ok
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("moveCacheWindow appears to have blocked on queue send")
	}

	// Since enqueue should fail, we should not have appended a placeholder.
	if len(pfm.cache.iovecs) != 2 {
		t.Fatalf("expected no new iovecs appended when enqueue fails, got %d", len(pfm.cache.iovecs))
	}
}
