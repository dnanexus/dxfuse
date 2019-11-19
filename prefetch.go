package dxfuse

// When a file is opened, it is added to the global prefetch map. Once removed,
// it can never return. This means that to check if a file is being streamed, all
// we need to do is check the map.
import (
	"context"
	"fmt"
	"log"
	"math/bits"
	"runtime"
	"sync"
	"time"

	"github.com/dnanexus/dxda"
	"github.com/hashicorp/go-retryablehttp" // use http libraries from hashicorp for implement retry logic
)

const (
	maxDeltaTime = 10 * 60 * time.Second
	periodicTime = 30 * time.Second

	prefetchMinIoSize = (256 * KiB)   // threshold for deciding the file is sequentially accessed
	prefetchMaxIoSize = (16 * MiB)    // maximal size of prefetch IO
	prefetchIoFactor = 4

	numSlotsInChunk = 64

	// An active stream can use a significant amount of memory to store prefetched data.
	// Limit the total number of streams we are tracking and prefetching.
	maxNumEntriesInTable = 10

	// maximum number of prefetch threads, regardless of machine size
	maxNumPrefetchThreads = 32

	minFileSize = 2 * MiB     // do not track files smaller than this size

	// An prefetch request time limit
	readRequestTimeout = 3 * 60 * time.Second
)

const (
	// enumerated type for the state of a PFM (file metadata)
	PFM_DETECT_SEQ = 1      // Beginning of the file, detecting if access is sequential
	PFM_PREFETCH_IN_PROGRESS = 2  // normal case --- prefetch is ongoing
	PFM_EOF = 3   // reached the end of the file

	// state of an io-vector
	IOV_HOLE = 1      // empty
	IOV_IN_FLIGHT = 2 // in progress
	IOV_DONE = 3      // completed successfully
	IOV_ERRORED = 4   // completed with an error
)

// A request that one of the IO-threads will pick up
type IoReq struct {
	fh         *FileHandle
	url         DxDownloadURL

	ioSize      int64   // The io size
	startByte   int64   // start byte, counting from the beginning of the file.
	endByte     int64
}

type Iovec struct {
	ioSize         int64   // The io size
	startByte      int64   // start byte, counting from the beginning of the file.
	endByte        int64
	touched        uint64  // mark the areas that have been accessed by the user
	data         []byte

	// io-vector statue (ongoing, done, errored)
	state          int

	// Allow user reads to wait until prefetch IO complete
	cond                *sync.Cond
}

// A cache of all the data retrieved from the platform, for one file.
// It is a contiguous range of chunks. All IOs are the same size.
type Cache struct {
	prefetchIoSize     int64  // size of the IO to issue when prefetching
	maxNumIovecs       int

	startByte          int64
	endByte            int64
	iovecs             [](*Iovec)
}

type MeasureWindow struct {
	timestamp            time.Time
	numIOs               int
	numBytesPrefetched   int64
	numPrefetchIOs       int
}

type PrefetchFileMetadata struct {
	// the file being tracked
	fh                  *FileHandle
	state                int

	lastIoTimestamp      time.Time  // Last time an IO hit this file
	hiUserAccessOfs      int64      // highest file offset accessed by the user
	sequentiallyAccessed bool       // estimate if this file is sequentially accessed
	mw                   MeasureWindow // statistics for stream

	// cached io vectors.
	// The assumption is that the user is accessing the last io-vector.
	// If this assumption isn't true, prefetch is ineffective. The algorithm
	// should detect and stop it.
	cache                Cache
}

func fileDesc(f File) string {
	return fmt.Sprintf("%d", f.Inode)
}

/*
Our iovec is A. The others are B, C, etc.
Here are the possible positions of A to any other range are:

           -------A------
---B---                     ----C-----
        ---D--  --E--  ---F---
        ---------------G---------------
*/
func (iov Iovec) intersect(startOfs int64, endOfs int64) bool {
	if iov.startByte > endOfs {
		// case B
		return false
	}
	if iov.endByte < startOfs {
		// case C
		return false
	}
	// there is some intersection
	return true
}

// presumption: there is some intersection
func (iov Iovec) intersectBuffer(startOfs int64, endOfs int64) []byte {
	// these are offsets in the entire file
	bgnByte := MaxInt64(iov.startByte, startOfs)
	endByte := MinInt64(iov.endByte, endOfs)

	// normalize, to offets inside the buffer
	bgnByte -= iov.startByte
	endByte -= iov.startByte

	return iov.data[bgnByte : endByte+1]
}

func (iov Iovec) copyBuffer(startOfs int64, endOfs int64, data []byte) int {
	subBuf := iov.intersectBuffer(startOfs, endOfs)
	len := copy(data, subBuf)
	return len
}

func (iov Iovec) stateString() string {
	switch iov.state {
	case IOV_HOLE: return "HOLE"
	case IOV_IN_FLIGHT: return "IN_FLIGHT"
	case IOV_DONE: return "DONE"
	case IOV_ERRORED: return "ERRORED"
	default:
		panic(fmt.Sprintf("bad state for iovec %d", iov.state))
	}
}

// write a log message, and add a header
func (pfm PrefetchFileMetadata) log(a string, args ...interface{}) {
	hdr := fmt.Sprintf("prefetch(%s)", fileDesc(pfm.fh.f))
	LogMsg(hdr, a, args...)
}

// report on the last delta time
func (pfm PrefetchFileMetadata) logReport(now time.Time) {
	delta := now.Sub(pfm.mw.timestamp)

	// bandwidth in megabytes per second
	bandwidthMiBSec := float64(pfm.mw.numBytesPrefetched) / (delta.Seconds() * MiB)

	pfm.log("numIovecs=%d size(cache)=%d #IOs=%d #prefetchIOs=%d bandwidth=%.1f MiB/sec",
		pfm.cache.maxNumIovecs, len(pfm.cache.iovecs),
		pfm.mw.numIOs, pfm.mw.numPrefetchIOs,
		bandwidthMiBSec)

	// reset the measurement window
	pfm.mw.timestamp = now
	pfm.mw.numIOs = 0
	pfm.mw.numBytesPrefetched = 0
	pfm.mw.numPrefetchIOs = 0
}


// global limits
type PrefetchGlobalState struct {
	verbose       bool
	verboseLevel  int
	mutex         sync.Mutex  // Lock used to control the files table
	files         map[*FileHandle](*PrefetchFileMetadata) // tracking state per file-id
	ioQueue       chan IoReq    // queue of IOs to prefetch
	wg            sync.WaitGroup
	numPrefetchThreads int
	maxNumChunksReadAhead int
}

// write a log message, and add a header
func (pgs *PrefetchGlobalState) log(a string, args ...interface{}) {
	LogMsg("prefetch", a, args...)
}

func NewPrefetchGlobalState(verboseLevel int) *PrefetchGlobalState {
	// We want to:
	// 1) allow all streams to have a worker available
	// 2) not have more than two workers per CPU
	// 3) not go over an overall limit, regardless of machine size
	numCPUs := runtime.NumCPU()
	numPrefetchThreads := MinInt(numCPUs * 2, maxNumPrefetchThreads)
	log.Printf("Number of prefetch threads=%d", numPrefetchThreads)

	// The number of read-ahead should be limited to 8
	maxNumChunksReadAhead := MinInt(8, numPrefetchThreads-1)
	maxNumChunksReadAhead = MaxInt(1, maxNumChunksReadAhead)

	// calculate how much memory will be used in the worst cast.
	// - Each stream uses two chunks.
	// - In addition, we are spreading around [maxNumChunksReadAhead] chunks.
	// Each chunk could be as large as [prefetchMaxIoSize].
	totalMemoryBytes := 2 * maxNumEntriesInTable * prefetchMaxIoSize
	totalMemoryBytes += maxNumChunksReadAhead * prefetchMaxIoSize

	log.Printf("maximal memory usage: %dMiB", totalMemoryBytes / MiB)
	log.Printf("number of prefetch worker threads: %d", numPrefetchThreads)
	log.Printf("maximal number of read-ahead chunks: %d", maxNumChunksReadAhead)

	pgs := &PrefetchGlobalState{
		verbose : verboseLevel >= 1,
		verboseLevel : verboseLevel,
		files : make(map[*FileHandle](*PrefetchFileMetadata)),
		ioQueue : make(chan IoReq),
		numPrefetchThreads: numPrefetchThreads,
		maxNumChunksReadAhead : maxNumChunksReadAhead,
	}

	// limit the number of prefetch IOs
	pgs.wg.Add(numPrefetchThreads)
	for i := 0; i < numPrefetchThreads; i++ {
		go pgs.prefetchIoWorker()
	}

	// start a periodic thread to cleanup the table if needed
	go pgs.tableCleanupWorker()

	return pgs
}

func (pgs *PrefetchGlobalState) Shutdown() {
	// signal all prefetch threads to stop
	close(pgs.ioQueue)

	// wait for all of them to complete
	pgs.wg.Wait()

	pgs.mutex.Lock()
	// clear the entire table
	for fh := range pgs.files {
		delete(pgs.files, fh)
	}
	pgs.mutex.Unlock()

	// we aren't waiting for the periodic cleanup thread.
	// it will take 60 seconds.
}

func check(value bool) {
	if !value {
		panic("assertion failed")
	}
}

// Got an error. Release all waiting IO.
func (pgs *PrefetchGlobalState) hitError(pfm *PrefetchFileMetadata) {
	for i, _ := range pfm.cache.iovecs {
		iovec := pfm.cache.iovecs[i]
		iovec.state = IOV_ERRORED
		iovec.cond.Broadcast()
	}
}

func (pgs *PrefetchGlobalState) readData(
	fh FileHandle,
	client *retryablehttp.Client,
	startByte int64,
	endByte int64,
	url DxDownloadURL) ([]byte, error) {
	// The data has not been prefetched. Get the data from DNAx with an
	// http request.
	expectedLen := endByte - startByte + 1
	if pgs.verbose {
		pgs.log("(%s) reading extent from DNAx ofs=%d len=%d", fileDesc(fh.f), startByte, expectedLen)
	}

	headers := make(map[string]string)

	// Copy the immutable headers
	for key, value := range url.Headers {
		headers[key] = value
	}
	headers["Range"] = fmt.Sprintf("bytes=%d-%d", startByte, endByte)

	// Safety procedure to force timeout to prevent hanging
	ctx, cancel := context.WithCancel(context.TODO())
	timer := time.AfterFunc(readRequestTimeout, func() {
		cancel()
	})
	defer timer.Stop()

	for tCnt := 0; tCnt < 3; tCnt++ {
		data, err := dxda.DxHttpRequest(ctx, client, NumRetriesDefault, "GET", url.URL, headers, []byte("{}"))
		if err != nil {
			if err == context.DeadlineExceeded || err == context.Canceled {
				continue
			}
			return nil, err
		}

		recvLen := int64(len(data))
		if recvLen != expectedLen {
			// retry (only) in the case of short read
			pgs.log("(%s) received length is wrong, got %d, expected %d. Retrying.",
				fileDesc(fh.f), recvLen, expectedLen)
			continue
		}

		if pgs.verbose {
			if err == nil {
				pgs.log("(%s) IO [%d -- %d] returned correctly",
					fileDesc(fh.f), startByte, endByte)
			} else {
				pgs.log("(%s) IO [%d -- %d] returned with error %s",
					fileDesc(fh.f), startByte, endByte, err.Error())
			}
		}
		return data, err
	}

	pgs.log("Did not received the data for IO [%d -- %d]", startByte, endByte)
	return nil, fmt.Errorf("Did not receive the data")
}

// Find the index for this chunk in the cache. The chunks may be different
// size, so we need to scan.
func findIovecIndex(pfm *PrefetchFileMetadata, ioReq IoReq) int {
	for i, iovec := range pfm.cache.iovecs {
		if iovec.startByte == ioReq.startByte &&
			iovec.endByte == ioReq.endByte {
			return i
		}
	}
	return -1
}

// We are holding the global lock at this point.
// Wake up waiting IOs, if any.
func (pgs *PrefetchGlobalState) addIoReqToCache(pfm *PrefetchFileMetadata, ioReq IoReq, data []byte, err error) {
	// Find the index for this chunk in the cache. The chunks may be different
	// size, so we need to scan.
	iovIdx := findIovecIndex(pfm, ioReq)
	if iovIdx == -1 {
		pfm.log("Dropping prefetch IO, matching entry in cache not found, ioReq=%v len(cache.iovecs)=%d",
			ioReq, len(pfm.cache.iovecs))
		return
	}
	check(pfm.cache.iovecs[iovIdx].data == nil)
	if err == nil {
		pfm.cache.iovecs[iovIdx].data = data
		pfm.cache.iovecs[iovIdx].state = IOV_DONE

		// statistics
		pfm.mw.numBytesPrefetched += int64(len(data))
		pfm.mw.numPrefetchIOs++
	} else {
		pfm.log("prefetch io error [%d -- %d] %s",
			ioReq.startByte, ioReq.endByte, err.Error())
		pfm.cache.iovecs[iovIdx].state = IOV_ERRORED
	}

	// wake up waiting user IOs
	pfm.cache.iovecs[iovIdx].cond.Broadcast()
}


func (pgs *PrefetchGlobalState) prefetchIoWorker() {
	// reuse this http client. The idea is to be able to reuse http connections.
	client := dxda.NewHttpClient(true)

	for true {
		ioReq, ok := <-pgs.ioQueue
		if !ok {
			pgs.wg.Done()
			return
		}

		// perform the IO. We don't want to hold any locks while we
		// are doing this, because this request could take a long time.
		data, err := pgs.readData(*ioReq.fh, client, ioReq.startByte, ioReq.endByte, ioReq.url)

		pgs.mutex.Lock()
		// Find the file this IO belongs to
		pfm, ok := pgs.files[ioReq.fh]
		if !ok {
			// file is not tracked anymore
			pgs.log("%s dropping prefetch IO [%d -- %d], file is no longer tracked",
				fileDesc(ioReq.fh.f), ioReq.startByte, ioReq.endByte)
		} else {
			pgs.addIoReqToCache(pfm, ioReq, data, err)
		}
		pgs.mutex.Unlock()
	}
}


// Check if a file is worth tracking.
func (pgs *PrefetchGlobalState) isWorthIt(pfm *PrefetchFileMetadata, now time.Time) bool {
	if (pfm.state == PFM_PREFETCH_IN_PROGRESS &&
		!pfm.sequentiallyAccessed) {
		// The file is no longer sequentially accessed
		pfm.log("no longer accessed sequentially")
		return false
	}

	if now.After(pfm.lastIoTimestamp.Add(maxDeltaTime)) {
		// File has not been accessed recently
		pfm.log("has not been accessed in last %s", maxDeltaTime.String())
		return false
	}

	// any other cases? add them here
	// we don't want to track files we don't need to.
	return true
}

func (pgs *PrefetchGlobalState) tableCleanupWorker() {
	for true {
		time.Sleep(periodicTime)

		// Files that are not worth tracking
		var toRemove [](*PrefetchFileMetadata)

		now := time.Now()
		pgs.mutex.Lock()
		// print a report for each stream
		for i, _ := range pgs.files {
			pfm := pgs.files[i]
			pfm.logReport(now)
		}

		// go over the table, and find all the files not worth tracking
		for _, pfm := range pgs.files {
			if !pgs.isWorthIt(pfm, now) {
				toRemove = append(toRemove, pfm)
			}
		}

		numUnworthy := len(toRemove)
		if pgs.verbose && numUnworthy > 0 {
			pgs.log("tableCleanupWorkder removing %d unworthy streams",
				numUnworthy)
		}

		for _, pfm := range toRemove {
			// wake up any pending IOs
			pgs.hitError(pfm)
			delete(pgs.files, pfm.fh)
		}
		pgs.mutex.Unlock()
	}
}

func (pgs *PrefetchGlobalState) newPrefetchFileMetadata(fh *FileHandle) *PrefetchFileMetadata {
	var entry PrefetchFileMetadata
	entry.fh = fh
	entry.lastIoTimestamp = time.Now()
	entry.hiUserAccessOfs = 0
	entry.sequentiallyAccessed = false
	entry.mw.timestamp = time.Now()

	// Initial state of the file; detect if it is accessed sequentially.
	entry.state = PFM_DETECT_SEQ

	// setup so we can detect a sequential stream.
	// There is no data held in cache yet.
	entry.cache = Cache{
		prefetchIoSize : prefetchMinIoSize,
		maxNumIovecs : 2,
		startByte : 0,
		endByte : prefetchMinIoSize - 1,
		iovecs : make([](*Iovec), 1),
	}
	entry.cache.iovecs[0] = &Iovec{
		ioSize : entry.cache.endByte - entry.cache.startByte + 1,
		startByte : entry.cache.startByte,
		endByte : entry.cache.endByte,
		touched : 0,
		data : nil,
		state : IOV_HOLE,
		cond : sync.NewCond(&pgs.mutex),
	}
	return &entry
}

func (pgs *PrefetchGlobalState) CreateStreamEntry(fh *FileHandle) {
	pgs.mutex.Lock()
	defer pgs.mutex.Unlock()

	// if the table is at the size limit, do not create a new entry
	if len(pgs.files) >= maxNumEntriesInTable {
		return
	}

	// The file has to have sufficient size, to merit an entry. We
	// don't want to waste entries on small files
	if fh.f.Size < minFileSize {
		return
	}

	if pgs.verbose {
		pgs.log("CreateStreamEntry (%s %d)", fh.f.Name, fh.f.Inode)
	}
	pgs.files[fh] = pgs.newPrefetchFileMetadata(fh)
}

func (pgs *PrefetchGlobalState) RemoveStreamEntry(fh *FileHandle, already_locked bool) {
	if !already_locked {
		pgs.mutex.Lock()
		defer pgs.mutex.Unlock()
	}

	if pfm, ok := pgs.files[fh]; ok {
		if pgs.verbose {
			pgs.log("RemoveStreamEntry (%s %d)", fh.f.Name, fh.f.Inode)
		}

		// wake up any waiting synchronous user IOs
		pgs.hitError(pfm)

		// remove from the table
		delete(pgs.files, fh)
	}
}

func (pfm *PrefetchFileMetadata) markRangeInIovec(iovec *Iovec, startOfs int64, endOfs int64) {
	startOfsBoth := MaxInt64(iovec.startByte, startOfs)
	endOfsBoth := MinInt64(iovec.endByte, endOfs)

	// now we know that there is some intersection
	slotSize := iovec.ioSize / numSlotsInChunk
	startSlot := (startOfsBoth - iovec.startByte) / slotSize
	endSlot := (endOfsBoth - iovec.startByte) / slotSize
	check(startSlot >= 0)
	if !(endSlot >= 0 && endSlot <= numSlotsInChunk) {
		pfm.log("offset(%d -- %d),  slots=(%d -- %d), iovec=(%d -- %d)",
			startOfs, endOfs,
			startSlot, endSlot,
			iovec.startByte, iovec.endByte)
	}
	check(endSlot >= 0 && endSlot <= numSlotsInChunk)

	for slot := startSlot; slot <= endSlot ; slot++ {
		// Sets the bit at position [slot]
		iovec.touched |= (1 << uint(slot))
	}
}

// Find the range of io-vectors in cache that this IO covers.
func (pgs *PrefetchGlobalState) findCoveredRange(
	pfm *PrefetchFileMetadata,
	startOfs int64,
	endOfs int64) (int, int) {
	// check if there is ANY intersection with cache
	if endOfs < pfm.cache.startByte ||
		pfm.cache.endByte < startOfs {
		return -1, -1
	}

	first := -1
	for k, iovec := range pfm.cache.iovecs {
		if iovec.startByte <= startOfs &&
			iovec.endByte >= startOfs {
			first = k
			break
		}
	}
	check(first >= 0)

	last := -1
	for k, iovec := range pfm.cache.iovecs {
		if iovec.startByte <= endOfs &&
			iovec.endByte >= endOfs {
			last = k
			break
		}
	}
	if last == -1 {
		// The IO ends after the cache.
		last = len(pfm.cache.iovecs) - 1
	}

	if pgs.verboseLevel >= 2 {
		pfm.log("findCoverRange: first,last=(%d,%d)  IO=[%d -- %d]",
			first, last, startOfs, endOfs)
	}

	return first, last
}


// Setup cache state for the next prefetch.
//
// 1) we want there to be several chunks ahead of us.
// 2) we don't want the cache for the file to go over pfm.cache.maxNumIovecs
//
// For example, if there are currently 2 chunks in cache, and the current IO falls
// in the second location then:
// assuming
//     maxNumIovecs = 2
// calculate
//     iovIndex = 1
//     nReadAheadChunks = iovIndex + maxNumIovecs - nIovec
//                      =  1       + 2            - 2      = 1
// If the IO landed on the first location, then iovIndex=0, and we will not issue
// additional readahead IOs.
func (pgs *PrefetchGlobalState) moveCacheWindow(pfm *PrefetchFileMetadata, iovIndex int) {
/*	if pgs.verbose {
		pfm.log("moveCacheWindow  iovIndex=%d  len(iovecs)=%d",
			iovIndex, len(pfm.cache.iovecs))
	}*/
	nIovecs := len(pfm.cache.iovecs)
	nReadAheadChunks := iovIndex + pfm.cache.maxNumIovecs - nIovecs
	if nReadAheadChunks > 0 {
		// We need to slide the cache window forward
		//
		// stretch the cache forward, but don't go over the file size. Add place holder
		// io-vectors, waiting for prefetch IOs to return.
		lastByteInFile := pfm.fh.f.Size - 1
		for i := 0; i < nReadAheadChunks; i++ {
			startByte := (pfm.cache.endByte + 1) + (int64(i) * int64(pfm.cache.prefetchIoSize))

			// don't go beyond the file size
			if startByte > lastByteInFile {
				break
			}
			endByte := MinInt64(startByte + int64(pfm.cache.prefetchIoSize) - 1, lastByteInFile)
			iov := &Iovec{
				ioSize : endByte - startByte + 1,
				startByte : startByte,
				endByte : endByte,
				touched : 0,
				data : nil,
				state : IOV_IN_FLIGHT,
				cond : sync.NewCond(&pgs.mutex),
			}
			pgs.ioQueue <- IoReq{
				fh : pfm.fh,
				url : *pfm.fh.url,
				ioSize : iov.ioSize,
				startByte : iov.startByte,
				endByte : iov.endByte,
			}
			check(iov.ioSize <= prefetchMaxIoSize)
			pfm.cache.iovecs = append(pfm.cache.iovecs, iov)

			if pgs.verbose {
				pfm.log("Adding chunk %d [%d -- %d]",
					len(pfm.cache.iovecs) - 1,
					iov.startByte,
					iov.endByte)
			}
		}
	}

	// we want to limit the amount of cached data.
	nIovecs = len(pfm.cache.iovecs)
	if nIovecs > pfm.cache.maxNumIovecs {
		// we chop off the beginning of the vector, and
		// reduce memory consumption.
		start := nIovecs - pfm.cache.maxNumIovecs
		nRemoved := 0
		for i := 0; i < start; i++ {
			if pfm.cache.iovecs[i].endByte < pfm.hiUserAccessOfs {
				// The user has already passed this point in the file.
				// it can be discarded.
				if pfm.state == PFM_PREFETCH_IN_PROGRESS {
					// Estimate if the file is sequentially accessed,
					// by checking if the chunk we are discarding was fully accessed.
					numAccessed := bits.OnesCount64(pfm.cache.iovecs[i].touched)
					fullyAccessed := (numAccessed == numSlotsInChunk)
					pfm.sequentiallyAccessed = pfm.sequentiallyAccessed && fullyAccessed
				}
				nRemoved++
			}
		}
		if nRemoved > 0 {
			pfm.cache.iovecs = pfm.cache.iovecs[nRemoved : ]
			if pgs.verbose {
				pfm.log("Removed %d chunks", nRemoved)
			}
		}
	}

	// recalculate the range covered by the cache
	nIovecs = len(pfm.cache.iovecs)
	check(nIovecs > 0)
	pfm.cache.startByte = pfm.cache.iovecs[0].startByte
	pfm.cache.endByte = pfm.cache.iovecs[nIovecs-1].endByte

	if pgs.verboseLevel >= 2 {
		pfm.log("range =[%d -- %d]", pfm.cache.startByte, pfm.cache.endByte)
		for i, iovec := range pfm.cache.iovecs {
			pfm.log("cache %d -> [%d -- %d]  %s", i, iovec.startByte, iovec.endByte,
				iovec.stateString())
		}
	}
}


func (pgs *PrefetchGlobalState) markAccessedAndMaybeStartPrefetch(
	pfm *PrefetchFileMetadata,
	startOfs int64,
	endOfs int64) {
	// Mark the areas in cache that this IO accessed
	first, last := pgs.findCoveredRange(pfm, startOfs, endOfs)
	if first == -1 && last == -1 {
		return
	}
	for i := first; i <= last; i++ {
		pfm.markRangeInIovec(pfm.cache.iovecs[i], startOfs, endOfs)
	}

	// find the iovec in cache where this IO falls. We use the right edge
	// of the IO.
	currentIovec := pfm.cache.iovecs[last]
	numAccessed := bits.OnesCount64(currentIovec.touched)
	if pgs.verboseLevel >= 2 {
		pfm.log("touch: ofs=%d  len=%d  numAccessed=%d",
			startOfs, endOfs - startOfs, numAccessed)
	}
	if numAccessed == numSlotsInChunk {
		// A sufficient number of the slots were accessed. Start a prefetch for
		// the next chunk(s)
		if pfm.state == PFM_DETECT_SEQ {
			pfm.state = PFM_PREFETCH_IN_PROGRESS
			pfm.sequentiallyAccessed = true
		}

		// increase io size, using a bounded exponential formula
		if pfm.cache.prefetchIoSize < prefetchMaxIoSize {
			pfm.cache.prefetchIoSize =
				MinInt64(prefetchMaxIoSize, pfm.cache.prefetchIoSize * prefetchIoFactor)
		}
		if pfm.cache.prefetchIoSize == prefetchMaxIoSize {
			// Give each stream at least one read-ahead request. If there
			// are only a few streams, we can give more.
			nStreams := len(pgs.files)
			nReadAhead := pgs.maxNumChunksReadAhead / nStreams
			nReadAhead = MaxInt(1, nReadAhead)

			pfm.cache.maxNumIovecs = nReadAhead + 1
		}
	}
	if pfm.state == PFM_PREFETCH_IN_PROGRESS {
		pgs.moveCacheWindow(pfm, last)

		// Have we reached the end of the file?
		if pfm.cache.endByte >= pfm.fh.f.Size - 1 {
			pfm.state = PFM_EOF
		}
	}
}

// assumption: the range is entirely in cache.
// copy the data into the buffer, and return true, and the length of the data.
//
// If there is a problem, return false, and zero length.
func (pgs *PrefetchGlobalState) getDataFromCache(
	pfm *PrefetchFileMetadata,
	startOfs int64,
	endOfs int64,
	data  []byte) (bool, int) {
	first, last := pgs.findCoveredRange(pfm, startOfs, endOfs)
	cursor := 0
	for i := first; i <= last; i++ {
		iov := pfm.cache.iovecs[i]
		switch iov.state {
		case IOV_HOLE:
			return false, 0

		case IOV_IN_FLIGHT:
			// waiting for prefetch to come back with data.
			// note: when we wake up, the IO may have come back
			// with an error.
			iov.cond.Wait()
			if iov.state != IOV_DONE {
				return false, 0
			}
			cursor += iov.copyBuffer(startOfs, endOfs, data[cursor:])

		case IOV_DONE:
			cursor += iov.copyBuffer(startOfs, endOfs, data[cursor:])

		case IOV_ERRORED:
			// there is no data.
			return false, 0
		}
	}
	return true, cursor
}

// This is done on behalf of a user read request, check if this range is cached of prefetched
// and return the data if so. Otherwise, return nil.
//
// we are holding the the lock here, so we don't want to wait for any conditions.
func (pgs *PrefetchGlobalState) CacheLookup(fh *FileHandle, startOfs int64, endOfs int64, data []byte) (bool, int) {
	pgs.mutex.Lock()
	defer pgs.mutex.Unlock()
	pfm, ok := pgs.files[fh]
	if !ok {
		// file is not tracked, no prefetch data is available
		return false, 0
	}
	now := time.Now()
	if !pgs.isWorthIt(pfm, now) {
		// file is not accessed in streaming fashion. It should be
		// removed from the table.
		pfm.log("accessed non sequentially")
		pgs.RemoveStreamEntry(pfm.fh, true)
		return false, 0
	}

	if !(pfm.cache.startByte <= startOfs &&
		pfm.cache.endByte >= endOfs) {
		// The IO is, at least partly, outside
		// the cached area. The file is not accessed
		// sequentially
		if pgs.verbose {
			pfm.log("IO outside cache  io=[%d -- %d]  cache=[%d -- %d]",
				startOfs, endOfs, pfm.cache.startByte, pfm.cache.endByte)
			pfm.sequentiallyAccessed = false
		}
		return false, 0
	}

	// accounting and statistics
	pfm.lastIoTimestamp = now
	pfm.hiUserAccessOfs = MaxInt64(pfm.hiUserAccessOfs, startOfs)
	pfm.mw.numIOs++

	switch pfm.state {
	case PFM_DETECT_SEQ:
		// No data is cached. Only detecting if there is sequential access.
		pgs.markAccessedAndMaybeStartPrefetch(pfm, startOfs, endOfs)
		return false, 0

	case PFM_PREFETCH_IN_PROGRESS:
		// ongoing prefetch IO, and the range is in cache
		pgs.markAccessedAndMaybeStartPrefetch(pfm, startOfs, endOfs)

		// we may need to wait for a prefetch IO to return
		return pgs.getDataFromCache(pfm, startOfs, endOfs, data)

	case PFM_EOF:
		// don't issue any additional IOs
		return pgs.getDataFromCache(pfm, startOfs, endOfs, data)

	default:
		panic(fmt.Sprintf("bad state %d for fileId=%s", pfm.state, pfm.fh.f.Id))
	}
}
