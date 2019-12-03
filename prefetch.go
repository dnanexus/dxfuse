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
	"sync/atomic"
	"time"

	"github.com/dnanexus/dxda"
	"github.com/hashicorp/go-retryablehttp" // use http libraries from hashicorp for implement retry logic
)

const (
	maxDeltaTime = 5 * 60 * time.Second
	periodicTime = 30 * time.Second
	slowIoThresh = 60 // when does a slow IO become worth reporting

	prefetchMinIoSize = (256 * KiB)   // threshold for deciding the file is sequentially accessed
	prefetchMaxIoSize = (1 * MiB)    // maximal size of prefetch IO
	prefetchIoFactor = 4

	numSlotsInChunk = 64

	// An active stream can use a significant amount of memory to store prefetched data.
	// Limit the total number of streams we are tracking and prefetching.
	maxNumEntriesInTable = 10

	// maximum number of prefetch threads, regardless of machine size
	maxNumPrefetchThreads = 32

	minFileSize = 1 * MiB     // do not track files smaller than this size

	// An prefetch request time limit
	readRequestTimeout = 90 * time.Second
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
	f           File
	url         DxDownloadURL

	ioSize      int64   // The io size
	startByte   int64   // start byte, counting from the beginning of the file.
	endByte     int64

	id          uint64  // a unique id for this IO
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
	mutex                sync.Mutex

	// the file being tracked
	f                    File
	url                  DxDownloadURL
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

// global limits
type PrefetchGlobalState struct {
	mutex         sync.Mutex  // Lock used to control the files table
	verbose       bool
	verboseLevel  int
	files         map[File](*PrefetchFileMetadata) // tracking state per file-id
	ioQueue       chan IoReq    // queue of IOs to prefetch
	wg            sync.WaitGroup
	numPrefetchThreads int
	maxNumChunksReadAhead int
	ioCounter     uint64
}

func fileDesc(f File) string {
	return fmt.Sprintf("%d", f.Inode)
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
func (pfm *PrefetchFileMetadata) log(a string, args ...interface{}) {
	hdr := fmt.Sprintf("prefetch(%s)", fileDesc(pfm.f))
	LogMsg(hdr, a, args...)
}

// report on the last delta time
func (pfm *PrefetchFileMetadata) logReport(now time.Time) {
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
		files : make(map[File](*PrefetchFileMetadata)),
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

	// clear the entire table
	var allFiles []File
	pgs.mutex.Lock()
	for f, _ := range pgs.files {
		allFiles = append(allFiles, f)
	}
	pgs.mutex.Unlock()

	for _, fid := range allFiles {
		pfm := pgs.getAndLockPfm(fid)
		if pfm != nil {
			pgs.streamDone(pfm)
			pfm.mutex.Unlock()
		}
	}

	pgs.mutex.Lock()
	pgs.files = nil
	pgs.mutex.Unlock()

	// we aren't waiting for the periodic cleanup thread.
}

func check(value bool) {
	if !value {
		panic("assertion failed")
	}
}

// Got an error. Release all waiting IO.
func (pgs *PrefetchGlobalState) streamDone(pfm *PrefetchFileMetadata) {
	for i, _ := range pfm.cache.iovecs {
		iovec := pfm.cache.iovecs[i]
		iovec.state = IOV_ERRORED
		iovec.cond.Broadcast()
	}
}

func (pgs *PrefetchGlobalState) reportIfSlowIO(
	startTs time.Time,
	f File,
	startByte int64,
	endByte int64) {
	endTs := time.Now()
	deltaSec := int(endTs.Sub(startTs).Seconds())
	if deltaSec > slowIoThresh {
		pgs.log("(%s) slow IO [%d -- %d] %d seconds",
			fileDesc(f), startByte, endByte, deltaSec)
	}
}

func (pgs *PrefetchGlobalState) readData(client *retryablehttp.Client, ioReq IoReq) ([]byte, error) {
	// The data has not been prefetched. Get the data from DNAx with an
	// http request.
	expectedLen := ioReq.endByte - ioReq.startByte + 1
	if pgs.verbose {
		pgs.log("(%s) (io=%d) reading extent from DNAx ofs=%d len=%d",
			fileDesc(ioReq.f), ioReq.id, ioReq.startByte, expectedLen)
	}

	headers := make(map[string]string)

	// Copy the immutable headers
	for key, value := range ioReq.url.Headers {
		headers[key] = value
	}
	headers["Range"] = fmt.Sprintf("bytes=%d-%d", ioReq.startByte, ioReq.endByte)

	// Safety procedure to force timeout to prevent hanging
	ctx, cancel := context.WithCancel(context.TODO())
	timer := time.AfterFunc(readRequestTimeout, func() {
		cancel()
	})
	defer timer.Stop()

	startTs := time.Now()
	defer pgs.reportIfSlowIO(startTs, ioReq.f, ioReq.startByte, ioReq.endByte)

	for tCnt := 0; tCnt < NumRetriesDefault; tCnt++ {
		data, err := dxda.DxHttpRequest(ctx, client, 1, "GET", ioReq.url.URL, headers, []byte("{}"))
		if err != nil {
			return nil, err
		}

		recvLen := int64(len(data))
		if recvLen != expectedLen {
			// retry (only) in the case of short read
			pgs.log("(%s) (io=%d) received length is wrong, got %d, expected %d. Retrying.",
				fileDesc(ioReq.f), ioReq.id, recvLen, expectedLen)
			continue
		}

		if pgs.verbose {
			if err == nil {
				pgs.log("(%s) (io=%d) [%d -- %d] returned correctly",
					fileDesc(ioReq.f), ioReq.id, ioReq.startByte, ioReq.endByte)
			} else {
				pgs.log("(%s) (io=%d) [%d -- %d] returned with error %s",
					fileDesc(ioReq.f), ioReq.id, ioReq.startByte, ioReq.endByte, err.Error())
			}
		}
		return data, err
	}

	pgs.log("Did not received the data for IO [%d -- %d]", ioReq.startByte, ioReq.endByte)
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

// We are holding the pfm lock at this point.
// Wake up waiting IOs, if any.
func (pgs *PrefetchGlobalState) addIoReqToCache(pfm *PrefetchFileMetadata, ioReq IoReq, data []byte, err error) {
	// Find the index for this chunk in the cache. The chunks may be different
	// size, so we need to scan.
	iovIdx := findIovecIndex(pfm, ioReq)
	if iovIdx == -1 {
		pfm.log("(#io=%d) Dropping prefetch IO, matching entry in cache not found, ioReq=%v len(cache.iovecs)=%d",
			ioReq.id, ioReq, len(pfm.cache.iovecs))
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
		pfm.log("(#io=%d) prefetch io error [%d -- %d] %s",
			ioReq.id, ioReq.startByte, ioReq.endByte, err.Error())
		pfm.cache.iovecs[iovIdx].state = IOV_ERRORED
	}

	// wake up waiting user IOs
	pfm.cache.iovecs[iovIdx].cond.Broadcast()
}


func (pgs *PrefetchGlobalState) getAndLockPfm(f File) *PrefetchFileMetadata {
	pgs.mutex.Lock()

	// Find the file this IO belongs to
	pfm, ok := pgs.files[f]
	if !ok {
		pgs.mutex.Unlock()
		return nil
	}
	pgs.mutex.Unlock()

	pfm.mutex.Lock()
	return pfm
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
		data, err := pgs.readData(client, ioReq)

		if pgs.verboseLevel >= 2 {
			pgs.log("(%s) (io=%d) adding returned data to file", fileDesc(ioReq.f), ioReq.id)
		}
		pfm := pgs.getAndLockPfm(ioReq.f)
		if pfm == nil {
			// file is not tracked anymore
			pgs.log("(%s) (io=%d) dropping prefetch IO [%d -- %d], file is no longer tracked",
				fileDesc(ioReq.f), ioReq.id, ioReq.startByte, ioReq.endByte)
			continue
		}

		if pgs.verboseLevel >= 2 {
			pgs.log("(%s) (%d) holding the PFM lock", fileDesc(ioReq.f), ioReq.id)
		}
		pgs.addIoReqToCache(pfm, ioReq, data, err)
		pfm.mutex.Unlock()

		if pgs.verboseLevel >= 2 {
			pgs.log("(%s) (%d) Done", fileDesc(ioReq.f), ioReq.id)
		}
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
		if pgs.verbose {
			pgs.log("tableCleanupWorker [")
		}

		// the entire list of file-handles
		var candidates [](File)

		// Files that are not worth tracking
		var toRemove [](File)

		pgs.mutex.Lock()
		for f, _ := range pgs.files {
			candidates = append(candidates, f)
		}
		pgs.mutex.Unlock()

		// go over the table, and find all the files not worth tracking
		now := time.Now()
		for _, f := range candidates {
			pfm := pgs.getAndLockPfm(f)
			if pfm != nil {
				// print a report for each stream
				pfm.logReport(now)
				if !pgs.isWorthIt(pfm, now) {
					// This stream isn't worth it. stop
					// the prefetch. Cancel pending user IOs.
					pgs.streamDone(pfm)
					toRemove = append(toRemove, f)
				}
				pfm.mutex.Unlock()
			}
		}

		numUnworthy := len(toRemove)
		if pgs.verbose && numUnworthy > 0 {
			pgs.log("tableCleanupWorkder removing %d unworthy streams",
				numUnworthy)
		}

		pgs.mutex.Lock()
		for _, f := range toRemove {
			delete(pgs.files, f)
		}
		pgs.mutex.Unlock()

		if pgs.verbose {
			pgs.log("tableCleanupWorker ]")
		}
	}
}

func (pgs *PrefetchGlobalState) newPrefetchFileMetadata(f File, url DxDownloadURL) *PrefetchFileMetadata {
	var pfm PrefetchFileMetadata
	pfm.f = f
	pfm.url = url
	pfm.lastIoTimestamp = time.Now()
	pfm.hiUserAccessOfs = 0
	pfm.sequentiallyAccessed = false
	pfm.mw.timestamp = time.Now()

	// Initial state of the file; detect if it is accessed sequentially.
	pfm.state = PFM_DETECT_SEQ

	// setup so we can detect a sequential stream.
	// There is no data held in cache yet.
	pfm.cache = Cache{
		prefetchIoSize : prefetchMinIoSize,
		maxNumIovecs : 2,
		startByte : 0,
		endByte : prefetchMinIoSize - 1,
		iovecs : make([](*Iovec), 1),
	}
	pfm.cache.iovecs[0] = &Iovec{
		ioSize : pfm.cache.endByte - pfm.cache.startByte + 1,
		startByte : pfm.cache.startByte,
		endByte : pfm.cache.endByte,
		touched : 0,
		data : nil,
		state : IOV_HOLE,
		cond : sync.NewCond(&pfm.mutex),
	}
	return &pfm
}

func (pgs *PrefetchGlobalState) CreateStreamEntry(f File, url DxDownloadURL) {
	pgs.mutex.Lock()
	defer pgs.mutex.Unlock()

	// if the table is at the size limit, do not create a new entry
	if len(pgs.files) >= maxNumEntriesInTable {
		return
	}

	// The file has to have sufficient size, to merit an entry. We
	// don't want to waste entries on small files
	if f.Size < minFileSize {
		return
	}

	if pgs.verbose {
		pgs.log("CreateStreamEntry (%s %d)", f.Name, f.Inode)
	}
	pgs.files[f] = pgs.newPrefetchFileMetadata(f, url)
}

func (pgs *PrefetchGlobalState) RemoveStreamEntry(f File) {
	pfm := pgs.getAndLockPfm(f)
	if pfm != nil {
		if pgs.verbose {
			pgs.log("RemoveStreamEntry (%s %d)", f.Name, f.Inode)
		}

		// wake up any waiting synchronous user IOs
		pgs.streamDone(pfm)
		pfm.mutex.Unlock()

		// remove from the table
		pgs.mutex.Lock()
		delete(pgs.files, f)
		pgs.mutex.Unlock()
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
	nIovecs := len(pfm.cache.iovecs)
	nReadAheadChunks := iovIndex + pfm.cache.maxNumIovecs - nIovecs
	if nReadAheadChunks > 0 {
		// We need to slide the cache window forward
		//
		// stretch the cache forward, but don't go over the file size. Add place holder
		// io-vectors, waiting for prefetch IOs to return.
		lastByteInFile := pfm.f.Size - 1
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
				cond : sync.NewCond(&pfm.mutex),
			}
			uniqueId := atomic.AddUint64(&pgs.ioCounter, 1)
			pgs.ioQueue <- IoReq{
				f : pfm.f,
				url : pfm.url,
				ioSize : iov.ioSize,
				startByte : iov.startByte,
				endByte : iov.endByte,
				id : uniqueId,
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
	if first == -1 {
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
		if pfm.cache.endByte >= pfm.f.Size - 1 {
			pfm.state = PFM_EOF
		}
	}
}


const (
	DATA_OUTSIDE_CACHE = 1 // data not in cache
	DATA_IN_CACHE      = 2 // data is in cache
	DATA_WAIT          = 3 // need to wait for some of the IOs
)

func cacheCode2string(retCode int) string {
	switch retCode {
	case DATA_OUTSIDE_CACHE: return "OUTSIDE_CACHE"
	case DATA_IN_CACHE: return "IN_CACHE"
	case DATA_WAIT : return "WAIT"
	default: panic(fmt.Sprintf("unknown cache code %d", retCode))
	}
}

func (pgs *PrefetchGlobalState) isDataInCache(
	pfm *PrefetchFileMetadata,
	startOfs int64,
	endOfs int64) int {
	first, last := pgs.findCoveredRange(pfm, startOfs, endOfs)
	if first == -1 {
		// The IO is, at least partly, outside
		// the cached area.
		return DATA_OUTSIDE_CACHE
	}

	for i := first; i <= last; i++ {
		iov := pfm.cache.iovecs[i]
		switch iov.state {
		case IOV_HOLE:
			return DATA_OUTSIDE_CACHE

		case IOV_IN_FLIGHT:
			// waiting for prefetch to come back with data.
			// note: when we wake up, the IO may have come back
			// with an error.
			if pgs.verboseLevel >= 2 {
				pfm.log("isDataInCache: wait")
			}
			iov.cond.Wait()
			return DATA_WAIT

		case IOV_DONE:
			// we're good
			continue

		case IOV_ERRORED:
			pfm.log("isDataInCache: IO errored")
			return DATA_OUTSIDE_CACHE
		}
	}
	return DATA_IN_CACHE
}


// The IO is entirely in the cache, and all the data
// is there.
func (pgs *PrefetchGlobalState) copyDataFromCache(
	pfm *PrefetchFileMetadata,
	startOfs int64,
	endOfs int64,
	data  []byte) (bool, int) {
	first, last := pgs.findCoveredRange(pfm, startOfs, endOfs)
	check(0 <= first && 0 <= last)
	cursor := 0
	for i := first; i <= last; i++ {
		iov := pfm.cache.iovecs[i]
		check (iov.state == IOV_DONE)
		subBuf := iov.intersectBuffer(startOfs, endOfs)
		len := copy(data[cursor:], subBuf)
		cursor += len
	}
	return true, cursor
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
	numTries := 3
	for i := 0; i < numTries; i++ {
		retCode := pgs.isDataInCache(pfm, startOfs, endOfs)
		if pgs.verboseLevel >= 2 {
			pfm.log("isDataInCache=%s", cacheCode2string(retCode))
		}
		switch retCode {
		case DATA_OUTSIDE_CACHE:
			return false, 0

		case DATA_IN_CACHE:
			return pgs.copyDataFromCache(pfm, startOfs, endOfs, data)

		case DATA_WAIT:
			// wait until the data arrives
			// release locks, and try again
			//
			// TODO: do we really need this?
			//pfm.mutex.Unlock()
			//pfm = pgs.getAndLockPfm(pfm.f)
			continue

		default:
			panic(fmt.Sprintf("bad value returned from isDataInCache %d", retCode))
		}
	}

	pfm.log("strange: we waited %d times for IOs to return from prefetch, but got nothing", numTries)
	return false, 0
}

// This is done on behalf of a user read request, check if this range is cached of prefetched
// and return the data if so. Otherwise, return nil.
//
// we are holding the the lock here, so we don't want to wait for any conditions.
func (pgs *PrefetchGlobalState) CacheLookup(f File, startOfs int64, endOfs int64, data []byte) (bool, int) {
	pfm := pgs.getAndLockPfm(f)
	if pfm == nil {
		// file is not tracked, no prefetch data is available
		return false, 0
	}
	// the PFM is locked now.
	// make sure it is unlocked when we leave.
	defer pfm.mutex.Unlock()

	// accounting and statistics
	pfm.lastIoTimestamp = time.Now()
	pfm.hiUserAccessOfs = MaxInt64(pfm.hiUserAccessOfs, startOfs)
	pfm.mw.numIOs++

	switch pfm.state {
	case PFM_DETECT_SEQ:
		// No data is cached. Only detecting if there is sequential access.
		pgs.markAccessedAndMaybeStartPrefetch(pfm, startOfs, endOfs)
		return false, 0

	case PFM_PREFETCH_IN_PROGRESS:
		// ongoing prefetch IO
		pgs.markAccessedAndMaybeStartPrefetch(pfm, startOfs, endOfs)
		return pgs.getDataFromCache(pfm, startOfs, endOfs, data)

	case PFM_EOF:
		// don't issue any more prefetch IOs, we have reached the end of the file
		return pgs.getDataFromCache(pfm, startOfs, endOfs, data)

	default:
		panic(fmt.Sprintf("bad state %d for fileId=%s", pfm.state, pfm.f.Id))
	}

}
