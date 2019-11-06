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
	maxDeltaTime = 2 * 60 * time.Second

	prefetchMinIoSize = (256 * KiB)   // threshold for deciding the file is sequentially accessed
	prefetchMaxIoSize = (16 * MiB)    // maximal size of prefetch IO
	prefetchIoFactor = 4

	numSlotsInChunk = 64

	// An active stream can use a significant amount of memory to store prefetched data.
	// Limit the total number of streams we are tracking and prefetching.
	maxNumEntriesInTable = 10

	// maximum number of prefetch threads, regardless of machine size
	maxNumPrefetchThreads = 32

	minFileSize = 8 * MiB     // do not track files smaller than this size
)

// enumerate type for the state of a PFM (file metadata)
const (
	PFM_DETECT_SEQ = 1      // Beginning of the file, detecting if access is sequential
	PFM_PREFETCH_IN_PROGRESS = 2  // normal case --- prefetch is ongoing
	PFM_IO_ERROR = 3
	PFM_EOF = 4   // reached the end of the file
)

// enumerated type for returning replies from a cache lookup
const (
	DATA_IN_CACHE = 1
	DATA_OUTSIDE_CACHE = 2
	DATA_WAITING_FOR_PREFETCH_IO = 3
	DATA_ACCESSED_NON_SEQUENTIALLY = 4
)


// A request that one of the IO-threads will pick up
type IoReq struct {
	fh         *FileHandle
	url         DxDownloadURL

	ioSize      int64   // The io size
	startByte   int64   // start byte, counting from the beginning of the file.
	endByte     int64
	data        []byte
}

type Iovec struct {
	ioSize     int64   // The io size
	startByte  int64   // start byte, counting from the beginning of the file.
	endByte    int64
	touched    uint64  // mark the areas that have been accessed by the user
	data       []byte

	// do we want the data? We leave holes when we are detecting
	// sequential access.
	shouldPrefetch bool
	submitted  bool    // have we issued a prefetch IO to get this chunk?
}

// A cache of all the data retrieved from the platform, for one file.
// It is a contiguous range of chunks. All IOs are the same size.
type Cache struct {
	prefetchIoSize     int64  // size of the IO to issue when prefetching
	numChunksReadAhead int
	maxNumIovecs       int

	startByte          int64
	endByte            int64
	iovecs             [](*Iovec)
}

type PrefetchFileMetadata struct {
	// the file being tracked
	fh                  *FileHandle
	state                int


	lastIoTimestamp      time.Time  // Last time an IO hit this file
	hiUserAccessOfs      int64      // highest file offset accessed by the user
	sequentiallyAccessed bool       // estimate if this file is sequentially accessed
	numIOs               int
	numPrefetchIOs       int

	// cached io vectors.
	// The assumption is that the user is accessing the last io-vector.
	// If this assumption isn't true, prefetch is ineffective. The algorithm
	// should detect and stop it.
	cache                Cache

	// Allow user reads to wait until prefetch IOs complete
	cond                *sync.Cond
}

func time2string(t time.Time) string {
	return fmt.Sprintf("%02d:%02d:%02d", t.Hour(), t.Minute(), t.Second())
}

// write a log message, and add a header
func (pfm PrefetchFileMetadata) log(a string, args ...interface{}) {
	msg := fmt.Sprintf(a, args...)
	now := time.Now()
	log.Printf("%s prefetch(%s): %s", time2string(now), pfm.fh.f.Name, msg)
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
func (pgs PrefetchGlobalState) log(a string, args ...interface{}) {
	msg := fmt.Sprintf(a, args...)
	now := time.Now()
	log.Printf("%s prefetch: %s", time2string(now), msg)
}

func NewPrefetchGlobalState(verboseLevel int) *PrefetchGlobalState {
	// We want to:
	// 1) allow all streams to have a worker available
	// 2) not have more than two workers per CPU
	// 3) not go over an overall limit, regardless of machine size
	numCPUs := runtime.NumCPU()
	numPrefetchThreads := MinInt(numCPUs * 2, maxNumPrefetchThreads)

	// don't allow a single stream to hog all the threads.
	maxNumChunksReadAhead := MinInt(8, numPrefetchThreads - 2)
	maxNumChunksReadAhead = MaxInt(1, maxNumChunksReadAhead)

	// calculate how much memory will be used in the worst cast
	streamBytes := 3 * prefetchMaxIoSize
	totalMemoryBytes := streamBytes * maxNumEntriesInTable
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

func (pgs *PrefetchGlobalState) readData(
	client *retryablehttp.Client,
	startByte int64,
	endByte int64,
	url DxDownloadURL) ([]byte, error) {
	// The data has not been prefetched. Get the data from DNAx with an
	// http request.
	if pgs.verbose {
		len := endByte - startByte + 1
		pgs.log("reading extent from DNAx ofs=%d len=%d", startByte, len)
	}

	headers := make(map[string]string)

	// Copy the immutable headers
	for key, value := range url.Headers {
		headers[key] = value
	}
	headers["Range"] = fmt.Sprintf("bytes=%d-%d", startByte, endByte)

	ctx := context.TODO()
	data, err := dxda.DxHttpRequest(ctx, client, "GET", url.URL, headers, []byte("{}"))
	if pgs.verbose {
		if err == nil {
			pgs.log("IO returned correctly len=%d", len(data))
		} else {
			pgs.log("IO returned with error %s", err.Error())
		}
	}
	return data, err
}


// figure out how many contiguous chunks of data we have in the cache.
func numContiguousChunks(pfm *PrefetchFileMetadata) int {
	contigLen := 0
	for _, iovec := range(pfm.cache.iovecs) {
		if iovec.shouldPrefetch &&
			iovec.data == nil {
			break
		}
		contigLen++
	}
	return contigLen
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
func (pgs *PrefetchGlobalState) addIoReqToCache(pfm *PrefetchFileMetadata, ioReq IoReq) {
	if pfm.state == PFM_IO_ERROR {
		pfm.log("Dropping prefetch IO, file has encountered an error (%s)",
			ioReq.fh.f.Id)
		return
	}

	// Find the index for this chunk in the cache. The chunks may be different
	// size, so we need to scan.
	iovIdx := findIovecIndex(pfm, ioReq)
	if iovIdx == -1 {
		pfm.log("Dropping prefetch IO, matching entry in cache not found, ioReq=%v len(cache.iovecs)=%d",
			ioReq, len(pfm.cache.iovecs))
		return
	}
	check(pfm.cache.iovecs[iovIdx].data == nil)
	pfm.cache.iovecs[iovIdx].data = ioReq.data

	// For GC purposes, not sure we actually need it.
	ioReq.data = nil

	// statistics
	pfm.numPrefetchIOs++

	// if we are extending the contiguous filled chunks, then signal
	contigLen := numContiguousChunks(pfm)
	if pgs.verboseLevel >= 2 {
		pgs.log("contiguity of cache chunks: %d  iovIdx=%d", contigLen, iovIdx)
	}
	if contigLen >= iovIdx + 1 {
		// Note: the contiguous length may extend -beyond- the current
		// chunk. This can happen if we just plugged a hole.
		pfm.cond.Broadcast()
	}
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
		data, err := pgs.readData(client, ioReq.startByte, ioReq.endByte, ioReq.url)
		ioReq.data = data

		pgs.mutex.Lock()
		// Find the file this IO belongs to
		pfm, ok := pgs.files[ioReq.fh]
		if !ok {
			// file is not tracked anymore
			pfm.log("Dropping prefetch IO, file is no longer tracked")
		} else {
			if err != nil {
				pfm.log("Prefetch error ofs=%d len=%d error=%s",
					ioReq.startByte, ioReq.endByte, err.Error())
				pfm.state = PFM_IO_ERROR
				pfm.cond.Broadcast()
			} else {
				pgs.addIoReqToCache(pfm, ioReq)
			}
		}
		pgs.mutex.Unlock()
	}
}


// Check if a file is worth tracking.
func (pgs *PrefetchGlobalState) isWorthIt(pfm *PrefetchFileMetadata, now time.Time) bool {
	if pfm.state == PFM_IO_ERROR {
		// we had an IO error, get rid of this file
		return false
	}
	if (pfm.state == PFM_PREFETCH_IN_PROGRESS &&
		!pfm.sequentiallyAccessed) {
		// The file is no longer sequentially accessed
		return false
	}

	if now.After(pfm.lastIoTimestamp.Add(maxDeltaTime)) {
		// File has not been accessed recently
		return false
	}

	// any other cases? add them here
	// we don't want to track files we don't need to.
	return true
}

func (pgs *PrefetchGlobalState) tableCleanupWorker() {
	for true {
		// sleep 60
		time.Sleep(60 * time.Second)

		// Files that are not worth tracking
		var toRemove [](*PrefetchFileMetadata)

		// go over the table, and find all the files not worth tracking
		now := time.Now()
		pgs.mutex.Lock()
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
			pfm.cond.Broadcast()
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
	entry.numIOs = 0
	entry.numPrefetchIOs = 0

	// Initial state of the file; detect if it is accessed sequentially.
	entry.state = PFM_DETECT_SEQ

	// setup so we can detect a sequential stream.
	// There is no data held in cache yet.
	entry.cache = Cache{
		prefetchIoSize : prefetchMinIoSize,
		numChunksReadAhead : 1,
		maxNumIovecs : 3,
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
		shouldPrefetch : false,
		submitted : false,
	}
	entry.cond = sync.NewCond(&pgs.mutex)
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
		pgs.log("CreateStreamEntry %s", fh.f.Name)
	}
	pgs.files[fh] = pgs.newPrefetchFileMetadata(fh)
}

func (pgs *PrefetchGlobalState) RemoveStreamEntry(fh *FileHandle) {
	pgs.mutex.Lock()
	defer pgs.mutex.Unlock()

	if pfm, ok := pgs.files[fh]; ok {
		if pgs.verbose {
			pgs.log("RemoveStreamEntry %s", fh.f.Name)
		}

		// wake up any waiting synchronous user IOs
		pfm.cond.Broadcast()

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

	if pgs.verboseLevel >= 2 {
		for i, iovec := range pfm.cache.iovecs {
			pfm.log("cache %d -> [%d -- %d]", i, iovec.startByte, iovec.endByte)
		}
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
// 1) we want there to be numChunksReadAhead chunks ahead of us.
// 2) we don't want the cache for the file to go over pfm.cache.maxNumIovecs
//
// For example, if there are currently 2 chunks in cache, and the current IO falls
// in the second location then:
// assuming
//     numChunksReadAhead = 1
// calculate
//     iovIndex = 1
//     nReadAheadChunks = 1 + 1 + 1 - 2 = 1
//
// If the IO landed on the first location, then iovIndex=0, and we don't need
// additional readahead IOs.
func (pgs *PrefetchGlobalState) moveCacheWindow(pfm *PrefetchFileMetadata, iovIndex int) {
	if pgs.verbose {
		pfm.log("moveCacheWindow  iovIndex=%d  len(iovecs)=%d",
			iovIndex, len(pfm.cache.iovecs))
	}
	nIovecs := len(pfm.cache.iovecs)
	nReadAheadChunks := iovIndex + 1 + pfm.cache.numChunksReadAhead - nIovecs
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
			emptyIov := &Iovec{
				ioSize : endByte - startByte + 1,
				startByte : startByte,
				endByte : endByte,
				touched : 0,
				data : nil,
				shouldPrefetch : true,
				submitted : false,
			}
			check(emptyIov.ioSize <= prefetchMaxIoSize)
			pfm.cache.iovecs = append(pfm.cache.iovecs, emptyIov)

			if pgs.verboseLevel >= 2 {
				pfm.log("Adding chunk %d [%d -- %d]", i, emptyIov.startByte, emptyIov.endByte)
			}
		}
	}

	nIovecs = len(pfm.cache.iovecs)
	if nIovecs > pfm.cache.maxNumIovecs {
		// we want to limit the amount of cached data.
		// we chop off the beginning of the vector, and
		// reduce memory consumption.
		start := nIovecs - pfm.cache.maxNumIovecs
		for i := 0; i < start; i++ {
			if pfm.cache.iovecs[i].endByte < pfm.hiUserAccessOfs  {
				// don't discard data that hasn't been read yet.
				break
			}
			// the user has already passed this point in the file.
			// it can be discarded.
			pfm.cache.iovecs[i].data = nil
			if pfm.state == PFM_PREFETCH_IN_PROGRESS &&
				pfm.cache.iovecs[i].submitted &&
				pfm.cache.iovecs[i].shouldPrefetch {
				// Estimate if the file is sequentially accessed,
				// by checking if the chunk we are discarding was fully accessed.
				numAccessed := bits.OnesCount64(pfm.cache.iovecs[i].touched)
				fullyAccessed := (numAccessed == numSlotsInChunk)
				pfm.sequentiallyAccessed = pfm.sequentiallyAccessed && fullyAccessed
			}
		}
		pfm.cache.iovecs = pfm.cache.iovecs[start:]
	}

	// recalculate the range covered by the cache
	nIovecs = len(pfm.cache.iovecs)
	check(nIovecs > 0)
	pfm.cache.startByte = pfm.cache.iovecs[0].startByte
	pfm.cache.endByte = pfm.cache.iovecs[nIovecs-1].endByte
}



// submit IOs for holes that we think require prefetch
func (pgs *PrefetchGlobalState) submitIoForHoles(pfm *PrefetchFileMetadata) {
	for _, chunk := range(pfm.cache.iovecs) {
		if chunk.shouldPrefetch && !chunk.submitted {
			pgs.ioQueue <- IoReq{
				fh : pfm.fh,
				url : *pfm.fh.url,
				ioSize : chunk.ioSize,
				startByte : chunk.startByte,
				endByte : chunk.endByte,
			}
			chunk.submitted = true
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
	if numAccessed < numSlotsInChunk {
		return
	}

	// A sufficient number of the slots were accessed. Start a prefetch for
	// the next chunk(s)
	if pfm.state == PFM_DETECT_SEQ {
		pfm.state = PFM_PREFETCH_IN_PROGRESS
		pfm.sequentiallyAccessed = true
	}

	// increase read ahead io size, using a bounded exponential formula
	if pfm.cache.prefetchIoSize < prefetchMaxIoSize {
		pfm.cache.prefetchIoSize =
			MinInt64(prefetchMaxIoSize, pfm.cache.prefetchIoSize * prefetchIoFactor)
	}
	if pfm.cache.prefetchIoSize == prefetchMaxIoSize {
		// normally, we give one read-head request to each stream. However,
		// if there is only one open stream, we give it a lot more. This
		// is an optimization for the special case of a single open file.
		nReadAhead := 1
		nStreams := len(pgs.files)
		if nStreams == 1 {
			nReadAhead = pgs.maxNumChunksReadAhead
		}
		pfm.cache.numChunksReadAhead = nReadAhead
		pfm.cache.maxNumIovecs = nReadAhead + 2
	}

	pgs.moveCacheWindow(pfm, last)
	pgs.submitIoForHoles(pfm)

	// Have we reached the end of the file?
	if pfm.cache.endByte >= pfm.fh.f.Size - 1 {
		pfm.state = PFM_EOF
	}
}

// If the range is entirely in cache, return a byte array with the data.
// Otherwise, return nil.
func (pgs *PrefetchGlobalState) getDataFromCache(
	pfm *PrefetchFileMetadata,
	startOfs int64,
	endOfs int64) []byte {
	// quick check to see if the data is in cache.
	if !(pfm.cache.startByte <= startOfs &&
		pfm.cache.endByte >= endOfs) {
		return nil
	}

	// go through the io-vectors, and check if they contain the range
	for _, iov := range pfm.cache.iovecs {
		if iov.startByte <= startOfs &&
			iov.endByte >= endOfs {
			// its inside this iovec
			if iov.data == nil {
				// waiting for prefetch to come back with data
				return nil
			}
			bgnByte := startOfs - iov.startByte
			endByte := endOfs - iov.startByte
			return iov.data[bgnByte : endByte+1]
		}
	}

	// FIXME: we don't handle IOs that stradle boundaries of
	// cached data. nil is returned for such cases.
	//
	if pgs.verbose {
		pfm.log("Data is in cache, but falls on an iovec boundary ofs=%d len=%d",
			startOfs, (endOfs - startOfs))
	}
	return nil
}

// This is done on behalf of a user read request, check if this range is cached of prefetched
// and return the data if so. Otherwise, return nil.
//
func (pgs *PrefetchGlobalState) cacheLookup1(fh *FileHandle, startOfs int64, endOfs int64) ([]byte, int) {
	pgs.mutex.Lock()
	defer pgs.mutex.Unlock()
	pfm, ok := pgs.files[fh]
	if !ok {
		// file is not tracked, no prefetch data is available
		return nil, DATA_OUTSIDE_CACHE
	}
	now := time.Now()
	if !pgs.isWorthIt(pfm, now) {
		// file is not accessed in streaming fashion. It should be
		// removed from the table.
		return nil, DATA_ACCESSED_NON_SEQUENTIALLY
	}

	// accounting and statistics
	pfm.lastIoTimestamp = now
	pfm.hiUserAccessOfs = MaxInt64(pfm.hiUserAccessOfs, startOfs)
	pfm.numIOs++

	switch pfm.state {
	case PFM_DETECT_SEQ:
		// No data is cached. Only detecting if there is sequential access.
		pgs.markAccessedAndMaybeStartPrefetch(pfm, startOfs, endOfs)
		return nil, DATA_OUTSIDE_CACHE

	case PFM_PREFETCH_IN_PROGRESS:
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
			return nil, DATA_ACCESSED_NON_SEQUENTIALLY
		}

		// ongoing prefetch IO, and the range is in cache
		pgs.markAccessedAndMaybeStartPrefetch(pfm, startOfs, endOfs)

		// we may need to wait for a prefetch IO to return
		for pfm.state == PFM_PREFETCH_IN_PROGRESS {
			data := pgs.getDataFromCache(pfm, startOfs, endOfs)
			if data != nil {
				return data, DATA_IN_CACHE
			}
			// data is not in cache yet, we are waiting for a
			// prefetch IO.
			if pgs.verbose {
				pfm.log("went to sleep io=[%d -- %d]", startOfs, endOfs)
			}
			pfm.cond.Wait()
			if pgs.verbose {
				pfm.log("woke up from waiting")
			}
		}
		return nil, DATA_IN_CACHE

	case PFM_IO_ERROR:
		// The prefetch IO incurred an error. We stopped
		// doing prefetch.
		return nil, DATA_OUTSIDE_CACHE

	case PFM_EOF:
		// don't issue any additional IOs
		data := pgs.getDataFromCache(pfm, startOfs, endOfs)
		return data, DATA_IN_CACHE

	default:
		panic(fmt.Sprintf("bad state %d for fileId=%s", pfm.state, pfm.fh.f.Id))
	}
}

func (pgs *PrefetchGlobalState) CacheLookup(fh *FileHandle, startOfs int64, endOfs int64) []byte {
	data, retval := pgs.cacheLookup1(fh, startOfs, endOfs)
	switch retval {
	case DATA_IN_CACHE:
		// note, the data may be nil here, but that is ok,
		// because the caller will do a synchronous IO instead.
		return data

	case DATA_OUTSIDE_CACHE:
		return nil

	case DATA_ACCESSED_NON_SEQUENTIALLY:
		// file is accessed non-sequentially, remove it from the table.
		pgs.mutex.Lock()
		pfm := pgs.files[fh]
		pgs.mutex.Unlock()
		if pfm != nil {
			pgs.RemoveStreamEntry(pfm.fh)
		}
		return nil

	default:
		panic(fmt.Sprintf("bad retval from cacheLookupOnce %d", retval))
	}
}
