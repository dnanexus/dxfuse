package dxfs2

// When a file is opened, it is added to the global prefetch map. Once removed,
// it can never return. This means that to check if a file is being streamed, all
// we need to do is check the map.
import (
	//"context"
	"fmt"
	"log"
	"math/bits"
	"sync"
	"time"

	"github.com/dnanexus/dxda"
	"github.com/hashicorp/go-retryablehttp" // use http libraries from hashicorp for implement retry logic
)

const (
	MAX_DELTA_TIME = 5 * 60 * time.Second

	PREFETCH_MIN_IO_SIZE = (256 * KiB)     // threshold for deciding the file is sequentially accessed
	PREFETCH_MAX_IO_SIZE = (4 * MiB)     // size of prefetch IO
	PREFETCH_IO_FACTOR = 4
	MAX_NUM_CHUNKS_READ_AHEAD = 8

	NUM_SLOTS_IN_CHUNK = 64

	// An active stream can use a significant amount of memory to store prefetched data.
	// Limit the total number of streams we are tracking and prefetching.
	MAX_NUM_ENTRIES_IN_TABLE = 8

	NUM_PREFETCH_THREADS = 10
	MIN_FILE_SIZE = 8 * MiB     // do not track files smaller than this size

	// enumerate type for the state of a PFM (file metadata)
	PFM_DETECT_SEQ = 1      // Beginning of the file, detecting if access is sequential
	PFM_PREFETCH_IN_PROGRESS = 2  // normal case --- prefetch is ongoing
	PFM_IO_ERROR = 3
	PFM_EOF = 4   // reached the end of the file

	// enumerated type for returning replies from a cache lookup
	DATA_IN_CACHE = 1
	DATA_OUTSIDE_CACHE = 2
	DATA_WAITING_FOR_PREFETCH_IO = 3
	DATA_ACCESSED_NON_SEQUENTIALLY = 4
)


// A request that one of the IO-threads will pick up
type IoReq struct {
	fileId     string
	url        DxDownloadURL

	ioSize     int64   // The io size
	startByte  int64   // start byte, counting from the beginning of the file.
	endByte    int64
	data       []byte
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

	// Last time an IO hit this file
	lastIo               time.Time
	sequentiallyAccessed bool  // estimate if this file is sequentially accessed
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

// global limits
type PrefetchGlobalState struct {
	verbose      bool
	verboseLevel int
	mutex        sync.Mutex  // Lock used to control the files table
	files        map[string](*PrefetchFileMetadata) // tracking state per file-id
	ioQueue      chan IoReq    // queue of IOs to prefetch
}

func (pgs *PrefetchGlobalState) Init(verboseLevel int) {
	pgs.verbose = verboseLevel >= 1
	pgs.verboseLevel = verboseLevel

	pgs.files = make(map[string](*PrefetchFileMetadata))
	pgs.ioQueue = make(chan IoReq)

	// limit the number of prefetch IOs
	for i := 0; i < NUM_PREFETCH_THREADS; i++ {
		go pgs.prefetchIoWorker()
	}

	// start a periodic thread to cleanup the table if needed
	go pgs.tableCleanupWorker()
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
		log.Printf("prefetch: reading extent from DNAx  ofs=%d len=%d", startByte, len)
	}

	headers := make(map[string]string)

	// Copy the immutable headers
	for key, value := range url.Headers {
		headers[key] = value
	}
	headers["Range"] = fmt.Sprintf("bytes=%d-%d", startByte, endByte)

	data, err := dxda.DxHttpRequest(client, "GET", url.URL, headers, []byte("{}"))
	if pgs.verbose {
		if err == nil {
			log.Printf("prefetch: IO returned correctly len=%d", len(data))
		} else {
			log.Printf("prefetch: IO returned with error %s", err.Error())
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

// We are holding the global code at this point.
// Wake up waiting IOs, if any.
func (pgs *PrefetchGlobalState) addIoReqToCache(pfm *PrefetchFileMetadata, ioReq IoReq) {
	if pfm.state == PFM_IO_ERROR {
		log.Printf("Dropping prefetch IO, file has encountered an error (%s)",
			ioReq.fileId)
		return
	}

	// Find the index for this chunk in the cache. The chunks may be different
	// size, so we need to scan.
	iovIdx := findIovecIndex(pfm, ioReq)
	if iovIdx == -1 {
		log.Printf("Dropping prefetch IO, matching entry in cache not found")
		log.Printf("ioReq=%v len(cache.iovecs)=%d", ioReq, len(pfm.cache.iovecs))
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
		log.Printf("contiguity of cache chunks: %d  iovIdx=%d", contigLen, iovIdx)
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
		ioReq := <-pgs.ioQueue

		// perform the IO. We don't want to hold any locks while we
		// are doing this, because this request could take a long time.
		data, err := pgs.readData(client, ioReq.startByte, ioReq.endByte, ioReq.url)
		ioReq.data = data

		pgs.mutex.Lock()
		// Find the file this IO belongs to
		pfm, ok := pgs.files[ioReq.fileId]
		if !ok {
			// file is not tracked anymore
			log.Printf("Dropping prefetch IO, file is no longer tracked (%s)", ioReq.fileId)
		} else {
			if err != nil {
				log.Printf("Prefetch error ofs=%d len=%d error=%s",
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

	if now.After(pfm.lastIo.Add(MAX_DELTA_TIME)) {
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
		if pgs.verbose {
			log.Printf("prefetch: tableCleanupWorkder")
		}

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

		for _, pfm := range toRemove {
			// wake up any pending IOs
			pfm.cond.Broadcast()
			delete(pgs.files, pfm.fh.f.FileId)
		}
		pgs.mutex.Unlock()
	}
}

func (pgs *PrefetchGlobalState) newPrefetchFileMetadata(fh *FileHandle) *PrefetchFileMetadata {
	var entry PrefetchFileMetadata
	entry.fh = fh
	entry.lastIo = time.Now()
	entry.sequentiallyAccessed = false
	entry.numIOs = 0
	entry.numPrefetchIOs = 0

	// Initial state of the file; detect if it is accessed sequentially.
	entry.state = PFM_DETECT_SEQ

	// setup so we can detect a sequential stream.
	// There is no data held in cache yet.
	entry.cache = Cache{
		prefetchIoSize : PREFETCH_MIN_IO_SIZE,
		numChunksReadAhead : 1,
		maxNumIovecs : 3,
		startByte : 0,
		endByte : PREFETCH_MIN_IO_SIZE - 1,
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

func (pgs *PrefetchGlobalState) CreateFileEntry(fh *FileHandle) {
	pgs.mutex.Lock()
	defer pgs.mutex.Unlock()

	// if the table is at the size limit, do not create a new entry
	if len(pgs.files) >= MAX_NUM_ENTRIES_IN_TABLE {
		return
	}

	// The file has to have sufficient size, to merit an entry. We
	// don't want to waste entries on small files
	if fh.f.Size < MIN_FILE_SIZE {
		return
	}

	if pgs.verbose {
		log.Printf("prefetch: CreateFileEntry %s", fh.f.Name)
	}
	pgs.files[fh.f.FileId] = pgs.newPrefetchFileMetadata(fh)
}

func (pgs *PrefetchGlobalState) RemoveFileEntry(fh *FileHandle) {
	pgs.mutex.Lock()
	defer pgs.mutex.Unlock()

	fileId := fh.f.FileId
	if pfm, ok := pgs.files[fileId]; ok {
		if pgs.verbose {
			log.Printf("prefetch: RemoveFileEntry %s", fh.f.Name)
		}

		// wake up any waiting synchronous user IOs
		pfm.cond.Broadcast()

		// remove from the table
		delete(pgs.files, fileId)
	}
}

func markRangeInIovec(iovec *Iovec, startOfs int64, endOfs int64) {
	startOfsBoth := MaxInt64(iovec.startByte, startOfs)
	endOfsBoth := MinInt64(iovec.endByte, endOfs)

	// now we know that there is some intersection
	slotSize := iovec.ioSize / NUM_SLOTS_IN_CHUNK
	startSlot := (startOfsBoth - iovec.startByte) / slotSize
	endSlot := (endOfsBoth - iovec.startByte) / slotSize
	check(startSlot >= 0)
	if !(endSlot >= 0 && endSlot <= NUM_SLOTS_IN_CHUNK) {
		log.Printf("offset(%d -- %d),  slots=(%d -- %d), iovec=(%d -- %d)",
			startOfs, endOfs,
			startSlot, endSlot,
			iovec.startByte, iovec.endByte)
	}
	check(endSlot >= 0 && endSlot <= NUM_SLOTS_IN_CHUNK)

	for slot := startSlot; slot <= endSlot ; slot++ {
		// Sets the bit at position [slot]
		iovec.touched |= (1 << uint(slot))
	}
}

// Find the range of io-vectors in cache that cover this IO
func (pgs *PrefetchGlobalState) findCoveringRange(
	pfm *PrefetchFileMetadata,
	startOfs int64,
	endOfs int64) (int, int) {
	first := -1
	last := -1

	if pgs.verboseLevel >= 2 {
		for i, iovec := range pfm.cache.iovecs {
			log.Printf("cache %d -> [%d -- %d]", i, iovec.startByte, iovec.endByte)
		}
	}

	for k, iovec := range pfm.cache.iovecs {
		if iovec.startByte <= startOfs &&
			iovec.endByte >= startOfs {
			first = k
			break
		}
	}

	for k, iovec := range pfm.cache.iovecs {
		if iovec.startByte <= endOfs &&
			iovec.endByte >= endOfs {
			last = k
			break
		}
	}

	check(first >= 0)
	check(last >= 0)
	if pgs.verboseLevel >= 2 {
		log.Printf("findCoveringRange: first,last=(%d,%d)  IO=[%d -- %d]",
			first, last, startOfs, endOfs)
	}

	return first, last
}


// Setup cache state for the next prefetch.
//
// 1) we want there to be numChunksReadAhead chunks ahead of us.
// 2) we don't want the entire file-cache to go above MAX_NUM_IOVECS_IN_FILE_CACHE
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
		log.Printf("moveCacheWindow  iovIndex=%d  len(iovecs)=%d",
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
			check(emptyIov.ioSize <= PREFETCH_MAX_IO_SIZE)
			pfm.cache.iovecs = append(pfm.cache.iovecs, emptyIov)

			if pgs.verboseLevel >= 2 {
				log.Printf("Adding chunk %d [%d -- %d]", i, emptyIov.startByte, emptyIov.endByte)
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
			pfm.cache.iovecs[i].data = nil
			if pfm.state == PFM_PREFETCH_IN_PROGRESS &&
				pfm.cache.iovecs[i].submitted &&
				pfm.cache.iovecs[i].shouldPrefetch {
				// Estimate if the file is sequentially accessed,
				// by checking if the chunk we are discarding was fully accessed.
				numAccessed := bits.OnesCount64(pfm.cache.iovecs[i].touched)
				fullyAccessed := (numAccessed == NUM_SLOTS_IN_CHUNK)
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
				fileId : pfm.fh.f.FileId,
				url : pfm.fh.url,
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
	first, last := pgs.findCoveringRange(pfm, startOfs, endOfs)
	for i := first; i <= last; i++ {
		markRangeInIovec(pfm.cache.iovecs[i], startOfs, endOfs)
	}

	// find the iovec in cache where this IO falls. We use the right edge
	// of the IO.
	currentIovec := pfm.cache.iovecs[last]
	numAccessed := bits.OnesCount64(currentIovec.touched)
	if pgs.verboseLevel >= 2 {
		log.Printf("prefetch touch: ofs=%d  len=%d  numAccessed=%d",
			startOfs, endOfs - startOfs, numAccessed)
	}
	if numAccessed < NUM_SLOTS_IN_CHUNK {
		return
	}

	// A sufficient number of the slots were accessed. Start a prefetch for
	// the next chunk(s)
	if pfm.state == PFM_DETECT_SEQ {
		pfm.state = PFM_PREFETCH_IN_PROGRESS
		pfm.sequentiallyAccessed = true
	}

	// increase read ahead io size, using a bounded exponential formula
	if pfm.cache.prefetchIoSize < PREFETCH_MAX_IO_SIZE {
		pfm.cache.prefetchIoSize =
			MinInt64(PREFETCH_MAX_IO_SIZE, pfm.cache.prefetchIoSize * PREFETCH_IO_FACTOR)
	}
	if pfm.cache.prefetchIoSize == PREFETCH_MAX_IO_SIZE {
		// if we are at the maximal io size, start issuing asynchronous
		// readahead requests.
		pfm.cache.numChunksReadAhead = MAX_NUM_CHUNKS_READ_AHEAD
		pfm.cache.maxNumIovecs = MAX_NUM_CHUNKS_READ_AHEAD + 2
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
		log.Printf("Data is in cache, but falls on an iovec boundary ofs=%d len=%d",
			startOfs, (endOfs - startOfs))
	}
	return nil
}

// This is done on behalf of a user read request, check if this range is cached of prefetched
// and return the data if so. Otherwise, return nil.
//
func (pgs *PrefetchGlobalState) cacheLookup1(fileId string, startOfs int64, endOfs int64) ([]byte, int) {
	pgs.mutex.Lock()
	defer pgs.mutex.Unlock()
	pfm, ok := pgs.files[fileId]
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
	pfm.lastIo = now
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
				log.Printf("IO outside cache  io=[%d -- %d]  cache=[%d -- %d]",
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
				log.Printf("prefetch: went to sleep io=[%d -- %d]", startOfs, endOfs)
			}
			pfm.cond.Wait()
			if pgs.verbose {
				log.Printf("prefetch: woke up from waiting")
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
		panic(fmt.Sprintf("bad state %d for fileId=%s", pfm.state, pfm.fh.f.FileId))
	}
}

func (pgs *PrefetchGlobalState) CacheLookup(fileId string, startOfs int64, endOfs int64) []byte {
	data, retval := pgs.cacheLookup1(fileId, startOfs, endOfs)
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
		pfm := pgs.files[fileId]
		pgs.mutex.Unlock()
		if pfm != nil {
			pgs.RemoveFileEntry(pfm.fh)
		}
		return nil

	default:
		panic(fmt.Sprintf("bad retval from cacheLookupOnce %d", retval))
	}
}
