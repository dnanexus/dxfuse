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

	"github.com/hashicorp/go-retryablehttp" // use http libraries from hashicorp for implement retry logic
)

const (
	CACHE_MAX_SIZE = 512 * MiB           // overall limit on the amount of memory used for prefetch

	MAX_DELTA_TIME = 5 * 60 * time.Second

	PREFETCH_IO_SIZE = (1 * MiB)         // size of prefetch IO
	NUM_CHUNKS_READ_AHEAD = 8
	MAX_NUM_IOVECS_IN_FILE_CACHE = NUM_CHUNKS_READ_AHEAD + 2

	NUM_SLOTS_IN_CHUNK = 64
	SLOT_SIZE = PREFETCH_IO_SIZE / NUM_SLOTS_IN_CHUNK

	// An active stream can use a significant amount of memory to store prefetched data.
	// Limit the total number of streams we are tracking and prefetching.
	MAX_NUM_ENTRIES_IN_TABLE = 8
	NUM_MEM_CHUNKS = CACHE_MAX_SIZE / PREFETCH_IO_SIZE

	NUM_PREFETCH_THREADS = 16
	MIN_FILE_SIZE = 8 * MiB     // do not track files smaller than this size

	// enumerate type for the state of a PFM (file metadata)
	PFM_DETECT_SEQ = 1      // Beginning of the file, detecting if access is sequential
	PFM_PREFETCH_IN_PROGRESS = 2  // normal case --- prefetch is ongoing
	PFM_IO_ERROR = 3
	PFM_EOF = 4   // reached the end of the file

	CACHE_LOOKUP_NUM_RETRIES = 3

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
}

// A cache of all the data retrieved from the platform, for one file.
// It is a contiguous range of chunks. All IOs are the same size.
type Cache struct {
	startByte   int64
	endByte     int64
	iovecs      [](*Iovec)
}

type PrefetchFileMetadata struct {
	// the file being tracked
	fh              *FileHandle
	state            int

	// Last time an IO hit this file
	lastIo           time.Time
	seqAccessed      bool  // estimate if this file is sequentially accessed
	numIOs           int
	numPrefetchIOs   int

	// cached io vectors.
	// The assumption is that the user is accessing the last io-vector.
	// If this assumption isn't true, prefetch is ineffective. The algorithm
	// should detect and stop it.
	cache            Cache

	// Allow user reads to wait until prefetch IOs complete
	cond            *sync.Cond
}

// global limits
type PrefetchGlobalState struct {
	debug        bool
	mutex        sync.Mutex  // Lock used to control the files table
	files        map[string](*PrefetchFileMetadata) // tracking state per file-id
	ioQueue      chan IoReq    // queue of IOs to prefetch
}

func (pgs *PrefetchGlobalState) Init(debug bool) {
	pgs.debug = debug

	pgs.files = make(map[string](*PrefetchFileMetadata))
	pgs.ioQueue = make(chan IoReq)

	// limit the number of prefetch IOs
	for i := 0; i < NUM_PREFETCH_THREADS; i++ {
		go pgs.prefetchIoWorker()
	}

	// start a periodic thread to cleanup the table if needed
	//go pgs.tableCleanupWorker()
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
	if pgs.debug {
		len := endByte - startByte + 1
		log.Printf("prefetch: reading extent from DNAx  ofs=%d len=%d", startByte, len)
	}

	headers := make(map[string]string)

	// Copy the immutable headers
	for key, value := range url.Headers {
		headers[key] = value
	}
	headers["Range"] = fmt.Sprintf("bytes=%d-%d", startByte, endByte)

	data, err := DxHttpRequest(client, "GET", url.URL, headers, []byte("{}"))
	if pgs.debug {
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
	for k := 0; k < len(pfm.cache.iovecs) ; k++ {
		if pfm.cache.iovecs[k].data == nil && k > 0 {
			break
		}
		contigLen++
	}
	return contigLen
}

// We are holding the global code at this point.
// Wake up waiting IOs, if any.
func (pgs *PrefetchGlobalState) addIoReqToCache(pfm *PrefetchFileMetadata, ioReq IoReq) {
	if pfm.state == PFM_IO_ERROR {
		log.Printf("Dropping prefetch IO, file has encountered an error (%s)",
			ioReq.fileId)
		return
	}

	// Count in chunks from the beginning of the file. Find the index
	// of this iovec according to this account.
	n := ioReq.startByte / PREFETCH_IO_SIZE
	bgnN := pfm.cache.startByte / PREFETCH_IO_SIZE

	iovIdx := int(n - bgnN)
	check(pfm.cache.iovecs[iovIdx].data == nil)
	if iovIdx > len(pfm.cache.iovecs)  {
		log.Printf("Dropping prefetch IO, iovec cache not large enough")
		log.Printf("iovIdx=%d len(cache.iovecs)=%d", iovIdx, len(pfm.cache.iovecs))
		return
	}
	pfm.cache.iovecs[iovIdx].data = ioReq.data

	// For GC purposes, not sure we actually need it.
	ioReq.data = nil

	// statistics
	pfm.numPrefetchIOs++

	// if we are extending the contiguous filled chunks, then signal
	contigLen := numContiguousChunks(pfm)
	if pgs.debug {
		log.Printf("contiguity of cache chunks: %d  iovIdx=%d", contigLen, iovIdx)
	}
	if contigLen == iovIdx + 1 {
		pfm.cond.Broadcast()
	}
}


func (pgs *PrefetchGlobalState) prefetchIoWorker() {
	// reuse this http client. The idea is to be able to reuse http connections.
	// I don't know if this actually happens.
	client := newHttpClient()

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
func (pgs *PrefetchGlobalState) isWorthIt(pfm *PrefetchFileMetadata) bool {
	if pfm.state == PFM_IO_ERROR {
		// we had an IO error, get rid of this file
		return false
	}
	if (pfm.state == PFM_PREFETCH_IN_PROGRESS &&
		!pfm.seqAccessed) {
		// The file is no longer sequentially accessed
		return false
	}

	now := time.Now()
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
		if pgs.debug {
			log.Printf("prefetch: tableCleanupWorkder")
		}

		// Files that are not worth tracking
		var toRemove [](*PrefetchFileMetadata)

		// go over the table, and find all the files not worth tracking
		pgs.mutex.Lock()
		for _, pfm := range pgs.files {
			if !pgs.isWorthIt(pfm) {
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

	if pgs.debug {
		log.Printf("prefetch: CreateFileEntry %s", fh.f.Name)
	}

	var entry PrefetchFileMetadata
	entry.fh = fh
	entry.lastIo = time.Now()
	entry.seqAccessed = true
	entry.numIOs = 0
	entry.numPrefetchIOs = 0

	// Initial state of the file; detect if it is accessed sequentially.
	entry.state = PFM_DETECT_SEQ

	// setup so we can detect a sequential stream.
	// There is no data held in cache yet.
	entry.cache.startByte = 0
	entry.cache.endByte = PREFETCH_IO_SIZE - 1
	entry.cache.iovecs = make([](*Iovec), 1)
	entry.cache.iovecs[0] = &Iovec{
		ioSize : PREFETCH_IO_SIZE,
		startByte : 0,
		endByte : PREFETCH_IO_SIZE - 1,
		touched : 0,
		data : nil,
	}
	entry.cond = sync.NewCond(&pgs.mutex)
	pgs.files[fh.f.FileId] = &entry
}

func (pgs *PrefetchGlobalState) RemoveFileEntry(fh *FileHandle) {
	pgs.mutex.Lock()
	defer pgs.mutex.Unlock()

	fileId := fh.f.FileId
	if pfm, ok := pgs.files[fileId]; ok {
		if pgs.debug {
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
	startSlot := (startOfsBoth - iovec.startByte) / SLOT_SIZE
	endSlot := (endOfsBoth - iovec.startByte) / SLOT_SIZE
	check(startSlot >= 0)
	if !(endSlot >= 0 && endSlot <= 63) {
		log.Printf("offset(%d -- %d),  slots=(%d -- %d), iovec=(%d -- %d)",
			startOfs, endOfs,
			startSlot, endSlot,
			iovec.startByte, iovec.endByte)
	}
	check(endSlot >= 0 && endSlot <= 63)

	for slot := startSlot; slot <= endSlot ; slot++ {
		// Sets the bit at position [slot]
		iovec.touched |= (1 << uint(slot))
	}
}

// Find the range of io-vectors in cache that cover this IO
func findCoveringRange(pfm *PrefetchFileMetadata, startOfs int64, endOfs int64) (int, int) {
	first := -1
	last := -1

	for k, iovec := range pfm.cache.iovecs {
		if iovec.startByte <= startOfs ||
			iovec.endByte >= startOfs {
			first = k
			break
		}
	}

	for k, iovec := range pfm.cache.iovecs {
		if iovec.startByte <= endOfs ||
			iovec.endByte >= endOfs {
			last = k
			break
		}
	}

	check(first >= 0)
	check(last >= 0)
	return first, last
}


// Setup state for the next prefetch.
func (pgs *PrefetchGlobalState) setupForPrefetch(pfm *PrefetchFileMetadata, iovIndex int) {
	// 1) we want there to be 8 NUM_CHUNKS_READ_AHEAD chunks ahead of us.
	// 2) we don't want the entire file-cache to go above MAX_NUM_IOVECS_IN_FILE_CACHE
	nIovecs := len(pfm.cache.iovecs)
	if (iovIndex + NUM_CHUNKS_READ_AHEAD) <= nIovecs {
		// There is already a sufficient number of prefetches ahead of us
		return
	}

	// stretch the cache forward, but don't go over the file size
	lastByteInFile := pfm.fh.f.Size - 1
	for i := nIovecs; i <= iovIndex + NUM_CHUNKS_READ_AHEAD; i++ {
		startByte := (int64(i) * int64(PREFETCH_IO_SIZE)) + pfm.cache.startByte

		// don't go beyond the file size
		if startByte > lastByteInFile {
			break
		}
		endByte := MinInt64(startByte + int64(PREFETCH_IO_SIZE) , lastByteInFile)

		emptyIov := &Iovec{
			ioSize : endByte - startByte + 1,
			startByte : startByte,
			endByte : endByte,
			touched : 0,
			data : nil,
		}
		pfm.cache.iovecs = append(pfm.cache.iovecs, emptyIov)

		// add a prefetch IO on the work queue
		pgs.ioQueue <- IoReq{
			fileId : pfm.fh.f.FileId,
			url : pfm.fh.url,
			ioSize : emptyIov.ioSize,
			startByte : emptyIov.startByte,
			endByte : emptyIov.endByte,
		}
	}

	nIovecs = len(pfm.cache.iovecs)
	if nIovecs >= MAX_NUM_IOVECS_IN_FILE_CACHE {
		// we want to limit the amount of cached data.
		// we chop off the beginning of the vector, and
		// reduce memory consumption.
		start := nIovecs - MAX_NUM_IOVECS_IN_FILE_CACHE
		for i := 0; i < start; i++ {
			pfm.cache.iovecs[i].data = nil
			if pfm.seqAccessed {
				// Estimate if the file is sequentially accessed.
				// Check if the last chunk has been fully accessed
				numAccessed := bits.OnesCount64(pfm.cache.iovecs[i].touched)
				pfm.seqAccessed = (numAccessed == 64)
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

func (pgs *PrefetchGlobalState) markAccessedAndMaybeStartPrefetch(
	pfm *PrefetchFileMetadata,
	startOfs int64,
	endOfs int64) {
	// Mark the areas in cache that this IO accessed
	first, last := findCoveringRange(pfm, startOfs, endOfs)
	for i := first; i <= last; i++ {
		markRangeInIovec(pfm.cache.iovecs[i], startOfs, endOfs)
	}

	// find the iovec in cache where this IO falls. We use the right edge
	// of the IO.
	currentIovec := pfm.cache.iovecs[last]

	numAccessed := bits.OnesCount64(currentIovec.touched)
	if pgs.debug {
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

		// the first chunk in cache has no data at this point, we need to remove
		// it
/*		pfm.cache.iovecs = make([]*(Iovec), 0)
		pfm.cache.startByte = 0
		pfm.cache. = 0*/
	}
	check(pfm.state == PFM_PREFETCH_IN_PROGRESS)

	pgs.setupForPrefetch(pfm, last)
}

func (pgs *PrefetchGlobalState) getDataFromCache(
	pfm *PrefetchFileMetadata,
	startOfs int64,
	endOfs int64) []byte {
	if !(pfm.cache.startByte <= startOfs &&
		pfm.cache.endByte >= endOfs) {
		// not in cache
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

	// FIXME: we don't handle partial IOs. That means, IOs that
	// fall on the boundaries of cached/prefetched data. nil is
	// returned for such cases.
	//

	if pgs.debug {
		log.Printf("Data is in cache, but falls on an iovec boundary ofs=%d len=%d",
			startOfs, (endOfs - startOfs))
	}
	return nil
}

// This is done on behalf of a user read request, check if this range is cached of prefetched
// and return the data if so. Otherwise, return nil.
//
func (pgs *PrefetchGlobalState) cacheLookupOnce(fileId string, startOfs int64, endOfs int64) ([]byte, int) {
	pgs.mutex.Lock()
	defer pgs.mutex.Unlock()
	pfm, ok := pgs.files[fileId]
	if !ok {
		// file is not tracked, no prefetch data is available
		return nil, DATA_OUTSIDE_CACHE
	}
	if !pfm.seqAccessed {
		// file is not accessed in streaming fashion. It should be
		// removed from the table.
		return nil, DATA_ACCESSED_NON_SEQUENTIALLY
	}

	// handle some simple cases first
	if !(pfm.cache.startByte <= startOfs &&
		pfm.cache.endByte >= endOfs) {
		// The IO is, at least partly, outside
		// the cached area.
		return nil, DATA_OUTSIDE_CACHE
	}

	// accounting and statistics
	pfm.lastIo = time.Now()
	pfm.numIOs++

	switch pfm.state {
	case PFM_DETECT_SEQ:
		// just detecting if we there is sequential access.
		// no data is cached.
		pgs.markAccessedAndMaybeStartPrefetch(pfm, startOfs, endOfs)
		return nil, DATA_OUTSIDE_CACHE

	case PFM_PREFETCH_IN_PROGRESS:
		// ongoing prefetch IO, and the range is in cache
		pgs.markAccessedAndMaybeStartPrefetch(pfm, startOfs, endOfs)

		// we are waiting for the prefetch to complete
		data := pgs.getDataFromCache(pfm, startOfs, endOfs)
		if data != nil {
			return data, DATA_IN_CACHE
		}
		pfm.cond.Wait()
		if pgs.debug {
			log.Printf("prefetch: woke up from waiting")
		}
		// We woke up from sleep, the operation needs to be retried
		// Note: we are currently holding the global lock. It will be released
		// when returing from this function by the "defer" mechanism.
		return nil, DATA_WAITING_FOR_PREFETCH_IO

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
	for i := 0; i < CACHE_LOOKUP_NUM_RETRIES; i++ {
		data, retval := pgs.cacheLookupOnce(fileId, startOfs, endOfs)
		switch retval {
		case DATA_IN_CACHE:
			// note, the data may be nil here, but that is ok,
			// because the caller will do a synchronous IO instead.
			return data

		case DATA_OUTSIDE_CACHE:
			return nil

		case DATA_WAITING_FOR_PREFETCH_IO:
			// need to try again

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
	return nil
}
