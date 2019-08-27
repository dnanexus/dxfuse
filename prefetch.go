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
	PREFETCH_EFFECT_THRESH = 0.8         // Prefetch effectiveness should be at least this

	PREFETCH_IO_SIZE = (1 * MiB)         // size of prefetch IO
	MAX_NUM_IOVECS_IN_FILE_CACHE = 9

	// An active stream can use a significant amount of memory to store prefetched data.
	// Limit the total number of streams we are tracking and prefetching.
	MAX_NUM_ENTRIES_IN_TABLE = 8
	NUM_MEM_CHUNKS = CACHE_MAX_SIZE / PREFETCH_MAX_IO_SIZE

	NUM_PREFETCH_THREADS = 16
	MIN_FILE_SIZE = 8 * MiB     // do not track files smaller than this size

	PFM_DETECT_SEQ = 1      // Beginning of the file, detecting if access is sequential
	PFM_PREFETCH_IN_PROGRESS = 2  // normal case --- prefetch is ongoing
	PFM_IO_ERROR = 3
	PFM_EOF = 4   // reached the end of the file
)


type Iovec struct {
	// The page this chunk belongs to
	pfm       *PrefetchFileMetadata

	ioSize     int64   // The io size
	startByte  int64   // start byte, counting from the beginning of the file.
	endByte    int64
	touched    uint64  // mark the areas that have been accessed by the user
	data       []byte

	// Allow user reads to wait until the IO completes
	cond      *sync.Cond
}

// A cache of all the data retrieved from the platform, for one file.
// It is a contiguous range of chunks. All IOs are the same size.
type PrefetchCache struct {
	startByte   int64
	endByte     int64
	iovecs      [](*Iovec)
}

type PrefetchFileMetadata struct {
	// the file being tracked
	fh        *FileHandle

	// Last time an IO hit this file
	lastIo     time.Time
	sequentiallyAccessed bool  // estimate if this file is sequentially accessed
	numPrefetchIOs int

	// cached io vectors.
	// The assumption is that the user is accessing the last io-vector.
	// If this assumption isn't true, prefetch is ineffective. The algorithm
	// should detect and stop it.
	cache      PrefetchCache

	state      int
}

// global limits
type PrefetchGlobalState struct {
	debug        bool
	mutex        sync.Mutex  // Lock used to control the files table
	files        map[string](*PrefetchFileMetadata) // tracking state per file-id
	ioQueue      chan (*Iovec)    // queue of IOs to prefetch
}

func (pgs *PrefetchGlobalState) Init(debug bool) {
	pgs.debug = debug

	pgs.files = make(map[string](*PrefetchFileMetadata))
	pgs.ioQueue = make(chan (*Iovec))

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


func (pfm *PrefetchFileMetadata) addIovecToCache(iovec *Iovec) {
	// Count in chunks from the beginning of the file. Find the index
	// of this iovec according to this account.
	n := iovec.startByte / PREFETCH_IO_SIZE
	bgnN := pfm.cache.startByte / PREFETCH_IO_SIZE
	requiredNumChunks := n - bgnN

	// increase the size of the vector, so we can place the new chunk in
	// the correct place
	for len(pfm.cache.iovecs) < requiredNumChunks {
		pfm.cache.iovecs = append(pfm.cache.iovecs, nil)
	}
	pfm.cache.iovecs[requiredNumChunks - 1] = iovec

	if len(pfm.cache.iovecs) >= MAX_NUM_IOVECS_IN_FILE_CACHE {
		// we want to limit the amount of cached data.
		// we chop off the beginning of the vector, and
		// reduce memory consumption.
		start := len(pfm.cache.iovecs) - MAX_NUM_IOVECS_IN_FILE_CACHE
		for i := 0; i < start; i++ {
			pfm.cache.iovecs[i].data = nil
			if pfm.sequentiallyAccessed {
				// Estimate if the file is sequentially accessed.
				// Check if the last chunk has been fully accessed
				numAccessed := bits.OnesCount64(pfm.cache.iovecs[i])
				pfm.sequentiallyAccessed = (numAccessed == 64)
			}
		}
		pfm.cache.iovecs = pfm.cache.iovecs[start:]
	}

	// recalculate the range covered by the cache
	nIovecs := len(pfm.cache.iovecs)
	if nIovecs == 0 {
		pfm.startByte = 0
		pfm.endByte = 0
	} else {
		pfm.cache.startByte = pfm.cache.iovecs[0].startByte
		pfm.cache.endByte = pfm.cache.iovecs[nIovecs-1].endByte
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
		data, err := pgs.readData(client, startByte, endByte, url)
		ioReq.data = data

		pgs.mutex.Lock()
		pfm := ioReq.pfm

		if err != nil {
			log.Printf("Prefetch error ofs=%d len=%d error=%s",
				pfm.current.startByte, pfm.current.ioSize, err.Error())
			pfm.state = PFM_IO_ERROR
		} else {
			pfm.addIovecToCache(ioReq)
		}
		pgs.mutex.Unlock()

		// release all the reads waiting for the prefetch IO to complete
		ioReq.cond.Broadcast()
	}
}


// Check if a file is worth tracking.
func (pgs *PrefetchGlobalState) isWorthIt(pfm *PrefetchFileMetadata) bool {
	if pfm.state == PFM_IO_ERROR {
		// we had an IO error, get rid of this file
		return false
	}
	if (pfm.state == PFM_PREFETCH_IN_PROGRESS &&
		!pfm.sequentiallyAccessed) {
		// The file is not sequentially accessed, but we
		// doing prefetch on it.
		return false
	}

	now := time.Now()
	if now.After(pfm.lastIo.Add(MAX_DELTA_TIME)) {
		// File has not been accessed recently
		return false
	}
	pfm.lastIo = now

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
		var toRemove []string

		// go over the table, and find all the files not worth tracking
		pgs.mutex.Lock()
		for fileId, pfm := range pgs.files {
			if !pgs.isWorthIt(pfm) {
				toRemove = append(toRemove, fileId)
			}
		}

		for _, fileId := range toRemove {
			delete(pgs.files, fileId)
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
	entry.cache = nil

	// setup so we can detect a sequential stream.
	// There is no data held in cache yet.
	entry.cache.startByte = 0
	entry.cache.endByte = PREFETCH_MIN_IO_SIZE - 1
	entry.cache.iovecs = make([](*Iovec), 1)
	entry.current.iovecs[0] = &Iovec{
		pfm : nil,
		ioSize : PREFETCH_MIN_IO_SIZE,
		startByte : 0,
		endByte : PREFETCH_MIN_IO_SIZE,
		touched : 0,
		data : nil,
		cond : nil,
	}

	// Initial state of the file; detect if it is accessed sequentially.
	entry.state = PFM_DETECT_SEQ

	pgs.files[fh.f.FileId] = &entry
}

func (pgs *PrefetchGlobalState) RemoveFileEntry(fh *FileHandle) {
	pgs.mutex.Lock()
	defer pgs.mutex.Unlock()

	fileId := fh.f.FileId
	if _, ok := pgs.files[fileId]; ok {
		if pgs.debug {
			log.Printf("prefetch: RemoveFileEntry %s", fh.f.Name)
		}
		delete(pgs.files, fileId)
	}
}


func (pgs *PrefetchGlobalState) markAccessedAndMaybeStartPrefetch(
	pfm *PrefetchFileMetadata,
	startOfs int64,
	endOfs int64) {
	if !(startOfs >= pfm.current.startByte &&
		endOfs <= pfm.current.endByte) {
		// IO is out of range
		return
	}
	if pfm.state == PFM_EOF {
		// we are already at the end of the file, no more
		// prefetch is necessary. When the user closes the file,
		// the state will be removed.
		return
	}

	// we have 64 bits, split the space evenly between them
	slotSize := pfm.current.ioSize / 64
	startSlot := (startOfs - pfm.current.startByte) / slotSize
	endSlot := (endOfs - pfm.current.startByte) / slotSize
	for slot := startSlot; slot <= endSlot ; slot++ {
		// Sets the bit at position [slot]
		pfm.current.touched |= (1 << uint(slot))
	}

	if pfm.state != PFM_DETECT_SEQ {
		return
	}

	numSlotsThresh := 64
	if pfm.current.ioSize == PREFETCH_MAX_IO_SIZE {
		numSlotsThresh = 16
	}

	// should we start prefetching?
	numAccessed := bits.OnesCount64(pfm.current.touched)
/*	if pgs.debug {
		log.Printf("prefetch touch: %d -- %d, numAccessed=%d",
			startSlot, endSlot, numAccessed)
	}*/
	if numAccessed < numSlotsThresh {
		return
	}

	// A sufficient number of the slots were accessed. Start a prefetch for the
	// the next chunk
	pfm.state = PFM_IO_IN_PROGRESS

	// Setup state for the next prefetch.
	// Exponentially increase the prefetch size, assuming it is successful,
	// until the maximal size.
	ioSize := pfm.current.ioSize * PREFETCH_FACTOR_MULTIPLIER
	if ioSize > PREFETCH_MAX_IO_SIZE {
		ioSize = PREFETCH_MAX_IO_SIZE
	}
	startByte := pfm.current.startByte + pfm.current.ioSize

	pfm.current.ioSize = ioSize
	pfm.current.startByte = startByte
	pfm.current.endByte = pfm.current.startByte + pfm.current.ioSize - 1
	if pfm.current.endByte >= pfm.fh.f.Size {
		// don't go over the file size
		pfm.current.endByte = pfm.fh.f.Size - 1
	}
	pfm.current.touched = 0

	// sanity check, don't go beyond the file size
	if pfm.current.startByte >= pfm.fh.f.Size - 1 {
		return
	}
	// enqueue on the work queue
	pgs.ioQueue <- pfm
}

func (pgs *PrefetchGlobalState) getDataFromCache(
	pfm *PrefetchFileMetadata,
	startOfs int64,
	endOfs int64) []byte {
	if pfm.cache == nil {
		return nil
	}
	if !(pfm.cache.startByte <= startOfs &&
		pfm.cache.endByte >= endOfs) {
		// not in cache
		return nil
	}

	// go through the io-vectors, and check if they contain the range
	for _, op := range pfm.cache.iovecs {
		if op.startByte <= startOfs &&
			op.endByte >= endOfs {
			// its inside this iovec
			pfm.numHits++
			bgnByte := startOfs - op.startByte
			endByte := endOfs - op.startByte
			return op.data[bgnByte : endByte+1]
		}
	}

	if pgs.debug {
		log.Printf("Data is in cache, but falls on an iovec boundary ofs=%d len=%d",
			startOfs, (endOfs - startOfs))
	}
	return nil
}

// This is done on behalf of a user read request, check if this range is cached of prefetched
// and return the data if so. Otherwise, return nil.
//
// note: we don't handle partial IOs. That means, IOs that fall on the boundaries of cached/prefetched
// data. nil is returned for such cases.
//
func (pgs *PrefetchGlobalState) Check(fileId string, url DxDownloadURL, startOfs int64, endOfs int64) []byte  {
	pgs.mutex.Lock()
	defer pgs.mutex.Unlock()
	pfm, ok := pgs.files[fileId]
	if !ok {
		// file is not tracked, no prefetch data is available
		return nil
	}

	pfm.numIOs++

	if pfm.current.endByte < endOfs {
		// IO is out of range
		return nil
	}

	// handle some simple cases first
	if (pfm.cache != nil &&
		pfm.cache.startByte > startOfs) {
		// The IO is, at least partly, outside
		// the cached area.
		return nil
	}

	switch pfm.state {
	case PFM_DETECT_SEQ:
		// just detecting if we there is sequential access.
		// no data is cached.
		pgs.markAccessedAndMaybeStartPrefetch(pfm, startOfs, endOfs)

	case PFM_IO_IN_PROGRESS:
		// ongoing prefetch IO
		if !(startOfs <= pfm.current.startByte &&
			endOfs <= pfm.current.endByte) {
			// We are not in the prefetch range, and not in cache
			return nil
		}

		// we are waiting for the prefetch to complete
		for pfm.state == PFM_IO_IN_PROGRESS {
			pfm.cond.Wait()
		}
		if pgs.debug {
			log.Printf("prefetch: woke up from waiting")
		}
		pgs.markAccessedAndMaybeStartPrefetch(pfm, startOfs, endOfs)
		// We woke up from sleep, holding the lock.
		// Get the data from cache, if it is there.

	case PFM_IO_ERROR:
		// The prefetch IO incurred an error.
		return nil

	case PFM_EOF:
		// do nothing

	default:
		panic(fmt.Sprintf("bad state %d for fileId=%s", pfm.state, pfm.fh.f.FileId))
	}

	return pgs.getDataFromCache(pfm, startOfs, endOfs)
}
