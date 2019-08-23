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
)

const (
	CACHE_MAX_SIZE = 1 * GiB               // overall limit on the amount of memory used for prefetch

	MAX_DELTA_TIME = 5 * 60 * time.Second
	PREFETCH_EFFECT_THRESH = 0.75      // Prefetch effectiveness should be at least this
	PREFETCH_EFFECT_MIN_NUM_IOS = 20   // Do not calculate effectiveness below this number of IOs

	PREFETCH_FACTOR_MULTIPLIER = 4
	PREFETCH_MIN_IO_SIZE = (256 * KiB) // Minimal size of IO to prefetch
	PREFETCH_MAX_IO_SIZE = (64 * MiB)  // Maximal size of IO to prefetch

	TRAILING_IO_SIZE = 2 * MiB

	// An active stream can use a significant amount of memory to store prefetched data.
	// In the worst case, this is one maximal size IO, and a trailing IO.
	MAX_NUM_ENTRIES_IN_TABLE = CACHE_MAX_SIZE / (PREFETCH_MAX_IO_SIZE + TRAILING_IO_SIZE)

	NUM_PREFETCH_THREADS = 10
	MIN_FILE_SIZE = 10 * MiB                  // do not track files smaller than this size

	PFM_DETECT_SEQ = 1
	PFM_IO_IN_PROGRESS = 2
	PFM_IO_ERROR = 3
)


type PrefetchOp struct {
	ioSize     int64   // The prefetch io size
	startByte  int64
	endByte    int64
	touched    uint64  // mark the areas that have been accessed by the user
	data       []byte
}

type PrefetchCachedData struct {
	startByte  int64
	endByte    int64
	iovecs     []PrefetchOp
}


type PrefetchFileMetadata struct {
	// the file being tracked
	fh        *FileHandle

	// Last time an IO hit this file
	lastIo     time.Time
	numIOs     int   // count how many IOs this files received
	numHits    int   // count how many IOs were hits
	numPrefetchIOs int

	// cached data
	cache     *PrefetchCachedData

	// prefetch IO that may be ongoing. Also used to detect
	// sequential access to the file.
	current    PrefetchOp

	// Allow user reads to wait until the prefetch IO completes
	mutex      sync.Mutex  // Lock used to control this entry
	cond      *sync.Cond

	state      int
}

// global limits
type PrefetchGlobalState struct {
	debug        bool
	tableMutex   sync.Mutex  // Lock used to control the files table
	files        map[string](*PrefetchFileMetadata) // tracking state per file-id
	ioQueue      chan (*PrefetchFileMetadata)   // queue of IOs to prefetch
}

func (pgs *PrefetchGlobalState) Init(debug bool) {
	pgs.debug = debug

	// The edge of the last IO has to:
	// 1. Include the last two slots, where a slot its the maximal possible size
	// 2. Be a multiple of metabyte
	// 3. Needs to be at least one MiB
	atLeast := (PREFETCH_MAX_IO_SIZE / 64)
	if TRAILING_IO_SIZE < atLeast {
		panic(fmt.Sprintf("trailing IO size is too small %d < %d",
			TRAILING_IO_SIZE, atLeast))
	}

	pgs.files = make(map[string](*PrefetchFileMetadata))
	pgs.ioQueue = make(chan (*PrefetchFileMetadata))

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

func (pgs *PrefetchGlobalState) leaveRightEdgeOnly(op *PrefetchOp) {
	edgeOfPrevCachedIo := make([]byte, TRAILING_IO_SIZE)
	ofs := op.ioSize - TRAILING_IO_SIZE
	copy(edgeOfPrevCachedIo, op.data[ofs:])

	// make sure the data is released
	// not sure this is actually needed
	op.data = nil

	op.ioSize = TRAILING_IO_SIZE
	op.startByte = (op.endByte + 1) - TRAILING_IO_SIZE
	op.data = edgeOfPrevCachedIo

	if pgs.debug {
		log.Printf("prefetch: lowered older data to size=%d", TRAILING_IO_SIZE)
	}
}

func (pgs *PrefetchGlobalState) prefetchIoWorker() {
	for true {
		pfm := <-pgs.ioQueue

		pfm.mutex.Lock()
		check(pfm.state == PFM_IO_IN_PROGRESS)

		startByte := pfm.current.startByte
		endByte := pfm.current.endByte
		url := pfm.fh.url
		pfm.numPrefetchIOs++
		pfm.mutex.Unlock()

		// perform the IO
		data, err := pgs.readData(startByte, endByte, url)

		pfm.mutex.Lock()
		check(pfm.state == PFM_IO_IN_PROGRESS)

		if err != nil {
			log.Printf("Prefetch error ofs=%d len=%d error=%s",
				pfm.current.startByte, pfm.current.ioSize, err.Error())
			pfm.state = PFM_IO_ERROR
		} else {
			// good case, we have the data
			pfm.state = PFM_DETECT_SEQ

			// make a separate copy of the prefetch IO description,
			// and add the data. The original should NEVER have
			// a pointer to the data.
			completedOp := pfm.current
			completedOp.data = data

			if pfm.cache == nil {
				// initialize the cache
				iovecs := make([]PrefetchOp, 1)
				iovecs[0] = completedOp
				pfm.cache = &PrefetchCachedData{
					startByte : completedOp.startByte,
					endByte :   completedOp.endByte,
					iovecs :    iovecs,
				}
			} else {
				// append to the cache
				check(pfm.cache.endByte + 1 == completedOp.startByte)
				check(len(pfm.cache.iovecs) <= 2)

				if len(pfm.cache.iovecs) == 2 {
					// we want to limit the amount of cached data.
					// Chop off the beginning of the vector, and
					// reduce memory consumption.
					pfm.cache.iovecs[0].data = nil
					pfm.cache.iovecs = pfm.cache.iovecs[1:]
				}
				check(len(pfm.cache.iovecs) == 1)
				if pfm.cache.iovecs[0].ioSize > TRAILING_IO_SIZE {
					// we don't want the older IO to be extermely large.
					// It costs memory, but only its right edge is likely to be accessed.
					pgs.leaveRightEdgeOnly(&pfm.cache.iovecs[0])
				}
				pfm.cache.iovecs = append(pfm.cache.iovecs, completedOp)
			}

			// recalculate the range covered by the cache
			nIovecs := len(pfm.cache.iovecs)
			check(nIovecs >= 1)
			pfm.cache.startByte = pfm.cache.iovecs[0].startByte
			pfm.cache.endByte = pfm.cache.iovecs[nIovecs-1].endByte
		}

		log.Printf("Prefetch #hits=%d #IOs=%d #prefetchIOs=%d ioSize=%d",
			pfm.numHits, pfm.numIOs, pfm.numPrefetchIOs, pfm.current.ioSize)

		// release all the reads waiting for the prefetch IO to complete
		pfm.cond.Broadcast()
		pfm.mutex.Unlock()
	}
}


// Check if a file is worth tracking.
func (pgs *PrefetchGlobalState) isWorthIt(pfm *PrefetchFileMetadata) bool {
	if pfm.state == PFM_IO_IN_PROGRESS {
		// file has ongoing IO
		return true
	}
	if pfm.state == PFM_IO_ERROR {
		// we had an IO error, get rid of this file
		return false
	}

	now := time.Now()
	if now.After(pfm.lastIo.Add(MAX_DELTA_TIME)) {
		// File has not been accessed recently
		return false
	}
	pfm.lastIo = now

	// is prefetch effective?
	if pfm.numIOs > PREFETCH_EFFECT_MIN_NUM_IOS {
		effectivness := float64(pfm.numHits) / float64(pfm.numIOs)
		if effectivness < PREFETCH_EFFECT_THRESH {
			// prefetch is not effective
			// reset the counters
			pfm.numIOs = 0
			pfm.numHits = 0
			return false
		}
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
		var toRemove []string

		// go over the table, and find all the files not worth tracking
		pgs.tableMutex.Lock()
		for fileId, pfm := range pgs.files {
			pfm.mutex.Lock()
			if !pgs.isWorthIt(pfm) {
				toRemove = append(toRemove, fileId)
			}
			pfm.mutex.Unlock()
		}

		for _, fileId := range toRemove {
			delete(pgs.files, fileId)
		}
		pgs.tableMutex.Unlock()
	}
}

func (pgs *PrefetchGlobalState) CreateFileEntry(fh *FileHandle) {
	pgs.tableMutex.Lock()
	defer pgs.tableMutex.Unlock()

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

	// setup so we can detect a sequential stream
	entry.current.ioSize = PREFETCH_MIN_IO_SIZE
	entry.current.startByte = 0
	entry.current.endByte = PREFETCH_MIN_IO_SIZE - 1
	entry.current.touched = 0

	entry.cond = sync.NewCond(&entry.mutex)
	entry.state = PFM_DETECT_SEQ

	pgs.files[fh.f.FileId] = &entry
}

func (pgs *PrefetchGlobalState) RemoveFileEntry(fh *FileHandle) {
	pgs.tableMutex.Lock()
	defer pgs.tableMutex.Unlock()

	fileId := fh.f.FileId
	if _, ok := pgs.files[fileId]; ok {
		if pgs.debug {
			log.Printf("prefetch: RemoveFileEntry %s", fh.f.Name)
		}
		delete(pgs.files, fileId)
	}
}


func (pgs *PrefetchGlobalState) readData(startByte int64, endByte int64, url DxDownloadURL) ([]byte, error) {
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

	data, err := DxHttpRequest("GET", url.URL, headers, []byte("{}"))
	if pgs.debug {
		if err == nil {
			log.Printf("prefetch: IO returned correctly len=%d", len(data))
		} else {
			log.Printf("prefetch: IO returned with error %s", err.Error())
		}
	}
	return data, err
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

	// we have 64 bits, split the space evenly between them
	slotSize := pfm.current.ioSize / 64
	startSlot := (startOfs - pfm.current.startByte) / slotSize
	endSlot := (endOfs - pfm.current.startByte) / slotSize
	for slot := startSlot; slot <= endSlot ; slot++ {
		// Sets the bit at position [slot]
		pfm.current.touched |= (1 << uint(slot))
	}

	// should we start prefetching?
	numAccessed := bits.OnesCount64(pfm.current.touched)
	if pgs.debug {
		log.Printf("prefetch touch: %d -- %d, numAccessed=%d",
			startSlot, endSlot, numAccessed)
	}

	if (numAccessed == 64 &&
		pfm.state != PFM_IO_IN_PROGRESS) {
		// All the slots were accessed, start a prefetch for the
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
		pfm.current.touched = 0

		// enqueue on the work queue
		pgs.ioQueue <- pfm
	}
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
	pgs.tableMutex.Lock()
	pfm, ok := pgs.files[fileId]
	if !ok {
		// file is not tracked, no prefetch data is available
		pgs.tableMutex.Unlock()
		return nil
	}
	// switch from the global lock to the entry lock
	pfm.mutex.Lock()
	pgs.tableMutex.Unlock()
	defer pfm.mutex.Unlock()

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

	default:
		panic(fmt.Sprintf("bad state %d for fileId=%s", pfm.state, pfm.fh.f.FileId))
	}

	return pgs.getDataFromCache(pfm, startOfs, endOfs)
}
