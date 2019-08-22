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
	MAX_DELTA_TIME = 5 * 60 * time.Second
	PREFETCH_EFFECT_THRESH = 0.75      // Prefetch effectiveness should be at least this
	PREFETCH_EFFECT_MIN_NUM_IOS = 20   // Do not calculate effectiveness below this number of IOs
	MAX_NUM_ENTRIES_IN_TABLE = 1024    // maximal number of entries
	PREFETCH_IO_SIZE = (1024 * 1024)   // Maximal size of IO to prefetch
	NUM_PREFETCH_THREADS = 10
	MIN_FILE_SIZE = 3 * PREFETCH_IO_SIZE   // do not track files smaller than this size
	MAX_NUM_IOVECS_IN_CACHE = 3

	SLOT_SIZE = PREFETCH_IO_SIZE / 64  // each slot takes up a bit

	PFM_DETECT_SEQ = 1
	PFM_IO_SUBMITTED = 2
	PFM_IO_ERROR = 3
)


type PrefetchCachedData struct {
	startByte  int64
	endByte    int64
	iovecs     [][]byte // an array of io-vectors
}

type PrefetchCurrentOp struct {
	startByte  int64
	endByte    int64
	touched    uint64  // mark the areas that have been accessed by the user
}

type PrefetchFileMetadata struct {
	// the file being tracked
	fh        *FileHandle

	// Last time an IO hit this file
	lastIo     time.Time
	numIOs     int   // count how many IOs this files received
	numHits    int   // count how many IOs were hits

	// cached data
	cache     *PrefetchCachedData

	// prefetch IO that may be ongoing. Also used to detect
	// sequential access to the file.
	current    PrefetchCurrentOp

	// Allow user reads to wait until the prefetch IO completes
	cond      *sync.Cond

	state      int
}

// global limits
type PrefetchGlobalState struct {
	debug        bool
	maxDeltaTime time.Duration
	mutex        sync.Mutex
	files        map[string](*PrefetchFileMetadata) // tracking state per file-id
	ioQueue      chan (*PrefetchFileMetadata)   // queue of IOs to prefetch
}

func (pgs *PrefetchGlobalState) Init(debug bool) {
	pgs.debug = debug
	if maxDeltaTime, err := time.ParseDuration("5m"); err != nil {
		panic("Cannot create a five minute duration")
	} else {
		pgs.maxDeltaTime = maxDeltaTime
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
	if value {
		panic("assertion failed")
	}
}

func (pgs *PrefetchGlobalState) prefetchIoWorker() {
	for true {
		pfm := <-pgs.ioQueue

		pgs.mutex.Lock()
		startByte := pfm.current.startByte
		endByte := pfm.current.endByte
		url := pfm.fh.url
		pgs.mutex.Unlock()

		// perform the IO
		data, err := pgs.readData(startByte, endByte, url)

		pgs.mutex.Lock()
		if err != nil {
			len := pfm.current.endByte - pfm.current.startByte + 1
			log.Printf("Prefetch error ofs=%d len=%d error=%s",
				pfm.current.startByte, len, err.Error())
			pfm.state = PFM_IO_ERROR
		} else {
			// good case, we have the data
			pfm.state = PFM_DETECT_SEQ

			if pfm.cache == nil {
				// initialize the cache
				iovecs := make([][]byte, 1)
				iovecs[0] = data
				pfm.cache = &PrefetchCachedData{
					startByte : pfm.current.startByte,
					endByte :   pfm.current.endByte,
					iovecs :    iovecs,
				}
			} else {
				// append to the cache
				check(pfm.cache.endByte + 1 == pfm.current.startByte)
				pfm.cache.endByte = endByte
				pfm.cache.iovecs = append(pfm.cache.iovecs, data)

				if len(pfm.cache.iovecs) > MAX_NUM_IOVECS_IN_CACHE {
					// we want to limit the amount of cached data.
					// we chop off the beginning of the vector, and
					// reduce memory consumption.
					start := len(pfm.cache.iovecs) - MAX_NUM_IOVECS_IN_CACHE
					for i := 0; i < start; i++ {
						pfm.cache.iovecs[i] = nil
					}
					pfm.cache.iovecs = pfm.cache.iovecs[start:]
				}

				// recalculate the start-byte
				nIovecs := len(pfm.cache.iovecs)
				check(nIovecs >= 1)
				pfm.cache.startByte = pfm.current.startByte - (int64((nIovecs - 1)) * int64(PREFETCH_IO_SIZE))
				check(pfm.cache.startByte % PREFETCH_IO_SIZE == 0)
			}
		}

		// release all the reads waiting for the prefetch IO to complete
		pfm.cond.Broadcast()
		pgs.mutex.Unlock()
	}
}


// Check if a file is worth tracking.
func (pgs *PrefetchGlobalState) isWorthIt(pfm PrefetchFileMetadata) bool {
	if pfm.state == PFM_IO_SUBMITTED {
		// file has ongoing IO
		return true
	}

	now := time.Now()
	if now.After(pfm.lastIo.Add(pgs.maxDeltaTime)) {
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

		// Files that are not worth tracking
		toRemove := make([]string, 0)

		// go over the table, and find all the files not worth tracking
		pgs.mutex.Lock()
		if len(pgs.files) > MAX_NUM_ENTRIES_IN_TABLE/2 {
			for fileId, pfm := range pgs.files {
				if !pgs.isWorthIt(*pfm) {
					toRemove = append(toRemove, fileId)
				}
			}
		}
		pgs.mutex.Unlock()

		for _, fileId := range toRemove {
			delete(pgs.files, fileId)
		}
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

	// setup so we can detect a sequential stream
	entry.current.startByte = 0
	entry.current.endByte = PREFETCH_IO_SIZE - 1
	entry.current.touched = 0

	entry.cond = sync.NewCond(&pgs.mutex)
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
	if !(startOfs <= pfm.current.startByte &&
		endOfs <= pfm.current.endByte) {
		return
	}

	startSlot := (startOfs - pfm.current.startByte) / SLOT_SIZE
	endSlot := (endOfs - pfm.current.startByte) / SLOT_SIZE
	for slot := startSlot; slot <= endSlot ; slot++ {
		// Sets the bit at position [slot]
		pfm.current.touched |= (1 << uint(slot))
	}

	// should we start prefetching?
	numAccessed := bits.OnesCount64(pfm.current.touched)
	/*if pgs.debug {
		log.Printf("prefetch touch: %d -- %d, numAccessed=%d",
			startSlot, endSlot, numAccessed)
	}*/

	if (numAccessed == 64 &&
		pfm.state != PFM_IO_SUBMITTED) {
		// All the slots were accessed, start a prefetch for the
		// the next chunk
		pfm.state = PFM_IO_SUBMITTED

		// setup state for the next chunk
		pfm.current.startByte += PREFETCH_IO_SIZE
		pfm.current.endByte += PREFETCH_IO_SIZE
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
	pfm.numHits++

	// go through the io-vectors, and check if they contain the range
	for i, data := range pfm.cache.iovecs {
		iovecStartOfs := pfm.cache.startByte + (int64(i) * PREFETCH_IO_SIZE)
		if iovecStartOfs <= startOfs &&
			iovecStartOfs + PREFETCH_IO_SIZE >= endOfs {
			// its inside this iovec
			bgnByte := startOfs - iovecStartOfs
			endByte := endOfs - iovecStartOfs
			return data[bgnByte : endByte+1]
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

	case PFM_IO_SUBMITTED:
		// ongoing prefetch IO
		if !(startOfs <= pfm.current.startByte &&
			endOfs <= pfm.current.endByte) {
			// We are not in the prefetch range, and not in cache
			return nil
		}

		// we are waiting for the prefetch to complete
		for pfm.state == PFM_IO_SUBMITTED {
			pfm.cond.Wait()
		}
		if pgs.debug {
			log.Printf("prefetch: woke up from waiting")
		}
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
