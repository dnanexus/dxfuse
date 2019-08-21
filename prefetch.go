package dxfs2

// When a file is opened, it is added to the global prefetch map. Once removed,
// it can never return. This means that to check if a file is being streamed, all
// we need to do is check the map.
import (
	"context"
	"fmt"
	"golang.org/x/sync/semaphore"
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
	NUM_PREFETCH_IOS = 10
	MIN_FILE_SIZE = 3 * PREFETCH_IO_SIZE   // do not track files smaller than this size

	SLOT_SIZE = PREFETCH_IO_SIZE / 64  // each slot takes up a bit

	PFM_INIT = 1
	PFM_IO_SUBMITTED = 2
	PFM_IO_COMPLETED = 3
	PFM_IO_ERROR = 4
)

type PrefetchFileMetadata struct {
	// the file being tracked
	file      *File

	// Last time an IO hit this file
	lastIo     time.Time
	numMisses  int   // count how many IOs missed the prefetched area
	numHits    int   // count how many IOs were hits

	startByte  int64
	endByte    int64
	touched    uint64  // mark the areas that have been accessed by the user
	data       []byte  // the data prefetched from DNAx

	// Allow user reads to wait for the prefetch IO
	cond      *sync.Cond

	state      int
}

// global limits
type PrefetchGlobalState struct {
	debug        bool
	maxDeltaTime time.Duration
	mutex        sync.Mutex
	files        map[string]PrefetchFileMetadata // tracking state per file-id
	sema        *semaphore.Weighted
}

func (pgs *PrefetchGlobalState) Init(debug bool) {
	pgs.debug = debug
	if maxDeltaTime, err := time.ParseDuration("5m"); err != nil {
		panic("Cannot create a five minute duration")
	} else {
		pgs.maxDeltaTime = maxDeltaTime
	}
	// limit the number of prefetch IOs
	pgs.sema = semaphore.NewWeighted(NUM_PREFETCH_IOS)
}


func (pgs *PrefetchGlobalState) CreateFileEntry(f *File) {
	pgs.mutex.Lock()
	defer pgs.mutex.Unlock()

	// if the table is at the size limit, do not create a new entry
	if len(pgs.files) >= MAX_NUM_ENTRIES_IN_TABLE {
		return
	}

	// The file has to have sufficient size, to merit an entry. We
	// don't want to waste entries on small files
	if f.Size < MIN_FILE_SIZE {
		return
	}

	var entry PrefetchFileMetadata
	entry.file = f
	entry.lastIo = time.Now()
	entry.numMisses = 0
	entry.numHits = 0
	entry.startByte = 0
	entry.endByte = PREFETCH_IO_SIZE - 1
	entry.touched = 0
	entry.data = make([]byte, 0)
	entry.cond = sync.NewCond(&pgs.mutex)
	entry.state = PFM_INIT

	pgs.files[f.FileId] = entry
}

func (pgs *PrefetchGlobalState) RemoveFileEntry(f *File) {
	pgs.mutex.Lock()
	defer pgs.mutex.Unlock()

	if _, ok := pgs.files[f.FileId]; ok {
		delete(pgs.files, f.FileId)
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
	totalNumIOs := pfm.numMisses + pfm.numHits
	if totalNumIOs > PREFETCH_EFFECT_MIN_NUM_IOS {
		effectivness := float64(pfm.numMisses) / float64(totalNumIOs)
		if effectivness < PREFETCH_EFFECT_THRESH {
			// prefetch is not effective
			// reset the counters
			pfm.numMisses = 0
			pfm.numHits = 0
			return false
		}
	}

	// any other cases? add them here
	// we don't want to track files we don't need to.
	return true
}

func (pgs *PrefetchGlobalState) readData(startByte int64, endByte int64, url DxDownloadURL) ([]byte, error) {
	// The data has not been prefetched. Get the data from DNAx with an
	// http request.
	len := endByte - startByte
	headers := make(map[string]string)

	// Copy the immutable headers
	for key, value := range url.Headers {
		headers[key] = value
	}

	// add an extent in the file that we want to read
	headers["Range"] = fmt.Sprintf("bytes=%d-%d", startByte, endByte)
	if pgs.debug {
		log.Printf("Prefetch ofs=%d  len=%d", startByte, len)
	}

	return DxHttpRequest("GET", url.URL, headers, []byte("{}"))
}


func takeSliceFromData(startOfs int64, endOfs int64, data []byte) []byte {
	bgnByte := startOfs % PREFETCH_IO_SIZE
	endByte := endOfs % PREFETCH_IO_SIZE
	return data[bgnByte : endByte]
}

// This is done on behalf of a user read request. Check if this range is currently
// cached.
func (pgs *PrefetchGlobalState) Check(fileId string, url DxDownloadURL, startOfs int64, endOfs int64) ([]byte, bool) {
	pgs.mutex.Lock()
	emptyArray := make([]byte, 0)

	pfm, ok := pgs.files[fileId]
	if !ok {
		// file is not tracked, no prefetch data is available
		pgs.mutex.Unlock()
		return emptyArray, false
	}

	if !pgs.isWorthIt(pfm) {
		// file is not worth tracking, prefetch has been ineffective.
		// remove it from the table.
		delete(pgs.files, fileId)
		pgs.mutex.Unlock()
		return emptyArray, false
	}

	if !(startOfs >= pfm.startByte &&
		endOfs <= pfm.endByte) {
		// We are not in the prefetch range
		pfm.numMisses++
		pgs.mutex.Unlock()
		return emptyArray, false
	}
	pfm.numHits++

	startPage := (startOfs - pfm.startByte) / SLOT_SIZE
	endPage := (endOfs - pfm.startByte) / SLOT_SIZE
	for slot := startPage; slot <= endPage ; slot++ {
		// Sets the bit at position [slot]
		pfm.touched |= 1 << uint(slot)
	}

	// should we start prefetching?
	numAccessed := bits.OnesCount64(pfm.touched)
	if (numAccessed == 64 &&
		(pfm.state == PFM_INIT || pfm.state == PFM_IO_COMPLETED)) {
		// All the slots were accessed, start a prefetch for the
		// the next chunk
		pfm.state = PFM_IO_SUBMITTED
		pfm.touched = 0
		pfm.startByte += PREFETCH_IO_SIZE
		pfm.endByte += PREFETCH_IO_SIZE

		if pgs.debug {
			log.Printf("#hits=%d #misses=%d", pfm.numHits, pfm.numMisses)
		}
		pfm.numHits = 0
		pfm.numMisses = 0

		// this is a blocking operation
		pgs.mutex.Unlock()

		// the semaphore limits the number of concurrent prefetch IOs
		ctx := context.TODO()
		if err := pgs.sema.Acquire(ctx, 1); err != nil {
			log.Printf("Could not acquire semaphore")
			return emptyArray, false
		}
		data, err := pgs.readData(pfm.startByte, pfm.endByte, url)
		pgs.sema.Release(1)

		pgs.mutex.Lock()
		if err != nil {
			len := endOfs - startOfs + 1
			log.Printf("Prefetch error  ofs=%d len=%d error=%s",
				pfm.startByte, len, err.Error())
			pfm.state = PFM_IO_ERROR
			pfm.data = emptyArray
		} else {
			// good case, we have the data
			pfm.data = data
		}

		pfm.state = PFM_IO_COMPLETED

		// release all the reads waiting for the prefetch IO to complete
		pfm.cond.Broadcast()
	}
	switch pfm.state {
	case PFM_INIT:
		pgs.mutex.Unlock()
		return emptyArray, false

	case PFM_IO_SUBMITTED:
		// we are waiting for the prefetch to complete
		pfm.cond.Wait()
		part := emptyArray
		// We woke up from sleep, holding the lock.
		// Check that nothing has changed in the meanwhile, and
		// we are still prefetching the same area.
		if (pfm.state == PFM_IO_COMPLETED &&
			startOfs >= pfm.startByte &&
			endOfs <= pfm.endByte) {
			part = takeSliceFromData(startOfs, endOfs, pfm.data)
		}
		pgs.mutex.Unlock()
		return part, false

	case PFM_IO_COMPLETED:
		// copy the data
		part := takeSliceFromData(startOfs, endOfs, pfm.data)
		pgs.mutex.Unlock()
		return part, false

	case PFM_IO_ERROR:
		// The prefetch IO incurred an error. Let's try it again, with a smaller size
		pgs.mutex.Unlock()
		return emptyArray, false

	default:
		pgs.mutex.Unlock()
		panic(fmt.Sprintf("bad state %d for fileId=%s", pfm.state, pfm.file.FileId))
	}
}
