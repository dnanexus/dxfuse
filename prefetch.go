package dxfs2

// When a file is opened, it is added to the global prefetch map. Once removed,
// it can never return. This means that to check if a file is being streamed, all
// we need to do is check the map.
import (
	"golang.org/x/sync/semaphore"
	"log"
	"math/bits"
	"sync"
)

const (
	MAX_DELTA_TIME = time.ParseDuration("5m")
	PREFETCH_EFFECT_THRESH = 0.75      // Prefetch effectiveness should be at least this
	PREFETCH_EFFECT_MIN_NUM_IOS = 20   // Do not calculate effectiveness below this number of IOs
	MAX_NUM_ENTRIES_IN_TABLE = 1024    // maximal number of entries
	PREFETCH_IO_SIZE = (1024 * 1024)   // Maximal size of IO to prefetch
	NUM_PREFETCH_IOS = 10

	SLOT_SIZE = PREFETCH_IO_SIZE / 64  // each slot takes up a bit

	PFM_INIT = 1
	PFM_IO_SUBMITTED = 1
	PFM_IO_COMPLETED = 2
	PFM_IO_ERROR = 3

	EMPTY_ARRAY make([]byte, 0)
)

type PrefetchFileMetadata struct {
	// Last time an IO hit this file
	lastIo     time.Time
	numMisses  int   // count how many IOs missed the prefetched area
	numHits    int   // count how many IOs were hits

	startByte  uint64
	endByte    uint64
	touched    uint64  // mark the areas that have been accessed by the user
	data       byte[]  // the data prefetched from DNAx

	// Allow user reads to wait for the prefetch IO
	cond       sync.Cond

	state      int
}

// description of an IO to perform, and a place to store it. One
// of the prefetch goroutines will perform it.
type PrefetchJobInfo struct {
	startByte uint64
	endByte   uint64
	url       DxDownloadURL
	pfm      *PrefetchFileMetadata // the file from which this IO originates
}

// global limits
type PrefetchGlobalState struct {
	debug bool
	lock sync.Mutex
	files map[string]PrefetchFileMetadata // tracking state per file-id
	sema semaphore.Weighted
}

func (pgs *PrefetchGlobalState) CreateFileEntry(f *File) {
	pgs.Lock()
	defer pgs.Unlock()

	// if the table is at the size limit, do not create a new entry
	if len(pgs.files) >= MAX_NUM_ENTRIES_IN_TABLE {
		return
	}

	// The file has to have sufficient size, to merit an entry. We
	// don't want to waste entries on small files
	if f.Size < MIN_FILE_SIZE {
		return
	}

	entry = &PrefetchFileMetadata{
		lastIo : time.Now()
		numMisses : 0
		numHits : 0
		startByte : 0
		endByte : PREFETCH_IO_SIZE - 1
		touched : 0
		data : EMPTY_ARRAY
		state : PFM_INIT
	}
	pgs[f.fileId] = entry
}

func (pgs *PrefetchGlobalState) RemoveFileEntry(f *File) {
	pgs.Lock()
	defer pgs.Unlock()

	delete(pgs.files, f.FileId)
}


// Sets the bit at pos in the integer n.
func setBit(n uint64, pos uint) int {
    n |= (1 << pos)
    return n
}

// remove from the table all files that aren't worth it
/*
func (pgs *PrefetchGlobalState) cleanUpTable() {
	// a set of all files not worth tracking
	var notWorthIt [string]bool

	// conditions for discarding a file from tracking
	now := time.Now()
	for fileId, pfm := range pgs.files {
		if pfm.state == PFN_IO_SUBMITTED {
			// skip any file with ongoing IO
			continue
		}
		if (now - pfm.lastIo) > MAX_DELTA_TIME {
			// File has not been accessed recently
			notWorthIt[fileId] = true
		}

		// is prefetch effective?
		totalNumIOs := pfm.numMisses + pfm.numHits
		if totalNumIOs > PREFETCH_EFFECT_MIN_NUM_IOS {
			effectivness := float64(pfm.numMisses) / float64(totalNumIOs)
			if effectivness < PREFETCH_EFFECT_THRESH {
				// prefetch is not effective
				notWorthIt[fileId] = true
			}

			// reset the counters
			pfm.numMisses = 0
			pfm.numHits = 0
		}

		// any other cases?
		// we don't want to track files we don't need to.
	}

	for fileId, _ := range notWorthIt {
		delete(pgs.file, fileId)
	}
}
*/

// periodically cleanup the table from files that are
// not worth prefetching.
/*
func cleanUpTableWorker(pgs *PrefetchGlobalState) {
	for true {
		// sleep for a minute
		time.Sleep(60 * time.Second)

		// if the table is large enough, check the entries
		pgs.Lock()
		if len(pgs.files) > (MAX_NUM_ENTRIES_IN_TABLE/2) {
			pgs.cleanUpTable()
		}
		pgs.Unlock()
	}
}
*/

func (pgs *PrefetchGlobalState) Init(debug bool) {
	pgs.debug = debug

	// limit the number of prefetch IOs
	pgs.sema = semaphore.NewWeighted(NUM_PREFETCH_IOS)

	pgs.cond = sync.NewCond(pgs.mutex)

	// start a background job to discard files not worth tracking
	//go cleanUpTableWorker(pgs)
}

func readData(startByte uint64,	endByte uint64, url DxDownloadURL) ([]byte, error) {
	// The data has not been prefetched. Get the data from DNAx with an
	// http request.
	len := endByte - startByte
	headers := make(map[string]string)

	// Copy the immutable headers
	for key, value := range url.Headers {
		headers[key] = value
	}

	// add an extent in the file that we want to read
	headers["Range"] = fmt.Sprintf("bytes=%d-%d", j.startByte, j.endByte)
	if pgs.debug {
		log.Printf("Prefetch ofs=%d  len=%d", j.startByte, len)
	}

	return DxHttpRequest("GET", url.URL, headers, []byte("{}"))
}


func takeSliceFromData(firstByte uint64, len uint64, data []byte) []byte {
	bgn := firstByte % PREFETCH_IO_SIZE
	last := bgn + len - 1
	return data[bgn:bLast]
}

// This is done on behalf of a user read request. Check if this range is currently
// cached.
func (pgs *PrefetchGlobalState) Check(
	fileId   string,
	url      DxDownloadURL,
	startOfs uint64,
	endOfs   uint64) (byte[], bool) {

	pgs.Lock()
	len := endOfs - startOfs + 1
	pfm, ok := pgs.files[fileId]
	if !ok {
		// file is not tracked, no prefetch data is available
		pgs.Unlock()
		return EMPTY_ARRAY, false
	}

	pfm.lastIo = time.Now()

	if !(startsOfs >= pfm.startByte &&
		endOfs <= pfm.endByte) {
		// We are not in the prefetch range
		pfm.numMisses++
		pgs.Unlock()
		return EMPTY_ARRAY, false
	}
	pfm.numHits++

	startPage := (startsOfs - pfm.startByte) / SLOT_SIZE
	endPage := (endOfs - pfm.startByte) / SLOT_SIZE
	for slot := startPage; slot <= endPage ; slot++ {
		pfm.touched = setBit(pfm.touched, slot)
	}

	// should we start prefetching?
	numAccessed := pfm.touched.OnesCount
	if (numAccessed == 64 &&
		(pfm.state == PFM_IO_INIT || pfm.state == PFM_IO_COMPLETED)) {
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
		pgs.Unlock()

		// the semaphore limits the number of concurrent prefetch IOs
		ctx := context.TODO()
		if err := pgs.sema.Acquire(ctx, 1); err != nil {
			panic("Could not acquire semaphore")
		}
		data, err := readData(pfm.startByte, pfm.endByte, url)
		pgs.sema.Release(1)

		pgs.Lock()
		if err != nil {
			log.Printf("Prefetch error  ofs=%d len=%d error=%s",
				pfm.startByte, len, error.Error())
			pfm.state = PFM_IO_ERROR
			pfm.data = EMPTY_ARRAY
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
		pgs.Unlock()
		return EMPTY_ARRAY, false

	case PFM_IO_SUBMITTED:
		// we are waiting for the prefetch to complete
		pfm.cond.Wait()
		part := EMPTY_ARRAY
		if pfm.state == PFM_IO_COMPLETED {
			firstByte := startOfs % PREFETCH_IO_SIZE
			lastByte := bs + len - 1
			part = takeSliceFromData(firstByte, lastByte, pfm.data)
		}
		pgs.Unlock()
		return part, nil

	case PFM_IO_COMPLETED:
		// copy the data
		part = takeSliceFromData(firstByte, lastByte, pfm.data)
		pgs.Unlock()
		return part, nil

	case PFM_ERROR:
		// The prefetch IO incurred an error. Let's try it again, with a smaller size
		pgs.Unlock()
		return EMPTY_ARRAY, false

	default:
		pgs.Unlock()
		panic(fmt.Sprintf("bad state %d for fileId=%s", pfm.state, pfm.fileId))
	}
}
