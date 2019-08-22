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

	SLOT_SIZE = PREFETCH_IO_SIZE / 64  // each slot takes up a bit

	PFM_INIT = 1
	PFM_IO_SUBMITTED = 2
	PFM_IO_COMPLETED = 3
	PFM_IO_ERROR = 4
)

type PrefetchFileMetadata struct {
	// the file being tracked
	fh        *FileHandle

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

func (pgs *PrefetchGlobalState) prefetchIoWorker() {
	for true {
		pfm := <-pgs.ioQueue

		pgs.mutex.Lock()
		startByte := pfm.startByte
		endByte := pfm.endByte
		url := pfm.fh.url
		pgs.mutex.Unlock()

		// perform the IO
		data, err := pgs.readData(startByte, endByte, url)

		pgs.mutex.Lock()
		if err != nil {
			len := pfm.endByte - pfm.startByte + 1
			log.Printf("Prefetch error ofs=%d len=%d error=%s",
				pfm.startByte, len, err.Error())
			pfm.state = PFM_IO_ERROR
			pfm.data = nil
		} else {
			// good case, we have the data
			pfm.state = PFM_IO_COMPLETED
			pfm.data = data
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
	/*totalNumIOs := pfm.numMisses + pfm.numHits
	if totalNumIOs > PREFETCH_EFFECT_MIN_NUM_IOS {
		effectivness := float64(pfm.numMisses) / float64(totalNumIOs)
		if effectivness < PREFETCH_EFFECT_THRESH {
			// prefetch is not effective
			// reset the counters
			pfm.numMisses = 0
			pfm.numHits = 0
			return false
		}
	}*/

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
	entry.startByte = 0
	entry.endByte = PREFETCH_IO_SIZE - 1
	entry.touched = 0
	entry.data = nil
	entry.cond = sync.NewCond(&pgs.mutex)
	entry.state = PFM_INIT

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


func takeSliceFromData(startOfs int64, endOfs int64, data []byte) []byte {
	bgnByte := startOfs % PREFETCH_IO_SIZE
	endByte := endOfs % PREFETCH_IO_SIZE
	return data[bgnByte : endByte+1]

/*	// make a copy of the slice of the data that we need
	len := endByte - bgnByte + 1
	dst := make([]byte, len)
	copy(dst, data[bgnByte : endByte])
	return dst*/
}

// This is done on behalf of a user read request. Check if this range is currently
// cached.
func (pgs *PrefetchGlobalState) Check(fileId string, url DxDownloadURL, startOfs int64, endOfs int64) []byte  {
	pgs.mutex.Lock()
	defer pgs.mutex.Unlock()

	pfm, ok := pgs.files[fileId]
	if !ok {
		// file is not tracked, no prefetch data is available
		return nil
	}

	if !(startOfs >= pfm.startByte &&
		endOfs <= pfm.endByte) {
		// We are not in the prefetch range
		return nil
	}

	startSlot := (startOfs - pfm.startByte) / SLOT_SIZE
	endSlot := (endOfs - pfm.startByte) / SLOT_SIZE
	for slot := startSlot; slot <= endSlot ; slot++ {
		// Sets the bit at position [slot]
		pfm.touched |= (1 << uint(slot))
	}

	// should we start prefetching?
	numAccessed := bits.OnesCount64(pfm.touched)
/*	if pgs.debug {
		log.Printf("prefetch touch: %d -- %d, numAccessed=%d",
			startSlot, endSlot, numAccessed)
	}*/

	if (numAccessed == 64 &&
		(pfm.state == PFM_INIT || pfm.state == PFM_IO_COMPLETED)) {
		// All the slots were accessed, start a prefetch for the
		// the next chunk
		pfm.state = PFM_IO_SUBMITTED
		pfm.touched = 0
		pfm.startByte += PREFETCH_IO_SIZE
		pfm.endByte += PREFETCH_IO_SIZE
		pfm.data = nil

		// enqueue a long IO to prefetch for this file
		pgs.ioQueue <- pfm
	}

	if !(startOfs >= pfm.startByte &&
		endOfs <= pfm.endByte) {
		// The prefetch range may have changed, check if we are in range
		return nil
	}

	// We are in the range
	switch pfm.state {
	case PFM_INIT:
		return nil

	case PFM_IO_SUBMITTED:
		// we are waiting for the prefetch to complete
		pfm.cond.Wait()
		log.Printf("woke up from waiting")

		// We woke up from sleep, holding the lock.
		// Check that nothing has changed in the meanwhile, and
		// we are still prefetching the same area.
		if (pfm.state == PFM_IO_COMPLETED &&
			startOfs >= pfm.startByte &&
			endOfs <= pfm.endByte) {
			return takeSliceFromData(startOfs, endOfs, pfm.data)
		}
		return nil

	case PFM_IO_COMPLETED:
		// copy the data
		return takeSliceFromData(startOfs, endOfs, pfm.data)

	case PFM_IO_ERROR:
		// The prefetch IO incurred an error.
		return nil

	default:
		panic(fmt.Sprintf("bad state %d for fileId=%s", pfm.state, pfm.fh.f.FileId))
	}
}
