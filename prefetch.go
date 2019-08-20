package dxfs2

// When a file is opened, it is added to the global prefetch map. Once removed,
// it can never return. This means that to check if a file is being streamed, all
// we need to do is check the map.
import (
	"sync"
	"math/bits"
)

const (
	MAX_DELTA_TIME = time.ParseDuration("5m")
	MISS_IO_THRESH = 5
	MAX_NUM_ENTRIES_IN_TABLE = 1000   // maximal number of entries
	PREFETCH_IO_SIZE = (1024 * 1024)   // Maximal size of IO to prefetch

	SLOT_SIZE = PREFETCH_IO_SIZE / 64  // each slot takes up a bit

	PFM_INIT = 1
	PFN_IO_SUBMITTED = 1
	PFN_IO_COMPLETED = 2

	EMPTY_ARRAY make([]byte, 0)
)

type PrefetchFileMetadata struct {
	// Last time an IO hit this file
	lastIo     time.Time
	numMisses  int   // count how many IOs missed the prefetched area

	startByte  uint64
	endByte    uint64
	touched    uint64  // mark the areas that have been accessed by the user
	data       byte[]  // the data prefetched from DNAx

	// Allow user reads to wait for the prefetch IO
	wg         WaitGroup

	state      int
}


// global limits
type PrefetchGlobalState struct {
	lock sync.Mutex
	files map[string]PrefetchFileMetadata // tracking state per file-id
}

func (gps *PrefetchGlobalState) CreateFileEntry(f *File) {
	pgs.Lock()
	defer pgs.Unlock()

	// if the table is at the size limit, do not create a new entry
	if len(pgs.files) > MAX_NUM_ENTRIES_IN_TABLE {
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
		startByte : 0
		endByte : 0
		touched : 0
		data : EMPTY_ARRAY
		state : PFN_INIT
	}
	pgs[f.fileId] = entry
}


// Sets the bit at pos in the integer n.
func setBit(n uint64, pos uint) int {
    n |= (1 << pos)
    return n
}

func prefetchWorker(pfm PrefetchFileMetadata) {
}

// This is done on behalf of a user read request. Check if this range is currently
// cached.
func (pgs *PrefetchGlobalState) Check(fileId string, startOfs uint64, endOfs uint64) (byte[], bool) {
	pgs.Lock()
	defer pgs.Unlock()

	fpm, ok := pgs.files[fileId]
	if !ok {
		// file is not tracked, no prefetch data is available
		return EMPTY_ARRAY, false
	}

	// conditions for discarding a file from tracking
	now = time.Now()
	if (now - pfm.lastIo) > MAX_DELTA_TIME {
		// File has not been accessed recently
		delete(pgs.file, fileId)
		return EMPTY_ARRAY, false
	}
	if pfm.numMisses > MISS_IO_THRESH {
		delete(pgs.file, fileId)
		return EMPTY_ARRAY, false
	}
	if !(startsOfs >= pfm.startByte &&
		endOfs <= pfm.endByte) {
		// We are not in the prefetch range
		pfm.numMisses++
		return EMPTY_ARRAY, false
	}
	pfm.lastIo = time.Now()
	pfm.numMisses = 0

	startPage := (startsOfs - pfm.startByte) / SLOT_SIZE
	endPage := (endOfs - pfm.startByte) / SLOT_SIZE
	for slot := startPage; slot <= endPage ; slot++ {
		pfm.touched = setBit(pfm.touched, slot)
	}

	// should we start prefetching?
	numAccessed := pfm.touched.OnesCount
	if numAccessed == 64 && pfm.state != PFM_IO_SUBMITTED {
		// All the slots were accessed, start a prefetch for the
		// the next chunk
		pfm.state == PFM_IO_SUBMITTED
		pfm.touched = 0
		pfm.startByte = pfm.startByte + PREFETCH_IO_SIZE
		pfm.endByte = pfm.end + PREFETCH_IO_SIZE

		go prefetchWorker(pfm)

		pfm.wg.Add(1)
	}

	// This isn't right either
	switch pfm.state {
	case PFM_IO_SUBMITTED:
		// we are waiting for the prefetch to complete

	case PFM_IO_COMPLETED:
		// copy the data

	default:
		return EMPTY_ARRAY, false
	}
}
