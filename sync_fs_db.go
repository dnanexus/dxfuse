package dxfuse

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/dnanexus/dxda"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/jacobsa/fuse"
)

const (
	sweepPeriodicTime = 1 * time.Minute
)

const (
	chunkMaxQueueSize = 10

	numFileThreads = 4
	numBulkDataThreads = 8
	minChunkSize = 16 * MiB

	fileCloseWaitTime = 5 * time.Second
	fileCloseMaxWaitTime = 10 * time.Minute
)

type Chunk struct {
	fileId  string
	index   int
	data  []byte
	fwg     *sync.WaitGroup

	// output from the operation
	err     error
}

type FileUploadReq struct {
	id           string
	partSize     int64
	uploadParams FileUploadParameters
	localPath    string
	fileSize     int64
}

type SyncDbDx struct {
	dxEnv           dxda.DXEnvironment
	options         Options
	projId2Desc     map[string]DxDescribePrj
	fileUploadQueue chan FileUploadReq
	chunkQueue      chan *Chunk
	wg              sync.WaitGroup
	mutex           sync.Mutex
	mdb            *MetadataDb
	ops            *DxOps
}

func NewSyncDbDx(
	options Options,
	dxEnv dxda.DXEnvironment,
	projId2Desc map[string]DxDescribePrj) *SyncDbDx {

	// the chunk queue size should be at least the size of the thread
	// pool.
	chunkQueueSize := MaxInt(numBulkDataThreads, chunkMaxQueueSize)

	sybx := &SyncDbDx{
		dxEnv : dxEnv,
		options : options,
		projId2Desc : projId2Desc,
		fileUploadQueue : make(chan FileUploadReq),

		// limit the size of the chunk queue, so we don't
		// have too many chunks stored in memory.
		chunkQueue : make(chan *Chunk, chunkQueueSize),

		mutex : sync.Mutex{},
		ops : NewDxOps(dxEnv, options),
	}

	// Create a bunch of threads
	sybx.wg.Add(numFileThreads)
	for i := 0; i < numFileThreads; i++ {
		go sybx.createFileWorker()
	}

	sybx.wg.Add(numBulkDataThreads)
	for i := 0; i < numBulkDataThreads; i++ {
		go sybx.bulkDataWorker()
	}

	// start a periodic thread to synchronize the database with
	// the platform
	go sybx.periodicSync()

	return sybx
}

// write a log message, and add a header
func (sybx *SyncDbDx) log(a string, args ...interface{}) {
	LogMsg("file_upload", a, args...)
}

func (sybx *SyncDbDx) Shutdown() {
	// signal all upload threads to stop
	close(sybx.fileUploadQueue)
	close(sybx.chunkQueue)

	// wait for all of them to complete
	sybx.wg.Wait()
}

// A worker dedicated to performing data-upload operations
func (sybx *SyncDbDx) bulkDataWorker() {
	// A fixed http client
	client := dxda.NewHttpClient(true)

	for true {
		chunk, ok := <- sybx.chunkQueue
		if !ok {
			sybx.wg.Done()
			return
		}
		if sybx.options.Verbose {
			sybx.log("Uploading chunk=%d len=%d", chunk.index, len(chunk.data))
		}

		// upload the data, and store the error code in the chunk
		// data structure.
		chunk.err = sybx.ops.DxFileUploadPart(
			context.TODO(),
			client,
			chunk.fileId, chunk.index, chunk.data)

		// release the memory used by the chunk, we no longer
		// need it. The file-thread is going to check the error code,
		// so the struct itself remains alive.
		chunk.data = nil
		chunk.fwg.Done()
	}
}


func divideRoundUp(x int64, y int64) int64 {
	return (x + y - 1) / y
}

// Check if a part size can work for a file
func checkPartSizeSolution(param FileUploadParameters, fileSize int64, partSize int64) bool {
	if partSize < param.MinimumPartSize {
		return false
	}
	if partSize > param.MaximumPartSize {
		return false
	}
	numParts := divideRoundUp(fileSize, partSize)
	if numParts > param.MaximumNumParts {
		return false
	}
	return true
}

func (sybx *SyncDbDx) calcPartSize(param FileUploadParameters, fileSize int64) (int64, error) {
	if param.MaximumFileSize < fileSize {
		return 0, errors.New(
			fmt.Sprintf("File is too large, the limit is %d, and the file is %d",
				param.MaximumFileSize, fileSize))
	}

	// The minimal number of parts we'll need for this file
	minNumParts := divideRoundUp(fileSize, param.MaximumPartSize)

	if minNumParts > param.MaximumNumParts {
		return 0, errors.New(
			fmt.Sprintf("We need at least %d parts for the file, but the limit is %d",
				minNumParts, param.MaximumNumParts))
	}

	// now we know that there is a solution. We'll try to use a small part size,
	// to reduce memory requirements. However, we don't want really small parts, which is why
	// we use [minChunkSize].
	preferedChunkSize := divideRoundUp(param.MinimumPartSize, minChunkSize) * minChunkSize
	for preferedChunkSize < param.MaximumPartSize {
		if (checkPartSizeSolution(param, fileSize, preferedChunkSize)) {
			return preferedChunkSize, nil
		}
		preferedChunkSize *= 2
	}

	// nothing smaller will work, we need to use the maximal file size
	return param.MaximumPartSize, nil
}

// read a range in a file
func readLocalFileExtent(filename string, ofs int64, len int) ([]byte, error) {
	fReader, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer fReader.Close()

	buf := make([]byte, len)
	recvLen, err := fReader.ReadAt(buf, ofs)
	if err != nil {
		return nil, err
	}
	if recvLen != len {
		log.Panicf("short read, got %d bytes instead of %d",
			recvLen, len)
	}
	return buf, nil
}

// Upload the parts. Small files are uploaded synchronously, large
// files are uploaded by worker threads.
//
// note: chunk indexes start at 1 (not zero)
func (sybx *SyncDbDx) uploadFileData(
	client *retryablehttp.Client,
	upReq FileUploadReq) error {
	if upReq.fileSize == 0 {
		log.Panicf("The file is empty")
	}

	if upReq.fileSize <= upReq.partSize {
		// This is a small file, upload it synchronously.
		// This ensures that only large chunks are uploaded by the bulk-threads,
		// improving fairness.
		data, err := readLocalFileExtent(upReq.localPath, 0, int(upReq.fileSize))
		if err != nil {
			return err
		}
		return sybx.ops.DxFileUploadPart(
			context.TODO(),
			client,
			upReq.id, 1, data)
	}

	// a large file, with more than a single chunk
	var fileWg sync.WaitGroup
	fileEndOfs := upReq.fileSize - 1
	ofs := int64(0)
	cIndex := 1
	fileParts := make([]*Chunk, 0)
	for ofs <= fileEndOfs {
		chunkEndOfs := MinInt64(ofs + upReq.partSize - 1, fileEndOfs)
		chunkLen := chunkEndOfs - ofs
		buf, err := readLocalFileExtent(upReq.localPath, ofs, int(chunkLen))
		if err != nil {
			return err
		}
		chunk := &Chunk{
			fileId : upReq.id,
			index : cIndex,
			data : buf,
			fwg : &fileWg,
			err : nil,
		}
		// enqueue an upload request. This can block, if there
		// are many chunks.
		fileWg.Add(1)
		sybx.chunkQueue <- chunk
		fileParts = append(fileParts, chunk)

		ofs += upReq.partSize
		cIndex++
	}

	// wait for all requests to complete
	fileWg.Wait()

	// check the error codes
	var finalErr error
	for _, chunk := range(fileParts) {
		if chunk.err != nil {
			sybx.log("failed to upload file %s part %d, error=%s",
				chunk.fileId, chunk.index, chunk.err.Error())
			finalErr = chunk.err
		}
	}

	return finalErr
}

func (sybx *SyncDbDx) createEmptyFile(
	httpClient *retryablehttp.Client,
	upReq FileUploadReq) error {
	// The file is empty
	if upReq.uploadParams.EmptyLastPartAllowed {
		// we need to upload an empty part, only
		// then can we close the file
		ctx := context.TODO()
		err := sybx.ops.DxFileUploadPart(ctx, httpClient, upReq.id, 1, make([]byte, 0))
		if err != nil {
			sybx.log("error uploading empty chunk to file %s", upReq.id)
			return err
		}
	} else {
		// The file can have no parts.
	}
	return nil
}

func (sybx *SyncDbDx) uploadFileDataAndWait(
	client *retryablehttp.Client,
	upReq FileUploadReq) error {
	if sybx.options.Verbose {
		sybx.log("Upload file-size=%d part-size=%d", upReq.fileSize, upReq.partSize)
	}

	if upReq.fileSize == 0 {
		// Create an empty file
		if err := sybx.createEmptyFile(client, upReq); err != nil {
			return err
		}
	} else {
		// loop over the parts, and upload them
		if err := sybx.uploadFileData(client, upReq); err != nil {
			return err
		}
	}

	if sybx.options.Verbose {
		sybx.log("Closing %s", upReq.id)
	}
	ctx := context.TODO()
	return sybx.ops.DxFileCloseAndWait(ctx, client, upReq.id, sybx.options.Verbose)
}

func (sybx *SyncDbDx) createFileWorker() {
	// A fixed http client. The idea is to be able to reuse http connections.
	client := dxda.NewHttpClient(true)

	for true {
		upReq, ok := <-sybx.fileUploadQueue
		if !ok {
			sybx.wg.Done()
			return
		}

		err := sybx.uploadFileDataAndWait(client, upReq)

		if err != nil {
			// Upload failed. Do not erase the local copy.
			//
			sybx.log("Error during upload of file %s",
				upReq.id, err.Error())
			continue
		}

		//sybx.uploadComplete(upReq.id)
	}
}

// enqueue a request to upload the file. This will happen in the background. Since
// we don't erase the local file, there is no rush.
func (sybx *SyncDbDx) UploadFile(f File, fileSize int64) error {
	projDesc, ok := sybx.projId2Desc[f.ProjId]
	if !ok {
		log.Panicf("project %s not found", f.ProjId)
	}

	partSize, err := sybx.calcPartSize(projDesc.UploadParams, fileSize)
	if err != nil {
		sybx.log(`
There is a problem with the file size, it cannot be uploaded
to the platform due to part size constraints. Error=%s`,
			err.Error())
		return fuse.EINVAL
	}

	sybx.fileUploadQueue <- FileUploadReq{
		id : f.Id,
		partSize : partSize,
		uploadParams : projDesc.UploadParams,
		localPath : f.InlineData,
		fileSize : fileSize,
	}
	return nil
}


// TODO
func (sybx *SyncDbDx) sweep() {
	// query the database, find all the files that have been
	// deleted, and remove them from the platform.

	// find all the dirty files and upload them to the platform.

	// find all the files whose metadata has changed, and upload
	// the new tags/properties
}

func (sybx *SyncDbDx) periodicSync() {
	for true {
		time.Sleep(sweepPeriodicTime)
		if sybx.options.Verbose {
			sybx.log("syncing database and platform [")
		}
		sybx.mutex.Lock()
		sybx.sweep()
		sybx.mutex.Unlock()

		if sybx.options.Verbose {
			sybx.log("]")
		}
	}
}

func (sybx *SyncDbDx) CmdSync() {
	sybx.mutex.Lock()
	sybx.sweep()
	sybx.mutex.Unlock()
}
