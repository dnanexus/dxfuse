package dxfuse

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/dnanexus/dxda"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/jacobsa/fuse"
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

type FileUploadGlobalState struct {
	dxEnv           dxda.DXEnvironment
	options         Options
	projId2Desc     map[string]DxDescribePrj
	fileUploadQueue chan FileUploadReq
	chunkQueue      chan *Chunk
	wg              sync.WaitGroup
	mutex           sync.Mutex
	mdb            *MetadataDb
	ops            *DxOps

	// list of files undergoing upload. If the flag is true, the file should be
	// uploaded. If it is false, the upload was cancelled.
	ongoingOps      map[string]bool
}

const (
	chunkMaxQueueSize = 10

	numFileThreads = 4
	numBulkDataThreads = 8
	minChunkSize = 16 * MiB

	fileCloseWaitTime = 5 * time.Second
	fileCloseMaxWaitTime = 10 * time.Minute
)

func NewFileUploadGlobalState(
	options Options,
	dxEnv dxda.DXEnvironment,
	projId2Desc map[string]DxDescribePrj) *FileUploadGlobalState {

	// the chunk queue size should be at least the size of the thread
	// pool.
	chunkQueueSize := MaxInt(numBulkDataThreads, chunkMaxQueueSize)

	fugs := &FileUploadGlobalState{
		dxEnv : dxEnv,
		options : options,
		projId2Desc : projId2Desc,
		fileUploadQueue : make(chan FileUploadReq),

		// limit the size of the chunk queue, so we don't
		// have too many chunks stored in memory.
		chunkQueue : make(chan *Chunk, chunkQueueSize),

		mutex : sync.Mutex{},
		ops : NewDxOps(dxEnv, options),
		ongoingOps : make(map[string]bool, 0),
	}

	// Create a bunch of threads
	fugs.wg.Add(numFileThreads)
	for i := 0; i < numFileThreads; i++ {
		go fugs.createFileWorker()
	}

	fugs.wg.Add(numBulkDataThreads)
	for i := 0; i < numBulkDataThreads; i++ {
		go fugs.bulkDataWorker()
	}

	return fugs
}

// write a log message, and add a header
func (fugs *FileUploadGlobalState) log(a string, args ...interface{}) {
	LogMsg("file_upload", a, args...)
}

func (fugs *FileUploadGlobalState) Shutdown() {
	// signal all upload threads to stop
	close(fugs.fileUploadQueue)
	close(fugs.chunkQueue)

	// wait for all of them to complete
	fugs.wg.Wait()
}

func (fugs *FileUploadGlobalState) CancelUpload(fileId string) {
	fugs.mutex.Lock()
	fugs.mutex.Unlock()
	_, ok := fugs.ongoingOps[fileId]
	if !ok {
		// The file is not being uploaded. We are done.
		return
	}
	fugs.ongoingOps[fileId] = false
}

// A worker dedicated to performing data-upload operations
func (fugs *FileUploadGlobalState) bulkDataWorker() {
	// A fixed http client
	client := dxda.NewHttpClient(true)

	for true {
		chunk, ok := <- fugs.chunkQueue
		if !ok {
			fugs.wg.Done()
			return
		}
		if fugs.options.Verbose {
			fugs.log("Uploading chunk=%d len=%d", chunk.index, len(chunk.data))
		}

		// upload the data, and store the error code in the chunk
		// data structure.
		chunk.err = fugs.ops.DxFileUploadPart(
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

func (fugs *FileUploadGlobalState) calcPartSize(param FileUploadParameters, fileSize int64) (int64, error) {
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
		panic(fmt.Sprintf("short read, got %d bytes instead of %d",
			recvLen, len))
	}
	return buf, nil
}

// Upload the parts. Small files are uploaded synchronously, large
// files are uploaded by worker threads.
//
// note: chunk indexes start at 1 (not zero)
func (fugs *FileUploadGlobalState) uploadFileData(
	client *retryablehttp.Client,
	upReq FileUploadReq) error {
	if upReq.fileSize == 0 {
		panic("The file is empty")
	}

	if upReq.fileSize <= upReq.partSize {
		// This is a small file, upload it synchronously.
		// This ensures that only large chunks are uploaded by the bulk-threads,
		// improving fairness.
		data, err := readLocalFileExtent(upReq.localPath, 0, int(upReq.fileSize))
		if err != nil {
			return err
		}
		return fugs.ops.DxFileUploadPart(
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
		fugs.chunkQueue <- chunk
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
			fugs.log("failed to upload file %s part %d, error=%s",
				chunk.fileId, chunk.index, chunk.err.Error())
			finalErr = chunk.err
		}
	}

	return finalErr
}

func (fugs *FileUploadGlobalState) createEmptyFile(
	httpClient *retryablehttp.Client,
	upReq FileUploadReq) error {
	// The file is empty
	if upReq.uploadParams.EmptyLastPartAllowed {
		// we need to upload an empty part, only
		// then can we close the file
		ctx := context.TODO()
		err := fugs.ops.DxFileUploadPart(ctx, httpClient, upReq.id, 1, make([]byte, 0))
		if err != nil {
			fugs.log("error uploading empty chunk to file %s", upReq.id)
			return err
		}
	} else {
		// The file can have no parts.
	}
	return nil
}

func (fugs *FileUploadGlobalState) uploadFileDataAndWait(
	client *retryablehttp.Client,
	upReq FileUploadReq) error {
	if fugs.options.Verbose {
		fugs.log("Upload file-size=%d part-size=%d", upReq.fileSize, upReq.partSize)
	}

	if upReq.fileSize == 0 {
		// Create an empty file
		if err := fugs.createEmptyFile(client, upReq); err != nil {
			return err
		}
	} else {
		// loop over the parts, and upload them
		if err := fugs.uploadFileData(client, upReq); err != nil {
			return err
		}
	}

	if fugs.options.Verbose {
		fugs.log("Closing %s", upReq.id)
	}
	ctx := context.TODO()
	return fugs.ops.DxFileCloseAndWait(ctx, client, upReq.id, fugs.options.Verbose)
}

// check if the upload has been cancelled
func (fugs *FileUploadGlobalState) shouldUpload(fileid string) bool {
	fugs.mutex.Lock()
	defer fugs.mutex.Unlock()

	return fugs.ongoingOps[fileid]
}

func (fugs *FileUploadGlobalState) uploadComplete(fileid string) {
	fugs.mutex.Lock()
	defer fugs.mutex.Unlock()

	delete(fugs.ongoingOps, fileid)
}

func (fugs *FileUploadGlobalState) createFileWorker() {
	// A fixed http client. The idea is to be able to reuse http connections.
	client := dxda.NewHttpClient(true)

	for true {
		upReq, ok := <-fugs.fileUploadQueue
		if !ok {
			fugs.wg.Done()
			return
		}

		// check if the upload has been cancelled
		if !fugs.shouldUpload(upReq.id) {
			fugs.log("file %s was removed, no need to upload", upReq.id)
			fugs.uploadComplete(upReq.id)
			continue
		}

		err := fugs.uploadFileDataAndWait(client, upReq)

		if err != nil {
			// Upload failed. Do not erase the local copy.
			//
			// Note: we have not entire eliminated the race condition
			// between uploading and removing a file. We may still
			// get errors in good path cases.
			if (fugs.shouldUpload(upReq.id)) {
				fugs.log("Error during upload of file %s", upReq.id)
				fugs.log(err.Error())
			}
			fugs.uploadComplete(upReq.id)
			continue
		}

		// Update the database to point to the remote file copy. This saves
		// space on the local drive.
		//fugs.mdb.UpdateFileMakeRemote(context.TODO(), upReq.id)

		fugs.uploadComplete(upReq.id)
	}
}

// enqueue a request to upload the file. This will happen in the background. Since
// we don't erase the local file, there is no rush.
func (fugs *FileUploadGlobalState) UploadFile(f File, fileSize int64) error {
	projDesc, ok := fugs.projId2Desc[f.ProjId]
	if !ok {
		panic(fmt.Sprintf("project %s not found", f.ProjId))
	}

	partSize, err := fugs.calcPartSize(projDesc.UploadParams, fileSize)
	if err != nil {
		fugs.log(`
There is a problem with the file size, it cannot be uploaded
to the platform due to part size constraints. Error=%s`,
			err.Error())
		return fuse.EINVAL
	}

	// Add the file to the "being-uploaded" list.
	// we will need this in the corner case of deleting the file while
	// it is being uploaded.
	fugs.mutex.Lock()
	fugs.ongoingOps[f.Id] = true
	fugs.mutex.Unlock()

	fugs.fileUploadQueue <- FileUploadReq{
		id : f.Id,
		partSize : partSize,
		uploadParams : projDesc.UploadParams,
		localPath : f.InlineData,
		fileSize : fileSize,
	}
	return nil
}
