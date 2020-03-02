package dxfuse

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/dnanexus/dxda"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/jacobsa/fuse"
)

const (
	maxNumBulkDataThreads = 8
	numFileThreads = 4
	sweepPeriodicTime = 1 * time.Minute
)

type Chunk struct {
	fileId         string
	index          int
	data         []byte
	fwg           *sync.WaitGroup
	errorReports   chan error   // report errors if any
}

type FileUpdateReq struct {
	dfi           DirtyFileInfo
	partSize      int64
	uploadParams  FileUploadParameters
}

type SyncDbDx struct {
	dxEnv               dxda.DXEnvironment
	options             Options
	projId2Desc         map[string]DxDescribePrj
	fileUpdateQueue     chan FileUpdateReq
	chunkQueue          chan *Chunk
	sweepStopChan       chan struct{}
	sweepStoppedChan    chan struct{}
	minChunkSize        int64
	numBulkDataThreads  int
	wg                  sync.WaitGroup
	mutex              *sync.Mutex
	mdb                *MetadataDb
	ops                *DxOps
	nonce              *Nonce
}

func NewSyncDbDx(
	options Options,
	dxEnv dxda.DXEnvironment,
	projId2Desc map[string]DxDescribePrj,
	mdb *MetadataDb,
	mutex *sync.Mutex) *SyncDbDx {

	numCPUs := runtime.NumCPU()
	numBulkDataThreads := MinInt(numCPUs, maxNumBulkDataThreads)

	// limit the size of the chunk queue, so we don't
	// have too many chunks stored in memory.
	chunkQueue := make(chan *Chunk, numBulkDataThreads)

	// determine the maximal size of a prefetch IO.
	//
	// TODO: make this dynamic based on network performance.
	var minChunkSize int64
	if dxEnv.DxJobId == "" {
		// on a remote machine the timeouts are too great
		// for large IO sizes.
		minChunkSize = 8 * MiB
	} else {
		// on a worker we can use large sizes, because
		// we have a good network connection to S3 and dnanexus servers
		minChunkSize = 16 * MiB
	}


	sybx := &SyncDbDx{
		dxEnv : dxEnv,
		options : options,
		projId2Desc : projId2Desc,
		fileUpdateQueue : nil,
		chunkQueue : chunkQueue,
		sweepStopChan : nil,
		sweepStoppedChan : nil,
		minChunkSize : minChunkSize,
		numBulkDataThreads : numBulkDataThreads,
		mutex : mutex,
		mdb : mdb,
		ops : NewDxOps(dxEnv, options),
		nonce : NewNonce(),
	}

	// bunch of background threads to upload bulk file data.
	//
	// These are never closed, because they are used during synchronization.
	// When we sync the filesystem, we upload all the files.
	for i := 0; i < numBulkDataThreads; i++ {
		go sybx.bulkDataWorker()
	}

	sybx.startBackgroundWorkers()
	sybx.startSweepWorker()

	return sybx
}

// write a log message, and add a header
func (sybx *SyncDbDx) log(a string, args ...interface{}) {
	LogMsg("sync_db_dx", a, args...)
}


func (sybx *SyncDbDx) startSweepWorker() {
	// start a periodic thread to synchronize the database with
	// the platform
	sybx.sweepStopChan = make(chan struct{})
	sybx.sweepStoppedChan = make(chan struct{})
	go sybx.periodicSync()
}

func (sybx *SyncDbDx) stopSweepWorker() {
	close(sybx.sweepStopChan)
	<- sybx.sweepStoppedChan

	sybx.sweepStopChan = nil
	sybx.sweepStoppedChan = nil
}

func (sybx *SyncDbDx) startBackgroundWorkers() {
	// Create a bunch of threads to update files and metadata
	sybx.fileUpdateQueue = make(chan FileUpdateReq)
	for i := 0; i < numFileThreads; i++ {
		sybx.wg.Add(1)
		go sybx.updateFileWorker()
	}
}

func (sybx *SyncDbDx) stopBackgroundWorkers() {
	// signal all upload and modification threads to stop
	close(sybx.fileUpdateQueue)

	// wait for all of them to complete
	sybx.wg.Wait()

	sybx.fileUpdateQueue = nil
}

func (sybx *SyncDbDx) Shutdown() {
	sybx.stopSweepWorker()
	sybx.stopBackgroundWorkers()
	close(sybx.chunkQueue)
}

// A worker dedicated to performing data-upload operations
func (sybx *SyncDbDx) bulkDataWorker() {
	// A fixed http client
	client := dxda.NewHttpClient(true)

	for true {
		chunk, ok := <- sybx.chunkQueue
		if !ok {
			return
		}
		if sybx.options.Verbose {
			sybx.log("Uploading chunk=%d len=%d", chunk.index, len(chunk.data))
		}

		// upload the data, and report the error if any
		err := sybx.ops.DxFileUploadPart(
			context.TODO(),
			client,
			chunk.fileId, chunk.index, chunk.data)
		if err != nil {
			sybx.log("failed to upload file %s part %d, error=%s",
				chunk.fileId, chunk.index, err)
			chunk.errorReports <- err
		}
		chunk.fwg.Done()

		if sybx.options.Verbose {
			sybx.log("Chunk %d complete", chunk.index)
		}
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
	//
	// Notes:
	// 1) We have seen that using the minimum-part-size as reported by AWS is actually a bit
	//    too small, so we add a little bit to it.
	// 2) To make it easy to understanding the part sizes we make them a multiple of MiB.
	minPartSize := MaxInt64(sybx.minChunkSize, param.MinimumPartSize + KiB)
	preferedChunkSize := divideRoundUp(minPartSize, MiB) * MiB
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
	upReq FileUpdateReq,
	fileId string) error {
	if upReq.dfi.FileSize == 0 {
		log.Panicf("The file is empty")
	}

	if upReq.dfi.FileSize <= upReq.partSize {
		// This is a small file, upload it synchronously.
		// This ensures that only large chunks are uploaded by the bulk-threads,
		// improving fairness.
		data, err := readLocalFileExtent(upReq.dfi.LocalPath, 0, int(upReq.dfi.FileSize))
		if err != nil {
			return err
		}
		return sybx.ops.DxFileUploadPart(
			context.TODO(),
			client,
			fileId, 1, data)
	}

	// a large file, with more than a single chunk

	// a channel for reporting errors. It needs to
	// accomodate the maximal number of errors, so that the worker
	// threads will not block.
	numParts := divideRoundUp(upReq.dfi.FileSize, upReq.partSize)
	errorReports := make(chan error, numParts)

	var fileWg sync.WaitGroup
	fileEndOfs := upReq.dfi.FileSize - 1
	ofs := int64(0)
	cIndex := 1
	for ofs <= fileEndOfs {
		chunkEndOfs := MinInt64(ofs + upReq.partSize - 1, fileEndOfs)
		chunkLen := chunkEndOfs - ofs
		buf, err := readLocalFileExtent(upReq.dfi.LocalPath, ofs, int(chunkLen))
		if err != nil {
			return err
		}
		chunk := &Chunk{
			fileId : fileId,
			index : cIndex,
			data : buf,
			fwg : &fileWg,
			errorReports : errorReports,
		}
		// enqueue an upload request. This can block, if there
		// are many chunks.
		fileWg.Add(1)
		sybx.chunkQueue <- chunk

		ofs += upReq.partSize
		cIndex++
	}

	// wait for all requests to complete
	fileWg.Wait()
	close(errorReports)

	// check the error codes
	for err := range(errorReports) {
		return err
	}
	return nil
}

func (sybx *SyncDbDx) createEmptyFileData(
	httpClient *retryablehttp.Client,
	upReq FileUpdateReq,
	fileId string) error {
	// The file is empty
	if upReq.uploadParams.EmptyLastPartAllowed {
		// we need to upload an empty part, only
		// then can we close the file
		ctx := context.TODO()
		err := sybx.ops.DxFileUploadPart(ctx, httpClient, fileId, 1, make([]byte, 0))
		if err != nil {
			sybx.log("error uploading empty chunk to file %s", fileId)
			return err
		}
	} else {
		// The file can have no parts.
	}
	return nil
}

func (sybx *SyncDbDx) uploadFileDataAndWait(
	client *retryablehttp.Client,
	upReq FileUpdateReq,
	fileId string) error {
	if sybx.options.Verbose {
		sybx.log("Upload file-size=%d part-size=%d", upReq.dfi.FileSize, upReq.partSize)
	}

	if upReq.dfi.FileSize == 0 {
		// Create an empty file
		if err := sybx.createEmptyFileData(client, upReq, fileId); err != nil {
			return err
		}
	} else {
		// loop over the parts, and upload them
		if err := sybx.uploadFileData(client, upReq, fileId); err != nil {
			return err
		}
	}

	if sybx.options.Verbose {
		sybx.log("Closing %s", fileId)
	}
	return sybx.ops.DxFileCloseAndWait(context.TODO(), client, fileId)
}


// Upload
func (sybx *SyncDbDx) updateFileData(
	client *retryablehttp.Client,
	upReq FileUpdateReq) (string, error) {

	// We need to lock the filesystem while we are doing this, because
	// a race could happen if the directory is removed while the file
	// is created.
	sybx.mutex.Lock()

	// create the file object on the platform.
	fileId, err := sybx.ops.DxFileNew(
		context.TODO(), client, sybx.nonce.String(),
		upReq.dfi.ProjId,
		upReq.dfi.Name,
		upReq.dfi.ProjFolder)
	if err != nil {
		sybx.mutex.Unlock()
		// an error could occur here if the directory has been removed
		// while we were trying to upload the file.
		sybx.log("Error in creating file (%s:%s/%s) on dnanexus: %s",
			upReq.dfi.ProjId, upReq.dfi.ProjFolder,	upReq.dfi.Name,
			err.Error())
		return "", err
	}

	// Update the database with the new ID.
	sybx.mdb.UpdateInodeFileId(upReq.dfi.Inode, fileId)
	sybx.mutex.Unlock()

	// Note: the file may have been deleted while it was being uploaded.
	// This means that an error could happen here, and it would be legal.
	err = sybx.uploadFileDataAndWait(client, upReq, fileId)
	if err != nil {
		// Upload failed.
		// TODO: erase the local copy.
		sybx.log("Error during upload of file %s: %s",
			fileId, err.Error())
		return "", err
	}

	// Erase the old file-id.
	if upReq.dfi.Id == "" {
		// This is the first time we are creating the file, there
		// is no older version on the platform.
		return fileId, nil
	}

	// remove the old version
	var oldFileId []string
	oldFileId = append(oldFileId, upReq.dfi.Id)
	err = sybx.ops.DxRemoveObjects(context.TODO(), client, upReq.dfi.ProjId, oldFileId)
	if err != nil {
		// TODO: if the file has already been removed on the platform, then
		// we will get an error here.
		return "", err
	}
	return fileId, nil
}

func (sybx *SyncDbDx) updateFileAttributes(client *retryablehttp.Client, dfi DirtyFileInfo) error {
	// describe the object state on the platform. The properties/tags have
	// changed.
	fDesc, err := DxDescribe(context.TODO(), client, &sybx.dxEnv, dfi.Id)
	if err != nil {
		sybx.log(err.Error())
		sybx.log("Failed ot describe file %v", dfi)
		return err
	}

	// Figure out the symmetric difference between the on-platform properties,
	// and what the filesystem has.
	dnaxProps := fDesc.Properties
	fsProps := dfi.Properties
	opProps := make(map[string]*string)

	for key, dnaxValue := range(dnaxProps) {
		fsValue, ok := fsProps[key]
		if !ok {
			// property was removed
			opProps[key] = nil
		} else if dnaxValue != fsValue {
			// value has changed
			opProps[key] = &fsValue
		}
	}

	for key, fsValue := range(fsProps) {
		_, ok := dnaxProps[key]
		if !ok {
			// a new property
			opProps[key] = &fsValue
		} else {
			// existing property, we already checked that case;
			// if the value changed, we set it in the map
		}
	}

	if len(opProps) > 0 {
		if sybx.options.Verbose {
			sybx.log("%s symmetric difference between properties %v ^ %v = %v",
				dfi.Id, dnaxProps, fsProps, opProps)
		}
		err := sybx.ops.DxSetProperties(context.TODO(), client, dfi.ProjId, dfi.Id, opProps)
		if err != nil {
			return err
		}
	}

	// figure out the symmetric difference between the old and new tags.
	dnaxTags := fDesc.Tags
	fsTags := dfi.Tags

	// make hash-tables for easy access
	dnaxTagsTbl := make(map[string]bool)
	for _, tag := range(dnaxTags) {
		dnaxTagsTbl[tag] = true
	}
	fsTagsTbl := make(map[string]bool)
	for _, tag := range(fsTags) {
		fsTagsTbl[tag] = true
	}

	var tagsRemoved []string
	for _, tag := range(dnaxTags) {
		_, ok := fsTagsTbl[tag]
		if !ok {
			tagsRemoved = append(tagsRemoved, tag)
		}
	}

	var tagsAdded []string
	for _, tag := range(fsTags) {
		_, ok := dnaxTagsTbl[tag]
		if !ok {
			tagsAdded = append(tagsAdded, tag)
		}
	}
	if sybx.options.Verbose {
		if len(tagsAdded) > 0 || len(tagsRemoved) > 0 {
			sybx.log("%s symmetric difference between tags %v ^ %v = (added=%v, removed=%v)",
				dfi.Id, dnaxTags, fsTags, tagsAdded, tagsRemoved)
		}
	}

	if len(tagsAdded) != 0  {
		err := sybx.ops.DxAddTags(context.TODO(), client, dfi.ProjId, dfi.Id, tagsAdded)
		if err != nil {
			return err
		}
	}
	if len(tagsRemoved) != 0 {
		err := sybx.ops.DxRemoveTags(context.TODO(), client, dfi.ProjId, dfi.Id, tagsRemoved)
		if err != nil {
			return err
		}
	}
	return nil
}

func (sybx *SyncDbDx) updateFileWorker() {
	// A fixed http client. The idea is to be able to reuse http connections.
	client := dxda.NewHttpClient(true)

	for true {
		upReq, ok := <-sybx.fileUpdateQueue
		if !ok {
			sybx.wg.Done()
			return
		}
		if sybx.options.Verbose {
			sybx.log("updateFile inode=%d part-size=%d", upReq.dfi.Inode, upReq.partSize)
		}

		// note: the file-id may be empty ("") if the file
		// has just been created on the local machine.
		var err error
		crntFileId := upReq.dfi.Id
		if upReq.dfi.dirtyData {
			crntFileId, err = sybx.updateFileData(client, upReq)
			if err != nil {
				sybx.log("Error in update-data: %s", err.Error())
				continue
			}
		}
		if upReq.dfi.dirtyMetadata {
			if crntFileId == "" {
				// create an empty file
				check(upReq.dfi.FileSize == 0)
				crntFileId, err = sybx.updateFileData(client, upReq)
				if err != nil {
					sybx.log("Error when creating a metadata-only file %s",
						err.Error())
					continue
				}
			}
			// file exists, figure out what needs to be
			// updated
			dfi := upReq.dfi
			dfi.Id = crntFileId
			sybx.updateFileAttributes(client, dfi)
		}
	}
}

// enqueue a request to upload the file. This will happen in the background. Since
// we don't erase the local file, there is no rush.
func (sybx *SyncDbDx) enqueueUpdateFileReq(dfi DirtyFileInfo) error {
	projDesc, ok := sybx.projId2Desc[dfi.ProjId]
	if !ok {
		log.Panicf("project (%s) not found", dfi.ProjId)
	}

	partSize, err := sybx.calcPartSize(projDesc.UploadParams, dfi.FileSize)
	if err != nil {
		sybx.log(`
There is a problem with the file size, it cannot be uploaded
to the platform due to part size constraints. Error=%s`,
			err.Error())
		return fuse.EINVAL
	}

	sybx.fileUpdateQueue <- FileUpdateReq{
		dfi : dfi,
		partSize : partSize,
		uploadParams : projDesc.UploadParams,
	}
	return nil
}

func (sybx *SyncDbDx) sweep(flag int) error {
	if sybx.options.Verbose {
		sybx.log("syncing database and platform [")
	}

	// find all the dirty files. We need to lock
	// the database while we are doing this.
	sybx.mutex.Lock()
	dirtyFiles, err := sybx.mdb.DirtyFilesGetAndReset(flag)
	if err != nil {
		sybx.mutex.Unlock()
		return err
	}
	sybx.mutex.Unlock()

	if sybx.options.Verbose {
		sybx.log("%d dirty files", len(dirtyFiles))
	}

	// enqueue them on the "to-upload" list
	for _, file := range(dirtyFiles) {
		sybx.enqueueUpdateFileReq(file)
	}

	if sybx.options.Verbose {
		sybx.log("]")
	}
	return nil
}

func (sybx *SyncDbDx) periodicSync() {
	sybx.log("starting sweep thread")
	lastSweepTs := time.Now()
	for true {
		// we need to wake up often to check if
		// the sweep has been disabled.
		time.Sleep(1 * time.Second)

		select {
		default:
			// normal case, we weren't stopped
		case <- sybx.sweepStopChan:
			sybx.log("stopped sweep thread")
			close(sybx.sweepStoppedChan)
			return
		}

		now := time.Now()
		if now.Before(lastSweepTs.Add(sweepPeriodicTime)) {
			continue
		}
		lastSweepTs = now

		if err := sybx.sweep(DIRTY_FILES_INACTIVE); err != nil {
			sybx.log("Error in sweep: %s", err.Error())
		}
	}
}

func (sybx *SyncDbDx) CmdSync() error {
	// we don't want to have two sweeps running concurrently
	sybx.stopSweepWorker()

	if err := sybx.sweep(DIRTY_FILES_ALL); err != nil {
		sybx.log("Error in sweep: %s", err.Error())
		return err
	}

	// now wait for the objects to be created and the data uploaded
	sybx.stopBackgroundWorkers()

	// start the background threads again
	sybx.startBackgroundWorkers()
	sybx.startSweepWorker()

	return nil
}
