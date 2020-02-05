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
	numMetadataThreads = 2
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
	deadObjectsQueue    chan []DeadFile
	enableSweep         bool
	wg                  sync.WaitGroup
	mutex              *sync.Mutex
	mdb                *MetadataDb
	ops                *DxOps
	nonce              *Nonce

	// a pool of http clients, for short requests, such as file creation,
	// or file describe.
	httpClientPool      chan(*retryablehttp.Client)
}

func NewSyncDbDx(
	options Options,
	dxEnv dxda.DXEnvironment,
	projId2Desc map[string]DxDescribePrj,
	httpClientPool chan(*retryablehttp.Client),
	mdb *MetadataDb,
	mutex *sync.Mutex) *SyncDbDx {
	// the chunk queue size should be at least the size of the thread
	// pool.
	chunkQueueSize := MaxInt(numBulkDataThreads, chunkMaxQueueSize)

	// limit the size of the chunk queue, so we don't
	// have too many chunks stored in memory.
	chunkQueue := make(chan *Chunk, chunkQueueSize)

	sybx := &SyncDbDx{
		dxEnv : dxEnv,
		options : options,
		projId2Desc : projId2Desc,
		fileUpdateQueue : nil,
		chunkQueue : chunkQueue,
		enableSweep : true,
		mutex : mutex,
		mdb : mdb,
		ops : NewDxOps(dxEnv, options),
		nonce : NewNonce(),
		httpClientPool : httpClientPool,
	}

	// bunch of background threads to upload bulk file data.
	//
	// These are never closed, because they are used during synchronization.
	// When we sync the filesystem, we upload all the files.
	for i := 0; i < numBulkDataThreads; i++ {
		go sybx.bulkDataWorker()
	}

	sybx.startBackgroundWorkers()

	// start a periodic thread to synchronize the database with
	// the platform
	go sybx.periodicSync()

	return sybx
}

// write a log message, and add a header
func (sybx *SyncDbDx) log(a string, args ...interface{}) {
	LogMsg("synx_db_dx", a, args...)
}

func (sybx *SyncDbDx) startBackgroundWorkers() {
	sybx.fileUpdateQueue = make(chan FileUpdateReq)
	sybx.deadObjectsQueue = make(chan []DeadFile)

	// Create a bunch of threads to update files and metadata
	for i := 0; i < numFileThreads; i++ {
		sybx.wg.Add(1)
		go sybx.updateFileWorker()
	}

	sybx.wg.Add(1)
	go sybx.deadObjectsRemover()
}

func (sybx *SyncDbDx) stopBackgroundWorkers() {
	// signal all upload and modification threads to stop
	close(sybx.fileUpdateQueue)

	// stop also the object-removal thread
	close(sybx.deadObjectsQueue)

	// Note: we don't close the chunkQeue

	// wait for all of them to complete
	sybx.wg.Wait()

	sybx.fileUpdateQueue = nil
	sybx.deadObjectsQueue = nil
}

func (sybx *SyncDbDx) Shutdown() {
	// disable to sweep thread
	sybx.mutex.Lock()
	sybx.enableSweep = false
	sybx.mutex.Unlock()

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
	var fileWg sync.WaitGroup
	fileEndOfs := upReq.dfi.FileSize - 1
	ofs := int64(0)
	cIndex := 1
	fileParts := make([]*Chunk, 0)
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
	// create the file object on the platform.
	fileId, err := sybx.ops.DxFileNew(
		context.TODO(), client, sybx.nonce.String(),
		upReq.dfi.ProjId,
		upReq.dfi.Name,
		upReq.dfi.ProjFolder)
	if err != nil {
		sybx.log("Error in creating file (%s:%s/%s) on dnanexus: %s",
			upReq.dfi.ProjId, upReq.dfi.ProjFolder,	upReq.dfi.Name,
			err.Error())
		return "", err
	}

	err = sybx.uploadFileDataAndWait(client, upReq, fileId)
	if err != nil {
		// Upload failed. Do not erase the local copy.
		//
		sybx.log("Error during upload of file %s: %s",
			fileId, err.Error())
		return "", err
	}

	// Update the database with the new ID.
	sybx.mdb.UpdateInodeFileId(upReq.dfi.Inode, fileId)

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
			sybx.log("updateFile %v", upReq)
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

func (sybx *SyncDbDx) deleteObjects(deadFiles []DeadFile) error {
	// remove all local data
	for _, df := range(deadFiles) {
		if df.Kind != FK_Regular {
			continue
		}

		// remove the file data so it does not take up space on disk.
		//
		// We know there isn't an ongoing upload, because we are locking
		// the sync-fs-db state here.
		localPath := df.InlineData
		if sybx.options.Verbose {
			sybx.log("Removing file %v local-path=%s", df, localPath)
		}
		if err := os.Remove(localPath); err != nil {
			sybx.log("Error removing file %v local-path=%s", df, localPath)
			sybx.log(err.Error())
		}
	}

	// delete all files from the platform
	httpClient := <- sybx.httpClientPool
	defer func() {
		sybx.httpClientPool <- httpClient
	} ()

	// Split into a per-project list
	projects := make(map[string][]DeadFile)
	for _, df := range(deadFiles) {
		dfa, ok := projects[df.ProjId]
		if !ok {
			dfa := make([]DeadFile, 1)
			dfa[0] = df
			projects[df.ProjId] = dfa
		} else {
			dfa = append(dfa, df)
			projects[df.ProjId] = dfa
		}

		if sybx.options.Verbose {
			sybx.log("split into per project lists %v", projects)
		}
	}

	for projId, files := range(projects) {
		var objIds []string
		for _,df := range(files) {
			objIds = append(objIds, df.Id)
		}
		err := sybx.ops.DxRemoveObjects(context.TODO(), httpClient, projId, objIds)
		if err != nil {
			sybx.log("Error in dx-remove-objects %s", err.Error())
		}
	}

	return nil
}

// Threads that continuously polls for objects to delete, and removes them
// from the local disk and the platform. These are primarily files that
// the user removed.
func (sybx *SyncDbDx) deadObjectsRemover() {
	for true {
		deadFiles, ok := <- sybx.deadObjectsQueue
		if !ok {
			sybx.wg.Done()
			return
		}
		sybx.deleteObjects(deadFiles)
	}
}

// query the database, find all the files that have been
// deleted, and enqueue them for deletion from the platform.
func (sybx *SyncDbDx) enqueueDeadObjects() error {
	deadFiles, err := sybx.mdb.DeadObjectsGetAllAndReset()
	if err != nil {
		return err
	}
	if deadFiles == nil || len(deadFiles) == 0 {
		if sybx.options.Verbose {
			sybx.log("found no dead objects to remove")
		}
		return nil
	}
	sybx.deadObjectsQueue <- deadFiles
	return nil
}

func (sybx *SyncDbDx) sweep() error {
	if sybx.options.Verbose {
		sybx.log("syncing database and platform [")
	}

	// query the database, find all the files that have been
	// deleted, and remove them from the platform.
	if err := sybx.enqueueDeadObjects(); err != nil {
		return err
	}

	// find all the dirty files
	dirtyFiles, err := sybx.mdb.DirtyFilesGetAllAndReset()
	if err != nil {
		return err
	}
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
	for true {
		time.Sleep(sweepPeriodicTime)

		sybx.mutex.Lock()
		if sybx.enableSweep {
			if err := sybx.sweep(); err != nil {
				sybx.log("Error in sweep: %s", err.Error())
			}
		}
		sybx.mutex.Unlock()
	}
}

func (sybx *SyncDbDx) CmdDeleteDeadObjects() error {
	sybx.mutex.Lock()
	defer sybx.mutex.Unlock()
	return sybx.enqueueDeadObjects()
}

func (sybx *SyncDbDx) CmdSync() error {
	sybx.mutex.Lock()
	if err := sybx.sweep(); err != nil {
		sybx.mutex.Unlock()
		sybx.log("Error in sweep: %s", err.Error())
		return err
	}
	sybx.enableSweep = false
	sybx.mutex.Unlock()

	// now wait for the objects to be created and the data uploaded
	sybx.stopBackgroundWorkers()

	// start the background work again, now that everything is done
	sybx.startBackgroundWorkers()

	sybx.mutex.Lock()
	sybx.enableSweep = true
	sybx.mutex.Unlock()
	return nil
}
