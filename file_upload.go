package dxfuse

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/dnanexus/dxda"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/jacobsa/fuse"
)

type RequestNewFile struct {
	ProjId   string `json:"project"`
	Name     string `json:"name"`
	Folder   string `json:"folder"`
	Parents  bool   `json:"parents"`
	Nonce    string `json:"nonce"`
}

type ReplyNewFile struct {
	Id string `json:"id"`
}

type RequestUploadChunk struct {
	Size  int     `json:"size"`
	Index int     `json:"index"`
	Md5   string  `json:"md5"`
}

type ReplyUploadChunk struct {
	Url     string            `json:"url"`
	Expires int64             `json:"expires"`
	Headers map[string]string `json:"headers"`
}

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
}

const (
	chunkMaxQueueSize = 10

	numFileThreads = 4
	numBulkDataThreads = 8
	minChunkSize = 16 * MiB

	fileCloseWaitTime = 5 * time.Second
	fileCloseMaxWaitTime = 10 * time.Minute
)

func DxFileNew(
	ctx context.Context,
	httpClient *retryablehttp.Client,
	dxEnv *dxda.DXEnvironment,
	nonceStr string,
	projId string,
	fname string,
	folder string) (string, error) {

	var request RequestNewFile
	request.ProjId = projId
	request.Name = fname
	request.Folder = folder
	request.Parents = false
	request.Nonce = nonceStr

	payload, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	repJs, err := dxda.DxAPI(ctx, httpClient, NumRetriesDefault, dxEnv, "file/new", string(payload))
	if err != nil {
		return "", err
	}

	var reply ReplyNewFile
	if err := json.Unmarshal(repJs, &reply); err != nil {
		return "", err
	}

	// got a file ID back
	return reply.Id, nil
}

func DxFileCloseAndWait(
	ctx context.Context,
	httpClient *retryablehttp.Client,
	dxEnv *dxda.DXEnvironment,
	fid string,
	verbose bool) error {

	_, err := dxda.DxAPI(
		ctx,
		httpClient,
		NumRetriesDefault,
		dxEnv,
		fmt.Sprintf("%s/close", fid),
		"{}")
	if err != nil {
		return err
	}

        // wait for file to achieve closed state
	start := time.Now()
	deadline := start.Add(fileCloseMaxWaitTime)
        for true {
		fDesc, err := DxDescribe(ctx, httpClient, dxEnv, fid, false)
		if err != nil {
			return err
		}
		switch fDesc.State {
		case "closed":
			// done. File is closed.
			return nil
		case "closing":
			// not done yet.
			if verbose {
				elapsed := time.Now().Sub(start)
				log.Printf("Waited %s for file %s to close", elapsed.String(), fid)
			}
			time.Sleep(fileCloseWaitTime)

			// don't wait too long
			if time.Now().After(deadline) {
				return fmt.Errorf("Waited %s for file %s to close, stopping the wait",
					fileCloseMaxWaitTime.String(), fid)
			}
			continue
		default:
			return fmt.Errorf("data object %s has bad state %s", fid, fDesc.State)
		}
	}
	return nil
}

func dxFileUploadPart(
	ctx context.Context,
	httpClient *retryablehttp.Client,
	dxEnv *dxda.DXEnvironment,
	fileId string,
	index int,
	data []byte) error {

	md5Sum := md5.Sum(data)
	uploadReq := RequestUploadChunk{
		Size: len(data),
		Index: index,
		Md5: hex.EncodeToString(md5Sum[:]),
	}
	log.Printf("%v", uploadReq)

	reqJson, err := json.Marshal(uploadReq)
	if err != nil {
		return err
	}
	replyJs, err := dxda.DxAPI(
		ctx,
		httpClient,
		NumRetriesDefault,
		dxEnv,
		fmt.Sprintf("%s/upload", fileId),
		string(reqJson))
	if err != nil {
		log.Printf(err.Error())
		return err
	}

	var reply ReplyUploadChunk
	if err = json.Unmarshal(replyJs, &reply); err != nil {
		return err
	}

	_, err = dxda.DxHttpRequest(ctx, httpClient, NumRetriesDefault, "PUT", reply.Url, reply.Headers, data)
	return err
}

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

		// limit the size of the chunk queue, so we don't spend
		// have too many chunks stored in memory.
		chunkQueue : make(chan *Chunk, chunkQueueSize),
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

func (fugs *FileUploadGlobalState) Shutdown() {
	// signal all upload threads to stop
	close(fugs.fileUploadQueue)
	close(fugs.chunkQueue)

	// wait for all of them to complete
	fugs.wg.Wait()
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
			log.Printf("Uploading chunk=%d len=%d", chunk.index, len(chunk.data))
		}

		// upload the data, and store the error code in the chunk
		// data structure.
		chunk.err = dxFileUploadPart(
			context.TODO(),
			client,
			&fugs.dxEnv,
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

// Upload the parts. The first chunk is uploaded synchronously, the
// others are uploaded by worker threads.
//
// note: chunk indexes start at 1 (not zero)
func (fugs *FileUploadGlobalState) uploadFileData(
	client *retryablehttp.Client,
	upReq FileUploadReq) error {
	if upReq.fileSize == 0 {
		panic("The file is empty")
	}

	fReader, err := os.Open(upReq.localPath)
	if err != nil {
		return err
	}
	defer fReader.Close()

	if upReq.fileSize <= upReq.partSize {
		// This is a small file, upload it synchronously.
		// This ensures that only large chunks are uploaded by the bulk-threads,
		// improving fairness.
		data, err := ioutil.ReadAll(fReader)
		if err != nil {
			return err
		}
		if int64(len(data)) != upReq.fileSize {
			panic(fmt.Sprintf("short read, got %d bytes instead of %d",
				len(data), upReq.fileSize))
		}
		return dxFileUploadPart(
			context.TODO(),
			client,
			&fugs.dxEnv,
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
		buf := make([]byte, chunkLen)
		len, err := fReader.ReadAt(buf, ofs)
		if err != nil {
			return err
		}
		if int64(len) != chunkLen {
			panic(fmt.Sprintf("short read, got %d bytes instead of %d",
				len, chunkLen))
		}
		chunk := &Chunk{
			fileId : upReq.id,
			index : cIndex,
			data : buf,
			fwg : &fileWg,
			err : nil,
		}
		// enqueue an upload request
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
			log.Printf("failed to upload file %s part %d, error=%s",
				chunk.fileId, chunk.index, chunk.err.Error())
			finalErr = chunk.err
		}
	}

	return finalErr
}

func (fugs *FileUploadGlobalState) createEmptyFile(
	httpClient *retryablehttp.Client,
	upReq FileUploadReq) {
	// The file is empty
	if upReq.uploadParams.EmptyLastPartAllowed {
		// we need to upload an empty part, only
		// then can we close the file
		ctx := context.TODO()
		err := dxFileUploadPart(ctx, httpClient, &fugs.dxEnv, upReq.id, 1, make([]byte, 0))
		if err != nil {
			log.Printf("error uploading empty chunk to file %s, error = %s",
				upReq.id, err.Error())
			return
		}
	} else {
		// The file can have no parts.
	}

	if fugs.options.Verbose {
		log.Printf("Closing %s", upReq.id)
	}
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
		fileSize := upReq.fileSize

		if fugs.options.Verbose {
			log.Printf("Upload file-size=%d part-size=%d", fileSize, upReq.partSize)
		}

		if fileSize == 0 {
			// Create an empty file, and continue to the next request
			fugs.createEmptyFile(client, upReq)
		} else {
			// loop over the parts, and upload them
			if err := fugs.uploadFileData(client, upReq); err != nil {
				log.Printf("upload error to file %s, error = %s", upReq.id, err.Error())
				continue
			}
		}

		if fugs.options.Verbose {
			log.Printf("Closing %s", upReq.id)
		}
		ctx := context.TODO()
		err := DxFileCloseAndWait(ctx, client, &fugs.dxEnv, upReq.id, fugs.options.Verbose)
		if err != nil {
			log.Printf("failed to close file %s, error = %s", upReq.id, err.Error())
		}
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
		log.Printf(`
There is a problem with the file size, it cannot be uploaded
to the platform due to part size constraints. Error=%s`,
			err.Error())
		return fuse.EINVAL
	}

	fugs.fileUploadQueue <- FileUploadReq{
		id : f.Id,
		partSize : partSize,
		uploadParams : projDesc.UploadParams,
		localPath : f.InlineData,
		fileSize : fileSize,
	}
	return nil
}
