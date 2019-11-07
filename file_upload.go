package dxfuse

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
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

const (
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
		log.Printf(err.Error())
		return "", err
	}
	// TODO: triage the errors
/*
	switch status {
		InvalidInput
		A nonce was reused in a request but some of the other inputs had changed signifying a new and different request

		PermissionDenied
		    UPLOAD access required
		InvalidType
		    project is not a project ID
		ResourceNotFound
		    The specified project is not found
		    The route in folder does not exist, and parents is false

	}*/

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
		log.Printf(err.Error())
		return err
	// Triage errors
	/*
	PermissionDenied
UPLOAD access required
InvalidState
fileUploadParameters.emptyLastPartAllowed is true and there are zero parts
At least one part is in the "pending" state
There exists a part, other than the one with the highest part index, whose size is less than fileUploadParameters.minimumPartSize bytes
fileUploadParameters.emptyLastPartAllowed is false and the part with the highest index has 0 bytes
The file has size larger than fileUploadParameters.maximumFileSize bytes
*/
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
	index int
	data []byte
}

func DxFileUploadPart(
	ctx context.Context,
	httpClient *retryablehttp.Client,
	dxEnv *dxda.DXEnvironment,
	fileId string,
	chunk Chunk) error {

	md5Sum := md5.Sum(chunk.data)
	uploadReq := RequestUploadChunk{
		Size: len(chunk.data),
		Index: chunk.index,
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

	_, err = dxda.DxHttpRequest(ctx, httpClient, NumRetriesDefault, "PUT", reply.Url, reply.Headers, chunk.data)
	return err
}

type UploadReq struct {
	id           string
	partSize     int64
	uploadParams FileUploadParameters
	localPath    string
	fileSize     int64
}

type FileUploadGlobalState struct {
	dxEnv        dxda.DXEnvironment
	options      Options
	projId2Desc  map[string]DxDescribePrj
	reqQueue     chan UploadReq
	wg           sync.WaitGroup
}

const (
	numUploadThreads = 10
	minChunkSize = 16 * MiB
)

func NewFileUploadGlobalState(
	options Options,
	dxEnv dxda.DXEnvironment,
	projId2Desc map[string]DxDescribePrj) *FileUploadGlobalState {

	fugs := &FileUploadGlobalState{
		dxEnv : dxEnv,
		options : options,
		projId2Desc : projId2Desc,
		reqQueue : make(chan UploadReq),
	}

	// limit the number of prefetch IOs
	fugs.wg.Add(numUploadThreads)
	for i := 0; i < numUploadThreads; i++ {
		go fugs.uploadIoWorker()
	}

	return fugs
}

func (fugs *FileUploadGlobalState) Shutdown() {
	// signal all upload threads to stop
	close(fugs.reqQueue)

	// wait for all of them to complete
	fugs.wg.Wait()
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

func (fugs *FileUploadGlobalState) uploadFileDataSequentially(
	httpClient *retryablehttp.Client,
	upReq UploadReq) error {

	fReader, err := os.OpenFile(upReq.localPath, os.O_RDONLY, os.ModeExclusive)
	if err != nil {
		return err
	}
	defer fReader.Close()

	if upReq.fileSize == 0 {
		panic("The file is empty")
	}
	fileEndOfs := upReq.fileSize - 1
	ofs := int64(0)

	// chunk indexes start at 1 (not zero)
	cIndex := 1
	for ofs <= fileEndOfs {
		chunkEndOfs := MinInt64(ofs + upReq.partSize - 1, fileEndOfs)
		chunkLen := chunkEndOfs - ofs
		buf := make([]byte, chunkLen)
		len, err := fReader.ReadAt(buf, ofs)
		if err != nil {
			return err
		}
		if int64(len) != chunkLen {
			return errors.New(fmt.Sprintf("short read, got %d bytes instead of %d",
				len, chunkLen))
		}
		chunk := Chunk {
			index : cIndex,
			data : buf,
		}
		if fugs.options.Verbose {
			log.Printf("Uploading chunk=%d len=%d", cIndex, chunkLen)
		}
		ctx := context.TODO()
		if err := DxFileUploadPart(ctx, httpClient, &fugs.dxEnv, upReq.id, chunk); err != nil {
			return err
		}
		ofs += upReq.partSize
		cIndex++
	}
	return nil
}

func (fugs *FileUploadGlobalState) createEmptyFile(
	httpClient *retryablehttp.Client,
	upReq UploadReq) {
	// The file is empty
	if upReq.uploadParams.EmptyLastPartAllowed {
		// we need to upload an empty part, only
		// then can we close the file
		chunk := Chunk{
			index: 1,
			data : make([]byte, 0),
		}
		ctx := context.TODO()
		err := DxFileUploadPart(ctx, httpClient, &fugs.dxEnv, upReq.id, chunk)
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

func (fugs *FileUploadGlobalState) uploadIoWorker() {
	// A fixed http client. The idea is to be able to reuse http connections.
	client := dxda.NewHttpClient(true)

	for true {
		upReq, ok := <-fugs.reqQueue
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
			if err := fugs.uploadFileDataSequentially(client, upReq); err != nil {
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
func (fugs *FileUploadGlobalState) UploadFile(fd *os.File, f File, fileSize int64) error {
	if fileSize > 0 {
		// flush and close the local file
		// We leave the local file in place. This allows reading from
		// it, without accessing the network.
		if err := fd.Sync(); err != nil {
			return err
		}
		if err := fd.Close(); err != nil {
			return err
		}
	}

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

	fugs.reqQueue <- UploadReq{
		id : f.Id,
		partSize : partSize,
		uploadParams : projDesc.UploadParams,
		localPath : f.InlineData,
		fileSize : fileSize,
	}
	return nil
}
