package dxfuse

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/dnanexus/dxda"
	"github.com/hashicorp/go-retryablehttp"
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

func DxFileNew(
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
	repJs, err := dxda.DxAPI(httpClient, dxEnv, "file/new", string(payload))
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

func DxFileClose(
	httpClient *retryablehttp.Client,
	dxEnv *dxda.DXEnvironment,
	fid string) error {

	_, err := dxda.DxAPI(httpClient,
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
	replyJs, err := dxda.DxAPI(httpClient,
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

	_, err = dxda.DxHttpRequest(httpClient,"PUT", reply.Url, reply.Headers, chunk.data)
	return err
}

type UploadReq struct {
	id           string
	partSize     int64
	uploadParams FileUploadParameters
	localPath    string
	fInfo        os.FileInfo
}

type FileUploadGlobalState struct {
	fsys      *Filesys
	reqQueue   chan UploadReq
}

const (
	numUploadThreads = 10
	minChunkSize = 16 * MiB
)

func (fugs *FileUploadGlobalState) Init(fsys *Filesys) {
	fugs.fsys = fsys
	fugs.reqQueue = make(chan UploadReq)

	// limit the number of prefetch IOs
	for i := 0; i < numUploadThreads; i++ {
		go fugs.uploadIoWorker()
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

func (fugs *FileUploadGlobalState) uploadFileDataSequentially(
	upReq UploadReq,
	httpClient *retryablehttp.Client) error {

	fReader, err := os.OpenFile(upReq.localPath, os.O_RDONLY, os.ModeExclusive)
	if err != nil {
		return err
	}
	defer fReader.Close()

	fileSize := upReq.fInfo.Size()
	ofs := int64(0)

	// chunk indexes start at 1 (not zero)
	cIndex := 1
	for ofs < fileSize {
		chunkLen := MinInt64(ofs + upReq.partSize , fileSize)
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
		if err := DxFileUploadPart(httpClient, &fugs.fsys.dxEnv, upReq.id, chunk); err != nil {
			return err
		}
		ofs += upReq.partSize
		cIndex++
	}
	return nil
}

func (fugs *FileUploadGlobalState) uploadIoWorker() {
	// A fixed http client. The idea is to be able to reuse http connections.
	client := dxda.NewHttpClient(true)

	for true {
		upReq := <-fugs.reqQueue
		fileSize := upReq.fInfo.Size()

		if fugs.fsys.options.Verbose {
			log.Printf("Upload file-size=%d part-size=%d", fileSize, upReq.partSize)
		}

		if fileSize == 0 {
			// The file is empty
			if upReq.uploadParams.EmptyLastPartAllowed {
				// we need to upload an empty part, only
				// then can we close the file
				chunk := Chunk{
					index: 1,
					data : make([]byte, 0),
				}
				err := DxFileUploadPart(client, &fugs.fsys.dxEnv, upReq.id, chunk)
				if err != nil {
					log.Printf("error uploading empty chunk to file %s, error = %s",
						upReq.id, err.Error())
					continue
				}
			} else {
				// no need to upload any parts, we
				// can just close the file
			}
		} else {
			// loop over the parts, and upload them
			if err := fugs.uploadFileDataSequentially(upReq, client); err != nil {
				log.Printf("upload error to file %s, error = %s", upReq.id, err.Error())
				continue
			}
		}

		if fugs.fsys.options.Verbose {
			log.Printf("Closing %s", upReq.id)
		}
		err := DxFileClose(client, &fugs.fsys.dxEnv, upReq.id)
		if err != nil {
			log.Printf("failed to close file %s, error = %s", upReq.id, err.Error())
		}
	}
}
