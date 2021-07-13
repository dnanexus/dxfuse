package dxfuse

import (
	"context"
	"sync"

	"github.com/dnanexus/dxda"
)

const (
	// Upload up to 4 parts concurrently
	maxUploadRoutines = 4
)

type UploadRequest struct {
	fh          *FileHandle
	writeBuffer []byte
	fileId      string
	partId      int
}

type FileUploader struct {
	verbose           bool
	uploadQueue       chan UploadRequest
	wg                sync.WaitGroup
	numUploadRoutines int
	// API to dx
	ops *DxOps
}

func NewFileUploader(verboseLevel int, options Options, dxEnv dxda.DXEnvironment) *FileUploader {

	uploader := &FileUploader{
		verbose:           verboseLevel >= 1,
		uploadQueue:       make(chan UploadRequest),
		numUploadRoutines: maxUploadRoutines,
		ops:               NewDxOps(dxEnv, options),
	}

	uploader.wg.Add(maxUploadRoutines)
	for i := 0; i < maxUploadRoutines; i++ {
		go uploader.uploadRoutine()
	}
	return uploader
}

func (uploader *FileUploader) uploadRoutine() {
	// reuse this http client
	httpClient := dxda.NewHttpClient()
	for true {
		uploadReq, ok := <-uploader.uploadQueue
		if !ok {
			uploader.wg.Done()
			return
		}
		uploader.ops.DxFileUploadPart(context.TODO(), httpClient, uploadReq.fileId, uploadReq.partId, uploadReq.writeBuffer)
		uploadReq.fh.wg.Done()
	}
}
