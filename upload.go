package dxfuse

import (
	"context"
	"math"
	"runtime"
	"sync"

	"github.com/dnanexus/dxda"
)

const (
	// Upload up to 4 parts concurrently
	maxUploadRoutines = 4
)

// TODO replace this with a more reasonble buffer pool for managing memory use
func (uploader *FileUploader) AllocateWriteBuffer(partId int, block bool) []byte {
	if partId < 1 {
		partId = 1
	}
	// Wait for available buffer
	if block {
		uploader.writeBufferChan <- struct{}{}
	}
	writeBufferCapacity := math.Min(InitialUploadPartSize*math.Pow(1.1, float64(partId)), MaxUploadPartSize)
	writeBufferCapacity = math.Round(writeBufferCapacity)

	writeBuffer := uploader.memoryManager.AllocateWriteBuffer(int64(writeBufferCapacity))
	if writeBuffer == nil {
		uploader.log("Failed to allocate write buffer")
		return nil
	}
	return writeBuffer
}

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
	// Max concurrent write buffers to reduce memory consumption
	writeBufferChan chan struct{}
	// API to dx
	ops           *DxOps
	memoryManager *MemoryManager
}

// write a log message, and add a header
func (uploader *FileUploader) log(a string, args ...interface{}) {
	LogMsg("uploader", a, args...)
}

func NewFileUploader(verboseLevel int, options Options, dxEnv dxda.DXEnvironment, memoryManager *MemoryManager) *FileUploader {
	concurrentWriteBufferLimit := 15
	if runtime.NumCPU()*3 > concurrentWriteBufferLimit {
		concurrentWriteBufferLimit = runtime.NumCPU() * 3
	}

	uploader := &FileUploader{
		verbose:           verboseLevel >= 1,
		uploadQueue:       make(chan UploadRequest),
		writeBufferChan:   make(chan struct{}, concurrentWriteBufferLimit),
		numUploadRoutines: maxUploadRoutines,
		ops:               NewDxOps(dxEnv, options),
		memoryManager:     memoryManager,
	}

	uploader.wg.Add(maxUploadRoutines)
	for i := 0; i < maxUploadRoutines; i++ {
		go uploader.uploadWorker()
	}
	return uploader
}

func (uploader *FileUploader) Shutdown() {
	// Close channel and wait for goroutines to complete
	close(uploader.uploadQueue)
	close(uploader.writeBufferChan)
	uploader.wg.Wait()
}

func (uploader *FileUploader) uploadWorker() {
	// reuse this http client
	httpClient := dxda.NewHttpClient()
	for true {
		uploadReq, ok := <-uploader.uploadQueue
		if !ok {
			uploader.wg.Done()
			return
		}
		err := uploader.ops.DxFileUploadPart(context.TODO(), httpClient, uploadReq.fileId, uploadReq.partId, uploadReq.writeBuffer)
		// Release the memory back to the pool
		uploader.memoryManager.ReleaseWriteBuffer(int64(cap(uploadReq.writeBuffer)))
		if err != nil {
			// Record upload error in FileHandle
			uploader.log("Error uploading %s, part %d, %s", uploadReq.fileId, uploadReq.partId, err.Error())
			uploadReq.fh.writeError = err
		}
		uploadReq.fh.wg.Done()
	}
}
