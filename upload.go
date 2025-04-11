package dxfuse

import (
	"context"
	"math"
	"runtime"
	"sync"

	"github.com/dnanexus/dxda"
)

func (uploader *FileUploader) AllocateWriteBuffer(partId int, block bool) []byte {
	if partId < 1 {
		partId = 1
	}
	// This is a blocking call, so it will wait until a write buffer is available
	// according to the number of concurrent write buffers
	if block {
		uploader.writeBufferChan <- struct{}{}
	}
	writeBufferCapacity := InitialUploadPartSize
	// If the partId is greater than 1, we need to increase the size of the write buffer
	// to avoid the overhead of multiple small writes
	if partId > 1 {
		computedCapacity := math.Min(InitialUploadPartSize*math.Pow(1.1, float64(partId)), MaxUploadPartSize)
		writeBufferCapacity = int(math.Round(computedCapacity))
	}

	// Ensure the write buffer size does not exceed MemoryManager.maxMemoryUsagePerModule
	if int64(writeBufferCapacity) > uploader.memoryManager.maxMemoryUsagePerModule {
		writeBufferCapacity = int(uploader.memoryManager.maxMemoryUsagePerModule)
	}

	// This is a blocking call, so it will wait until memory for a write buffer is available
	// according to shared read/write memory buffer limits
	uploader.log("Allocating %.2f MiB for write buffer", float64(writeBufferCapacity)/1024/1024)
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
	verbose     bool
	uploadQueue chan UploadRequest
	wg          sync.WaitGroup
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
		verbose:         verboseLevel >= 1,
		uploadQueue:     make(chan UploadRequest),
		writeBufferChan: make(chan struct{}, concurrentWriteBufferLimit),
		ops:             NewDxOps(dxEnv, options),
		memoryManager:   memoryManager,
	}

	uploader.wg.Add(concurrentWriteBufferLimit)
	for i := 0; i < concurrentWriteBufferLimit; i++ {
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
	for {
		uploadReq, ok := <-uploader.uploadQueue
		if !ok {
			uploader.wg.Done()
			return
		}
		uploader.log("Uploading %s, part %d, size %.2f MiB", uploadReq.fileId, uploadReq.partId, float64(len(uploadReq.writeBuffer))/1024/1024)
		// Upload the part
		err := uploader.ops.DxFileUploadPart(context.TODO(), httpClient, uploadReq.fileId, uploadReq.partId, uploadReq.writeBuffer)
		// Release the memory back to the pool
		uploader.memoryManager.ReleaseWriteBuffer(uploadReq.writeBuffer)
		if err != nil {
			// Record upload error in FileHandle
			uploader.log("Error uploading %s, part %d, %s", uploadReq.fileId, uploadReq.partId, err.Error())
			uploadReq.fh.writeError = err
		}
		uploadReq.fh.wg.Done()
	}
}
