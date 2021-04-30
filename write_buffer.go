package dxfuse

import "github.com/jacobsa/fuse/fuseops"

type UploadBuffer struct {
	hid   fuseops.HandleID
	inode int64
	size  int64
	url   DxDownloadURL

	ioSize    int64 // The io size
	startByte int64 // start byte, counting from the beginning of the file.
	endByte   int64

	id uint64 // a unique id for this IO
}
