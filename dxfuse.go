package dxfuse

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dnanexus/dxda"
	"github.com/hashicorp/go-retryablehttp" // use http libraries from hashicorp for implement retry logic
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"

	// for the sqlite driver
	_ "github.com/mattn/go-sqlite3"
)

func NewDxfuse(
	dxEnv dxda.DXEnvironment,
	manifest Manifest,
	options Options) (*Filesys, error) {
	// Create a fresh SQL database
	dbParentFolder := filepath.Dir(DatabaseFile)
	if _, err := os.Stat(dbParentFolder); os.IsNotExist(err) {
		os.Mkdir(dbParentFolder, 0755)
	}
	log.Printf("Removing old version of the database (%s)", DatabaseFile)
	if err := os.RemoveAll(DatabaseFile); err != nil {
		log.Fatalf("error removing old database %b", err)
	}

	// Create a directory for new files
	os.RemoveAll(CreatedFilesDir)
	if _, err := os.Stat(CreatedFilesDir); os.IsNotExist(err) {
		os.Mkdir(CreatedFilesDir, 0755)
	}

	// initialize a pool of http-clients.
	httpIoPool := make(chan *retryablehttp.Client, HttpClientPoolSize)
	for i:=0; i < HttpClientPoolSize; i++ {
		httpIoPool <- dxda.NewHttpClient(true)
	}
	nonce := NewNonce()

	fsys := &Filesys{
		dxEnv : dxEnv,
		options: options,
		dbFullPath : DatabaseFile,
		mutex : sync.Mutex{},
		httpClientPool : httpIoPool,
		fhTable : make(map[fuseops.HandleID]*FileHandle),
		fhFreeList : make([]fuseops.HandleID, 0),
		dhTable : make(map[fuseops.HandleID]*DirHandle),
		dhFreeList : make([]fuseops.HandleID, 0),
		nonce : nonce,
		tmpFileCounter : 0,
	}

	// create the metadata database
	mdb, err := NewMetadataDb(fsys.dbFullPath, dxEnv, httpIoPool, nonce, options)
	if err != nil {
		return nil, err
	}
	fsys.mdb = mdb
	if err := fsys.mdb.Init(); err != nil {
		return nil, err
	}
	if err := fsys.mdb.PopulateRoot(manifest); err != nil {
		return nil, err
	}

	// initialize prefetching state
	fsys.pgs = NewPrefetchGlobalState(options.VerboseLevel)

	// describe all the projects, we need their upload parameters
	httpClient := <- fsys.httpClientPool
	defer func() {
		fsys.httpClientPool <- httpClient
	} ()

	projId2Desc := make(map[string]DxDescribePrj)
	for _, d := range manifest.Directories {
		pDesc, err := DxDescribeProject(httpClient, &dxEnv, d.ProjId)
		if err != nil {
			log.Printf("Could not describe project %s, check permissions", d.ProjId)
			return nil, err
		}
		projId2Desc[pDesc.Id] = *pDesc
	}


	// initialize background upload state
	fsys.fugs = NewFileUploadGlobalState(options, dxEnv, projId2Desc)

	return fsys, nil
}

func (fsys *Filesys) Destroy() {
	// Close the sql database.
	//
	// If there is an error, we report it. There is nothing actionable
	// to do with it.
	//
	// We do not remove the metadata database file, so it could be inspected offline.
	log.Printf("Destroy: shutting down dxfuse")

	// stop any background operations the metadata database may be running.
	fsys.mdb.Shutdown()

	// stop the running threads in the prefetch module
	fsys.pgs.Shutdown()

	// complete pending uploads
	fsys.fugs.Shutdown()
}


func (fsys *Filesys) StatFS(ctx context.Context, op *fuseops.StatFSOp) error {
	return nil
}

func (fsys *Filesys) calcExpirationTime(a fuseops.InodeAttributes) time.Time {
	if !a.Mode.IsDir() && a.Mode != 0444 {
		// A file created locally. It is probably being written to,
		// so there is no sense in caching for a long time
		if fsys.options.Verbose {
			log.Printf("Setting zero attribute expiration time")
		}

		return time.Now().Add(1 * time.Second)
	}

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	return time.Now().Add(365 * 24 * time.Hour)
}

func (fsys *Filesys) LookUpInode(ctx context.Context, op *fuseops.LookUpInodeOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()

	node, ok, err := fsys.mdb.LookupByInode(int64(op.Parent))
	if err != nil {
		return err
	}
	if !ok {
		// parent directory does not exist
		return fuse.ENOENT
	}
	parentDir := node.(Dir)

	node, ok, err = fsys.mdb.LookupInDir(&parentDir, op.Name)
	if err != nil {
		return err
	}
	if !ok {
		// file does not exist
		return fuse.ENOENT
	}

	// Fill in the response.
	op.Entry.Child = node.GetInode()
	op.Entry.Attributes = node.GetAttrs()

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.Entry.AttributesExpiration = fsys.calcExpirationTime(op.Entry.Attributes)
	op.Entry.EntryExpiration = op.Entry.AttributesExpiration

	return nil
}

func (fsys *Filesys) GetInodeAttributes(ctx context.Context, op *fuseops.GetInodeAttributesOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()

	// Grab the inode.
	node, ok, err := fsys.mdb.LookupByInode(int64(op.Inode))
	if err != nil {
		log.Printf(err.Error())
		return fmt.Errorf("GetInodeAttributes error")
	}
	if !ok {
		return fuse.ENOENT
	}
	if fsys.options.Verbose {
		log.Printf("GetInodeAttributes(inode=%d, %v)", int64(op.Inode), node)
	}

	// Fill in the response.
	op.Attributes = node.GetAttrs()
	op.AttributesExpiration = fsys.calcExpirationTime(op.Attributes)

	return nil
}

// Allocate an unused file handle.
//
// Note: We want to have a guarantied O(1) algorithm, otherwise, we would use a
// randomized approach.
//
func (fsys *Filesys) insertIntoFileHandleTable(fh *FileHandle) fuseops.HandleID {
	numFree := len(fsys.fhFreeList)

	var id fuseops.HandleID
	if numFree > 0 {
		// reuse old file handles
		id = fsys.fhFreeList[numFree - 1]
		fsys.fhFreeList = fsys.fhFreeList[:numFree-1]
	} else {
		// all file handles are in use, choose a new one.
		id = fuseops.HandleID(len(fsys.fhTable))
	}

	fsys.fhTable[id] = fh
	return id
}

func (fsys *Filesys) insertIntoDirHandleTable(dh *DirHandle) fuseops.HandleID {
	numFree := len(fsys.dhFreeList)

	var id fuseops.HandleID
	if numFree > 0 {
		// reuse old file handles
		id = fsys.dhFreeList[numFree - 1]
		fsys.dhFreeList = fsys.dhFreeList[:numFree-1]
	} else {
		// all file handles are in use, choose a new one.
		id = fuseops.HandleID(len(fsys.dhTable))
	}

	fsys.dhTable[id] = dh
	return id
}

// A CreateRequest asks to create and open a file (not a directory).
//
func (fsys *Filesys) CreateFile(ctx context.Context, op *fuseops.CreateFileOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()

	if fsys.options.Verbose {
		log.Printf("Create(%s)", op.Name)
	}

	// the parent is supposed to be a directory
	node, ok, err := fsys.mdb.LookupByInode(int64(op.Parent))
	if err != nil {
		return err
	}
	if !ok {
		// parent directory does not exist
		return fuse.ENOENT
	}
	parentDir := node.(Dir)

	// Check if the file already exists
	_, ok, err = fsys.mdb.LookupInDir(&parentDir, op.Name)
	if err != nil {
		return err
	}
	if ok {
		// The file already exists
		return fuse.EEXIST
	}

	// we now know that the parent directory exists, and the file
	// does not.

	// Create a temporary file in a protected directory, used only
	// by dxfuse.
	cnt := atomic.AddUint64(&fsys.tmpFileCounter, 1)
	localPath := fmt.Sprintf("%s/%d_%s", CreatedFilesDir, cnt, op.Name)

	file, err := fsys.mdb.CreateFile(&parentDir, op.Name, op.Mode, localPath)
	if err != nil {
		return err
	}

	// Set up attributes for the child.
	now := time.Now()
	childAttrs := fuseops.InodeAttributes{
		Nlink:  1,
		Mode:   op.Mode,
		Atime:  now,
		Mtime:  now,
		Ctime:  now,
		Crtime: now,
		Uid:    fsys.options.Uid,
		Gid:    fsys.options.Gid,
	}

	// We need a short time window, because the file attributes are likely to
	// soon change. We are writing new content into the file.
	tWindow := fsys.calcExpirationTime(childAttrs)
	op.Entry = fuseops.ChildInodeEntry{
		Child : fuseops.InodeID(file.Inode),
		Attributes : childAttrs,
		AttributesExpiration : tWindow,
		EntryExpiration : tWindow,
	}

	writer, err := os.OpenFile(localPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}

	fh := FileHandle{
		fKind : RW_File,
		f : file,
		url : nil,
		localPath : &localPath,
		fd : writer,
	}
	op.Handle = fsys.insertIntoFileHandleTable(&fh)
	return nil
}


func (fsys *Filesys) openRegularFile(ctx context.Context, op *fuseops.OpenFileOp, f File) (*FileHandle, error) {
	// create a download URL for this file
	const secondsInYear int = 60 * 60 * 24 * 365
	payload := fmt.Sprintf("{\"project\": \"%s\", \"duration\": %d}",
		f.ProjId, secondsInYear)

	// used a shared http client
	httpClient := <- fsys.httpClientPool
	body, err := dxda.DxAPI(httpClient, &fsys.dxEnv, fmt.Sprintf("%s/download", f.Id), payload)
	fsys.httpClientPool <- httpClient
	if err != nil {
		return nil, err
	}
	var u DxDownloadURL
	json.Unmarshal(body, &u)

	// Check if a file has a local copy. If so, return the path to the copy.
	var fh *FileHandle
	if f.InlineData != "" {
		// a regular file that has a local copy
		reader, err := os.OpenFile(f.InlineData, os.O_RDONLY, 0644)
		if err != nil {
			log.Printf("Could not open local file %s, err=%s", f.InlineData, err.Error())
			return nil, err
		}
		fh = &FileHandle{
			fKind: RO_LocalCopy,
			f : f,
			url: nil,
			localPath : &f.InlineData,
			fd : reader,
		}
	} else {
		fh = &FileHandle{
			fKind: RO_Remote,
			f : f,
			url: &u,
			localPath : nil,
			fd : nil,
		}
	}

	// Create an entry in the prefetch table, if the file is eligable
	fsys.pgs.CreateStreamEntry(fh)

	return fh, nil
}

func (fsys *Filesys) OpenFile(ctx context.Context, op *fuseops.OpenFileOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()

	// find the file by its inode
	node, ok, err := fsys.mdb.LookupByInode(int64(op.Inode))
	if err != nil {
		return err
	}
	if !ok {
		// file doesn't exist
		return fuse.ENOENT
	}

	var file File
	switch node.(type) {
	case Dir:
		// not allowed to open a directory
		return syscall.EPERM
	case File:
		// cast to a File type
		file = node.(File)
	default:
		panic(fmt.Sprintf("bad type for node %v", node))
	}

	var fh *FileHandle
	switch file.Kind {
	case FK_Regular:
		fh, err = fsys.openRegularFile(ctx, op, file)
		if err != nil {
			return err
		}
	case FK_Symlink:
		// A symbolic link can use the remote URL address
		// directly. There is no need to generate a preauthenticated
		// URL.
		fh = &FileHandle{
			fKind : RO_Remote,
			f : file,
			url : &DxDownloadURL{
				URL : file.InlineData,
				Headers : nil,
			},
			localPath : nil,
			fd : nil,
		}
	default:
		// can't open an applet/workflow/etc.
		return fuse.ENOSYS
	}

	// add to the open-file table, so we can recognize future accesses to
	// the same handle.
	op.Handle = fsys.insertIntoFileHandleTable(fh)
	op.KeepPageCache = true
	op.UseDirectIO = true
	return nil
}


func (fsys *Filesys) ReleaseFileHandle(ctx context.Context, op *fuseops.ReleaseFileHandleOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()

	fh, ok := fsys.fhTable[op.Handle]
	if !ok {
		// File handle doesn't exist
		return nil
	}

	// Clear the state involved with this open file descriptor
	switch fh.fKind {
	case RO_Remote:
		// Read-only file that is accessed remotely
		fsys.pgs.RemoveStreamEntry(fh)
		return nil

	case RW_File:
		// A new file created locally. We need to upload it
		// to the platform.
		if fsys.options.Verbose {
			log.Printf("Close new file(%s)", fh.f.Name)
		}

		// flush and close the local file
		// We leave the local file in place. This allows reading from
		// it, without accessing the network.
		if err := fh.fd.Sync(); err != nil {
			return err
		}
		// make this file read only
		if err := fh.fd.Chmod(fileReadOnlyMode); err != nil {
			return err
		}

		fInfo, err := fh.fd.Stat()
		if err != nil {
			return err
		}
		if err := fh.fd.Close(); err != nil {
			return err
		}
		fh.fd = nil

		// update database entry
		if err := fsys.mdb.UpdateFile(fh.f, fInfo); err != nil {
			return err
		}

		// initiate a background request to upload the file to the cloud
		return fsys.fugs.UploadFile(fh.f, fInfo)

	case RO_LocalCopy:
		// Read-only file with a local copy
		fh.fd.Close()
		return nil

	default:
		panic(fmt.Sprintf("Invalid file kind %d", fh.fKind))
	}

	// release the file handle itself
	delete(fsys.fhTable, op.Handle)
	fsys.fhFreeList = append(fsys.fhFreeList, op.Handle)
	return nil
}


func (fsys *Filesys) readRemoteFile(ctx context.Context, op *fuseops.ReadFileOp, fh *FileHandle) error {
	// This is a regular file
	reqSize := int64(len(op.Dst))
	if fh.f.Size == 0 || reqSize == 0 {
		// The file is empty
		return nil
	}
	if fh.f.Size <= op.Offset {
		// request is beyond the size of the file
		return nil
	}
	endOfs := op.Offset + reqSize - 1

	// make sure we don't go over the file size
	lastByteInFile := fh.f.Size - 1
	endOfs = MinInt64(lastByteInFile, endOfs)

	// See if the data has already been prefetched.
	// This call will wait, if a prefetch IO is in progress.
	prefetchData := fsys.pgs.CacheLookup(fh, op.Offset, endOfs)
	if prefetchData != nil {
		op.BytesRead = copy(op.Dst, prefetchData)
		return nil
	}

	// The data has not been prefetched. Get the data from DNAx with an
	// http request.
	headers := make(map[string]string)

	// Copy the immutable headers
	for key, value := range fh.url.Headers {
		headers[key] = value
	}

	// add an extent in the file that we want to read
	headers["Range"] = fmt.Sprintf("bytes=%d-%d", op.Offset, endOfs)
	if fsys.options.Verbose {
		log.Printf("Read  ofs=%d len=%d endOfs=%d lastByteInFile=%d",
			op.Offset, reqSize, endOfs, lastByteInFile)
	}

	// Take an http client from the pool. Return it when done.
	httpClient := <- fsys.httpClientPool
	body,err := dxda.DxHttpRequest(httpClient, "GET", fh.url.URL, headers, []byte("{}"))
	fsys.httpClientPool <- httpClient
	if err != nil {
		return err
	}

	op.BytesRead = copy(op.Dst, body)
	return nil
}

func (fsys *Filesys) ReadFile(ctx context.Context, op *fuseops.ReadFileOp) error {
	fsys.mutex.Lock()

	// Here, we start from the file handle
	fh,ok := fsys.fhTable[op.Handle]
	if !ok {
		// invalid file handle. It doesn't exist in the table
		fsys.mutex.Unlock()
		return fuse.EINVAL
	}
	fsys.mutex.Unlock()

	// TODO: is there a scenario where two threads will run into a conflict
	// because one is holding the handle, and the other is mutating it?

	switch fh.f.Kind {
	case FK_Regular:
		if fh.fKind == RO_Remote {
			return fsys.readRemoteFile(ctx, op, fh)
		} else {
			// the file has a local copy
			n, err := fh.fd.ReadAt(op.Dst, op.Offset)
			if err == io.ErrUnexpectedEOF || err == io.EOF {
				// we don't report EOF to FUSE, it notices
				// it because the number of bytes read is smaller
				// than requested.
				err = nil
			}
			op.BytesRead = n
			return err
		}

	case FK_Symlink:
		return fsys.readRemoteFile(ctx, op, fh)

	default:
		// can only read files
		return syscall.EPERM
	}
}


func (fsys *Filesys) findWritableFileHandle(handle fuseops.HandleID) (*FileHandle, error) {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()

	// Here, we start from the file handle
	fh,ok := fsys.fhTable[handle]
	if !ok {
		// invalid file handle. It doesn't exist in the table
		return nil, fuse.EINVAL
	}
	if fh.fKind != RW_File {
		// This file isn't open for writing
		return nil, fuse.EINVAL
	}
	if fh.fd == nil {
		panic("file descriptor is empty")
	}
	return fh, nil
}

// Writes to files.
//
// A file is created locally, and writes go to the local location. When
// the file is closed, it becomes read only, and is then uploaded to the cloud.
func (fsys *Filesys) WriteFile(ctx context.Context, op *fuseops.WriteFileOp) error {
	fh,err := fsys.findWritableFileHandle(op.Handle)
	if err != nil {
		return err
	}
	// we are not holding the global lock while we are doing IO
	_, err = fh.fd.WriteAt(op.Data, op.Offset)
	return err
}

func (fsys *Filesys) readEntireDir(ctx context.Context, dir Dir) ([]fuseutil.Dirent, error) {
	// normalize the filename. For example, we can get "//" when reading
	// the root directory (instead of "/").
	if fsys.options.Verbose {
		log.Printf("ReadDirAll dir=(%s)\n", dir.FullPath)
	}

	dxObjs, subdirs, err := fsys.mdb.ReadDirAll(&dir)
	if err != nil {
		return nil, err
	}

	if fsys.options.Verbose {
		log.Printf("%d data objects, %d subdirs", len(dxObjs), len(subdirs))
	}

	var dEntries []fuseutil.Dirent

	// Add entries for Unix files, representing DNAx data objects
	for oname, oDesc := range dxObjs {
		var dType fuseutil.DirentType
		switch oDesc.Kind {
		case FK_Regular:
			dType = fuseutil.DT_File
		case FK_Symlink:
			dType = fuseutil.DT_File
		default:
			// There is no good way to represent these
			// in the filesystem.
			dType = fuseutil.DT_Block
		}
		dirEnt := fuseutil.Dirent{
			Offset: 0,
			Inode : fuseops.InodeID(oDesc.Inode),
			Name : oname,
			Type : dType,
		}
		dEntries = append(dEntries, dirEnt)
	}

	// Add entries for subdirs
	for subDirName, dirDesc := range subdirs {
		dirEnt := fuseutil.Dirent{
			Offset: 0,
			Inode : fuseops.InodeID(dirDesc.Inode),
			Name : subDirName,
			Type : fuseutil.DT_Directory,
		}
		dEntries = append(dEntries, dirEnt)
	}

	// directory entries need to be sorted
	sort.Slice(dEntries, func(i, j int) bool { return dEntries[i].Name < dEntries[j].Name })
	if fsys.options.Verbose {
		log.Printf("dentries=%v", dEntries)
	}

	// fix the offsets. We need to do this -after- sorting, because the sort
	// operation reorders the entries.
	//
	// The offsets are one-based, due to how OpenDir works.
	for i := 0; i < len(dEntries); i++ {
		dEntries[i].Offset = fuseops.DirOffset(i) + 1
	}

	return dEntries, nil
}


// OpenDir return nil error allows open dir
// COMMON for drivers
func (fsys *Filesys) OpenDir(ctx context.Context, op *fuseops.OpenDirOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()

	// the parent is supposed to be a directory
	node, ok, err := fsys.mdb.LookupByInode(int64(op.Inode))
	if err != nil {
		return err
	}
	if !ok {
		return fuse.ENOENT
	}
	dir := node.(Dir)

	// Read the entire directory into memory, and sort
	// the entries properly.
	dentries, err := fsys.readEntireDir(ctx, dir)
	if err != nil {
		return err
	}

	dh := &DirHandle{
		d : dir,
		entries: dentries,
	}
	op.Handle = fsys.insertIntoDirHandleTable(dh)
	return nil
}

// ReadDir lists files into readdirop
func (fsys *Filesys) ReadDir(ctx context.Context, op *fuseops.ReadDirOp) (err error) {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()

	dh, ok := fsys.dhTable[op.Handle]
	if !ok {
		return fuse.EIO
	}

	index := int(op.Offset)

	if index > len(dh.entries) {
		return fuse.EINVAL
	}
	var i int
	op.BytesRead = 0
	for i = index; i < len(dh.entries); i++ {
		n := fuseutil.WriteDirent(op.Dst[op.BytesRead:], dh.entries[i])
		if n == 0 {
			break
		}
		op.BytesRead += n
	}
	if fsys.options.Verbose {
		log.Printf("ReadDir  offset=%d  bytesRead=%d nEntriesReported=%d",
			index, op.BytesRead, i)
	}
	return
}

// ReleaseDirHandle deletes file handle entry
func (fsys *Filesys) ReleaseDirHandle(ctx context.Context, op *fuseops.ReleaseDirHandleOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()

	delete(fsys.dhTable, op.Handle)
	fsys.dhFreeList = append(fsys.dhFreeList, op.Handle)
	return nil
}
