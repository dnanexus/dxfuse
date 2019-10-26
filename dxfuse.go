package dxfuse

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dnanexus/dxda"
	"github.com/hashicorp/go-retryablehttp" // use http libraries from hashicorp for implement retry logic
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"

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

	// create a connection to the database, that will be kept open
	db, err := sql.Open("sqlite3", DatabaseFile + "?mode=rwc")
	if err != nil {
		return nil, err
	}

	// initialize a pool of http-clients.
	httpClientPool := make(chan *retryablehttp.Client, HttpClientPoolSize)
	for i:=0; i < HttpClientPoolSize; i++ {
		httpClientPool <- dxda.NewHttpClient(true)
	}

	fsys := &Filesys{
		dxEnv : dxEnv,
		options: options,
		dbFullPath : DatabaseFile,
		mutex : sync.Mutex{},
		inodeCnt : InodeRoot + 2,
		db : db,
		httpClientPool : httpClientPool,
		baseDir2ProjectId : make(map[string]string),
		projId2Desc : make(map[string]DxDescribePrj),
		fhTable : make(map[fuseops.HandleID]*FileHandle),
		fhFreeList : make([]fuseops.HandleID, 0),
		dhTable : make(map[fuseops.HandleID]*DirHandle),
		dhFreeList : make([]fuseops.HandleID, 0),
		nonce : nonceMake(),
		tmpFileCounter : 0,
	}

	// create the metadata database
	if err := fsys.MetadataDbInit(); err != nil {
		return nil, err
	}

	if err := fsys.MetadataDbPopulateRoot(manifest); err != nil {
		return nil, err
	}

	// initialize prefetching state
	fsys.pgs = NewPrefetchGlobalState(options.VerboseLevel)

	// initialize background upload state
	fsys.fugs = NewFileUploadGlobalState(fsys)

	return fsys, nil
}

func (fsys *Filesys) Shutdown() {
	// Close the sql database.
	//
	// If there is an error, we report it. There is nothing actionable
	// to do with it.
	//
	// We do not remove the metadata database file, so it could be inspected offline.
	log.Printf("shutting down dxfuse")

	// stop the running threads in the prefetch module
	fsys.pgs.Shutdown()

	// complete pending uploads
	fsys.fugs.Shutdown()

	if err := fsys.db.Close(); err != nil {
		log.Printf("Error closing the sqlite database %s, err=%s",
			fsys.dbFullPath,
			err.Error())
	}
}


func (fsys *Filesys) StatFS(ctx context.Context, op *fuseops.StatFSOp) error {
	return nil
}

func (fsys *Filesys) lookupFileByInode(inode int64) (*File, error) {
	// find the file by its inode
	node, err := fsys.MetadataDbLookupByInode(inode)
	if err != nil {
		return nil, err
	}
	var file File
	switch (*node).(type) {
	case Dir:
		// can't open a directory
		return nil, fuse.ENOSYS
	case File:
		// cast to a File type
		return (*node).(*File), nil
	default:
		panic(fmt.Sprintf("bad type for node %v", node))
	}
}


func (fsys *Filesys) lookupDirByInode(inode int64) (*Dir, error) {
	// find the file by its inode
	node, err := fsys.MetadataDbLookupByInode(inode)
	if err != nil {
		return nil, err
	}
	switch (*node).(type) {
	case Dir:
		// cast to Dir type
		return (*node).(*Dir), nil
	case File:
		return nil, fuse.ENOSYS
	default:
		panic(fmt.Sprintf("bad type for node %v", node))
	}
}


func (fsys *Filesys) LookUpInode(ctx context.Context, op *fuseops.LookUpInodeOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()

	dir, err := fsys.lookupDirByInode(int64(op.Parent))
	if err != nil {
		return err
	}
	node, err := fsys.MetadataDbLookupInDir(dir.FullPath, op.Name)
	if err != nil {
		return err
	}

	// Fill in the response.
	op.Entry.Child = node.GetInode()
	op.Entry.Attributes = node.Attrs(fsys)

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.Entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
	op.Entry.EntryExpiration = op.Entry.AttributesExpiration

	return nil
}

func (fsys *Filesys) GetInodeAttributes(ctx context.Context, op *fuseops.GetInodeAttributesOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()

	// Grab the inode.
	node, err := fsys.MetadataDbLookupByInode(int64(op.Inode))
	if err != nil {
		return err
	}

	// Fill in the response.
	op.Attributes = (*node).Attrs(fsys)

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)

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
		id := fsys.fhFreeList[numFree - 1]
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
		id := fsys.dhFreeList[numFree - 1]
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
	dir, err := fsys.lookupDirByInode(int64(op.Parent))
	if err != nil {
		return err
	}

	// Create a temporary file in a protected directory, used only
	// by dxfuse.
	cnt := atomic.AddUint64(&dir.Fsys.tmpFileCounter, 1)
	localPath := fmt.Sprintf("%s/%d_%s", CreatedFilesDir, cnt, op.Name)

	file, err := fsys.MetadataDbCreateFile(dir, op.Name, localPath)
	if err != nil {
		// The error here will be EEXIST, if the file already exists
		return err
	}

	// Set up attributes for the child.
	now := time.Now()
	childAttrs := fuseops.InodeAttributes{
		Nlink:  1,
		Mode:   op.mode,
		Atime:  now,
		Mtime:  now,
		Ctime:  now,
		Crtime: now,
		Uid:    fsys.options.Uid,
		Gid:    fsys.options.Gid,
	}

	// We need a short time window, because the file attributes are likely to
	// soon change. We are writing new content into the file.
	shortTimeWin := now.Add(5)
	op.Entry = fuseops.ChildInodeEntry{
		Child : file.Inode,
		Attributes : childAttrs,
		AttributesExpiration : shortTimeWin,
		EntryExpiration : shortTimeWin,
	}

	writer, err := os.OpenFile(localPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}

	fh := FileHandle{
		fKind : RW_File,
		f : *file,
		url : nil,
		localPath : &localPath,
		fd : writer,
	}
	op.Handle = fsys.insertIntoFileHandleTable(&fh)
	return nil
}


func (fsys *Filesys) openRegularFile(ctx context.Context, op *fuseops.OpenFileOp, f *File) (*FileHandle, error) {
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
			f : *f,
			url: nil,
			localPath : &f.InlineData,
			fd : reader,
		}
	} else {
		fh = &FileHandle{
			fKind: RO_Remote,
			f : *f,
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

	// existing files are read only. The only way
	// to open a file for writing, is to create a new one.
	if !req.Flags.IsReadOnly() {
		return fuse.ENOSYS
	}

	// find the file by its inode
	file, err := fsys.lookupFileByInode(op.Inode)
	if err != nil {
		return err
	}

	var fh *FileHandle
	switch file.Kind {
	case FK_Regular:
		fh, err := fsys.openRegularFile(ctx, op, file)
		if err != nil {
			return err
		}
	case FK_Symlink:
		// A symbolic link can use the remote URL address
		// directly. There is no need to generate a preauthenticated
		// URL.
		fh = &FileHandle{
			fKind : RO_Remote,
			f : *file,
			url : &DxDownloadURL{
				URL : f.InlineData,
				Headers : nil,
			},
			localPath : nil,
			fd : nil,
		}
	default:
		// can't open an applet/workflow/etc.
		return nil, fuse.ENOSYS
	}

	// add to the open-file table, so we can recognize future accesses to
	// the same handle.
	op.HandleID = fsys.insertIntoFileHandleTable(fh)
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
		fInfo, err := fh.fd.Stat()
		if err != nil {
			return err
		}
		// update database entry
		if err := fsys.MetadataDbUpdateFile(fh.f, fInfo); err != nil {
			return err
		}

		// initiate a background request to upload the file to the cloud
		return fsys.fugs.UploadFile(*fh, fInfo)

	case RO_LocalCopy:
		// Read-only file with a local copy
		fh.fd.Close()
		return nil

	default:
		panic(fmt.Sprintf("Invalid file kind %d", fh.fKind))
	}

	// release the file handle itself
	fsys.fhTable.Delete(op.Handle)
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
	endOfs := op.Offset + reqSize - 1

	// make sure we don't go over the file size
	lastByteInFile := fh.f.Size - 1
	endOfs = MinInt64(lastByteInFile, endOfs)

	// See if the data has already been prefetched.
	// This call will wait, if a prefetch IO is in progress.
	prefetchData := fsys.pgs.CacheLookup(fh, op.Offset, endOfs)
	if prefetchData != nil {
		op.BytesRead = copy(op.Data, prefetchData)
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
	headers["Range"] = fmt.Sprintf("bytes=%d-%d", req.Offset, endOfs)
	if fh.f.Fsys.options.Verbose {
		log.Printf("Read  ofs=%d  len=%d\n", req.Offset, req.Size)
	}

	// Take an http client from the pool. Return it when done.
	httpClient := <- fsys.httpClientPool
	body,err := dxda.DxHttpRequest(httpClient, "GET", fh.url.URL, headers, []byte("{}"))
	fsys.httpClientPool <- httpClient
	if err != nil {
		return err
	}

	op.BytesRead = copy(op.Data, body)
	return nil
}

func (fsys *Filesys) ReadFile(ctx context.Context, op *fuseops.ReadFileOp) error {
	// TODO: think about reducing the locking scope. We don't want
	// to lock the entire filesystem when performing one read-IO.
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()

	// Here, we start from the file handle
	fh,ok := fsys.fhTable[op.Handle]
	if !ok {
		// invalid file handle. It doesn't exist in the table
		return fuse.EINVAL
	}

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
		return fh.readRemoteFile(fh, ctx, op)

	default:
		// This isn't a regular file. It is an applet/workflow/...
		msg := fmt.Sprintf("A dnanexus %s", fh.f.Id)
		resp.Data = []byte(msg)
		return nil
	}
}


func (fsys *Filesys) findWritableFileHandle(handle fuseops.Handle) (*FileHandle, error) {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()

	// Here, we start from the file handle
	fh,ok := fsys.fhTable[handle]
	if !ok {
		// invalid file handle. It doesn't exist in the table
		return fuse.EINVAL
	}
	if fh.fKind != RW_FILE {
		// This file isn't open for writing
		return fuse.EINVAL
	}
	if fh.fd == nil {
		panic("file descriptor is empty")
	}
	return fh
}

// Writes to files.
//
// A file is created locally, and writes go to the local location. When
// the file is closed, it becomes read only, and is then uploaded to the cloud.
func (fsys *Filesys) WriteFile(ctx context.Context, op *fuseops.WriteFileOp) error {
	fh,err := findWritableFileHandle(op.Handle)
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
	fullPath := filepath.Clean(dir.FullPath)
	if dir.Fsys.options.Verbose {
		log.Printf("ReadDirAll dir=(%s)\n", dir.fullPath)
	}

	dxObjs, subdirs, err := fsys.MetadataDbReadDirAll(fullPath)
	if err != nil {
		return nil, err
	}

	if dir.Fsys.options.Verbose {
		log.Printf("%d data objects, %d subdirs", len(dxObjs), len(subdirs))
	}

	var dEntries []fuse.Dirent
	index := 0

	// Add entries for Unix files, representing DNAx data objects
	for oname, oDesc := range dxObjs {
		var dType fuse.DirentType
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
			Offset: fuseutil.DirOffset(index),
			Inode : fuseops.InodeID(oDesc.Inode),
			Name : oname,
			Type : dType,
		}
		dEntries = append(dEntries, dirEnt)
		index++
	}

	// Add entries for subdirs
	for subDirName, dirDesc := range subdirs {
		dirEnt := fuseutil.Dirent{
			Offset : fuseutil.DirOffset(index),
			Inode : fuseops.InodeID(dirDesc.Inode),
			Name : subDirName,
			Type : fuseutil.DT_Directory,
		}
		dEntries = append(dEntries, dirEnt)
		index++
	}

	// directory entries need to be sorted
	sort.Slice(dEntries, func(i, j int) bool { return dEntries[i].Name < dEntries[j].Name })
	if dir.Fsys.options.Verbose {
		log.Printf("dentries=%v", dEntries)
	}

	return dEntries, nil
}


// OpenDir return nil error allows open dir
// COMMON for drivers
func (fsys *Filesys) OpenDir(ctx context.Context, op *fuseops.OpenDirOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()

	// the parent is supposed to be a directory
	dir, err := fsys.lookupDirByInode(op.Parent)
	if err != nil {
		return err
	}

	// Read the entire directory into memory, and sort
	// the entries properly.
	dentries, err := fsys.readEntireDir(context, dir)
	if err != nil {
		return err
	}

	dh := &DirHandle{
		d : dir,
		entries: dentries,
	}
	op.Handle = fsys.insertIntoDirHandleTable(dh)
	return
}

// ReadDir lists files into readdirop
func (fs *BaseFS) ReadDir(ctx context.Context, op *fuseops.ReadDirOp) (err error) {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()

	dh, ok := fys.dhTable[op.Handle]
	if !ok {
		return fuse.EIO
	}

	index := int(op.Offset)
	if index >= len(dh.entries) {
		return fuse.EINVAL
	}
	for i := index; i < len(dh.entries); i++ {
		n := fuseutil.WriteDirent(op.Dst[op.BytesRead:], dh.entries[i])
		if n == 0 {
			break
		}
		op.BytesRead += n
	}
	return
}

// ReleaseDirHandle deletes file handle entry
func (fsys *Filesys) ReleaseDirHandle(ctx context.Context, op *fuseops.ReleaseDirHandleOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()

	fsys.dhTable.Delete(op.Handle)
	fsys.dhFreeList = append(fsys.dhFreeList, op.Handle)
	return
}
