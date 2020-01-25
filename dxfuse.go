package dxfuse

import (
	"bytes"
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

	// initialize a pool of http-clients.
	httpIoPool := make(chan *retryablehttp.Client, HttpClientPoolSize)
	for i:=0; i < HttpClientPoolSize; i++ {
		httpIoPool <- dxda.NewHttpClient(true)
	}
	fsys := &Filesys{
		dxEnv : dxEnv,
		options: options,
		dbFullPath : DatabaseFile,
		mutex : sync.Mutex{},
		httpClientPool: httpIoPool,
		ops : NewDxOps(dxEnv, options),
		fhCounter : 1,
		fhTable : make(map[fuseops.HandleID]*FileHandle),
		dhCounter : 1,
		dhTable : make(map[fuseops.HandleID]*DirHandle),
		nonce : NewNonce(),
		tmpFileCounter : 0,
		shutdownCalled : false,
	}

	// Create a fresh SQL database
	dbParentFolder := filepath.Dir(DatabaseFile)
	if _, err := os.Stat(dbParentFolder); os.IsNotExist(err) {
		os.Mkdir(dbParentFolder, 0755)
	}
	fsys.log("Removing old version of the database (%s)", DatabaseFile)
	if err := os.RemoveAll(DatabaseFile); err != nil {
		fsys.log("error removing old database %b", err)
		os.Exit(1)
	}

	// Create a directory for new files
	os.RemoveAll(CreatedFilesDir)
	if _, err := os.Stat(CreatedFilesDir); os.IsNotExist(err) {
		os.Mkdir(CreatedFilesDir, 0755)
	}

	// create the metadata database
	mdb, err := NewMetadataDb(fsys.dbFullPath, dxEnv, options)
	if err != nil {
		return nil, err
	}
	fsys.mdb = mdb
	if err := fsys.mdb.Init(); err != nil {
		return nil, err
	}

	oph := fsys.OpOpen()
	if err := fsys.mdb.PopulateRoot(context.TODO(), oph, manifest); err != nil {
		fsys.OpClose(oph)
		return nil, err
	}
	fsys.OpClose(oph)

	fsys.pgs = NewPrefetchGlobalState(options.VerboseLevel, dxEnv)

	// describe all the projects, we need their upload parameters
	httpClient := <- fsys.httpClientPool
	defer func() {
		fsys.httpClientPool <- httpClient
	} ()

	if options.ReadOnly {
		// we don't need the file upload module
		return fsys, nil
	}

	projId2Desc := make(map[string]DxDescribePrj)
	for _, d := range manifest.Directories {
		pDesc, err := DxDescribeProject(context.TODO(), httpClient, &fsys.dxEnv, d.ProjId)
		if err != nil {
			fsys.log("Could not describe project %s, check permissions", d.ProjId)
			return nil, err
		}
		projId2Desc[pDesc.Id] = *pDesc
	}

	// initialize background upload state
	fsys.fugs = NewFileUploadGlobalState(options, dxEnv, projId2Desc)

	// Provide the upload module with a reference to the database.
	// This is needed to report the end of an upload.
	fsys.fugs.mdb = mdb
	return fsys, nil
}

// write a log message, and add a header
func (fsys *Filesys) log(a string, args ...interface{}) {
	LogMsg("dxfuse", a, args...)
}

func (fsys *Filesys) OpOpen() *OpHandle {
	txn, err := fsys.mdb.BeginTxn()
	if err != nil {
		log.Panic("Could not open transaction")
	}
	httpClient := <- fsys.httpClientPool

	return &OpHandle{
		httpClient : httpClient,
		txn : txn,
		err : nil,
	}
}

func (fsys *Filesys) OpClose(oph *OpHandle) {
	fsys.httpClientPool <- oph.httpClient

	if oph.err == nil {
		err := oph.txn.Commit()
		if err != nil {
			log.Panic("could not commit transaction")
		}
	} else {
		err := oph.txn.Rollback()
		if err != nil {
			log.Panic("could not rollback transaction")
		}
	}
}

func (fsys *Filesys) Shutdown() {
	if fsys.shutdownCalled {
		// shutdown has already been called.
		// We are not waiting for anything, and just
		// unmounting the filesystem here.
		fsys.log("Shutdown called a second time, skipping the normal sequence")
		return
	}
	fsys.shutdownCalled = true

	// Close the sql database.
	//
	// If there is an error, we report it. There is nothing actionable
	// to do with it.
	//
	// We do not remove the metadata database file, so it could be inspected offline.
	fsys.log("Shutting down dxfuse")

	// stop any background operations the metadata database may be running.
	fsys.mdb.Shutdown()

	// stop the running threads in the prefetch module
	fsys.pgs.Shutdown()

	// complete pending uploads
	if !fsys.options.ReadOnly {
		fsys.fugs.Shutdown()
	}
}

func (fsys *Filesys) dxErrorToFilesystemError(dxErr dxda.DxError) error {
	switch dxErr.EType {
	case "InvalidInput":
		return fuse.EINVAL
	case "PermissionDenied":
		return syscall.EPERM
	case "InvalidType":
		return fuse.EINVAL
	case "ResourceNotFound":
		return fuse.ENOENT
	case "Unauthorized":
		return syscall.EPERM
	default:
		fsys.log("unexpected dnanexus error type (%s), returning EIO which will unmount the filesystem",
			dxErr.EType)
		return fuse.EIO
	}
}

func (fsys *Filesys) translateError(err error) error {
	switch err.(type) {
	case *dxda.DxError:
		// A dnanexus error
		dxErr := err.(*dxda.DxError)
		return fsys.dxErrorToFilesystemError(*dxErr)
	default:
		// A "regular" error
		fsys.log("A regular error from an API call %s, converting to EIO", err.Error())
		return fuse.EIO
	}
}

func (fsys *Filesys) StatFS(ctx context.Context, op *fuseops.StatFSOp) error {
	return nil
}

func (fsys *Filesys) calcExpirationTime(a fuseops.InodeAttributes) time.Time {
	if !a.Mode.IsDir() && a.Mode != 0444 {
		// A file created locally. It is probably being written to,
		// so there is no sense in caching for a long time
		if fsys.options.Verbose {
			fsys.log("Setting small attribute expiration time")
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
	oph := fsys.OpOpen()
	defer fsys.OpClose(oph)

	parentDir, ok, err := fsys.mdb.LookupDirByInode(ctx, oph, int64(op.Parent))
	if err != nil {
		fsys.log("database error in LookupInode: %s", err.Error())
		return fuse.EIO
	}
	if !ok {
		// parent directory does not exist
		return fuse.ENOENT
	}

	node, ok, err := fsys.mdb.LookupInDir(ctx, oph, &parentDir, op.Name)
	if err != nil {
		fsys.log("database error in LookUpInode: %s", err.Error())
		return fuse.EIO
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
	oph := fsys.OpOpen()
	defer fsys.OpClose(oph)

	// Grab the inode.
	node, ok, err := fsys.mdb.LookupByInode(ctx, oph, int64(op.Inode))
	if err != nil {
		fsys.log("database error in GetInodeAttributes: %s", err.Error())
		return fuse.EIO
	}
	if !ok {
		return fuse.ENOENT
	}
	if fsys.options.Verbose {
		fsys.log("GetInodeAttributes(inode=%d, %v)", int64(op.Inode), node)
	}

	// Fill in the response.
	op.Attributes = node.GetAttrs()
	op.AttributesExpiration = fsys.calcExpirationTime(op.Attributes)

	return nil
}

// if the file is writable, we can modify some of the attributes.
// otherwise, this is a permission error.
func (fsys *Filesys) SetInodeAttributes(ctx context.Context, op *fuseops.SetInodeAttributesOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()
	oph := fsys.OpOpen()
	defer fsys.OpClose(oph)

	// Grab the inode.
	node, ok, err := fsys.mdb.LookupByInode(ctx, oph, int64(op.Inode))
	if err != nil {
		fsys.log("SetInodeAttributes: database error %s", err.Error())
		return fuse.EIO
	}
	if !ok {
		return fuse.ENOENT
	}
	if fsys.options.Verbose {
		fsys.log("SetInodeAttributes(inode=%d, %v)", int64(op.Inode), node)
	}

	var file File
	switch node.(type) {
	case File:
		file = node.(File)
	case Dir:
		// can't modify directory attributes
		return syscall.EPERM
	}

	// we know it is a file.
	// check if this is a read-only file.
	attrs := file.GetAttrs()
	if attrs.Mode == fileReadOnlyMode {
		return syscall.EPERM
	}

	// update the file
	oldSize := attrs.Size
	if op.Size != nil {
		attrs.Size = *op.Size
	}
	if op.Mode != nil {
		attrs.Mode = *op.Mode
	}
	if op.Mtime != nil {
		attrs.Mtime = *op.Mtime
	}
	// we don't handle atime
	if err := fsys.mdb.UpdateFile(ctx, oph, file, int64(attrs.Size), attrs.Mtime, attrs.Mode); err != nil {
		fsys.log("database error in OpenFile %s", err.Error())
		return fuse.EIO
	}

	if op.Size != nil && *op.Size != oldSize {
		// The size changed, truncate the file
		localPath := file.InlineData
		if err := os.Truncate(localPath, int64(*op.Size)); err != nil {
			fsys.log("Error truncating inode=%d from %d to %d",
				op.Inode, oldSize, op.Size)
			return err
		}
	}

	// Fill in the response.
	op.Attributes = attrs
	op.AttributesExpiration = fsys.calcExpirationTime(attrs)

	return nil
}

// make a pass through the open handles, and
// release handles that reference this inode.
func (fsys *Filesys) removeFileHandlesWithInode(inode int64) {
	handles := make([]fuseops.HandleID, 0)
	for hid, fh := range fsys.fhTable {
		if fh.f.Inode == inode {
			handles = append(handles, hid)
		}
	}

	for _, hid := range handles {
		delete(fsys.fhTable, hid)
	}
}

func (fsys *Filesys) removeDirHandlesWithInode(inode int64) {
	handles := make([]fuseops.HandleID, 0)
	for did, dh := range fsys.dhTable {
		if dh.d.Inode == inode {
			handles = append(handles, did)
		}
	}

	for _, did := range handles {
		delete(fsys.dhTable, did)
	}
}

// This may be the wrong way to do it. We may need to actually delete the inode at this point,
// instead of inside RmDir/Unlink.
func (fsys *Filesys) ForgetInode(ctx context.Context, op *fuseops.ForgetInodeOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()

	if fsys.options.Verbose {
		fsys.log("ForgetInode (%d)", op.Inode)
	}

	fsys.removeFileHandlesWithInode(int64(op.Inode))
	fsys.removeDirHandlesWithInode(int64(op.Inode))

	return nil
}

func (fsys *Filesys) MkDir(ctx context.Context, op *fuseops.MkDirOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()
	oph := fsys.OpOpen()
	defer fsys.OpClose(oph)

	if fsys.options.Verbose {
		fsys.log("CreateDir(%s)", op.Name)
	}

	// the parent is supposed to be a directory
	parentDir, ok, err := fsys.mdb.LookupDirByInode(ctx, oph, int64(op.Parent))
	if err != nil {
		fsys.log("database error in MkDir")
		return fuse.EIO
	}
	if !ok {
		// parent directory does not exist
		return fuse.ENOENT
	}

	// Check if the directory exists
	_, ok, err = fsys.mdb.LookupInDir(ctx, oph, &parentDir, op.Name)
	if err != nil {
		fsys.log("database error in MkDir")
		return fuse.EIO
	}
	if ok {
		// The directory already exists
		return fuse.EEXIST
	}

	// The mode must be 777 for fuse to work properly
	// We -ignore- the mode set by the user.
	mode := dirReadWriteMode

	// create the directory on dnanexus
	folderFullPath := parentDir.ProjFolder + "/" + op.Name
	err = fsys.ops.DxFolderNew(ctx, oph.httpClient, parentDir.ProjId, folderFullPath)
	if err != nil {
		fsys.log("Error in creating directory (%s:%s) on dnanexus: %s",
			parentDir.ProjId, folderFullPath, err.Error())
		oph.RecordError(err)
		return fsys.translateError(err)
	}

	// Add the directory to the database
	now := time.Now()
	nowSeconds := now.Unix()
	dnode, err := fsys.mdb.CreateDir(
		oph,
		parentDir.ProjId,
		folderFullPath,
		nowSeconds,
		nowSeconds,
		mode,
		parentDir.FullPath + "/" + op.Name)
	if err != nil {
		fsys.log("database error in MkDir")
		return fuse.EIO
	}

	// Fill in the response, the details for the new subdirectory
	childAttrs := fuseops.InodeAttributes{
		Nlink:  1,
		Mode:   mode,
		Atime:  now,
		Mtime:  now,
		Ctime:  now,
		Crtime: now,
		Uid:    fsys.options.Uid,
		Gid:    fsys.options.Gid,
	}
	tWindow := fsys.calcExpirationTime(childAttrs)
	op.Entry = fuseops.ChildInodeEntry{
		Child : fuseops.InodeID(dnode),
		Attributes : childAttrs,
		AttributesExpiration : tWindow,
		EntryExpiration : tWindow,
	}
	return nil
}


func (fsys *Filesys) RmDir(ctx context.Context, op *fuseops.RmDirOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()
	oph := fsys.OpOpen()
	defer fsys.OpClose(oph)

	if fsys.options.Verbose {
		fsys.log("Remove Dir(%s)", op.Name)
	}

	// the parent is supposed to be a directory
	parentDir, ok, err := fsys.mdb.LookupDirByInode(ctx, oph, int64(op.Parent))
	if err != nil {
		fsys.log("database error in RmDir")
		return fuse.EIO
	}
	if !ok {
		// parent directory does not exist
		return fuse.ENOENT
	}

	// Check if the directory exists
	childNode, ok, err := fsys.mdb.LookupInDir(ctx, oph, &parentDir, op.Name)
	if err != nil {
		fsys.log("database error in RmDir")
		return fuse.EIO
	}
	if !ok {
		// The directory does not exist
		return fuse.ENOENT
	}

	var childDir Dir
	switch childNode.(type) {
	case File:
		return fuse.ENOTDIR
	case Dir:
		childDir = childNode.(Dir)
	}

	// check that the directory is empty
	dentries, err := fsys.readEntireDir(ctx, oph, childDir)
	if err != nil {
		return err
	}
	if len(dentries) > 0 {
		return fuse.ENOTEMPTY
	}
	if !childDir.faux {
		// The directory exists and is empty, we can remove it.
		folderFullPath := parentDir.ProjFolder + "/" + op.Name
		err = fsys.ops.DxFolderRemove(ctx, oph.httpClient, parentDir.ProjId, folderFullPath)
		if err != nil {
			fsys.log("Error in removing directory (%s:%s) on dnanexus: %s",
				parentDir.ProjId, folderFullPath, err.Error())
			oph.RecordError(err)
			return fsys.translateError(err)
		}
	} else {
		// A faux directory doesn't have a matching project folder.
		// It exists only on the local machine.
	}

	// Remove the directory from the database
	if err := fsys.mdb.RemoveEmptyDir(oph, childDir.Inode); err != nil {
		return err
	}
	return nil
}

// Allocate an unused file handle.
//
// Note: We want to have a guarantied O(1) algorithm, otherwise, we would use a
// randomized approach.
//
func (fsys *Filesys) insertIntoFileHandleTable(fh *FileHandle) fuseops.HandleID {
	fsys.fhCounter++
	hid := fuseops.HandleID(fsys.fhCounter)

	fsys.fhTable[hid] = fh
	fh.hid = hid
	return hid
}

func (fsys *Filesys) insertIntoDirHandleTable(dh *DirHandle) fuseops.HandleID {
	fsys.dhCounter++
	did := fuseops.HandleID(fsys.dhCounter)
	fsys.dhTable[did] = dh
	return did
}

// A CreateRequest asks to create and open a file (not a directory).
//
func (fsys *Filesys) CreateFile(ctx context.Context, op *fuseops.CreateFileOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()
	oph := fsys.OpOpen()
	defer fsys.OpClose(oph)

	if fsys.options.Verbose {
		fsys.log("CreateFile(%s)", op.Name)
	}

	// the parent is supposed to be a directory
	parentDir, ok, err := fsys.mdb.LookupDirByInode(ctx, oph, int64(op.Parent))
	if err != nil {
		return err
	}
	if !ok {
		// parent directory does not exist
		return fuse.ENOENT
	}
	if parentDir.faux {
		// cannot write new files into faux directories
		return syscall.EPERM
	}

	// Check if the file already exists
	_, ok, err = fsys.mdb.LookupInDir(ctx, oph, &parentDir, op.Name)
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

	// create the file object on the platform.
	fileId, err := fsys.ops.DxFileNew(
		ctx, oph.httpClient, fsys.nonce.String(),
		parentDir.ProjId, op.Name, parentDir.ProjFolder)
	if err != nil {
		fsys.log("Error in creating file (%s:%s/%s) on dnanexus: %s",
			parentDir.ProjId, parentDir.ProjFolder, op.Name,
			err.Error())
		oph.RecordError(err)
		return fsys.translateError(err)
	}

	file, err := fsys.mdb.CreateFile(ctx, oph, &parentDir, fileId, op.Name, op.Mode, localPath)
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

	// Note: we can't open the file in exclusive mode, because another process
	// may read it before it is closed.
	writer, err := os.OpenFile(localPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		oph.RecordError(err)
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

func (fsys *Filesys) CreateLink(ctx context.Context, op *fuseops.CreateLinkOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()
	oph := fsys.OpOpen()
	defer fsys.OpClose(oph)

	if fsys.options.Verbose {
		fsys.log("CreateLink (inode=%d) -> (parent-inode=%d name=%s)",
			op.Target, op.Parent, op.Name)
	}

	// parent is supposed to be a directory
	parentDir, ok, err := fsys.mdb.LookupDirByInode(ctx, oph, int64(op.Parent))
	if err != nil {
		return err
	}
	if !ok {
		// parent directory does not exist
		return fuse.ENOENT
	}

	// Make sure the destination doesn't already exist
	_, ok, err = fsys.mdb.LookupInDir(ctx, oph, &parentDir, op.Name)
	if err != nil {
		return err
	}
	if ok {
		// The link file already exists
		return fuse.EEXIST
	}

	// make sure that target node exists
	targetNode, ok, err := fsys.mdb.LookupByInode(ctx, oph, int64(op.Target))
	if err != nil {
		return err
	}
	if !ok {
		return fuse.ENOENT
	}

	var targetFile File
	switch targetNode.(type) {
	case Dir:
		// can't make a hard link to a directory
		return fuse.EINVAL
	case File:
		targetFile = targetNode.(File)
	}

	if targetFile.Name != op.Name {
		fsys.log("cloning is only allowed if the destination and source names are the same")
		return fuse.EINVAL
	}

	if fsys.options.Verbose {
		fsys.log("CreateLink %s/%s -> %s",
			parentDir.FullPath, op.Name, targetFile.Name)
	}

	// create a link on the platform. This is done with the clone call.
	ok, err = fsys.ops.DxClone(
		ctx, oph.httpClient,
		targetFile.ProjId,    // source project
		targetFile.Id,        // source id
		parentDir.ProjId,     // destination project id
		parentDir.ProjFolder)  // destination folder
	if err != nil {
		fsys.log("dx clone error %s", err.Error())
		oph.RecordError(err)
		return fsys.translateError(err)
	}
	if !ok {
		fsys.log("(%s) object not cloned because it already exists in the target project (%s)",
			targetFile.Id, parentDir.ProjId)
		return syscall.EINVAL
	}

	destFile, err := fsys.mdb.CreateLink(ctx, oph, targetFile, parentDir, op.Name)
	if err != nil {
		fsys.log("database error in create-link %s", err.Error())
		return fuse.EIO
	}

	// fill in child information
	op.Entry.Child = destFile.GetInode()
	op.Entry.Attributes = destFile.GetAttrs()

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.Entry.AttributesExpiration = fsys.calcExpirationTime(op.Entry.Attributes)
	op.Entry.EntryExpiration = op.Entry.AttributesExpiration

	return nil
}

func (fsys *Filesys) renameFile(
	ctx context.Context,
	oph *OpHandle,
	oldParentDir Dir,
	newParentDir Dir,
	file File,
	newName string) error {
	if oldParentDir.Inode == newParentDir.Inode {
		// /file-xxxx/rename  API call
		err := fsys.ops.DxRename(ctx, oph.httpClient, file.ProjId, file.Id, newName)
		if err != nil {
			fsys.log("Error in renaming file (%s:%s%s) on dnanexus: %s",
				file.ProjId, oldParentDir.ProjFolder, file.Name,
				err.Error())
			oph.RecordError(err)
			return fsys.translateError(err)
		}
	} else {
		// /project-xxxx/move     {objects, folders}  -> destination

		// move the file on the platform
		var objIds []string
		objIds = append(objIds, file.Id)
		err := fsys.ops.DxMove(ctx, oph.httpClient, file.ProjId,
			objIds, nil, newParentDir.ProjFolder)
		if err != nil {
			fsys.log("Error in moving file (%s:%s/%s) on dnanexus: %s",
				file.ProjId, oldParentDir.ProjFolder, file.Name,
				err.Error())
			oph.RecordError(err)
			return fsys.translateError(err)
		}
	}


	err := fsys.mdb.MoveFile(ctx, oph, file.Inode, newParentDir, newName)
	if err != nil {
		fsys.log("database error in rename")
		return fuse.EIO
	}

	return nil
}

func (fsys *Filesys) renameDir(
	ctx context.Context,
	oph *OpHandle,
	oldParentDir Dir,
	newParentDir Dir,
	oldDir Dir,
	newName string) error {
	projId := oldParentDir.ProjId

	if oldParentDir.Inode == newParentDir.Inode {
		// rename a folder, but leave it under the same parent
		err := fsys.ops.DxRenameFolder(
			ctx, oph.httpClient,
			projId,
			oldDir.ProjFolder,
			newName)
		if err != nil {
			fsys.log("Error in folder rename %s -> %s on dnanexus, %s",
				oldDir.FullPath, newName, err.Error())
			oph.RecordError(err)
			return fsys.translateError(err)
		}
	} else {
		// we are moving a directory to another directory. For example:
		//   mkdir A
		//   mkdir B
		//   mv A B/
		// The name "A" should not change.
		check(newName == filepath.Base(oldDir.Dname))

		// move a folder to a new parent
		objIds := make([]string, 0)
		folders := make([]string, 1)
		folders[0] = oldDir.ProjFolder

		err := fsys.ops.DxMove(
			ctx, oph.httpClient,
			projId,
			objIds, folders,
			newParentDir.ProjFolder)
		if err != nil {
			fsys.log("Error in moving directory %s:%s -> %s on dnanexus: %s",
				projId, oldDir.ProjFolder, newParentDir.ProjFolder,
				err.Error())
			oph.RecordError(err)
			return fsys.translateError(err)
		}
	}

	err := fsys.mdb.MoveDir(ctx, oph, oldParentDir, newParentDir, oldDir, newName)
	if err != nil {
		fsys.log("Database error in moving directory %s -> %s/%s",
			oldDir.FullPath, newParentDir.FullPath, newName)
		return fuse.EIO
	}

	return nil
}

func (fsys *Filesys) Rename(ctx context.Context, op *fuseops.RenameOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()
	oph := fsys.OpOpen()
	defer fsys.OpClose(oph)

	if fsys.options.Verbose {
		fsys.log("Rename (inode=%d name=%s) -> (inode=%d, name=%s)",
			op.OldParent, op.OldName,
			op.NewParent, op.NewName)
	}

	// the old parent is supposed to be a directory
	oldParentDir, ok, err := fsys.mdb.LookupDirByInode(ctx, oph, int64(op.OldParent))
	if err != nil {
		return err
	}
	if !ok {
		// parent directory does not exist
		return fuse.ENOENT
	}

	// the new parent is supposed to be a directory
	newParentDir, ok, err := fsys.mdb.LookupDirByInode(ctx, oph, int64(op.NewParent))
	if err != nil {
		return err
	}
	if !ok {
		// parent directory does not exist
		return fuse.ENOENT
	}
	if newParentDir.faux {
		fsys.log("can not move files into a faux dir")
		return syscall.EPERM
	}

	// Find the source file
	srcNode, ok, err := fsys.mdb.LookupInDir(ctx, oph, &oldParentDir, op.OldName)
	if err != nil {
		return err
	}
	if !ok {
		// The source file doesn't exist
		return fuse.ENOENT
	}

	// check if the target exists.
	_, ok, err = fsys.mdb.LookupInDir(ctx, oph, &newParentDir, op.NewName)
	if err != nil {
		return err
	}
	if ok {
		fsys.log(`
Target already exists. We do not support atomically remove in conjunction with
a rename. You will need to issue a separate remove operation prior to rename.
`)
		return syscall.EPERM
	}

	oldDir := filepath.Clean(oldParentDir.FullPath + "/" + op.OldName)
	if oldDir == "/" {
		fsys.log("can not move the root directory")
		return syscall.EPERM
	}
	if oldParentDir.Inode == InodeRoot {
		// project directories are immediate children of the root.
		// these cannot be moved
		fsys.log("Can not move a project directory")
		return syscall.EPERM
	}
	if newParentDir.Inode == InodeRoot {
		// can't move into the root directory
		fsys.log("Can not move into the root directory")
		return syscall.EPERM
	}
	if oldParentDir.ProjId != newParentDir.ProjId {
		// can't move between projects
		fsys.log("Can not move objects between projects")
		return syscall.EPERM
	}

	if oldParentDir.Inode == newParentDir.Inode &&
		op.OldName == op.NewName {
		fsys.log("can't move a file onto itself")
		return syscall.EPERM
	}

	switch srcNode.(type) {
	case File:
		return fsys.renameFile(ctx, oph, oldParentDir, newParentDir, srcNode.(File), op.NewName)
	case Dir:
		srcDir := srcNode.(Dir)
		if srcDir.faux {
			fsys.log("can not move a faux directory")
			return syscall.EPERM
		}
		return fsys.renameDir(ctx, oph, oldParentDir, newParentDir, srcDir, op.NewName)
	default:
		log.Panic(fmt.Sprintf("bad type for srcNode %v", srcNode))
	}
	return nil
}

// Decrement the link count, and remove the file if it hits zero.
func (fsys *Filesys) Unlink(ctx context.Context, op *fuseops.UnlinkOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()
	oph := fsys.OpOpen()
	defer fsys.OpClose(oph)

	if fsys.options.Verbose {
		fsys.log("Unlink(%s)", op.Name)
	}

	// the parent is supposed to be a directory
	parentDir, ok, err := fsys.mdb.LookupDirByInode(ctx, oph, int64(op.Parent))
	if err != nil {
		return err
	}
	if !ok {
		// parent directory does not exist
		return fuse.ENOENT
	}

	// Make sure the file exists
	childNode, ok, err := fsys.mdb.LookupInDir(ctx, oph, &parentDir, op.Name)
	if err != nil {
		return err
	}
	if !ok {
		// The file does not exist
		return fuse.ENOENT
	}

	var fileToRemove File
	switch childNode.(type) {
	case File:
		fileToRemove = childNode.(File)
	case Dir:
		// can't unlink a directory
		return fuse.EINVAL
	}
	check(fileToRemove.Nlink > 0)

	// Report to the upload module, that we are cancelling the upload
	// for this file.
	fsys.fugs.CancelUpload(fileToRemove.Id)

	// remove the file on the platform
	objectIds := make([]string, 1)
	objectIds[0] = fileToRemove.Id
	if err := fsys.ops.DxRemoveObjects(ctx, oph.httpClient, parentDir.ProjId, objectIds); err != nil {
		fsys.log("Error in removing file (%s:%s/%s) on dnanexus: %s",
			parentDir.ProjId, parentDir.ProjFolder, op.Name,
			err.Error())
		return fsys.translateError(err)
	}

	if err := fsys.mdb.Unlink(ctx, oph, fileToRemove); err != nil {
		fsys.log("database error in unlink %s", err.Error())
		return fuse.EIO
	}

	return nil
}

// ===
// Directory handling
//

func (fsys *Filesys) readEntireDir(ctx context.Context, oph *OpHandle, dir Dir) ([]fuseutil.Dirent, error) {
	// normalize the filename. For example, we can get "//" when reading
	// the root directory (instead of "/").
	if fsys.options.Verbose {
		fsys.log("ReadDirAll dir=(%s)\n", dir.FullPath)
	}

	dxObjs, subdirs, err := fsys.mdb.ReadDirAll(ctx, oph, &dir)
	if err != nil {
		fsys.log("database error in read entire dir %s", err.Error())
		return nil, fuse.EIO
	}

	if fsys.options.Verbose {
		fsys.log("%d data objects, %d subdirs", len(dxObjs), len(subdirs))
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
		fsys.log("dentries=%v", dEntries)
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
	oph := fsys.OpOpen()
	defer fsys.OpClose(oph)

	// the parent is supposed to be a directory
	dir, ok, err := fsys.mdb.LookupDirByInode(ctx, oph, int64(op.Inode))
	if err != nil {
		fsys.log("database error in OpenDir %s", err.Error())
		return fuse.EIO
	}
	if !ok {
		return fuse.ENOENT
	}

	// Read the entire directory into memory, and sort
	// the entries properly.
	dentries, err := fsys.readEntireDir(ctx, oph, dir)
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
		fsys.log("ReadDir  offset=%d  bytesRead=%d nEntriesReported=%d",
			index, op.BytesRead, i)
	}
	return
}

// ReleaseDirHandle deletes file handle entry
func (fsys *Filesys) ReleaseDirHandle(ctx context.Context, op *fuseops.ReleaseDirHandleOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()

	delete(fsys.dhTable, op.Handle)
	return nil
}


// ===
// File handling
//
func (fsys *Filesys) openRegularFile(ctx context.Context, oph *OpHandle, op *fuseops.OpenFileOp, f File) (*FileHandle, error) {
	if f.InlineData != "" {
		// a regular file that has a local copy
		reader, err := os.Open(f.InlineData)
		if err != nil {
			fsys.log("Could not open local file %s, err=%s", f.InlineData, err.Error())
			return nil, err
		}
		fh := &FileHandle{
			fKind: RO_LocalCopy,
			f : f,
			url: nil,
			localPath : &f.InlineData,
			fd : reader,
		}

		return fh, nil
	}

	// A remote (immutable) file.
	// create a download URL for this file.
	const secondsInYear int = 60 * 60 * 24 * 365
	payload := fmt.Sprintf("{\"project\": \"%s\", \"duration\": %d}",
		f.ProjId, secondsInYear)

	body, err := dxda.DxAPI(ctx, oph.httpClient, NumRetriesDefault, &fsys.dxEnv, fmt.Sprintf("%s/download", f.Id), payload)
	if err != nil {
		oph.RecordError(err)
		return nil, err
	}
	var u DxDownloadURL
	json.Unmarshal(body, &u)

	fh := &FileHandle{
		fKind: RO_Remote,
		f : f,
		url: &u,
		localPath : nil,
		fd : nil,
	}

	return fh, nil
}

func (fsys *Filesys) OpenFile(ctx context.Context, op *fuseops.OpenFileOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()
	oph := fsys.OpOpen()
	defer fsys.OpClose(oph)

	if fsys.options.Verbose {
		fsys.log("OpenFile inode=%d", op.Inode)
	}

	// find the file by its inode
	node, ok, err := fsys.mdb.LookupByInode(ctx, oph, int64(op.Inode))
	if err != nil {
		fsys.log("database error in OpenFile %s", err.Error())
		return fuse.EIO
	}
	if !ok {
		// file doesn't exist
		return fuse.ENOENT
	}

	var file File
	switch node.(type) {
	case Dir:
		// not allowed to open a directory
		return syscall.EACCES
	case File:
		// cast to a File type
		file = node.(File)
	default:
		log.Panic(fmt.Sprintf("bad type for node %v", node))
	}

	if file.State == "open" {
		fsys.log("File (%s,%s) is in the open state, it cannot be accessed",
			file.Name, file.Id)
		return syscall.EACCES
	}
	if file.ArchivalState != "live" {
		fsys.log("File (%s,%s) is in state %s, it cannot be accessed",
			file.Name, file.Id, file.ArchivalState)
		return syscall.EACCES
	}

	var fh *FileHandle
	switch file.Kind {
	case FK_Regular:
		fh, err = fsys.openRegularFile(ctx, oph, op, file)
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

	if fh.fKind == RO_Remote {
		// Create an entry in the prefetch table, if the file is eligable
		fsys.pgs.CreateStreamEntry(fh.hid, fh.f, *fh.url)
	}
	return nil
}


func (fsys *Filesys) getWritableFD(ctx context.Context, handle fuseops.HandleID) (*os.File, error) {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()

	fh, ok := fsys.fhTable[handle]
	if !ok {
		// File handle doesn't exist
		return nil, fuse.EINVAL
	}

	if fh.fKind != RW_File {
		// This isn't a writeable file, there is no dirty data to flush
		return nil, nil
	}
	if fh.fd == nil {
		return nil, nil
	}
	return fh.fd, nil
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
	len := fsys.pgs.CacheLookup(fh.hid, op.Offset, endOfs, op.Dst)
	if len > 0 {
		op.BytesRead = len
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
		fsys.log("network read  (%s %d) ofs=%d len=%d endOfs=%d lastByteInFile=%d",
			fh.f.Name, fh.f.Inode, op.Offset, reqSize, endOfs, lastByteInFile)
	}

	// Take an http client from the pool. Return it when done.
	httpClient := <- fsys.httpClientPool
	body,err := dxda.DxHttpRequest(ctx, httpClient, NumRetriesDefault, "GET", fh.url.URL, headers, []byte("{}"))
	fsys.httpClientPool <- httpClient
	if err != nil {
		return err
	}

	op.BytesRead = copy(op.Dst, body)
	return nil
}

func (fsys *Filesys) ReadFile(ctx context.Context, op *fuseops.ReadFileOp) error {
	// Here, we start from the file handle
	fsys.mutex.Lock()
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
		return nil, syscall.EPERM
	}
	if fh.fd == nil {
		log.Panic("file descriptor is empty")
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

func (fsys *Filesys) FlushFile(ctx context.Context, op *fuseops.FlushFileOp) error {
	if fsys.options.Verbose {
		fsys.log("Flush inode %d", op.Inode)
	}

	fd, err := fsys.getWritableFD(ctx, op.Handle)
	if err != nil {
		return err
	}
	if fd == nil {
		return nil
	}

	// we aren't holding the filesystem lock at this point
	return fd.Sync()
}

func (fsys *Filesys) SyncFile(ctx context.Context, op *fuseops.SyncFileOp) error {
	if fsys.options.Verbose {
		fsys.log("Sync inode %d", op.Inode)
	}

	fd, err := fsys.getWritableFD(ctx, op.Handle)
	if err != nil {
		return err
	}
	if fd == nil {
		return nil
	}

	// we aren't holding the filesystem lock at this point
	return fd.Sync()
}

func (fsys *Filesys) ReleaseFileHandle(ctx context.Context, op *fuseops.ReleaseFileHandleOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()
	oph := fsys.OpOpen()
	defer fsys.OpClose(oph)

	fh, ok := fsys.fhTable[op.Handle]
	if !ok {
		// File handle doesn't exist
		return nil
	}

	// release the file handle itself
	delete(fsys.fhTable, op.Handle)

	// Clear the state involved with this open file descriptor
	switch fh.fKind {
	case RO_Remote:
		// Read-only file that is accessed remotely
		fsys.pgs.RemoveStreamEntry(fh.hid)
		return nil

	case RW_File:
		// A new file created locally. We need to upload it
		// to the platform.
		if fsys.options.Verbose {
			fsys.log("Close new file(%s)", fh.f.Name)
		}

		// flush and close the local file
		// We leave the local file in place. This allows reading from
		// it, without accessing the network.
		if err := fh.fd.Sync(); err != nil {
			return err
		}
		if err := fh.fd.Close(); err != nil {
			return err
		}
		fh.fd = nil

		// make this file read only
		if err := os.Chmod(*fh.localPath, fileReadOnlyMode); err != nil {
			return err
		}

		fInfo, err := os.Lstat(*fh.localPath)
		if err != nil {
			return err
		}
		fileSize := fInfo.Size()
		modTime := fInfo.ModTime()

		// update database entry
		if err := fsys.mdb.UpdateFile(ctx, oph, fh.f, fileSize, modTime, fileReadOnlyMode); err != nil {
			fsys.log("database error in OpenFile %s", err.Error())
			return fuse.EIO
		}

		// initiate a background request to upload the file to the cloud
		return fsys.fugs.UploadFile(fh.f, fileSize)

	case RO_LocalCopy:
		// Read-only file with a local copy
		if fh.fd != nil {
			fh.fd.Close()
		}
		return nil

	default:
		log.Panicf("Invalid file kind %d", fh.fKind)
	}
	return nil
}

// ===
// Extended attributes (xattrs)
//

func (fsys *Filesys) RemoveXattr(context.Context, *fuseops.RemoveXattrOp) error {
	// not supported right now
	return syscall.EPERM
}


func (fsys *Filesys) getXattrFill(op *fuseops.GetXattrOp, val_str string) error {
	value := []byte(val_str)
	op.BytesRead = len(value)
	if len(value) > len(op.Dst) {
		return syscall.ERANGE
	}
	op.BytesRead = copy(op.Dst, value)
	return nil
}

func (fsys *Filesys) GetXattr(ctx context.Context, op *fuseops.GetXattrOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()
	oph := fsys.OpOpen()
	defer fsys.OpClose(oph)

	if fsys.options.Verbose {
		fsys.log("GetXattr %d", op.Inode)
	}

	// Grab the inode.
	node, ok, err := fsys.mdb.LookupByInode(ctx, oph, int64(op.Inode))
	if err != nil {
		fsys.log("database error in GetXattr: %s", err.Error())
		return fuse.EIO
	}
	if !ok {
		return fuse.ENOENT
	}

	var file File
	switch node.(type) {
	case File:
		file = node.(File)
	case Dir:
		// directories do not have attributes
		return syscall.EINVAL
	}

	// look for the attribute

	// Is it one of the tags?
	// If so, return an empty string
	for _, tag := range file.Tags {
		if tag == op.Name {
			op.BytesRead = 0
			return nil
		}
	}

	// Is it one of the properties?
	// If so, return the value.
	for key, value := range file.Properties {
		if key == op.Name {
			return fsys.getXattrFill(op, value)
		}
	}

	// Is it one of {state, archivalState/archivedState, id}?
	// There is no other way of reporting it, so we allow querying these
	// attributes here.
	switch op.Name {
	case "state":
		return fsys.getXattrFill(op, file.State)
	case "archivalState":
		return fsys.getXattrFill(op, file.ArchivalState)
	case "id" :
		return fsys.getXattrFill(op, file.Id)
	}

	// There is no such attribute
	return fuse.ENOATTR
}

// Make a list of all the extended attributes
func (fsys *Filesys) ListXattr(ctx context.Context, op *fuseops.ListXattrOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()
	oph := fsys.OpOpen()
	defer fsys.OpClose(oph)

	if fsys.options.Verbose {
		fsys.log("ListXattr %d", op.Inode)
	}

	// Grab the inode.
	node, ok, err := fsys.mdb.LookupByInode(ctx, oph, int64(op.Inode))
	if err != nil {
		fsys.log("database error in GetXattr: %s", err.Error())
		return fuse.EIO
	}
	if !ok {
		return fuse.ENOENT
	}

	var file File
	switch node.(type) {
	case File:
		file = node.(File)
	case Dir:
		// directories do not have attributes
		return syscall.EINVAL
	}

	var xattrNames []string

	for _, tag := range file.Tags {
		xattrNames = append(xattrNames, tag)
	}

	// Is it one of the properties?
	// If so, return the value.
	for key, _ := range file.Properties {
		xattrNames = append(xattrNames, key)
	}

	// Is it one of {state, archivalState/archivedState, id}?
	// There is no other way of reporting it, so we allow querying these
	// attributes here.
	for _, key := range []string{ "state", "archivalState", "id"} {
		xattrNames = append(xattrNames, key)
	}

	// encode the names as a sequence of null-terminated strings
	var buffer bytes.Buffer
	for _, aName := range xattrNames {
		buffer.Write([]byte(aName))
		buffer.WriteByte(0)
	}

	op.BytesRead = buffer.Len()
	if op.BytesRead > len(op.Dst) {
		return syscall.ERANGE
	}
	op.BytesRead = copy(op.Dst, buffer.Bytes())
	return nil
}

func (fsys *Filesys) SetXattr(context.Context, *fuseops.SetXattrOp) error {
	// not supported right now
	return syscall.EPERM
}
