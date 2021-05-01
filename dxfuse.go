package dxfuse

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dnanexus/dxda"
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"

	// for the sqlite driver
	_ "github.com/mattn/go-sqlite3"
)

const (
	// namespace for xattrs
	XATTR_TAG  = "tag"
	XATTR_PROP = "prop"
	XATTR_BASE = "base"
)

type Filesys struct {
	// inherit empty implementations for all the filesystem
	// methods we do not implement
	fuseutil.NotImplementedFileSystem

	// configuration information for accessing dnanexus servers
	dxEnv dxda.DXEnvironment

	// various options
	options Options

	// Lock for protecting shared access to the database
	mutex *sync.Mutex

	// a pool of http clients, for short requests, such as file creation,
	// or file describe.
	httpClientPool chan (*http.Client)

	// metadata database
	mdb *MetadataDb

	// prefetch state for all files
	pgs *PrefetchGlobalState

	// sync daemon
	sybx *SyncDbDx

	// API to dx
	ops *DxOps

	// A way to send external commands to the filesystem
	cmdSrv *CmdServer

	// description for each mounted project
	projId2Desc map[string]DxDescribePrj

	// all open files
	fhCounter uint64
	fhTable   map[fuseops.HandleID]*FileHandle

	// all open directories
	dhCounter uint64
	dhTable   map[fuseops.HandleID]*DirHandle

	tmpFileCounter uint64

	// is the the system shutting down (unmounting)
	shutdownCalled bool
}

// Files can be in two access modes: remote-read-only or remote-append-only
const (
	// read only file that is on the cloud
	AM_RO_Remote = 1

	// 'open' file that is being appended to on the platform
	// file is not readable until it is in the 'closed' state
	// at which point it is set to readonly and AM_RO_Remote
	AM_WO_Remote = 2
)

type FileHandle struct {
	// a lock allowing multiple readers or a single writer.
	accessMode int
	inode      int64
	size       int64 // this is up-to-date only for remote files
	hid        fuseops.HandleID

	Id string // To avoid looking up the file-id for each /upload call

	// URL used for downloading file ranges.
	// Used for read-only files.
	url *DxDownloadURL

	// For writeable files only
	// Used to indiciate the last part number written
	// Incremented for each buffer that is uploaded to the cloud
	// Used to determine next part and buffer size
	lastPartId      int
	nextWriteOffset int64
	uploadBuffer    []byte
	// Keep track of bytes written to buffer
	wb int
	// Lock for writing
	mutex *sync.Mutex
}

type DirHandle struct {
	d       Dir
	entries []fuseutil.Dirent
}

func NewDxfuse(
	dxEnv dxda.DXEnvironment,
	manifest Manifest,
	options Options) (*Filesys, error) {

	// initialize a pool of http-clients.
	httpIoPool := make(chan *http.Client, HttpClientPoolSize)
	for i := 0; i < HttpClientPoolSize; i++ {
		httpIoPool <- dxda.NewHttpClient()
	}

	dxfuseBaseDir := MakeFSBaseDir()
	fsys := &Filesys{
		dxEnv:          dxEnv,
		options:        options,
		mutex:          &sync.Mutex{},
		httpClientPool: httpIoPool,
		ops:            NewDxOps(dxEnv, options),
		fhCounter:      1,
		fhTable:        make(map[fuseops.HandleID]*FileHandle),
		dhCounter:      1,
		dhTable:        make(map[fuseops.HandleID]*DirHandle),
		tmpFileCounter: 0,
		shutdownCalled: false,
	}

	// Create a fresh SQL database
	databaseFile := dxfuseBaseDir + "/" + DatabaseFile
	fsys.log("Removing old version of the database (%s)", databaseFile)
	if err := os.RemoveAll(databaseFile); err != nil {
		fsys.log("error removing old database %s", err.Error())
		os.Exit(1)
	}

	// create the metadata database
	mdb, err := NewMetadataDb(databaseFile, dxEnv, options)
	if err != nil {
		return nil, err
	}
	fsys.mdb = mdb
	if err := fsys.mdb.Init(); err != nil {
		return nil, err
	}

	oph := fsys.opOpen()
	if err := fsys.mdb.PopulateRoot(context.TODO(), oph, manifest); err != nil {
		fsys.opClose(oph)
		return nil, err
	}
	fsys.opClose(oph)

	fsys.pgs = NewPrefetchGlobalState(options.VerboseLevel, dxEnv)

	// describe all the projects, we need their upload parameters
	httpClient := <-fsys.httpClientPool
	defer func() {
		fsys.httpClientPool <- httpClient
	}()

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
	fsys.projId2Desc = projId2Desc

	// initialize sync daemon
	//fsys.sybx = NewSyncDbDx(options, dxEnv, projId2Desc, mdb, fsys.mutex)

	// create an endpoint for communicating with the user
	fsys.cmdSrv = NewCmdServer(options, fsys.sybx)
	fsys.cmdSrv.Init()

	return fsys, nil
}

// write a log message, and add a header
func (fsys *Filesys) log(a string, args ...interface{}) {
	LogMsg("dxfuse", a, args...)
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

	// close the command server, this frees up the port
	fsys.cmdSrv.Close()

	// Stop the synchronization daemon. Do not complete
	// outstanding operations.
	if fsys.sybx != nil {
		fsys.sybx.Shutdown()
	}
}

// check if a user has sufficient permissions to read/write a project
func (fsys *Filesys) checkProjectPermissions(projId string, requiredPerm int) bool {
	if fsys.options.ReadOnly {
		// if the filesystem is mounted read-only, we
		// allow only operations that require VIEW level access
		if requiredPerm > PERM_VIEW {
			return false
		}
	}
	pDesc := fsys.projId2Desc[projId]
	return pDesc.Level >= requiredPerm
}

func (fsys *Filesys) opOpen() *OpHandle {
	txn, err := fsys.mdb.BeginTxn()
	if err != nil {
		log.Panic("Could not open transaction")
	}
	httpClient := <-fsys.httpClientPool

	return &OpHandle{
		httpClient: httpClient,
		txn:        txn,
		err:        nil,
	}
}

func (fsys *Filesys) opOpenNoHttpClient() *OpHandle {
	txn, err := fsys.mdb.BeginTxn()
	if err != nil {
		log.Panic("Could not open transaction")
	}

	return &OpHandle{
		httpClient: nil,
		txn:        txn,
		err:        nil,
	}
}

func (fsys *Filesys) opClose(oph *OpHandle) {
	if oph.httpClient != nil {
		fsys.httpClientPool <- oph.httpClient
	}

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
	//return fuse.ENOSYS
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
	oph := fsys.opOpen()
	defer fsys.opClose(oph)

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
	oph := fsys.opOpen()
	defer fsys.opClose(oph)

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
	oph := fsys.opOpen()
	defer fsys.opClose(oph)

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
	if !fsys.checkProjectPermissions(file.ProjId, PERM_VIEW) {
		return syscall.EPERM
	}

	// we know it is a file.
	// check if this is a read-only file.
	attrs := file.GetAttrs()
	if attrs.Mode == fileReadOnlyMode {
		return syscall.EPERM
	}

	// update the file
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
	err = fsys.mdb.UpdateFileAttrs(ctx, oph, file.Inode, int64(attrs.Size), attrs.Mtime, &attrs.Mode)
	if err != nil {
		fsys.log("database error in OpenFile %s", err.Error())
		return fuse.EIO
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
		if fh.inode == inode {
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
	oph := fsys.opOpen()
	defer fsys.opClose(oph)

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
	if !fsys.checkProjectPermissions(parentDir.ProjId, PERM_CONTRIBUTE) {
		return syscall.EPERM
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
		parentDir.FullPath+"/"+op.Name)
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
		Child:                fuseops.InodeID(dnode),
		Attributes:           childAttrs,
		AttributesExpiration: tWindow,
		EntryExpiration:      tWindow,
	}
	return nil
}

func (fsys *Filesys) RmDir(ctx context.Context, op *fuseops.RmDirOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()
	oph := fsys.opOpen()
	defer fsys.opClose(oph)

	if fsys.options.Verbose {
		fsys.log("RemoveDir(%s)", op.Name)
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
	if !fsys.checkProjectPermissions(parentDir.ProjId, PERM_CONTRIBUTE) {
		return syscall.EPERM
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
	oph := fsys.opOpen()
	defer fsys.opClose(oph)

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
	if !fsys.checkProjectPermissions(parentDir.ProjId, PERM_UPLOAD) {
		return syscall.EPERM
	}

	// we now know that the parent directory exists, and the file does not.
	// Create a remote file for appending data and then update the metadata db
	file, err := fsys.mdb.CreateFile(ctx, oph, &parentDir, op.Name, op.Mode)
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
		Child:                fuseops.InodeID(file.Inode),
		Attributes:           childAttrs,
		AttributesExpiration: tWindow,
		EntryExpiration:      tWindow,
	}

	fh := FileHandle{
		accessMode:      AM_WO_Remote,
		inode:           file.Inode,
		size:            file.Size,
		url:             nil,
		Id:              file.Id,
		lastPartId:      0,
		nextWriteOffset: 0,
		uploadBuffer:    make([]byte, 0, 64*1024*1024),
		wb:              0,
		mutex:           &sync.Mutex{},
	}
	op.Handle = fsys.insertIntoFileHandleTable(&fh)
	return nil
}

func (fsys *Filesys) CreateLink(ctx context.Context, op *fuseops.CreateLinkOp) error {
	// not supporting creation of hard links now
	return fuse.ENOSYS
}

func (fsys *Filesys) renameFile(
	ctx context.Context,
	oph *OpHandle,
	oldParentDir Dir,
	newParentDir Dir,
	file File,
	newName string) error {
	err := fsys.mdb.MoveFile(ctx, oph, file.Inode, newParentDir, newName)
	if err != nil {
		fsys.log("database error in rename")
		return fuse.EIO
	}

	if file.Id == "" {
		// The file has not been uploaded to the platform yet
		return nil
	}

	// The file is on the platform, we need to move it on the backend.
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
	oph := fsys.opOpen()
	defer fsys.opClose(oph)

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
	if !fsys.checkProjectPermissions(oldParentDir.ProjId, PERM_CONTRIBUTE) {
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
		log.Panicf("bad type for srcNode %v", srcNode)
	}
	return nil
}

// Decrement the link count, and remove the file if it hits zero.
func (fsys *Filesys) Unlink(ctx context.Context, op *fuseops.UnlinkOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()
	oph := fsys.opOpen()
	defer fsys.opClose(oph)

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
	if !fsys.checkProjectPermissions(parentDir.ProjId, PERM_CONTRIBUTE) {
		return syscall.EPERM
	}

	var fileToRemove File
	switch childNode.(type) {
	case File:
		fileToRemove = childNode.(File)
	case Dir:
		// can't unlink a directory
		return fuse.EINVAL
	}

	if err := fsys.mdb.Unlink(ctx, oph, fileToRemove); err != nil {
		fsys.log("database error in unlink %s", err.Error())
		return fuse.EIO
	}

	// The file has not been created on the platform yet, there is no need to
	// remove it
	if fileToRemove.Id == "" {
		return nil
	}

	// remove the file on the platform
	objectIds := make([]string, 1)
	objectIds[0] = fileToRemove.Id
	if err := fsys.ops.DxRemoveObjects(ctx, oph.httpClient, parentDir.ProjId, objectIds); err != nil {
		fsys.log("Error in removing file (%s:%s/%s) on dnanexus: %s",
			parentDir.ProjId, parentDir.ProjFolder, op.Name,
			err.Error())
		return fsys.translateError(err)
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
			Inode:  fuseops.InodeID(oDesc.Inode),
			Name:   oname,
			Type:   dType,
		}
		dEntries = append(dEntries, dirEnt)
	}

	// Add entries for subdirs
	for subDirName, dirDesc := range subdirs {
		dirEnt := fuseutil.Dirent{
			Offset: 0,
			Inode:  fuseops.InodeID(dirDesc.Inode),
			Name:   subDirName,
			Type:   fuseutil.DT_Directory,
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
	oph := fsys.opOpen()
	defer fsys.opClose(oph)

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
		d:       dir,
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
func (fsys *Filesys) openRegularFile(
	ctx context.Context,
	oph *OpHandle,
	op *fuseops.OpenFileOp,
	f File) (*FileHandle, error) {

	if f.dirtyData {
		fh := &FileHandle{
			accessMode:      AM_WO_Remote,
			inode:           f.Inode,
			size:            f.Size,
			Id:              f.Id,
			url:             nil,
			lastPartId:      0,
			nextWriteOffset: 0,
			// 5MB slice capacity
			uploadBuffer: make([]byte, 0, 5*1024*1024),
			wb:           0,
			mutex:        &sync.Mutex{},
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
		accessMode:      AM_RO_Remote,
		inode:           f.Inode,
		size:            f.Size,
		url:             &u,
		Id:              f.Id,
		lastPartId:      0,
		nextWriteOffset: 0,
		uploadBuffer:    nil,
		wb:              0,
		mutex:           nil,
	}

	return fh, nil
}

// Note: What happens if the file is opened for writing?
//
func (fsys *Filesys) OpenFile(ctx context.Context, op *fuseops.OpenFileOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()
	oph := fsys.opOpen()
	defer fsys.opClose(oph)

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

	if file.State != "closed" {
		fsys.log("File (%s,%s) is not closed, it cannot be accessed",
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
			accessMode: AM_RO_Remote,
			inode:      file.Inode,
			size:       file.Size,
			Id:         file.Id,
			url: &DxDownloadURL{
				URL:     file.Symlink,
				Headers: nil,
			},
			lastPartId:      0,
			nextWriteOffset: 0,
			uploadBuffer:    nil,
			wb:              0,
			mutex:           nil,
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

	if fh.accessMode == AM_RO_Remote {
		// Create an entry in the prefetch table, if the file is eligable
		fsys.pgs.CreateStreamEntry(fh.hid, file, *fh.url)
	}
	return nil
}

// read a remote immutable file
//
func (fsys *Filesys) readRemoteFile(ctx context.Context, op *fuseops.ReadFileOp, fh *FileHandle) error {
	// This is a regular file
	reqSize := int64(len(op.Dst))
	if fh.size == 0 || reqSize == 0 {
		// The file is empty
		return nil
	}
	if fh.size <= op.Offset {
		// request is beyond the size of the file
		return nil
	}
	endOfs := op.Offset + reqSize - 1

	// make sure we don't go over the file size
	lastByteInFile := fh.size - 1
	endOfs = MinInt64(lastByteInFile, endOfs)
	reqSize = endOfs - op.Offset + 1

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
		fsys.log("network read (inode=%d) ofs=%d len=%d endOfs=%d lastByteInFile=%d",
			fh.inode, op.Offset, reqSize, endOfs, lastByteInFile)
	}

	// Take an http client from the pool. Return it when done.
	httpClient := <-fsys.httpClientPool
	err := dxda.DxHttpRequestData(
		ctx,
		httpClient,
		"GET",
		fh.url.URL,
		headers,
		[]byte("{}"),
		int(reqSize),
		op.Dst)
	fsys.httpClientPool <- httpClient
	op.BytesRead = int(reqSize)
	return err
}

func (fsys *Filesys) ReadFile(ctx context.Context, op *fuseops.ReadFileOp) error {
	// Here, we start from the file handle
	fsys.mutex.Lock()
	fh, ok := fsys.fhTable[op.Handle]
	if !ok {
		// invalid file handle. It doesn't exist in the table
		fsys.mutex.Unlock()
		return fuse.EINVAL
	}
	fsys.mutex.Unlock()

	switch fh.accessMode {
	case AM_RO_Remote:
		return fsys.readRemoteFile(ctx, op, fh)
	case AM_WO_Remote:
		// the file is being appened to, not readable in this state
		return syscall.EPERM
	default:
		log.Panicf("Invalid file access mode %d", fh.accessMode)
		return fuse.EIO
	}
}

// Writes to files.
//
// Note: the file-open operation doesn't state if the file is going to be opened for
// reading or writing.
func (fsys *Filesys) WriteFile(ctx context.Context, op *fuseops.WriteFileOp) error {
	fsys.mutex.Lock()
	fh, ok := fsys.fhTable[op.Handle]
	if !ok {
		// invalid file handle. It doesn't exist in the table
		fsys.mutex.Unlock()
		return fuse.EINVAL
	}
	fsys.log("Unlocking fs")
	fsys.mutex.Unlock()
	if !ok {
		// invalid file handle. It doesn't exist in the table
		return fuse.EINVAL
	}
	// One write at a time per fh so that sequential offsets work properly
	fh.mutex.Lock()
	fsys.log("ACQUIRED filehandle LOCK")
	defer fh.mutex.Unlock()

	fsys.log("op.Offest: %d, fh.nextWriteOffest: %d", op.Offset, fh.nextWriteOffset)
	if op.Offset != fh.nextWriteOffset {
		fsys.log("ERROR: Only sequential writes are supported")
		return syscall.ENOTSUP
	}

	numBytesToWrite := int64(len(op.Data))

	oph := fsys.opOpen()
	defer fsys.opClose(oph)

	// until all op.Data bytes are copied to upload buffer
	//   copy bytes to buffer
	//   update nextWriteOffset
	//   if buffer is now full
	//     increment part index
	//     upload file part with full buffer
	//   if all bytes have been copied break
	//   strip bytes copied from op.Data for next copy

	for {
		fsys.log("Copying into buffer")
		fsys.log("bufferLen: %d bufferCap: %d opDataLen: %d", len(fh.uploadBuffer), cap(fh.uploadBuffer), len(op.Data))
		sliceUpperBound := fh.wb + len(op.Data)
		if sliceUpperBound > cap(fh.uploadBuffer) {
			sliceUpperBound = cap(fh.uploadBuffer)
		}
		// expand slice
		fh.uploadBuffer = fh.uploadBuffer[:sliceUpperBound]
		fsys.log("%v, %v", fh.wb, sliceUpperBound)
		// copy data into slice
		bytesCopied := copy(fh.uploadBuffer[fh.wb:sliceUpperBound], op.Data)
		fsys.log("Copied: %v", bytesCopied)
		// increment next write offset
		fh.nextWriteOffset += int64(bytesCopied)
		//fsys.log("%v", fh.uploadBuffer)
		fsys.log("bufferLen: %d bufferCap: %d opDataLen: %d", len(fh.uploadBuffer), cap(fh.uploadBuffer), len(op.Data))
		if len(fh.uploadBuffer) == cap(fh.uploadBuffer) {
			// increment part id
			fh.lastPartId++
			partId := fh.lastPartId
			// upload full 5MB buffer!
			fsys.log("uploading part")
			fsys.ops.DxFileUploadPart(context.TODO(), oph.httpClient, fh.Id, partId, fh.uploadBuffer)
			fsys.log("zeroing buffer i hope")
			fh.uploadBuffer = fh.uploadBuffer[:0]
			fh.wb = 0
			fsys.log("%v", fh.uploadBuffer)

		}
		// increment current buffer slice counter
		fh.wb += bytesCopied
		// all data copied into buffer slice, break
		if bytesCopied == len(op.Data) {
			break
		}
		// trim data
		op.Data = op.Data[bytesCopied:]

	}

	// Try to efficiently calculate the size and mtime, instead
	// of doing a filesystem call.
	fSize := MaxInt64(op.Offset+numBytesToWrite, fh.size)
	mtime := time.Now()

	// Update the file attributes in the database (size, mtime)
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()

	if err := fsys.mdb.UpdateFileAttrs(ctx, oph, fh.inode, fSize, mtime, nil); err != nil {
		fsys.log("database error in updating attributes for WriteFile %s", err.Error())
		return fuse.EIO
	}
	return nil
}

func (fsys *Filesys) FlushFile(ctx context.Context, op *fuseops.FlushFileOp) error {
	if fsys.options.Verbose {
		fsys.log("Flush inode %d", op.Inode)
	}
	fh, ok := fsys.fhTable[op.Handle]
	if !ok {
		// File handle doesn't exist
		return fuse.EINVAL
	}
	if fh == nil {
		return nil
	}
	if fh.accessMode != AM_WO_Remote {
		// This isn't a writeable file, there is no dirty data to flush
		return nil
	}

	oph := fsys.opOpen()
	defer fsys.opClose(oph)

	if len(fh.uploadBuffer) > 0 {
		fh.lastPartId++
		partId := fh.lastPartId
		fsys.ops.DxFileUploadPart(context.TODO(), oph.httpClient, fh.Id, int(partId), fh.uploadBuffer)
	}
	file, _, _ := fsys.lookupFileByInode(ctx, oph, int64(op.Inode))

	if fsys.options.Verbose {
		fsys.log("Flush and closing inode %d, %s", op.Inode, fh.Id)
	}
	fsys.ops.DxFileCloseAndWait(context.TODO(), oph.httpClient, file.ProjId, fh.Id)
	return nil
}

func (fsys *Filesys) SyncFile(ctx context.Context, op *fuseops.SyncFileOp) error {
	if fsys.options.Verbose {
		fsys.log("Sync inode %d", op.Inode)
	}

	return nil
}

func (fsys *Filesys) ReleaseFileHandle(ctx context.Context, op *fuseops.ReleaseFileHandleOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()
	oph := fsys.opOpen()
	defer fsys.opClose(oph)

	fh, ok := fsys.fhTable[op.Handle]
	if !ok {
		// File handle doesn't exist
		return nil
	}

	// release the file handle itself
	delete(fsys.fhTable, op.Handle)

	// Clear the state involved with this open file descriptor
	switch fh.accessMode {
	case AM_RO_Remote:
		// Read-only file that is accessed remotely
		fsys.pgs.RemoveStreamEntry(fh.hid)
		return nil

	case AM_WO_Remote:
		return nil

	default:
		log.Panicf("Invalid file access mode %d", fh.accessMode)
	}
	return nil
}

// ===
// Extended attributes (xattrs)
//

func (fsys *Filesys) lookupFileByInode(ctx context.Context, oph *OpHandle, inode int64) (File, bool, error) {
	node, ok, err := fsys.mdb.LookupByInode(ctx, oph, inode)
	if err != nil {
		fsys.log("database error in xattr op: %s", err.Error())
		return File{}, false, fuse.EIO
	}
	if !ok {
		return File{}, false, fuse.ENOENT
	}

	var file File
	switch node.(type) {
	case File:
		file = node.(File)
	case Dir:
		// directories do not have attributes
		return File{}, true, syscall.EINVAL
	}
	return file, false, nil
}

func (fsys *Filesys) xattrParseName(name string) (string, string, error) {
	prefixLen := strings.Index(name, ".")
	if prefixLen == -1 {
		fsys.log("property must start with one of {%s ,%s, %s}",
			XATTR_TAG, XATTR_PROP, XATTR_BASE)
		return "", "", fuse.EINVAL
	}
	namespace := name[:prefixLen]
	attrName := name[len(namespace)+1:]
	return namespace, attrName, nil
}

func (fsys *Filesys) RemoveXattr(ctx context.Context, op *fuseops.RemoveXattrOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()
	oph := fsys.opOpen()
	defer fsys.opClose(oph)

	if fsys.options.Verbose {
		fsys.log("RemoveXattr %d", op.Inode)
	}

	// Grab the inode.
	file, _, err := fsys.lookupFileByInode(ctx, oph, int64(op.Inode))
	if err != nil {
		return err
	}
	if !fsys.checkProjectPermissions(file.ProjId, PERM_CONTRIBUTE) {
		return syscall.EPERM
	}

	// look for the attribute
	namespace, attrName, err := fsys.xattrParseName(op.Name)
	if err != nil {
		return err
	}

	attrExists := false
	switch namespace {
	case XATTR_TAG:
		// this is in the tag namespace
		for _, tag := range file.Tags {
			if tag == attrName {
				attrExists = true
				break
			}
		}
	case XATTR_PROP:
		// in the property namespace
		for key, _ := range file.Properties {
			if key == attrName {
				attrExists = true
				break
			}
		}
	case XATTR_BASE:
		fsys.log("property must start with one of {%s ,%s}", XATTR_TAG, XATTR_PROP)
		return fuse.EINVAL
	}

	if !attrExists {
		return fuse.ENOATTR
	}

	// remove the key from in-memory representation
	switch namespace {
	case XATTR_TAG:
		var tags []string
		for _, tag := range file.Tags {
			if tag != attrName {
				tags = append(tags, attrName)
			}
		}
		file.Tags = tags
	case XATTR_PROP:
		delete(file.Properties, attrName)
	default:
		log.Panicf("sanity: invalid namespace %s", namespace)
	}

	// update the database
	if err := fsys.mdb.UpdateFileTagsAndProperties(ctx, oph, file); err != nil {
		fsys.log("database error in RemoveXattr: %s", err.Error())
		return fuse.EIO
	}
	return nil
}

func (fsys *Filesys) getXattrFill(op *fuseops.GetXattrOp, val_str string) error {
	value := []byte(val_str)
	op.BytesRead = len(value)
	if len(op.Dst) == 0 {
		return nil
	}
	if len(value) > len(op.Dst) {
		return syscall.ERANGE
	}
	op.BytesRead = copy(op.Dst, value)
	return nil
}

func (fsys *Filesys) GetXattr(ctx context.Context, op *fuseops.GetXattrOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()
	oph := fsys.opOpen()
	defer fsys.opClose(oph)

	if fsys.options.VerboseLevel > 1 {
		fsys.log("GetXattr %d", op.Inode)
	}

	// Grab the inode.
	file, isDir, err := fsys.lookupFileByInode(ctx, oph, int64(op.Inode))
	if isDir {
		return fuse.ENOATTR
	}
	if err != nil {
		return err
	}

	// look for the attribute
	namespace, attrName, err := fsys.xattrParseName(op.Name)
	if err != nil {
		return err
	}

	switch namespace {
	case XATTR_TAG:
		// Is it one of the tags?
		// If so, return an empty string
		for _, tag := range file.Tags {
			if tag == attrName {
				return fsys.getXattrFill(op, "")
			}
		}
	case XATTR_PROP:
		// Is it one of the properties?
		// If so, return the value.
		for key, value := range file.Properties {
			if key == attrName {
				return fsys.getXattrFill(op, value)
			}
		}
	case XATTR_BASE:
		// Is it one of {state, archivalState/archivedState, id}?
		// There is no other way of reporting it, so we allow querying these
		// attributes here.
		switch attrName {
		case "state":
			return fsys.getXattrFill(op, file.State)
		case "archivalState":
			return fsys.getXattrFill(op, file.ArchivalState)
		case "id":
			return fsys.getXattrFill(op, file.Id)
		}
	}

	// There is no such attribute
	return fuse.ENOATTR
}

// Make a list of all the extended attributes
func (fsys *Filesys) ListXattr(ctx context.Context, op *fuseops.ListXattrOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()
	oph := fsys.opOpen()
	defer fsys.opClose(oph)

	if fsys.options.Verbose {
		fsys.log("ListXattr %d", op.Inode)
	}

	// Grab the inode.
	file, _, err := fsys.lookupFileByInode(ctx, oph, int64(op.Inode))
	if err != nil {
		return err
	}

	// collect all the properties into one array
	var xattrKeys []string
	for _, tag := range file.Tags {
		xattrKeys = append(xattrKeys, XATTR_TAG+"."+tag)
	}
	for key, _ := range file.Properties {
		xattrKeys = append(xattrKeys, XATTR_PROP+"."+key)
	}
	// Special attributes
	for _, key := range []string{"state", "archivalState", "id"} {
		xattrKeys = append(xattrKeys, XATTR_BASE+"."+key)
	}
	if fsys.options.Verbose {
		fsys.log("attribute keys: %v", xattrKeys)
		fsys.log("output buffer len=%d", len(op.Dst))
	}

	// encode the names as a sequence of null-terminated strings
	dst := op.Dst[:]
	for _, key := range xattrKeys {
		keyLen := len(key) + 1

		if len(dst) >= keyLen {
			copy(dst, key)
			dst[keyLen-1] = 0 // null terminate the string
			dst = dst[keyLen:]
		} else if len(op.Dst) != 0 {
			return syscall.ERANGE
		}
		op.BytesRead += keyLen
	}

	return nil
}

func (fsys *Filesys) SetXattr(ctx context.Context, op *fuseops.SetXattrOp) error {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()
	oph := fsys.opOpen()
	defer fsys.opClose(oph)

	if fsys.options.Verbose {
		fsys.log("SetXattr %d", op.Inode)
	}

	// Grab the inode.
	node, ok, err := fsys.mdb.LookupByInode(ctx, oph, int64(op.Inode))
	if err != nil {
		fsys.log("database error in SetXattr: %s", err.Error())
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
		//
		// Note: we may want to change this for directories
		// representing projects. This would allow reporting project
		// tags and properties.
		return syscall.EINVAL
	}
	if !fsys.checkProjectPermissions(file.ProjId, PERM_CONTRIBUTE) {
		return syscall.EPERM
	}

	// Check if the property already exists
	// convert
	//   "tag.foo" ->  namespace=TAGS  pName="foo"
	//   "prop.foo" -> namespace=PROP  pName="foo"
	attrExists := false
	namespace, attrName, err := fsys.xattrParseName(op.Name)
	if err != nil {
		return err
	}

	switch namespace {
	case XATTR_TAG:
		// this is in the tag namespace
		for _, tag := range file.Tags {
			if tag == attrName {
				attrExists = true
				break
			}
		}
	case XATTR_PROP:
		// in the property namespace
		for key, _ := range file.Properties {
			if key == attrName {
				attrExists = true
				break
			}
		}
	default:
		fsys.log("property must start with one of {%s ,%s}", XATTR_TAG, XATTR_PROP)
		return fuse.EINVAL
	}

	// cases of early return
	switch op.Flags {
	case 0x1:
		if attrExists {
			return fuse.EEXIST
		}
	case 0x2:
		if !attrExists {
			return fuse.ENOATTR
		}
	case 0x0:
		// can accept both cases
	default:
		fsys.log("invalid SetAttr flag value %d, expecting one of {0x0, 0x1, 0x2}",
			op.Flags)
		return syscall.EINVAL
	}

	// update the file in-memory representation
	switch namespace {
	case XATTR_TAG:
		if !attrExists {
			file.Tags = append(file.Tags, attrName)
		} else {
			// The tag is already set. There is no need
			// to tag again.
		}
	case XATTR_PROP:
		// The key may already exist, in which case we are updating
		// the value.
		file.Properties[attrName] = string(op.Value)
	default:
		log.Panicf("sanity: invalid namespace %s", namespace)
	}

	// update the database
	if err := fsys.mdb.UpdateFileTagsAndProperties(ctx, oph, file); err != nil {
		fsys.log("database error in SetXattr %s", err.Error())
		return fuse.EIO
	}
	return nil
}
