package dxfuse

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/hashicorp/go-retryablehttp" // use http libraries from hashicorp for implement retry logic
	"github.com/dnanexus/dxda"
	"golang.org/x/net/context"

	// for the sqlite driver
	_ "github.com/mattn/go-sqlite3"
)

func initUid(options Options) (int,int) {
	// This is current the root user, because the program is run under
	// sudo privileges. The "user" variable is used only if we don't
	// get command line uid/gid.
	user, err := user.Current()
	if err != nil {
		panic(err)
	}

	// get the user ID
	uid := options.Uid
	if uid == -1 {
		var err error
		uid, err = strconv.Atoi(user.Uid)
		if err != nil {
			panic(err)
		}
	}

	// get the group ID
	gid := options.Gid
	if gid == -1 {
		var err error
		gid, err = strconv.Atoi(user.Gid)
		if err != nil {
			panic(err)
		}
	}
	return uid,gid
}

// Mount the filesystem:
//  - setup the debug log to the FUSE kernel log (I think)
//  - mount as read-only
func Mount(
	mountpoint string,
	dxEnv dxda.DXEnvironment,
	manifest Manifest,
	options Options) error {
	uid,gid := initUid(options)

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
	if _, err := os.Stat(CreatedFilesDir); os.IsNotExist(err) {
		os.Mkdir(CreatedFilesDir, 0755)
	}

	// create a connection to the database, that will be kept open
	db, err := sql.Open("sqlite3", DatabaseFile + "?mode=rwc")
	if err != nil {
		return err
	}

	// initialize a pool of http-clients.
	httpClientPool := make(chan *retryablehttp.Client, HttpClientPoolSize)
	for i:=0; i < HttpClientPoolSize; i++ {
		httpClientPool <- dxda.NewHttpClient(true)
	}

	fsys := &Filesys{
		dxEnv : dxEnv,
		options: options,
		uid : uint32(uid),
		gid : uint32(gid),
		dbFullPath : DatabaseFile,
		mutex : sync.Mutex{},
		inodeCnt : InodeRoot + 2,
		db : db,
		httpClientPool : httpClientPool,
		baseDir2ProjectId : make(map[string]string),
		nonce : nonceMake(),
		tmpFileCounter : 0,
	}

	// extra debugging information from FUSE
	if fsys.options.DebugFuse {
		fuse.Debug = func(msg interface{}) {
			log.Print(msg)
		}
	}

	// create the metadata database
	if err := fsys.MetadataDbInit(); err != nil {
		return err
	}

	if err := fsys.MetadataDbPopulateRoot(manifest); err != nil {
		return err
	}

	// initialize prefetching state
	fsys.pgs.Init(options.VerboseLevel)

	// Fuse mount
	log.Printf("mounting dxfuse")
	c, err := fuse.Mount(
		mountpoint,
		fuse.AllowOther(),
		fuse.MaxReadahead(128 * 1024))
	if err != nil {
		return err
	}
	defer c.Close()

	// This method does not return. close the database,
	// and unmount the fuse filesystem when done.
	defer unmount(fsys, mountpoint)

	if err := fs.Serve(c, fsys); err != nil {
		return err
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		return err
	}

	// This returns only when the filesystem is mounted
	return nil
}

func unmount(fsys *Filesys, dirname string) error {
	// Close the sql database.
	//
	// If there is an error, we report it. There is nothing actionable
	// to do with it.
	//
	// We do not remove the metadata database file, so it could be inspected offline.
	log.Printf("unmounting dxfuse from %s\n", dirname)

	if err := fsys.db.Close(); err != nil {
		log.Printf("Error closing the sqlite database %s, err=%s",
			fsys.dbFullPath,
			err.Error())
	}

	if err := fuse.Unmount(dirname); err != nil {
		return err
	}
	return nil
}

func (fsys *Filesys) Root() (fs.Node, error) {
	fsys.mutex.Lock()
	defer fsys.mutex.Unlock()

	return fsys.MetadataDbRoot()
}

func (dir *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	// this can be retained in cache indefinitely (a year is an approximation)
	a.Valid = time.Until(time.Unix(1000 * 1000 * 1000, 0))
	a.Inode = uint64(dir.Inode)
	a.Size = 4096  // dummy size
	a.Blocks = 8
	a.Mode = os.ModeDir | 0555
	a.Nlink = 1
	a.Uid = dir.Fsys.uid
	a.Gid = dir.Fsys.uid
	a.BlockSize = 4 * 1024

	// get the timestamps from the toplevel project
	a.Mtime = dir.Mtime
	a.Ctime = dir.Ctime
	a.Crtime = dir.Ctime

	return nil
}

func (dir *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	dir.Fsys.mutex.Lock()
	defer dir.Fsys.mutex.Unlock()

	// normalize the filename. For example, we can get "//" when reading
	// the root directory (instead of "/").
	fullPath := filepath.Clean(dir.FullPath)
	if dir.Fsys.options.Verbose {
		log.Printf("ReadDirAll dir=(%s)\n", fullPath)
	}

	dxObjs, subdirs, err := dir.Fsys.MetadataDbReadDirAll(fullPath)
	if err != nil {
		return nil, err
	}

	if dir.Fsys.options.Verbose {
		log.Printf("%d data objects, %d subdirs\n", len(dxObjs), len(subdirs))
	}

	var dEntries []fuse.Dirent

	// Add entries for Unix files, representing DNAx data objects
	for oname, oDesc := range dxObjs {
		var dType fuse.DirentType
		switch oDesc.Kind {
		case FK_Regular:
			dType = fuse.DT_File
		case FK_Symlink:
			dType = fuse.DT_File
		default:
			// There is no good way to represent these
			// in the filesystem.
			dType = fuse.DT_Block
		}

		dEntries = append(dEntries, fuse.Dirent{
			Inode : uint64(oDesc.Inode),
			Type : dType,
			Name : oname,
		})
	}

	// Add entries for subdirs
	for subDirName, dirDesc := range subdirs {
		dEntries = append(dEntries, fuse.Dirent{
			Inode : uint64(dirDesc.Inode),
			Type : fuse.DT_Dir,
			Name : subDirName,
		})
	}

	// directory entries need to be sorted
	sort.Slice(dEntries, func(i, j int) bool { return dEntries[i].Name < dEntries[j].Name })
	if dir.Fsys.options.Verbose {
		log.Printf("dentries=%v", dEntries)
	}

	return dEntries, nil
}

var _ = fs.HandleReadDirAller(&Dir{})

var _ = fs.NodeRequestLookuper(&Dir{})

func (dir *Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	dir.Fsys.mutex.Lock()
	defer dir.Fsys.mutex.Unlock()

	fullPath := filepath.Clean(dir.FullPath)
	return dir.Fsys.MetadataDbLookupInDir(fullPath, req.Name)
}


var _ = fs.NodeCreater(&Dir{})

// A CreateRequest asks to create and open a file (not a directory).
func (dir *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	dir.Fsys.mutex.Lock()
	defer dir.Fsys.mutex.Unlock()
	if dir.Fsys.options.Verbose {
		log.Printf("Create(%s)", req.Name)
	}

	file, err := dir.Fsys.CreateFile(dir, req.Name)
	if err != nil {
		return nil, nil, err
	}

	// Create a temporary file in a protected directory, used only
	// by dxfuse.
	cnt := atomic.AddUint64(&dir.Fsys.tmpFileCounter, 1)
	tmpFilePath := fmt.Sprintf("%s/%d_%s", CreatedFilesDir, cnt, req.Name)
	writer, err := os.OpenFile(tmpFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, nil, err
	}
	fh := &FileHandle{
		fKind : RW_File,
		f : file,
		url : nil,
		localPath : &tmpFilePath,
		writer : writer,
	}

	return file, fh, nil
}


func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Size = uint64(f.Size)

	a.Mtime = f.Mtime
	a.Ctime = f.Ctime
	a.Crtime = f.Ctime
	a.Mode = 0400 // read only access
	a.Nlink = uint32(f.Nlink)
	a.Uid = f.Fsys.uid
	a.Gid = f.Fsys.gid
	//a.BlockSize = 1024 * 1024
	return nil
}

var _ = fs.NodeOpener(&File{})

func (f *File) openRegularFile(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	// create a download URL for this file
	const secondsInYear int = 60 * 60 * 24 * 365
	payload := fmt.Sprintf("{\"project\": \"%s\", \"duration\": %d}",
		f.ProjId, secondsInYear)

	// used a shared http client
	httpClient := <-f.Fsys.httpClientPool
	body, err := dxda.DxAPI(httpClient, &f.Fsys.dxEnv, fmt.Sprintf("%s/download", f.Id), payload)
	f.Fsys.httpClientPool <- httpClient
	if err != nil {
		return nil, err
	}
	var u DxDownloadURL
	json.Unmarshal(body, &u)

	fh := &FileHandle{
		fKind: RO_File,
		f : f,
		url: &u,
		localPath : nil,
		writer : nil,
	}

	// Create an entry in the prefetch table, if the file is eligable
	f.Fsys.pgs.CreateFileEntry(fh)

	return fh, nil
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	// existing files are read only. The only way
	// to open a file for writing, is to create a new one.
	if !req.Flags.IsReadOnly() {
		return nil, fuse.Errno(syscall.EACCES)
	}
	switch f.Kind {
	case FK_Regular:
		return f.openRegularFile(ctx, req, resp)
	case FK_Symlink:
		// A symbolic link can use the remote URL address
		// directly. There is no need to generate a preauthenticated
		// URL.
		fh := &FileHandle{
			fKind : RO_File,
			f : f,
			url : &DxDownloadURL{
				URL : f.InlineData,
				Headers : nil,
			},
			localPath : nil,
			writer : nil,
		}
		return fh, nil
	default:
		// these don't contain data
		fh := &FileHandle{
			fKind : RO_File,
			f : f,
			url : nil,
			localPath : nil,
			writer : nil,
		}
		return fh, nil
	}
}

var _ fs.Handle = (*FileHandle)(nil)

var _ fs.HandleReleaser = (*FileHandle)(nil)


func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	switch fh.fKind {

	case RO_File:
		// Read-only file that is accessed remotely
		fh.f.Fsys.pgs.RemoveFileEntry(fh)
		return nil

	case RW_File:
		// A new file created locally. We need to upload it
		// to the platform.
		if fh.f.Fsys.options.Verbose {
			log.Printf("Close new file(%s)", fh.f.Name)
		}

		fInfo, err := fh.writer.Stat()
		if err != nil {
			return err
		}

		// flush and close the local file
		if err := fh.writer.Sync(); err != nil {
			return err
		}
		if err := fh.writer.Close(); err != nil {
			return err
		}
		fh.writer = nil

		// upload to the platform
		//DxFileUpload(fh.f, fh.localPath)
		//DxFileClose(fh.f.fsys, fh.)

		// remove local file
		os.Remove(*fh.localPath)
		fh.localPath = nil

		// update database entry
		return fh.f.Fsys.MetadataDbUpdateFile(*fh.f, fInfo)

	default:
		panic(fmt.Sprintf("Invalid file kind %", fh.fKind))
	}
}

var _ = fs.HandleReader(&FileHandle{})

func (fh *FileHandle) readFile(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	// This is a regular file
	if fh.f.Size == 0 || req.Size == 0 {
		// The file is empty
		return nil
	}
	endOfs := req.Offset + int64(req.Size) - 1

	// make sure we don't go over the file size
	lastByteInFile := fh.f.Size - 1
	endOfs = MinInt64(lastByteInFile, endOfs)

	// See if the data has already been prefetched.
	// This call will wait, if a prefetch IO is in progress.
	prefetchData := fh.f.Fsys.pgs.CacheLookup(fh, req.Offset, endOfs)
	if prefetchData != nil {
		resp.Data = prefetchData
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
	httpClient := <-fh.f.Fsys.httpClientPool
	body,err := dxda.DxHttpRequest(httpClient, "GET", fh.url.URL, headers, []byte("{}"))
	fh.f.Fsys.httpClientPool <- httpClient
	if err != nil {
		return err
	}

	resp.Data = body
	return nil
}

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	switch fh.f.Kind {
	case FK_Regular:
		return fh.readFile(ctx, req, resp)
	case FK_Symlink:
		return fh.readFile(ctx, req, resp)
	default:
		// This isn't a regular file. It is an applet/workflow/...
		msg := fmt.Sprintf("A dnanexus %s", fh.f.Id)
		resp.Data = []byte(msg)
		return nil
	}
}

var _ = fs.HandleFlusher(&FileHandle{})

func (fh *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	return nil
}

// Writes to files.
//
// A file is created locally, and writes go to the local location. When
// the file is closed, it becomes read only, and is then uploaded to the cloud.
var _ = fs.HandleWriter(&FileHandle{})

func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	if fh.writer == nil {
		return fuse.EPERM
	}
	len, err := fh.writer.WriteAt(req.Data, req.Offset)
	resp.Size = len
	return err
}
