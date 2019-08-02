package dxfs2

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
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/dnanexus/dxda"
	"golang.org/x/net/context"

	// for the sqlite driver
	_ "github.com/mattn/go-sqlite3"
)

// A URL generated with the /file-xxxx/download API call, that is
// used to download file ranges.
type DxDownloadURL struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
}

type FileHandle struct {
	f *File

	// URL used for downloading file ranges
	url DxDownloadURL
}

// global static value for the dxfs2 filesystem
var gFsys *Filesys = nil

// Mount the filesystem:
//  - setup the debug log to the FUSE kernel log (I think)
//  - mount as read-only
func Mount(
	mountpoint string,
	dxEnv dxda.DXEnvironment,
	projectId string,
	options Options) error {

	c, err := fuse.Mount(mountpoint, fuse.AllowOther(), fuse.ReadOnly(),
		fuse.MaxReadahead(4 * 1024 * 1024), fuse.AsyncRead())
	if err != nil {
		return err
	}
	defer c.Close()

	// get the Unix uid and gid
	user, err := user.Current()
	if err != nil {
		return err
	}
	uid, err := strconv.Atoi(user.Uid)
	if err != nil {
		return err
	}
	gid, err := strconv.Atoi(user.Gid)
	if err != nil {
		return err
	}

	// Create a fresh SQL database
	dbParentFolder := filepath.Dir(DB_PATH)
	if _, err := os.Stat(dbParentFolder); os.IsNotExist(err) {
		os.Mkdir(dbParentFolder, 0755)
	}
	os.Remove(DB_PATH)

	// create a connection to the database, that will be kept open
	db, err := sql.Open("sqlite3", DB_PATH + "?cache=shared&mode=rwc")
	if err != nil {
		return err
	}

	gFsys = &Filesys{
		dxEnv : dxEnv,
		options: options,
		uid : uint32(uid),
		gid : uint32(gid),
		projectId : projectId,
		dbFullPath : DB_PATH,
		mutex : sync.Mutex{},
		inodeCnt : INODE_INITIAL,
		db : db,
	}

	// create the metadata database
	if err = MetadataDbInit(gFsys); err != nil {
		return err
	}

	if err := fs.Serve(c, gFsys); err != nil {
		return err
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		return err
	}

	// extra debugging information from FUSE
	if gFsys.options.Debug {
		fuse.Debug = func(msg interface{}) {
			log.Print(msg)
		}
	}

	if gFsys.options.Debug {
		log.Printf("mounted dxfs2\n")
	}
	return nil
}

func Unmount(dirname string) error {
	// Close the sql database.
	//
	// If there is an error, we report it. There is nothing actionable
	// to do with it.
	//
	// We do not remove the metadata database file, so it could be inspected offline.
	if gFsys.options.Debug {
		log.Printf("unmounting dxfs2 from %s\n", dirname)
	}

	if err := gFsys.db.Close(); err != nil {
		log.Printf("Error closing the sqlite database %s, err=%s",
			gFsys.dbFullPath,
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

	return MetadataDbRoot(fsys)
}


func (dir *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	// this can be retained in cache indefinitely (a year is an approximation)
	a.Valid = time.Until(time.Unix(1000 * 1000 * 1000, 0))
	a.Inode = dir.Inode
	a.Size = 4096  // dummy size
	a.Blocks = 8
	a.Mode = os.ModeDir | 0555
	a.Nlink = 1
	a.Uid = dir.Fsys.uid
	a.Gid = dir.Fsys.uid
	a.BlockSize = 4 * 1024
	return nil
}

func (dir *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	dir.Fsys.mutex.Lock()
	defer dir.Fsys.mutex.Unlock()

	if dir.Fsys.options.Debug {
		log.Printf("ReadDirAll dir=%s\n", dir.FullPath)
	}

	files, subdirs, err := MetadataDbReadDirAll(dir.Fsys, dir.FullPath)
	if err != nil {
		return nil, err
	}

	if dir.Fsys.options.Debug {
		log.Printf("%d files, %d subdirs\n", len(files), len(subdirs))
	}

	dEntries := make([]fuse.Dirent, 0, len(files) + len(subdirs))

	// Add entries for files
	for filename, fDesc := range files {
		dEntries = append(dEntries, fuse.Dirent{
			Inode : fDesc.Inode,
			Type : fuse.DT_File,
			Name : filename,
		})
	}

	// Add entries for subdirs
	for subDirName, dirDesc := range subdirs {
		dEntries = append(dEntries, fuse.Dirent{
			Inode : dirDesc.Inode,
			Type : fuse.DT_Dir,
			Name : subDirName,
		})
	}

	// TODO: we need to add entries for '.' and '..'

	// directory entries need to be sorted
	sort.Slice(dEntries, func(i, j int) bool { return dEntries[i].Name < dEntries[j].Name })
	return dEntries, nil
}

var _ = fs.HandleReadDirAller(&Dir{})

var _ = fs.NodeRequestLookuper(&Dir{})

// We ignore the directory, because it is always the root of the filesystem.
func (dir *Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	if dir.Fsys.options.Debug {
		log.Printf("Lookup dir=%s filename=%s\n", dir.FullPath, req.Name)
	}

	// lookup in the database
	return MetadataDbLookupInDir(dir.Fsys, dir.FullPath, req.Name)
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Size = f.Size
	//log.Printf("Attr  size=%d\n", a.Size)

	// because the platform has only immutable files, these
	// timestamps are all the same
	a.Mtime = f.Mtime
	a.Ctime = f.Ctime
	a.Crtime = f.Ctime
	a.Mode = 0400 // read only access
	a.Nlink = 1
	a.Uid = f.Fsys.uid
	a.Gid = f.Fsys.gid
	//a.BlockSize = 1024 * 1024
	return nil
}

var _ = fs.NodeOpener(&File{})

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	// these files are read only
	if !req.Flags.IsReadOnly() {
		return nil, fuse.Errno(syscall.EACCES)
	}

	// create a download URL for this file
	const secondsInYear int = 60 * 60 * 24 * 365
	payload := fmt.Sprintf("{\"project\": \"%s\", \"duration\": %d}",
		f.ProjId, secondsInYear)

	body, err := DxAPI(&f.Fsys.dxEnv, fmt.Sprintf("%s/download", f.FileId), payload)
	if err != nil {
		return nil, err
	}
	var u DxDownloadURL
	json.Unmarshal(body, &u)

	fh := &FileHandle{
		f : f,
		url: u,
	}
	return fh, nil
}

var _ fs.Handle = (*FileHandle)(nil)

var _ fs.HandleReleaser = (*FileHandle)(nil)

func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	// nothing to do
	return nil
}

var _ = fs.HandleReader(&FileHandle{})

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	headers := make(map[string]string)

	// Copy the immutable headers
	for key, value := range fh.url.Headers {
		headers[key] = value
	}

	// add an extent in the file that we want to read
	endOfs := req.Offset + int64(req.Size) - 1
	headers["Range"] = fmt.Sprintf("bytes=%d-%d", req.Offset, endOfs)
	if fh.f.Fsys.options.Debug {
		log.Printf("Read  ofs=%d  len=%d\n", req.Offset, req.Size)
	}

	body,err := DxHttpRequest("GET", fh.url.URL, headers, []byte("{}"))
	if err != nil {
		return err
	}

	resp.Data = body
	return nil
}
