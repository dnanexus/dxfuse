package dxfuse

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/user"
	"strconv"
	"strings"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	// The dxda package has the get-environment code
	"github.com/dnanexus/dxda"

	"golang.org/x/net/context"
)

//
type FS struct {
	// configuration information for accessing dnanexus servers
	dxEnv dxda.DXEnvironment

	// overall project-id. Convenience variable, we'll git rid of this
	// limitation shortly
	projId string

	// Fixed list of file ids that are exposed by this mount point.
	fileIds []string

	uid uint32
	gid uint32
}

type Dir struct {
	fs    *FS
	path   string
}

type File struct {
	fs       *FS
	projId    string
	fileId    string
	size      uint64
	ctime     time.Time  // creation time
}

// A URL generated with the /file-xxxx/download API call, that is
// used to download file ranges.
type DXDownloadURL struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
}

type FileHandle struct {
	f *File

	// URL used for downloading file ranges
	url DXDownloadURL
}


// Mount the filesystem:
//  - setup the debug log to the FUSE kernel log (I think)
//  - mount as read-only
func Mount(mountpoint string, dxEnv dxda.DXEnvironment, projId string, fileIds []string) error {
	log.Printf("mounting dxfuse\n")
	c, err := fuse.Mount(mountpoint, fuse.AllowOther(), fuse.ReadOnly(),
		fuse.MaxReadahead(16 * 1024 * 1024), fuse.AsyncRead())
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

	// input sanity checks
	if !(strings.HasPrefix(projId, "project-")) {
		return errors.New("This isn't a project " + projId)
	}

	filesys := &FS{
		dxEnv : dxEnv,
		projId : projId,
		fileIds : fileIds,
		uid : uint32(uid),
		gid : uint32(gid),
	}
	if err := fs.Serve(c, filesys); err != nil {
		return err
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		return err
	}

	return nil
}

var _ fs.FS = (*FS)(nil)

func (f *FS) Root() (fs.Node, error) {
	log.Printf("Get root directory\n")
	n := &Dir{
		fs : f,
		path : "/",
	}
	return n, nil
}

// Make sure that Dir implements the fs.Node interface
var _ fs.Node = (*Dir)(nil)


// We only support the root directory
func (dir *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	if dir.path != "/" {
		return fuse.ENOSYS;
	}
	// this can be retained in cache indefinitely (a year is an approximation)
	a.Valid = time.Until(time.Unix(1000 * 1000 * 1000, 0))
	a.Inode = 1
	a.Size = 4096  // dummy size
	a.Blocks = 8
	a.Atime = time.Now()
	a.Mtime = time.Now()
	a.Ctime = time.Now()
	a.Mode = os.ModeDir | 0777
	a.Nlink = 1
	a.Uid = dir.fs.uid
	a.Gid = dir.fs.uid
	a.BlockSize = 4 * 1024
	return nil
}

func (dir *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	log.Printf("ReadDirAll dir=%s\n", dir.path)

	// create a directory entry for each of the file ids
	dEntries := make([]fuse.Dirent, len(dir.fs.fileIds))
	for index, fid := range dir.fs.fileIds {
		dEntries[index].Inode = uint64(index + 10)
		dEntries[index].Type = fuse.DT_File
		dEntries[index].Name = fid
	}
	return dEntries, nil
}

var _ = fs.HandleReadDirAller(&Dir{})

var _ = fs.NodeRequestLookuper(&Dir{})

// We ignore the directory, because it is always the root of the filesystem.
func (dir *Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	log.Printf("Lookup dir=%s  filename=%s\n", dir.path, req.Name)

	// describe the file
	desc, err := Describe(&dir.fs.dxEnv, dir.fs.projId, req.Name)
	if err != nil {
		return nil, err
	}
	child := &File{
		fs: dir.fs,
		projId: desc.ProjId,
		fileId: desc.FileId,
		size : desc.Size,
		ctime : desc.Created,
	}
	return child, nil
}

var _ fs.Node = (*File)(nil)

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Size = f.size

	// because the platform has only immutable files, these
	// timestamps are all the same
	a.Mtime = f.ctime
	a.Ctime = f.ctime
	a.Crtime = f.ctime
	a.Mode = 0400 // read only access
	a.Nlink = 1
	a.Uid = f.fs.uid
	a.Gid = f.fs.uid
	a.BlockSize = 1024 * 1024
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
		f.projId, secondsInYear)

	body, err := DXAPI(&f.fs.dxEnv, fmt.Sprintf("%s/download", f.fileId), payload)
	if err != nil {
		return nil, err
	}
	var u DXDownloadURL
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
	log.Printf("Read  ofs=%d  len=%d\n", req.Offset, req.Size)

	reqUrl := fh.url.URL + "/" + fh.f.projId
	body,err := MakeRequest("GET", reqUrl, headers, []byte("{}"))
	if err != nil {
		return err
	}

	resp.Data = body
	return nil
}
