package dxfuse

import (
	"encoding/json"
	"fmt"
	"log"
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
	dxEnv dxda.DXEnvironment
}

type Dir struct {
	dxEnv *dxda.DXEnvironment
	path   string
}

type File struct {
	dxEnv    *dxda.DXEnvironment
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
	url DXDownloadURL
}


// Mount the filesystem:
//  - setup the debug log to the FUSE kernel log (I think)
//  - mount as read-only
func Mount(mountpoint string, dxEnv dxda.DXEnvironment) error {
	log.Printf("mounting dxfuse\n")
	c, err := fuse.Mount(mountpoint, fuse.AllowOther(), fuse.ReadOnly())
	if err != nil {
		return err
	}
	defer c.Close()

	filesys := &FS{
		dxEnv : dxEnv,
	}
	if err := fs.Serve(c, filesys); err != nil {
		return err
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		return err
	}

	// write out to the FUSE log
	fuse.Debug = func(msg interface{}) {
		log.Print(msg)
	}

	return nil
}

var _ fs.FS = (*FS)(nil)

func (f *FS) Root() (fs.Node, error) {
	log.Printf("Get root directory\n")
	n := &Dir{
		dxEnv : &f.dxEnv,
		path : "/",
	}
	return n, nil
}

// Make sure that Dir implements the fs.Node interface
var _ fs.Node = (*Dir)(nil)

// we don't support directory operations
func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	// We only support the root directory
	if d.path == "/" {
		a.Size = 4096
		a.Mode = 755
		a.Mtime = time.Now()
		a.Ctime = time.Now()
		return nil
	} else {
		return fuse.ENOSYS
	}
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	log.Printf("ReadDirAll dir=%s\n", d.path)
	return nil, fuse.ENOSYS
}

var _ = fs.HandleReadDirAller(&Dir{})

var _ = fs.NodeRequestLookuper(&Dir{})

// We ignore the directory, because it is always the root of the filesystem.
func (d *Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	log.Printf("Lookup dir=%s  filename=%s\n", d.path, req.Name)
	basename := req.Name

	// check that the name is of the form project-xxxx:file-yyyy
	parts := strings.Split(basename, ":")
	if parts == nil || len(parts) != 2 {
		return nil, fuse.ENOENT
	}
	projId := parts[0]
	if !(strings.HasPrefix(projId, "project-")) {
		return nil, fuse.ENOENT
	}
	fileId := parts[1]
	if !(strings.HasPrefix(fileId, "file-")) {
		return nil, fuse.ENOENT
	}

	// describe the file
	desc,err := Describe(d.dxEnv, projId, fileId)
	if err != nil {
		return nil, err
	}
	child := &File{
		dxEnv: d.dxEnv,
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
	a.Mode = 0400 // read only access

	// because the platform has only immutable files, these
	// timestamps are all the same
	a.Mtime = f.ctime
	a.Ctime = f.ctime
	a.Crtime = f.ctime
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

	body, err := DXAPI(f.dxEnv, fmt.Sprintf("%s/download", f.fileId), payload)
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
	headers["Range"] = fmt.Sprintf("bytes=%d-%d", req.Offset, req.Offset + int64(req.Size) - 1)

	reqUrl := fh.url.URL + "/" + fh.f.projId
	body,err := MakeRequest("GET", reqUrl, headers, []byte("{}"))
	if err != nil {
		return err
	}

	resp.Data = body
	return nil
}
