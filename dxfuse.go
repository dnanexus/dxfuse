package dxfuse

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/dnanexus/dxda"
	"golang.org/x/net/context"
)

var progName = filepath.Base(os.Args[0])

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", progName)
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", progName)
	flag.PrintDefaults()
}

func main() {
	log.SetFlags(0)
	log.SetPrefix(progName + ": ")

	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 1 {
		usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)

	dxEnv, method, err := dxda.GetDxEnvironment()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf(fmt.Sprintf("Obtained token using %s\n", method))

	if err := mount(mountpoint, dxEnv); err != nil {
		log.Fatal(err)
	}
}

func mount(mountpoint string, dxEnv dxda.DXEnvironment) error {
	c, err := fuse.Mount(mountpoint, fuse.AllowOther())
	if err != nil {
		return err
	}
	defer c.Close()

	filesys := &FS{
		dxEnv : dxEnv
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

//
type FS struct {
	dxEnv dxda.DXEnvironment
}

type Dir struct {
	dxEnv *dxda.DXEnvironment,
	path   string,
}

type File struct {
	dxEnv    *dxda.DXEnvironment
	ProjId    string
	FileId    string
	Size      uint64
	Ctime     time.Time  // creation time
}

var _ fs.FS = (*FS)(nil)

func (f *FS) Root() (fs.Node, error) {
	n := &Dir{
		dxEnv : &f.dxEnv,
		path : "/"
	}
	return n, nil
}

// Make sure that Dir implements the fs.Node interface
var _ fs.Node = (*Dir)(nil)

// we don't support directory operations
func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	return fuse.ENOSYS
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	return fuse.ENOSYS
}

var _ = fs.HandleReadDirAller(&Dir{})

var _ = fs.NodeRequestLookuper(&Dir{})

// We ignore the directory, because it is always the root of the filesystem.
func (d *Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	basename := req.Name

	// check that the name is of the form project-xxxx:file-yyyy
	parts := strings.Split(basename)
	if parts.length != 2 {
		return nil, fuse.ENOENT
	}
	projId := parts[0]
	if !(projId.HashPrefix("project-")) {
		return nil, fuse.ENOENT
	}
	fileId := parts[1]
	if !(fileId.HashPrefix("file-")) {
		return nil, fuse.ENOENT
	}

	// describe the file
	desc := utils.Describe(d.dxEnv, projId, fileId)
	child := &File{
		ProjId: desc.ProjId,
		FileId: desc.FileId,
		Size : desc.Size,
		Ctime : desc.Created
	}
	return child, nil
}

var _ fs.Node = (*File)(nil)

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Size = f.Size
	a.Mode = 0400 // read only access

	// because the platform has only immutable files, these
	// timestamps are all the same
	a.Mtime = f.Ctime
	a.Ctime = f.Ctime
	a.Crtime = f.Ctime
}

var _ = fs.NodeOpener(&File{})

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	r, err := f.file.Open()
	if err != nil {
		return nil, err
	}
	// these files are read only
	resp.Flags |= fuse.OpenReadOnly
	return &FileHandle{r: r}, nil
}

type FileHandle struct {
	r *File
}

var _ fs.Handle = (*FileHandle)(nil)

var _ fs.HandleReleaser = (*FileHandle)(nil)

func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	// nothing to do
}

var _ = fs.HandleReader(&FileHandle{})

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	headers := make(map[string]string)
	headers["Range"] = fmt.Sprintf("bytes=%d-%d", req.Offset, req.Offset + req.Size - 1)

	url = fmt.Sprintf("%s/download", fs.r.FileId)
	_, body := makeRequest("GET", url, headers,  []byte("{}"))

	resp.Data = body
	return nil
}
