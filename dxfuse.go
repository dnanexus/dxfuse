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

var _ fs.FS = (*FS)(nil)

func (f *FS) Root() (fs.Node, error) {
	n := &Dir{
		path : "/"
		dxEnv : &f.dxEnv
	}
	return n, nil
}

type Dir struct {
	path string,
	dxEnv *dxda.DXEnvironment
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
		return nil, ENOENT
	}
	projId := parts[0]
	if !(projId.HashPrefix("project-")) {
		return nil, ENOENT
	}
	fileId := parts[1]
	if !(fileId.HashPrefix("file-")) {
		return nil, ENOENT
	}

	// describe the file
	utils.Describe(d.dxEnv, projId, fileId)
	child := &File{
		projectId: projId,
		fileId: fileId
	}
	return child, nil
}

type File struct {
	fileId    string
	projectId string
	Size      uint64
	Ctime     time.Time  // creation time
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
	// individual entries inside a zip file are not seekable
	resp.Flags |= fuse.OpenNonSeekable
	return &FileHandle{r: r}, nil
}

type FileHandle struct {
	r io.ReadCloser
}

var _ fs.Handle = (*FileHandle)(nil)

var _ fs.HandleReleaser = (*FileHandle)(nil)

func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return fh.r.Close()
}

var _ = fs.HandleReader(&FileHandle{})

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	// We don't actually enforce Offset to match where previous read
	// ended. Maybe we should, but that would mean'd we need to track
	// it. The kernel *should* do it for us, based on the
	// fuse.OpenNonSeekable flag.
	//
	// One exception to the above is if we fail to fully populate a
	// page cache page; a read into page cache is always page aligned.
	// Make sure we never serve a partial read, to avoid that.
	buf := make([]byte, req.Size)
	n, err := io.ReadFull(fh.r, buf)
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		err = nil
	}
	resp.Data = buf[:n]
	return err
}
