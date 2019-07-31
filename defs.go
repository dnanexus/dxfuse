package dxfs2

import (
	"time"

	"bazil.org/fuse/fs"
	"github.com/dnanexus/dxda"
)

type Options struct {
	Debug bool
}


type Filesys struct {
	// configuration information for accessing dnanexus servers
	dxEnv dxda.DXEnvironment

	// various options
	options Options

	uid uint32
	gid uint32

	// the project being mounted
	projectId string

	// A file holding a sqllite database with all the files and
	// directories collected thus far.
	dbPath string

	// TODO: add an open handle for the sql DB
}

var _ fs.FS = (*Filesys)(nil)

type Dir struct {
	Fs    *Filesys
	Parent string  // the parent directory, used for debugging
	Dname  string  // This is the last part of the full path
	Inode  uint64
}

// Make sure that Dir implements the fs.Node interface
var _ fs.Node = (*Dir)(nil)


type File struct {
	Fs       *Filesys
	FileId    string  // Required to build a download URL
	ProjId    string  // -"-
	Name      string
	Size      uint64
	Mtime     time.Time
	Ctime     time.Time
	Inode     uint64
}

// Make sure that File implements the fs.Node interface
var _ fs.Node = (*File)(nil)
