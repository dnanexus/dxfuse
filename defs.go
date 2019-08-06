package dxfs2

import (
	"database/sql"
	"sync"
	"time"

	"bazil.org/fuse/fs"
	"github.com/dnanexus/dxda"
)

const (
	MAX_DIR_SIZE int     = 10 * 1000
	INODE_ROOT_DIR int64 = 1
	INODE_INITIAL int64  = 10
	DB_PATH              = "/var/dxfs2/metadata.db"
)

// Description of a DNAx data object
type DxDescribe struct {
	FileId    string
	ProjId    string
	Name      string
	Folder    string
	Size      uint64
	Ctime     time.Time
	Mtime     time.Time
}

// a DNAx directory. It holds files and sub-directories.
type DxFolder struct {
	path  string  // Full directory name, for example: { "/A/B/C", "foo/bar/baz" }
	files map[string]DxDescribe
	subdirs []string
}


type Options struct {
	Debug bool
	DebugFuse bool
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
	dbFullPath string

	// Lock for protecting shared access to the database
	mutex sync.Mutex
	inodeCnt int64

	// an open handle to the database
	db  *sql.DB
}

var _ fs.FS = (*Filesys)(nil)

type Dir struct {
	Fsys  *Filesys
	Parent string  // the parent directory, used for debugging
	Dname  string  // This is the last part of the full path
	FullPath string // combine parent and dname, then normalize
	Inode  int64
}

// Make sure that Dir implements the fs.Node interface
var _ fs.Node = (*Dir)(nil)


type File struct {
	Fsys     *Filesys
	FileId    string  // Required to build a download URL
	ProjId    string  // -"-
	Name      string
	Size      int64
	Inode     int64
	Ctime     time.Time
	Mtime     time.Time
}

// Make sure that File implements the fs.Node interface
var _ fs.Node = (*File)(nil)
