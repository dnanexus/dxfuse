package dxfs2

import (
	"sync"
	"time"

	"bazil.org/fuse/fs"
	"github.com/dnanexus/dxda"
	_ "github.com/mattn/go-sqlite3"
)

const (
	MAX_DIR_SIZE int     = 10 * 1000
	INODE_ROOT_DIR int64 = 1
	INODE_INITIAL int64  = 10
	DB_PATH              = "/var/dxfs2"
	DB_NAME              = "metadata.db"
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
	dbFullPath string

	// Lock for protecting shared access to the database
	mutex sync.Mutex
	inodeCnt int64

	// an open handle to the database
	db  *SQLiteConn
}

var _ fs.FS = (*Filesys)(nil)

type Dir struct {
	Fsys  *Filesys
	Parent string  // the parent directory, used for debugging
	Dname  string  // This is the last part of the full path
	Inode  uint64
}

// Make sure that Dir implements the fs.Node interface
var _ fs.Node = (*Dir)(nil)


type File struct {
	Fsys     *Filesys
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
