package dxfuse

import (
	"database/sql"
	"sync"
	"time"

	"bazil.org/fuse/fs"
	"github.com/dnanexus/dxda"
	"github.com/hashicorp/go-retryablehttp"
)

const (
	DatabaseFile       = "/var/dxfuse/metadata.db"
	HttpClientPoolSize = 4
	LogFile            = "/var/log/dxfuse.log"
	MaxDirSize         = 10 * 1000
	Version            = "v0.11"
)
const (
	InodeInvalid       = 0
	InodeRoot          = 1
)
const (
	KiB                   = 1024
	MiB                   = 1024 * KiB
	GiB                   = 1024 * MiB
)

// A URL generated with the /file-xxxx/download API call, that is
// used to download file ranges.
type DxDownloadURL struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
}

type Options struct {
	DebugFuse      bool
	Verbose        bool
	VerboseLevel   int
	Uid            int
	Gid            int
}


type Filesys struct {
	// configuration information for accessing dnanexus servers
	dxEnv dxda.DXEnvironment

	// various options
	options Options

	uid uint32
	gid uint32

	// A file holding a sqlite3 database with all the files and
	// directories collected thus far.
	dbFullPath string

	// Lock for protecting shared access to the database
	mutex sync.Mutex
	inodeCnt int64

	// an open handle to the database
	db  *sql.DB

	// prefetch state for all files
	pgs PrefetchGlobalState

	httpClientPool chan(*retryablehttp.Client)
}

var _ fs.FS = (*Filesys)(nil)

type Dir struct {
	Fsys     *Filesys
	Parent    string  // the parent directory, used for debugging
	Dname     string  // This is the last part of the full path
	FullPath  string // combine parent and dname, then normalize
	Inode     int64
	Ctime     time.Time // DNAx does not record times per directory.
	Mtime     time.Time // we use the project creation time, and mtime as an approximation.
}

// Make sure that Dir implements the fs.Node interface
var _ fs.Node = (*Dir)(nil)


// Kinds of files
const (
	FK_Regular = 10
	FK_Symlink = 11
	FK_Applet = 12
	FK_Workflow = 13
	FK_Record = 14
	FK_Database = 15
	FK_Other = 16
)

// A Unix file can stand for any DNAx data object. For example, it could be a workflow or an applet.
// We distinguish between them based on the Id (file-xxxx, applet-xxxx, workflow-xxxx, ...).
type File struct {
	Fsys      *Filesys
	Kind       int     // Kind of object this is
	Id         string  // Required to build a download URL
	ProjId     string  // Note: this could be a container
	Name       string
	Size       int64
	Inode      int64
	Ctime      time.Time
	Mtime      time.Time
	Nlink      int
	InlineData string  // holds the path for a symlink.
}

// Make sure that File implements the fs.Node interface
var _ fs.Node = (*File)(nil)

// Files can be opened in read-only mode, or read-write mode.
const (
	RO_File = 1
	RW_File = 2
)

type FileHandle struct {
	fKind int
	f *File

	// URL used for downloading file ranges.
	// Used for read-only files.
	url *DxDownloadURL

	// temporary local file; used for read-write files, while
	// they are being updated.
	localPath *string
	stream
}

// Utility functions
func MaxInt64(x, y int64) int64 {
    if x < y {
        return y
    }
    return x
}

func MinInt64(x, y int64) int64 {
    if x > y {
        return y
    }
    return x
}
