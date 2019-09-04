package dxfs2

import (
	"database/sql"
	"sync"
	"time"

	"bazil.org/fuse/fs"
	"github.com/dnanexus/dxda"
	"github.com/hashicorp/go-retryablehttp"
)

const (
	MAX_DIR_SIZE int      = 10 * 1000
	INODE_ROOT_DIR int64  = 1
	INODE_INITIAL int64   = 10
	HTTP_CLIENT_POOL_SIZE = 4
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

type DxDescribePrj struct {
	Id           string
	Name         string
	Region       string
	Version      int
	DataUsageGiB float64
	Ctime        time.Time
	Mtime        time.Time
}

// a DNAx directory. It holds files and sub-directories.
type DxFolder struct {
	path  string  // Full directory name, for example: { "/A/B/C", "foo/bar/baz" }
	files map[string]DxDescribe
	subdirs []string
}


type Options struct {
	DebugFuse      bool
	MetadataDbPath string
	Verbose        bool
	VerboseLevel   int
}


type Filesys struct {
	// configuration information for accessing dnanexus servers
	dxEnv dxda.DXEnvironment

	// various options
	options Options

	uid uint32
	gid uint32

	// the project being mounted
	project *DxDescribePrj

	// A file holding a sqllite database with all the files and
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

type FileHandle struct {
	f *File

	// URL used for downloading file ranges
	url DxDownloadURL
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
