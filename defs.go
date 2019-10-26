package dxfuse

import (
	"database/sql"
	"os"
	"sync"
	"time"

	"github.com/dnanexus/dxda"
	"github.com/hashicorp/go-retryablehttp"

	"github.com/jacobsa/fuse/fuseutil"
	"github.com/jacobsa/fuse/fuseops"
)

const (
	CreatedFilesDir = "/var/dxfuse/created_files"
	DatabaseFile       = "/var/dxfuse/metadata.db"
	HttpClientPoolSize = 4
	LogFile            = "/var/log/dxfuse.log"
	MaxDirSize         = 10 * 1000
	MaxNumFileHandles  = 1000 * 1000
	Version            = "v0.12"
)
const (
	InodeInvalid       = 0
	InodeRoot          = fuseops.RootInodeID  // This is an OS constant
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
	ReadOnly       bool
	Verbose        bool
	VerboseLevel   int
	Uid            uint32
	Gid            uint32
}


type Filesys struct {
	// inherit empty implementations for all the filesystem
	// methods we do not implement
	fuseutil.NotImplementedFileSystem

	// configuration information for accessing dnanexus servers
	dxEnv dxda.DXEnvironment

	// various options
	options Options

	// A file holding a sqlite3 database with all the files and
	// directories collected thus far.
	dbFullPath string

	// Lock for protecting shared access to the database
	mutex sync.Mutex
	inodeCnt int64

	// an open handle to the database
	db  *sql.DB

	// prefetch state for all files
	pgs *PrefetchGlobalState

	// background upload state
	fugs *FileUploadGlobalState

	// a pool of http clients, for short requests, such as file creation,
	// or file describe.
	httpClientPool chan(*retryablehttp.Client)

	// mapping from mounted directory to project ID
	baseDir2ProjectId map[string]string
	projId2Desc map[string]DxDescribePrj

	// all open files
	fhTable map[fuseops.HandleID]*FileHandle
	fhFreeList []fuseops.HandleID

	// all open directories
	dhTable map[fuseops.HandleID]*DirHandle
	dhFreeList []fuseops.HandleID

	nonce *Nonce
	tmpFileCounter uint64
}

// A node is a generalization over files and directories
type Node interface {
	GetInode() fuseops.InodeID
	Attrs(fsys *Filesys) fuseops.InodeAttributes
}


// directories
type Dir struct {
	Fsys     *Filesys
	Parent    string  // the parent directory, used for debugging
	Dname     string  // This is the last part of the full path
	FullPath  string // combine parent and dname, then normalize
	Inode     int64
	Ctime     time.Time // DNAx does not record times per directory.
	Mtime     time.Time // we use the project creation time, and mtime as an approximation.
}

func (d Dir) Attrs(fsys *Filesys) (a fuseops.InodeAttributes) {
	a.Size = 4096
	a.Nlink = 1
	a.Mode = os.ModeDir | 0555
	a.Mtime = a.Mtime
	a.Ctime = a.Ctime
	a.Crtime = a.Ctime
	a.Uid = fsys.options.Uid
	a.Gid = fsys.options.Gid
	return
}

func (d Dir) GetInode() fuseops.InodeID {
	return fuseops.InodeID(d.Inode)
}


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

	// for a symlink, it holds the path.
	// For a regular file, a path to a local copy (if any).
	InlineData string
}

func (f File) Attrs(fsys *Filesys) (a fuseops.InodeAttributes) {
	a.Size = uint64(f.Size)
	a.Nlink = uint32(f.Nlink)
	a.Mode = 0444   // What about files in write mode?
	a.Mtime = a.Mtime
	a.Ctime = a.Ctime
	a.Crtime = a.Ctime
	a.Uid = fsys.options.Uid
	a.Gid = fsys.options.Gid
	return
}

func (f File) GetInode() fuseops.InodeID {
	return fuseops.InodeID(f.Inode)
}


// Files can be opened in read-only mode, or read-write mode.
const (
	RO_Remote = 1     // read only file that is on the cloud
	RW_File = 2       // newly created file
	RO_LocalCopy = 3  // read only file that has a local copy
)

type FileHandle struct {
	fKind int
	f File

	// URL used for downloading file ranges.
	// Used for read-only files.
	url *DxDownloadURL

	// Local file copy, may be empty.
	localPath *string

	// 1. Used for reading from an immutable local copy
	// 2. Used for writing to newly created files.
	fd *os.File
}

type DirHandle struct {
	d Dir
	entries []fuseutil.Dirent
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

// convert time in seconds since 1-Jan 1970, to the equivalent
// golang structure
func SecondsToTime(t int64) time.Time {
	return time.Unix(t, 0)
}
