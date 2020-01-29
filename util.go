package dxfuse

import (
	"database/sql"
	"fmt"
	"log"
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
	NumRetriesDefault  = 3
	Version            = "v0.19"
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
const (
	// It turns out that in order for regular users to be able to create file,
	// we need to use 777 permissions for directories.
	dirReadOnlyMode = 0555 | os.ModeDir
	dirReadWriteMode = 0777 | os.ModeDir
	fileReadOnlyMode = 0444
	fileReadWriteMode = 0644
)

// A URL generated with the /file-xxxx/download API call, that is
// used to download file ranges.
type DxDownloadURL struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
}

type Options struct {
	ReadOnly            bool
	Verbose             bool
	VerboseLevel        int
	Uid                 uint32
	Gid                 uint32
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

	// a pool of http clients, for short requests, such as file creation,
	// or file describe.
	httpClientPool    chan(*retryablehttp.Client)

	// metadata database
	mdb *MetadataDb

	// prefetch state for all files
	pgs *PrefetchGlobalState

	// sync daemon
	sybx *SyncDbDx

	// API to dx
	ops *DxOps

	// all open files
	fhCounter uint64
	fhTable map[fuseops.HandleID]*FileHandle

	// all open directories
	dhCounter uint64
	dhTable map[fuseops.HandleID]*DirHandle

	nonce *Nonce
	tmpFileCounter uint64

	// is the the system shutting down (unmounting)
	shutdownCalled bool
}

// A node is a generalization over files and directories
type Node interface {
	GetInode() fuseops.InodeID
	GetAttrs() fuseops.InodeAttributes
}


// directories
type Dir struct {
	Parent      string  // the parent directory, used for debugging
	Dname       string  // This is the last part of the full path
	FullPath    string // combine parent and dname, then normalize
	Inode       int64
	Ctime       time.Time // DNAx does not record times per directory.
	Mtime       time.Time // we use the project creation time, and mtime as an approximation.
	Mode        os.FileMode  // uint32
	Uid         uint32
	Gid         uint32

	// extra information, used internally
	ProjId      string
	ProjFolder  string
	Populated   bool

	// is this a faux dir?
	faux        bool
}

func (d Dir) GetAttrs() (a fuseops.InodeAttributes) {
	a.Size = 4096
	a.Nlink = 1
	a.Mtime = d.Mtime
	a.Ctime = d.Ctime
	a.Mode = os.ModeDir | d.Mode
	a.Crtime = d.Ctime
	a.Uid = d.Uid
	a.Gid = d.Gid
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
	Kind       int     // Kind of object this is
	Id         string  // Required to build a download URL
	ProjId     string  // Note: this could be a container

	// One of {open, closing, closed}.
	// Closed is the only state where the file can be read
	State      string

	// One of {live, archival, archived, unarchiving}.
	// Live is the only state where the file can be read.
	ArchivalState string

	Name       string
	Size       int64
	Inode      int64
	Ctime      time.Time
	Mtime      time.Time
	Mode       os.FileMode  // uint32
	Nlink      int
	Uid        uint32
	Gid        uint32

	// tags and properties
	Tags       []string
	Properties map[string]string

	// for a symlink, it holds the path.
	// For a regular file, a path to a local copy (if any).
	InlineData string
}

func (f File) GetAttrs() (a fuseops.InodeAttributes) {
	a.Size = uint64(f.Size)
	a.Nlink = uint32(f.Nlink)
	a.Mtime = f.Mtime
	a.Ctime = f.Ctime
	a.Mode = f.Mode
	a.Crtime = f.Ctime
	a.Uid = f.Uid
	a.Gid = f.Gid
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
	hid fuseops.HandleID

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

// A handle used when operating on a filesystem
// operation. We normally need a transaction and an http client.
type OpHandle struct {
	httpClient *retryablehttp.Client
	txn        *sql.Tx
	err         error
}

func (oph *OpHandle) RecordError(err error) error {
	oph.err = err
	return err
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

func MaxInt(x, y int) int {
    if x < y {
        return y
    }
    return x
}

func MinInt(x, y int) int {
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

func Time2string(t time.Time) string {
	return fmt.Sprintf("%02d:%02d:%02d.%03d", t.Hour(), t.Minute(), t.Second(), t.Nanosecond()/1000000)
}

// add a timestamp and module name, to a log message
func LogMsg(moduleName string, a string, args ...interface{}) {
	msg := fmt.Sprintf(a, args...)
	now := time.Now()
	log.Printf("%s %s: %s", Time2string(now), moduleName, msg)
}


//
// 1024   => 1KB
// 10240  => 10KB
// 1100000 => 1MB
func BytesToString(numBytes int64) string {
	byteModifier := []string {"B", "KB", "MB", "GB", "TB", "EB", "ZB" }

	digits := make([]int, 0)
	for numBytes > 0 {
		d := int(numBytes % 1024)
		digits = append(digits, d)
		numBytes = numBytes / 1024
	}

	// which digit is the most significant?
	msd := len(digits)
	if msd >= len(byteModifier) {
		// This number is so large that we don't have a modifier
		// for it.
		return fmt.Sprintf("%dB", numBytes)
	}
	return fmt.Sprintf("%d%s", digits[msd], byteModifier[msd])
}
