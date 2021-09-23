package dxfuse

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/user"
	"time"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/shirou/gopsutil/process"
)

const (
	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
)
const (
	DatabaseFile = "metadata.db"
	LogFile      = "dxfuse.log"
)
const (
	HttpClientPoolSize        = 25
	FileWriteInactivityThresh = 5 * time.Minute
	MaxDirSize                = 255 * 1000
	MaxNumFileHandles         = 1000 * 1000
	NumRetriesDefault         = 10
	InitialPartSize           = 16 * MiB
	MaxPartSize               = 700 * MiB
	Version                   = "v1.0.0-rc.2"
)
const (
	InodeInvalid = 0
	InodeRoot    = fuseops.RootInodeID // This is an OS constant
)
const (
	// It turns out that in order for regular users to be able to create file,
	// we need to use 777 permissions for directories.
	dirReadOnlyMode   = 0555 | os.ModeDir
	dirReadWriteMode  = 0777 | os.ModeDir
	fileReadOnlyMode  = 0444
	fileWriteOnlyMode = 0222
)
const (
	// flags for writing files to disk
	DIRTY_FILES_ALL      = 14 // all modified files
	DIRTY_FILES_INACTIVE = 15 // only files there were unmodified recently
)

const (
	// Permissions
	PERM_VIEW       = 1
	PERM_UPLOAD     = 2
	PERM_CONTRIBUTE = 3
	PERM_ADMINISTER = 4
)

// A URL generated with the /file-xxxx/download API call, that is
// used to download file ranges.
type DxDownloadURL struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
}

type Options struct {
	ReadOnly     bool
	Verbose      bool
	VerboseLevel int
	Uid          uint32
	Gid          uint32
}

// A node is a generalization over files and directories
type Node interface {
	GetInode() fuseops.InodeID
	GetAttrs() fuseops.InodeAttributes
}

// directories
type Dir struct {
	Parent   string // the parent directory, used for debugging
	Dname    string // This is the last part of the full path
	FullPath string // combine parent and dname, then normalize
	Inode    int64
	Ctime    time.Time   // DNAx does not record times per directory.
	Mtime    time.Time   // we use the project creation time, and mtime as an approximation.
	Mode     os.FileMode // uint32
	Uid      uint32
	Gid      uint32

	// extra information, used internally
	ProjId     string
	ProjFolder string
	Populated  bool

	// is this a faux dir?
	faux bool
}

func (d Dir) GetAttrs() (a fuseops.InodeAttributes) {
	a.Size = 4096
	a.Nlink = 1
	a.Atime = d.Mtime
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
	FK_Regular  = 10
	FK_Symlink  = 11
	FK_Applet   = 12
	FK_Workflow = 13
	FK_Record   = 14
	FK_Database = 15
	FK_Other    = 16
)

// A Unix file can stand for any DNAx data object. For example, it could be a workflow or an applet.
// We distinguish between them based on the Id (file-xxxx, applet-xxxx, workflow-xxxx, ...).
//
// Note: this struct is immutable by convention. The up-to-date data is always on the database,
// not in memory.
type File struct {
	Kind   int    // Kind of object this is
	Id     string // Required to build a download URL
	ProjId string // Note: this could be a container

	// One of {open, closing, closed}.
	// Closed is the only state where the file can be read
	State string

	// One of {live, archival, archived, unarchiving}.
	// Live is the only state where the file can be read.
	ArchivalState string

	Name  string
	Size  int64
	Inode int64
	Ctime time.Time
	Mtime time.Time
	Mode  os.FileMode // uint32
	Uid   uint32
	Gid   uint32

	// tags and properties
	Tags       []string
	Properties map[string]string

	// for a symlink, it holds the path.
	Symlink string

	// is the file modified
	dirtyData     bool
	dirtyMetadata bool
}

func (f File) GetAttrs() (a fuseops.InodeAttributes) {
	a.Size = uint64(f.Size)
	a.Nlink = 1
	a.Atime = f.Mtime
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

// A file that is scheduled for removal
type DeadFile struct {
	Kind      int    // Kind of object this is
	Id        string // Required to build a download URL
	ProjId    string // Note: this could be a container
	Inode     int64
	LocalPath string
}

// Information required to upload file data to the platform.
// It also includes updated tags and properties of a data-object.
//
// Not that not only files have attributes, applets and workflows
// have them too.
//
type DirtyFileInfo struct {
	Inode         int64
	dirtyData     bool
	dirtyMetadata bool

	// will be "" for files created locally, and not uploaded yet
	Id         string
	FileSize   int64
	Mtime      int64
	LocalPath  string
	Tags       []string
	Properties map[string]string
	Name       string
	Directory  string
	ProjFolder string
	ProjId     string
}

// A handle used when operating on a filesystem
// operation. We normally need a transaction and an http client.
type OpHandle struct {
	httpClient *http.Client
	txn        *sql.Tx
	err        error
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
	byteModifier := []string{"B", "KB", "MB", "GB", "TB", "EB", "ZB"}

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

func check(value bool) {
	if !value {
		log.Panicf("assertion failed")
		os.Exit(1)
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func intToBool(x int) bool {
	if x > 0 {
		return true
	}
	return false
}

// create a directory for all the dxfuse files in  $HOME/.dxfuse
func MakeFSBaseDir() string {
	user, err := user.Current()
	if err != nil {
		log.Printf("error, could not describe the user")
		os.Exit(1)
	}
	dxfuseBaseDir := user.HomeDir + "/.dxfuse"
	if _, err := os.Stat(dxfuseBaseDir); os.IsNotExist(err) {
		os.Mkdir(dxfuseBaseDir, 0700)
	}
	return dxfuseBaseDir
}

func GetTgid(pid uint32) (tgid int32, err error) {
	p, err := process.NewProcess(int32(pid))
	if err != nil {
		return -1, err
	}
	tgid, err = p.Tgid()
	if err != nil {
		return -1, err
	}
	return tgid, nil
}
