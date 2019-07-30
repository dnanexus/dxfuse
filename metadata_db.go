package dxfs2

import (
	"database/sql"
	"fmt"
	"os"
	"sync"

	"database/sql"

	"github.com/dnanexus/dxda"
	_ "github.com/mattn/go-sqlite3"         // Following canonical example on go-sqlite3 'simple.go'
)

// We need some limit of the number of objects, because we are downloading
// all of this metadata from the platform before mounting the filesystem.
// This is initial guess at a number that is big enough to be useful, but
// not so large, as to be prohibitive.
//
// We need to download the file metadata because we must make sure that there
// are no POSIX violations, and, we need to disambiguate versions of the same file.
const (
	MAX_DIR_SIZE int     = 10 * 1000
	INODE_ROOT_DIR int64 = 1
	INODE_INITIAL int64  = 10
	KIND_FILE            = 1
	KIND_DIR             = 2
	KIND_OTHER           = 3
)

var mutex = &sync.Mutex{}
var inodeCnt = INODE_INITIAL

// Construct a local sql database that holds metadata for
// a large number of dx:files. This metadata_db will be consulted
// when performing dxfs2 operations. For example, a read-dir is
// translated into a query for all the files inside a directory.

// construct an initial empty database, representing an entire project.
func MetadataDbInit(
	dxEnv *dxda.DXEnvironment
	projId string
	dbFname string) error {

	// insert into an sqllite database
	os.Remove(dbFname)
	db, err := sql.Open("sqlite3", dbFname + ".db?cache=shared&mode=rwc")
	if err != nil {
		return "", err
	}
	defer db.Close()

	// Create table for files and subdirectories. In Unix, "everything is a file",
	// so, we treating directories as special files. The 'kind' field
	// denotes if this is a file, directory, or otherwise.
	//
	// The encoding is
	// 1 : File
	// 2 : Directory
	// 3 : Other
	//
	sqlStmt := `
	CREATE TABLE files (
		file_id text,
		proj_id text,
		name text,
		folder text,
		size integer,
                ctime bigint,
                mtime bigint,
                inode bigint,
                kind  int
	);
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		return err
	}

	// Add a root directory
	_, err = db.Exec("BEGIN TRANSACTION")
	if err != nil {
		return err
	}

	// TODO: describe the project, and get the mtime, ctime
	sqlStmt = fmt.Sprintf(`
 		        INSERT INTO files
			VALUES ('%s', '%s', '%s', '%s', %d, '%d', '%d', '%d', '%d');
			`,
		"", projId, "/", "", 4096, 0, 0, INODE_ROOT_DIR, DIR_TYPE)
	_, err = db.Exec(sqlStmt)
	if err != nil {
		return err
	}
	_, err = db.Exec("END TRANSACTION")
	if err != nil {
		return err
	}
	return nil
}

// Allocate an inode number. These must remain stable during the
// lifetime of the mount.
//
// Note: this call should perform while holding the mutex
func getInodeNum() int64 {
	inodeCnt++
	return inodeCnt
}

// Add a directory with its contents to an exisiting database
func MetadataDbAddDir(
	dxEnv *dxda.DXEnvironment,
	dbFname string,
	projectId string,
	dirName string) error {

	// limit the number of files
	if len(fileIds) > MAX_DIR_SIZE {
		err := fmt.Errorf("Too many objects (%d), the limit is %d", len(descs), MAX_DIR_SIZE)
		return "", err
	}

	// describe all the files
	dxDir, err := DxDescribeFolder(dxEnv, projectId, dirName)
	if err != nil {
		return "", err
	}

	// TODO: check for files with the same name, and modify their directories.
	// For example, if we have two version of file X.txt under directory foo,
	// then they should be renamed:
	//   foo/X.txt
	//      /1/X.txt

	// TODO: check for posix violations

	// This may be overly restrictive, but we are locking the database
	// here, so there could be no conflicting write accesses.
	mutex.Lock()
	defer mutex.Unlock()

	// insert into an sqllite database
	db, err := sql.Open("sqlite3", dbFname + ".db?cache=shared&mode=rwc")
	if err != nil {
		return "", err
	}
	defer db.Close()
	sqlStmt := `
	CREATE TABLE files (
		file_id text,
		proj_id text,
		name text,
		folder text,
		size integer,
                ctime bigint,
                mtime bigint,
                inode bigint
	);
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		return "", err
	}

	_, err = db.Exec("BEGIN TRANSACTION")
	if err != nil {
		return "", err
	}

	// Create a database entry for each file
	for _, d := range dxDir.files {
		inode := getInodeNum()
		sqlStmt = fmt.Sprintf(`
 		        INSERT INTO files
			VALUES ('%s', '%s', '%s', '%s', %d, '%d', '%d', '%d', '%d');
			`,
			d.FileId, d.ProjId, d.Name, d.Folder, d.Size, d.Ctime, d.Mtime, inode, KIND_FILE)
		_, err = db.Exec(sqlStmt)
		if err != nil {
			return "", err
		}
	}

	// Create a database entry for each sub-directory
	for _, subDirName := range dxDir.subdirs {
		inode := getInodeNum()

		// DNAx stores directories as full paths. For example: /A/B has
		// as subdirectories  "A/B/C", "A/B/D", "A/B/KKK". A POSIX
		// filesystem represents these as: "A/B", {"C", "D", "KKK"}
		//
		subDirLastPart := strings.trimPrefix(subDirName, dirName)
		sqlStmt = fmt.Sprintf(`
 		        INSERT INTO files
			VALUES ('%s', '%s', '%s', '%s', %d, '%d', '%d', '%d', '%d');
			`,
			"", // folders don't have an object-id
			projectId,
			subDirLastPart,
			dirName,
			4096,
			0,  // folders don't have a creation time
			0,  // -"-     don't have a modification time
			inode,
			KIND_FILE)
		_, err = db.Exec(sqlStmt)
		if err != nil {
			return "", err
		}
	}

	_, err = db.Exec("END TRANSACTION")
	if err != nil {
		return "", err
	}
	return dbName, nil
}

func LookupRoot()
