package dxfs2

import (
	"database/sql"
	"fmt"
	"os"

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
	MAX_DIR_SIZE int    = 10 * 1000
	INODE_INITIAL int64 = 10
	FILE_TYPE           = 1
	DIR_TYPE            = 2
	OTHER_TYPE          = 3
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
	dbName := dbFname + ".db?cache=shared&mode=rwc"
	os.Remove(dbName)
	db, err := sql.Open("sqlite3", dbName)
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
		return "", err
	}

	// Add a root directory
	_, err = db.Exec("BEGIN TRANSACTION")
	if err != nil {
		return "", err
	}

	// TODO: describe the project, and the mtime, ctime
	sqlStmt = fmt.Sprintf(`
 		        INSERT INTO files
			VALUES ('%s', '%s', '%s', '%s', %d, '%d', '%d', '%d', '%d');
			`,
		"", projId, "/", "", 4096, 0, 0, getInodeNum(), DIR_TYPE)
	_, err = db.Exec(sqlStmt)
	if err != nil {
		return "", err
	}
	_, err = db.Exec("END TRANSACTION")
	if err != nil {
		return "", err
	}
	return dbName, nil
}

func getInodeNum() int64 {
	mutex.Lock()
	inodeCnt++
	mutex.Unlock()
	return inodeCnt
}

// an entire range of inodes
func getInodeRange(r int) int64 {
	assert(r > 0)
	mutex.Lock()
	retval := inodeCnt + 1
	inodeCnt += int64(r)
	mutex.Unlock()
	return retval
}

// Add a directory with its contents to an exisiting database
func MetadataDbAddDir(
	dxEnv *dxda.DXEnvironment,
	dbFname string,
	dirName string) error {

	// limit the number of files
	if len(fileIds) > DB_NUM_OBJECTS_LIMIT {
		err := fmt.Errorf("Too many objects (%d), the limit is %d", len(descs), DB_NUM_OBJECTS_LIMIT)
		return "", err
	}

	// describe all the files
	descs, err := DescribeBulk(dxEnv, fileIds)
	if err != nil {
		return "", err
	}

	// TODO: check for files with the same name, and modify their directories.
	// For example, if we have two version of file X.txt under directory foo,
	// then they should be renamed:
	//   foo/X.txt
	//      /1/X.txt

	// TODO: check for posix violations

	// insert into an sqllite database
	dbName := dbFname + ".db?cache=shared&mode=rwc"
	os.Remove(dbName)
	db, err := sql.Open("sqlite3", dbName)
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

	inodeCnt := BASE_FILE_INODE
	for _, d := range descs {
		sqlStmt = fmt.Sprintf(`
 		        INSERT INTO files
			VALUES ('%s', '%s', '%s', '%s', %d, '%d', '%d');
			`,
			d.FileId, d.ProjId, d.Name, d.Folder, d.Size, d.Ctime, d.Mtime, inodeCnt)
		_, err = db.Exec(sqlStmt)
		if err != nil {
			return "", err
		}
		inodeCnt++
	}
	_, err = db.Exec("END TRANSACTION")
	if err != nil {
		return "", err
	}
	return dbName, nil
}
