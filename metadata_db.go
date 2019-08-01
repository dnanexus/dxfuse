package dxfs2

import (
	"database/sql"
	"fmt"
	"os"
	"sync"

	"github.com/dnanexus/dxda"
	_ "github.com/mattn/go-sqlite3"         // Following canonical example on go-sqlite3 'simple.go'
)


// Construct a local sql database that holds metadata for
// a large number of dx:files. This metadata_db will be consulted
// when performing dxfs2 operations. For example, a read-dir is
// translated into a query for all the files inside a directory.

// construct an initial empty database, representing an entire project.
func MetadataDbInit(
	dxEnv *dxda.DXEnvironment,
	projId string,
	dbFullPath string) error {

	if _, err := os.Stat(DB_PATH); os.IsNotExist(err) {
		os.Mkdir(path, mode)
	}
	// insert into an sqllite database
	os.Remove(dbFullPath)
	db, err := sql.Open("sqlite3", dbFullPath + "?cache=shared&mode=rwc")
	if err != nil {
		return "", err
	}
	defer db.Close()

	// Create table for files. The folders are represented as their full
	// path from the base of the project.
	sqlStmt := `
	CREATE TABLE files (
		file_id text,
		proj_id text,
		fname text,
		folder text PRIMARY KEY,
		size integer,
                ctime bigint,
                mtime bigint,
                inode bigint,
	);
	`
	if stmt, err = db.Prepare(sqlStmt); err != nil {
		return err
	}
	if _, err = stmt.Exec(); err != nil {
		return err
	}


	// Create a table for directories. The folder here is the parent directory.
	// For example, directory /A/B/C will be represented with record:
	//    dname="C"
	//    folder="/A/B"
	//
	// If the inode is -1, then, the directory does not exist on the platform.
	sqlStmt = `
	CREATE TABLE directories (
		proj_id text,
		dname text,
		folder text PRIMARY KEY,
                inode bigint,
	);
	`
	if _, err = db.Exec(sqlStmt); err != nil {
		return err
	}

	// Adding a root directory
	sqlStmt = fmt.Sprintf(`
 		        INSERT INTO directories
			VALUES ('%s', '%s', '%s', '%s', %d, '%d', '%d', '%d', '%d');
			`,
		projId, "/", "", INODE_ROOT_DIR)
	if _, err = db.Exec(sqlStmt); err != nil {
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
func MetadataDbReadDirAll(
	fsys *Filesys
	dirName string) (map[string]File, map[string]Dir, error) {

	if retval, err := directoryExists(fsys); err != nil {
		return err
	}
	if retval {
		// the directory already exists, read it, and return
		// all the entries.
		//
		// If the inode is -1, then, the directory does not
		// exist on the platform.
		return nil, nil, fuse.ENOENT
	}

	// The directory has not been queried yet.
	//
	// describe all the files
	dxDir, err := DxDescribeFolder(dxEnv, projectId, dirName)
	if err != nil {
		return "", err
	}

	// limit the number of files
	if len(dxDir.files) > MAX_DIR_SIZE {
		err := fmt.Errorf(
			"Too many files (%d) in a directory, the limit is %d",
			len(dxDir.fiels),
			MAX_DIR_SIZE)
		return "", err
	}


	// TODO: check for files with the same name, and modify their directories.
	// For example, if we have two version of file X.txt under directory foo,
	// then they should be renamed:
	//   foo/X.txt
	//      /1/X.txt

	if _, err = db.Exec("BEGIN TRANSACTION"); err != nil {
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
	return err
}

// Look for file [filename] in directory [parent]/[dname].
//
// 1. Look if the directory has already been downloaded and placed in the DB
// 2. If not, populate it
// 3. Do a lookup in the directory.
//
// Note: the file might not exist.
func MetadataDbLookupFileInDir(
	fsys *Filesys,
	parent string,
	dname string,
	filename string) (*File, error) {
	if files, subdirs, err := MetadataDbReadDirAll(fsys, parent + "/" + dname); err != nil {
		return nil, err
	}
	elem, ok := files[filename]
	if !ok {
		return nil, fuse.ENOENT
	}
	return elem, nil
}

// Return the root directory
func MetadataDbRoot(fsys Filesys) (*Dir, error) {
	return nil, nil
}
