package dxfs2

import (
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)


// Construct a local sql database that holds metadata for
// a large number of dx:files. This metadata_db will be consulted
// when performing dxfs2 operations. For example, a read-dir is
// translated into a query for all the files inside a directory.

// construct an initial empty database, representing an entire project.
func MetadataDbInit(fsys *Filesys) error {
	if fsys.options.Debug {
		log.Printf("Initializing metadata database\n")
	}

	if _, err := fsys.db.Exec("BEGIN TRANSACTION"); err != nil {
		return err
	}

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
                inode bigint
	);
	`
	if _, err := fsys.db.Exec(sqlStmt); err != nil {
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
                inode bigint
	);
	`
	if _, err := fsys.db.Exec(sqlStmt); err != nil {
		return err
	}

	// Adding a root directory
	sqlStmt = fmt.Sprintf(`
 		        INSERT INTO directories
			VALUES ('%s', '%s', '%s', '%d');
			`,
		fsys.projectId, "/", "", INODE_ROOT_DIR)
	if _, err := fsys.db.Exec(sqlStmt); err != nil {
		return err
	}

	if _, err := fsys.db.Exec("END TRANSACTION"); err != nil {
		return err
	}

	if fsys.options.Debug {
		log.Printf("Completed creating files and directories tables\n")
	}
	return nil
}

// Allocate an inode number. These must remain stable during the
// lifetime of the mount.
//
// Note: this call should perform while holding the mutex
func allocInodeNum(fsys *Filesys) int64 {
	fsys.inodeCnt++
	return fsys.inodeCnt
}

// Three options:
// 1. Directory has been read from DNAx
// 2. Directory does not exist on DNAx
// 3. Directory has been fully read, and is in the database
func directoryExists(fsys *Filesys, dirFullName string) (bool, error) {
	// split the directory into parent into last part:
	// "A/B/C" ---> "A/B", "C"
	parentFolder := filepath.Dir(dirFullName)
	base := filepath.Base(dirFullName)

	sqlStmt := fmt.Sprintf(`
 		        SELECT inode FROM directories
			WHERE folder = %s
                        AND dname = %s;
			`,
		parentFolder, base)
	rows, err := fsys.db.Query(sqlStmt);
	if err != nil {
		return false, err
	}

	// There could be at most one such entry
	var inode int64
	numRows := 0
	for rows.Next() {
		rows.Scan(&inode)
		numRows++
	}
	rows.Close()

	switch numRows {
	case 0:
		// The directory has not been read from from DNAx
		return false, nil
	case 1:
		// The directory has already been read
		if inode == -1 {
			// There is no such directory on the platform
			return true, fuse.ENOENT
		}
		// The directory exists on DNAx
		return true, nil
	default:
		err = fmt.Errorf(
			"Two many values returned from db query, zero or one are expected, received %d",
			numRows)
		return true, err
	}
}

// The directory is in the database, read it in its entirety.
func directoryReadAllEntries(
	fsys * Filesys,
	dirFullName string) (map[string]File, map[string]Dir, error) {

	// Find the subdirectories
	sqlStmt := fmt.Sprintf(`
 		        SELECT dname,inode FROM directories
			WHERE folder = %s;
			`,
		dirFullName)
	rows, err := fsys.db.Query(sqlStmt)
	if err != nil {
		return nil, nil, err
	}

	subdirs := make(map[string]Dir)
	for rows.Next() {
		var d Dir
		d.Fsys = fsys
		d.Parent = dirFullName
		rows.Scan(&d.Dname, &d.Inode)
		subdirs[d.Dname] = d
	}
	rows.Close()

	// Find the files in the directory
	sqlStmt = fmt.Sprintf(`
 		        SELECT file_id,proj_id,fname,size,ctime,mtime,inode FROM files
			WHERE folder = %s;
			`,
		dirFullName)
	rows, err = fsys.db.Query(sqlStmt)
	if err != nil {
		return nil, nil, err
	}
	files := make(map[string]File)
	for rows.Next() {
		var f File
		f.Fsys = fsys
		rows.Scan(&f.FileId, &f.ProjId, &f.Name, &f.Size, &f.Ctime, &f.Mtime, &f.Inode)
		files[f.Name] = f
	}

	return files, subdirs, nil
}

// Add a directory with its contents to an exisiting database
func MetadataDbReadDirAll(
	fsys *Filesys,
	dirFullName string) (map[string]File, map[string]Dir, error) {

	retval, err := directoryExists(fsys, dirFullName)
	if err != nil {
		return nil, nil, err
	}
	if retval {
		// the directory already exists. read it, and return
		// all the entries.
		//
		return directoryReadAllEntries(fsys, dirFullName)
	}

	// The directory has not been queried yet.
	//
	// describe all the files
	dxDir, err := DxDescribeFolder(&fsys.dxEnv, fsys.projectId, dirFullName)
	if err != nil {
		return nil, nil, err
	}

	// limit the number of files
	if len(dxDir.files) > MAX_DIR_SIZE {
		err := fmt.Errorf(
			"Too many files (%d) in a directory, the limit is %d",
			len(dxDir.files),
			MAX_DIR_SIZE)
		return nil, nil, err
	}

	// TODO: check for files with the same name, and modify their directories.
	// For example, if we have two version of file X.txt under directory foo,
	// then they should be renamed:
	//   foo/X.txt
	//      /1/X.txt

	if _, err = fsys.db.Exec("BEGIN TRANSACTION"); err != nil {
		return nil, nil, err
	}

	// Create a database entry for each file
	for _, d := range dxDir.files {
		inode := allocInodeNum(fsys)
		sqlStmt := fmt.Sprintf(`
 		        INSERT INTO files
			VALUES ('%s', '%s', '%s', '%s', %d, '%d', '%d', '%d');
			`,
			d.FileId, d.ProjId, d.Name, d.Folder, d.Size, d.Ctime, d.Mtime, inode)
		_, err = fsys.db.Exec(sqlStmt)
		if err != nil {
			return nil, nil, err
		}
	}

	// Create a database entry for each sub-directory
	for _, subDirName := range dxDir.subdirs {
		inode := allocInodeNum(fsys)

		// DNAx stores directories as full paths. For example: /A/B has
		// as subdirectories  "A/B/C", "A/B/D", "A/B/KKK". A POSIX
		// filesystem represents these as: "A/B", {"C", "D", "KKK"}
		//
		subDirLastPart := strings.TrimPrefix(subDirName, dirFullName)
		sqlStmt := fmt.Sprintf(`
 		        INSERT INTO directories
			VALUES ('%s', '%s', '%s', '%d');
			`,
			fsys.projectId,
			subDirLastPart,
			dirFullName,
			inode)

		_, err = fsys.db.Exec(sqlStmt)
		if err != nil {
			return nil, nil, err
		}
	}

	if _, err = fsys.db.Exec("END TRANSACTION"); err != nil {
		return nil, nil, err
	}

	// Now that the directory is in the database, we can read it with a local query.
	return directoryReadAllEntries(fsys, dirFullName)
}

// Look for file [filename] in directory [parent]/[dname].
//
// 1. Look if the directory has already been downloaded and placed in the DB
// 2. If not, populate it
// 3. Do a lookup in the directory.
//
// Note: the file might not exist.
func MetadataDbLookupInDir(
	fsys *Filesys,
	parent string,
	dname string,
	dirOrFileName string) (fs.Node, error) {
	files, subdirs, err := MetadataDbReadDirAll(fsys, parent + "/" + dname)
	if err != nil {
		return nil, err
	}

	// Is this a file?
	fileElem, ok := files[dirOrFileName]
	if ok {
		return &fileElem, nil
	}

	// Is this a directory
	dirElem, ok := subdirs[dirOrFileName]
	if ok {
		return &dirElem, nil
	}

	// no such entry
	return nil, fuse.ENOENT
}

// Return the root directory
func MetadataDbRoot(fsys *Filesys) (*Dir, error) {
	sqlStmt := fmt.Sprintf(`
 		        SELECT dname,inode FROM directories
			WHERE dname = "/";
			`)
	rows, err := fsys.db.Query(sqlStmt)
	if err != nil {
		return nil, err
	}

	var d Dir
	d.Fsys = fsys
	d.Parent = ""
	numRows := 0
	for rows.Next() {
		rows.Scan(&d.Dname, &d.Inode)
		numRows++
	}
	rows.Close()

	switch numRows {
	case 0:
		return nil, fmt.Errorf("Could not find root directory")
	case 1:
		return &d, nil
	default:
		return nil, fmt.Errorf("Found more than one root directory")
	}
}
