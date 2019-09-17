package dxfs2

import (
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
	"runtime/debug"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)


const (
	nsDirType = 1
	nsFileType = 2

	// return code from a directoryExists call
	dirDoesNotExist = 1
	dirExistsButNotPopulated = 2
	dirExistAndPopulated = 3

	maxDirSize     = 10 * 1000
)

type DirInfo struct {
	inode int64
	projId string
	projFolder string
	ctime int64
	mtime int64
}


// Construct a local sql database that holds metadata for
// a large number of dx:files. This metadata_db will be consulted
// when performing dxfs2 operations. For example, a read-dir is
// translated into a query for all the files inside a directory.

// Split a path into a parent and child. For example:
//
//   /A/B/C  -> "/A/B", "C"
//   / ->       "", "/"
func splitPath(fullPath string) (parentDir string, basename string) {
	if fullPath == "/" {
		// The anomalous case.
		//   Dir/Base returns:    "/", "/"
		//   but what we want is  "",  "/"
		return "", "/"
	} else {
		return filepath.Dir(fullPath), filepath.Base(fullPath)
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// convert time in milliseconds since 1970, in the equivalent
// golang structure
func millisecToTime(t int64) time.Time {
	sec := int64(t/1000)
	millisec := int64(t % 1000)
	return time.Unix(sec, millisec)
}

// Print the error and the stack trace.
//
// This method should be used the first time we encounter an error. For example,
// when calling the database.
//
func printErrorStack(err error) error {
//	var buf []byte
//	runtime.Stack(buf, false)
	debug.PrintStack()
	log.Printf(err.Error())
	return err
}

func (fsys *Filesys) metadataDbInitCore(txn *sql.Tx) error {
	// Create table for files.
	//
	// mtime and ctime are measured in seconds since 1st of January 1970
	// (Unix time).
	sqlStmt := `
	CREATE TABLE files (
		file_id text,
		proj_id text,
                inode bigint,
		size bigint,
                ctime bigint,
                mtime bigint,
                PRIMARY KEY (inode)
	);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		return printErrorStack(err)
	}

	// Create a table for the namespace relationships. All members of a directory
	// are listed here under their parent. Linking all the tables are the inode numbers.
	//
	// For example, directory /A/B/C will be represented with record:
	//    dname="C"
	//    folder="/A/B"
	//
	sqlStmt = `
	CREATE TABLE namespace (
		parent text,
		name text,
                obj_type int,
                inode bigint,
                PRIMARY KEY (parent,name)
	);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		return printErrorStack(err)
	}

	sqlStmt = `
	CREATE INDEX parent_index
	ON namespace (parent);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		return printErrorStack(err)
	}

	// we need to be able to get from the files/tables, back to the namespace
	// with an inode ID.
	sqlStmt = `
	CREATE INDEX inode_rev_index
	ON namespace (inode);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		return printErrorStack(err)
	}

	// A separate table for directories.
	//
	// If the inode is -1, then, the directory does not exist on the platform.
	// If poplated is zero, we haven't described the directory yet.
	sqlStmt = `
	CREATE TABLE directories (
                inode bigint,
                proj_id text,
                proj_folder text,
                populated int,
                ctime bigint,
                mtime bigint,
                PRIMARY KEY (inode)
	);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		return printErrorStack(err)
	}

	// Adding a root directory. The root directory does
	// not belong to any one project. This allows mounting
	// several projects from the same root. This is denoted
	// by marking the project as the empty string.
	sqlStmt = fmt.Sprintf(`
 		        INSERT INTO directories
			VALUES ('%d', '%s', '%s', '%d', '%d', '%d');`,
		InodeRoot, "", "", boolToInt(false),
		time.Now().Unix(), time.Now().Unix())
	if _, err := txn.Exec(sqlStmt); err != nil {
		return printErrorStack(err)
	}

	sqlStmt = fmt.Sprintf(`
 		        INSERT INTO namespace
			VALUES ('%s', '%s', '%d', '%d');`,
		"", "/", nsDirType, InodeRoot)
	if _, err := txn.Exec(sqlStmt); err != nil {
		return printErrorStack(err)
	}

	return nil
}

// construct an initial empty database, representing an entire project.
func (fsys *Filesys) MetadataDbInit() error {
	if fsys.options.Verbose {
		log.Printf("Initializing metadata database\n")
	}

	txn, err := fsys.db.Begin()
	if err != nil {
		return printErrorStack(err)
	}

	if err := fsys.metadataDbInitCore(txn); err != nil {
		txn.Rollback()
		return printErrorStack(err)
	}

	if err := txn.Commit(); err != nil {
		txn.Rollback()
		return printErrorStack(err)
	}

	if fsys.options.Verbose {
		log.Printf("Completed creating files and directories tables\n")
	}
	return nil
}

// Allocate an inode number. These must remain stable during the
// lifetime of the mount.
//
// Note: this call should perform while holding the mutex
func (fsys *Filesys) allocInodeNum() int64 {
	fsys.inodeCnt += 1
	return fsys.inodeCnt
}


// The directory is in the database, read it in its entirety.
func (fsys *Filesys) directoryReadAllEntries(
	dirFullName string) (map[string]File, map[string]Dir, error) {
	if fsys.options.Verbose {
		log.Printf("directoryReadAllEntries %s", dirFullName)
	}

	// Extract information for all the subdirectories
	sqlStmt := fmt.Sprintf(`
 		        SELECT directories.inode, directories.proj_id, namespace.name, directories.ctime, directories.mtime
                        FROM directories
                        JOIN namespace
                        ON directories.inode = namespace.inode
			WHERE namespace.parent = '%s' AND namespace.obj_type = '%d';
			`, dirFullName, nsDirType)
	rows, err := fsys.db.Query(sqlStmt)
	if err != nil {
		return nil, nil, printErrorStack(err)
	}

	subdirs := make(map[string]Dir)
	for rows.Next() {
		var inode int64
		var dname string
		var projId string
		var ctime int64
		var mtime int64
		rows.Scan(&inode, &projId, &dname, &ctime, &mtime)

		subdirs[dname] = Dir{
			Fsys : fsys,
			Parent : dirFullName,
			Dname : dname,
			FullPath : filepath.Join(dirFullName, dname),
			Inode : inode,
			Ctime : millisecToTime(ctime),
			Mtime : millisecToTime(mtime),
		}
	}
	rows.Close()

	// Extract information for all the files
	sqlStmt = fmt.Sprintf(`
 		        SELECT files.file_id,files.proj_id,files.inode,files.size,files.ctime,files.mtime,namespace.name
                        FROM files
                        JOIN namespace
                        ON files.inode = namespace.inode
			WHERE namespace.parent = '%s' AND namespace.obj_type = '%d';
			`, dirFullName, nsFileType)
	rows, err = fsys.db.Query(sqlStmt)
	if err != nil {
		return nil, nil, printErrorStack(err)
	}

	// Find the files in the directory
	files := make(map[string]File)
	for rows.Next() {
		var fileId string
		var projId string
		var fname string
		var size int64
		var inode int64
		var ctime int64
		var mtime int64
		rows.Scan(&fileId, &projId, &inode, &size, &ctime, &mtime, &fname)

		files[fname] = File{
			Fsys : fsys,
			FileId : fileId,
			ProjId : projId,
			Name : fname,
			Size : size,
			Inode : inode,
			Ctime : millisecToTime(ctime),
			Mtime : millisecToTime(mtime),
		}
	}

	//log.Printf("  #files=%d", len(files))
	//log.Printf("]")
	return files, subdirs, nil
}

// Create an entry representing one remote file. This is used by
// dxWDL as a way to stream individual files.
func (fsys *Filesys) createRemoteFile(
	txn *sql.Tx,
	projId string,
	fileId string,
	size int64,
	ctime int64,
	mtime int64,
	parentDir string,
	fname string) (int64, error) {
	// choose unused inode number. It is on stable stoage, and will not change.
	inode := fsys.allocInodeNum()
	if fsys.options.Verbose {
		log.Printf("createRemoteFile %s:%s %s",	projId, fileId, parentDir + "/" + fname)
	}

	sqlStmt := fmt.Sprintf(`
 		        INSERT INTO namespace
			VALUES ('%s', '%s', '%d', '%d');`,
		parentDir, fname, nsFileType, inode)
	if _, err := txn.Exec(sqlStmt); err != nil {
		return 0, printErrorStack(err)
	}

	// Create an entry for the file
	sqlStmt = fmt.Sprintf(`
 		        INSERT INTO files
			VALUES ('%s', '%s', '%d', '%d', '%d', '%d');`,
		fileId, projId, inode, size, ctime, mtime)
	if _, err := txn.Exec(sqlStmt); err != nil {
		return 0, printErrorStack(err)
	}
	return inode, nil
}

// Create an empty directory, and return the inode
//
// Assumption: the directory does not already exist in the database.
func (fsys *Filesys) createEmptyDir(
	txn *sql.Tx,
	projId string,
	projFolder string,
	ctime int64,
	mtime int64,
	dirPath string,
	populated bool) (int64, error) {
	if dirPath[0] != '/' {
		panic("directory must start with a slash")
	}

	// choose unused inode number. It is on stable stoage, and will not change.
	inode := fsys.allocInodeNum()
	parentDir, basename := splitPath(dirPath)
	if fsys.options.Verbose {
		log.Printf("createEmptyDir %s:%s %s populated=%t",
			projId, projFolder, dirPath, populated)
	}

	sqlStmt := fmt.Sprintf(`
 		        INSERT INTO namespace
			VALUES ('%s', '%s', '%d', '%d');`,
		parentDir, basename, nsDirType,	inode)
	if _, err := txn.Exec(sqlStmt); err != nil {
		return 0, printErrorStack(err)
	}

	// Create an entry for the subdirectory
	sqlStmt = fmt.Sprintf(`
                       INSERT INTO directories
                       VALUES ('%d', '%s', '%s', '%d', '%d', '%d');`,
		inode, projId, projFolder, boolToInt(populated), ctime, mtime)
	if _, err := txn.Exec(sqlStmt); err != nil {
		return 0, printErrorStack(err)
	}
	return inode, nil
}

// Update the directory populated flag to TRUE
func (fsys *Filesys) setDirectoryToPopulated(txn *sql.Tx, dinode int64) error {
	sqlStmt := fmt.Sprintf(`
		UPDATE directories
                SET populated = '1'
                WHERE inode = '%d'`,
		dinode)
	if _, err := txn.Exec(sqlStmt); err != nil {
		return printErrorStack(err)
	}
	return nil
}

// Create a directory with: an i-node, files, and empty unpopulated subdirectories.
func (fsys *Filesys) populateDir(
	txn *sql.Tx,
	dinode int64,
	projId string,
	projFolder string,
	ctime int64,
	mtime int64,
	dirPath string,
	files []DxDescribeDataObject,
	subdirs []string) error {
	if fsys.options.Verbose {
		var fileNames []string
		for _, fDesc := range files {
			fileNames = append(fileNames, fDesc.Name)
		}
		log.Printf("populateDir(%s)  files=%v  subdirs=%v", dirPath, fileNames, subdirs)
	}

	// Create a database entry for each file
	if fsys.options.Verbose {
		log.Printf("inserting files")
	}

	for _, f := range files {
		fInode := fsys.allocInodeNum()

		sqlStmt := fmt.Sprintf(`
 		        INSERT INTO namespace
			VALUES ('%s', '%s', '%d', '%d');`,
			dirPath, f.Name, nsFileType, fInode)
		if _, err := txn.Exec(sqlStmt); err != nil {
			return err
		}

		//log.Printf("fInode = %d", fInode)
		sqlStmt = fmt.Sprintf(`
 		        INSERT INTO files
			VALUES ('%s', '%s', '%d', '%d', '%d', '%d');`,
			f.FileId, f.ProjId, fInode, f.Size, f.CtimeMillisec, f.MtimeMillisec)
		if _, err := txn.Exec(sqlStmt); err != nil {
			return printErrorStack(err)
		}
	}

	// Create a database entry for each sub-directory
	if fsys.options.Verbose {
		log.Printf("inserting subdirs")
	}
	for _, subDirName := range subdirs {
		// Create an entry for the subdirectory.
		// We haven't described it yet from DNAx, so the populate flag
		// is false.
		_, err := fsys.createEmptyDir(
			txn,
			projId, filepath.Clean(projFolder + "/" + subDirName),
			ctime, mtime, filepath.Clean(dirPath + "/" + subDirName),
			false)
		if err != nil {
			return printErrorStack(err)
		}
	}

	if fsys.options.Verbose {
		log.Printf("setting populated for directory %s", dirPath)
	}

	// Update the directory populated flag to TRUE
	fsys.setDirectoryToPopulated(txn, dinode)
	return nil
}

// Query DNAx about a folder, and encode all the information in the database.
//
// assumptions:
// 1. An empty directory has been created on the database.
// 1. The directory has not been queried yet.
// 2. The global lock is held
func (fsys *Filesys) directoryReadFromDNAx(
	dinode int64,
	projId string, projFolder string,
	ctime int64, mtime int64,
	dirFullName string) error {

	if fsys.options.Verbose {
		log.Printf("describe folder %s:%s", projId, projFolder)
	}

	// describe all the files
	httpClient := <- fsys.httpClientPool
	dxDir, err := DxDescribeFolder(httpClient, &fsys.dxEnv, projId, projFolder)
	fsys.httpClientPool <- httpClient
	if err != nil {
		fmt.Printf("Describe error: %s", err.Error())
		return err
	}

	if fsys.options.Verbose {
		log.Printf("read dir from DNAx #files=%d #subdirs=%d",
			len(dxDir.files),
			len(dxDir.subdirs))
	}

	// limit the number of directory elements
	numElementsInDir := len(dxDir.files) + len(dxDir.subdirs)
	if numElementsInDir > maxDirSize {
		return fmt.Errorf(
			"Too many elements (%d) in a directory, the limit is %d",
			numElementsInDir, maxDirSize)
	}

	// Approximate the ctime/mtime using the file timestamps.
	// - The directory creation time is the minimum of all file creates.
	// - The directory modification time is the maximum across all file modifications.
	ctimeApprox := ctime
	mtimeApprox := mtime
	for _, f := range dxDir.files {
		ctimeApprox = MinInt64(ctimeApprox, f.CtimeMillisec)
		mtimeApprox = MaxInt64(mtimeApprox, f.MtimeMillisec)
	}

	// The DNAx storage system does not adhere to POSIX. Try
	// to fix the elements in the directory, so they would comply. This
	// comes at the cost of renaming the original files, which can
	// very well mislead the user.
	posixDir, err := PosixFixDir(fsys, dxDir)
	if err != nil {
		return err
	}

	txn, err := fsys.db.Begin()
	if err != nil {
		return printErrorStack(err)
	}

	// build the top level directory
	err = fsys.populateDir(
		txn, dinode,
		projId, projFolder,
		ctimeApprox, mtimeApprox,
		dirFullName, posixDir.files, posixDir.subdirs)
	if err != nil {
		txn.Rollback()
		return printErrorStack(err)
	}

	// create the faux sub directories. These have no additional depth, and are fully
	// populated. They contains all the files with multiple versions.
	//
	// Note: these directories DO NOT have a matching project folder.
	for dName, fauxFiles := range posixDir.fauxSubdirs {
		fauxDirPath := filepath.Clean(dirFullName + "/" + dName)

		// create the directory in the namespace, as if it is unpopulated.
		fauxDirInode, err := fsys.createEmptyDir(
			txn, projId, "", ctimeApprox, mtimeApprox, fauxDirPath, true)
		if err != nil {
			txn.Rollback()
			return printErrorStack(err)
		}

		var no_subdirs []string
		err = fsys.populateDir(
			txn, fauxDirInode,
			projId, "",
			ctimeApprox, mtimeApprox,
			fauxDirPath, fauxFiles, no_subdirs)
		if err != nil {
			txn.Rollback()
			return printErrorStack(err)
		}
	}

	txn.Commit()
	return nil
}

// Look for a directory. Return:
//  1. Directory exists or not, and if its populated
//  2. inode
func (fsys *Filesys) directoryLookup(dirPath string) (int, *DirInfo) {
	parentDir, basename := splitPath(dirPath)
	sqlStmt := fmt.Sprintf(`
 		        SELECT inode
                        FROM namespace
			WHERE parent = '%s' AND name = '%s' AND obj_type = '%d';`,
		parentDir, basename, nsDirType)
	rows, err := fsys.db.Query(sqlStmt)
	if err != nil {
		panic(err)
	}

	// There could be at most one such entry
	var inode int64
	numRows := 0
	for rows.Next() {
		rows.Scan(&inode)
		numRows++
	}
	rows.Close()
	if numRows == 0 {
		log.Printf("directory %s not found", dirPath)
		return dirDoesNotExist, nil
	} else if numRows > 1 {
		panic(fmt.Sprintf("Found %d entries for directory %s", numRows, dirPath))
	}

	// There is exactly one entry
	// Extract the populated flag
	sqlStmt = fmt.Sprintf(`
 		        SELECT populated, proj_id, proj_folder, ctime, mtime
                        FROM directories
			WHERE inode = '%d';`, inode)
	rows, err = fsys.db.Query(sqlStmt)
	if err != nil {
		panic(err)
	}

	var populated int
	var dInfo DirInfo
	dInfo.inode = inode
	numRows = 0
	for rows.Next() {
		rows.Scan(&populated, &dInfo.projId, &dInfo.projFolder, &dInfo.ctime, &dInfo.mtime)
		numRows++
	}
	rows.Close()

	if numRows == 0 {
		panic(fmt.Sprintf("directory %s found in namespace but not in table", dirPath))
	} else if numRows > 1 {
		panic(fmt.Sprintf("%d entries found for directory %s in table", numRows, dirPath))
	}

	var retCode int
	switch populated {
	case 0:
		retCode = dirExistsButNotPopulated
	case 1:
		retCode = dirExistAndPopulated
	default:
		panic(fmt.Sprintf("illegal value for populated field (%d)", populated))
	}
	return retCode, &dInfo
}

// We want to check if directory D exists. Denote:
//     P = parent(dirFullName)
//     B = basename(dirFullName)
//
// For example, if the directory is "/A/B/C" then:
//    P = "/A/B"
//    B = "C"
//
// The filesystem allows accessing directory D, only if its parent
// exists. Therefore, this method will be called only if the parent
// exists, and is in sqlite3.
//
// 1. Make sure the parent directory P has been fully populated.
// 2. Query the parent P, check if B is a member. If not, return "dirDoesNotExist".
// 3. Having got this far, we know that D exists. Now, check if it is already fully
//    populated.
//
func (fsys *Filesys) directoryExists(dirPath string) (int, *DirInfo, error) {
	if dirPath == "/" {
		// The root directory has the unique property, that it does
		// not have a parent.
		//
		// Skip the parent checking phase.
		retCode, dInfo := fsys.directoryLookup(dirPath)
		return retCode, dInfo, nil
	}

	parentDir := filepath.Dir(dirPath)

	// Make sure the parent exists, and that it is populated.
	retCode, parentDirInfo := fsys.directoryLookup(parentDir)
	if retCode == dirDoesNotExist {
		panic(fmt.Sprintf(
			"Accessing directory (%s) even though parent (%s) has not been accessed",
			dirPath, parentDir))
	}
	if retCode == dirExistsButNotPopulated {
		// Parent exists, but it has not been populated yet
		if fsys.options.Verbose {
			log.Printf("parent directory (%s) has not been populated yet", parentDir)
		}
		if parentDir == "/" {
			panic("The subdirectory should not inherit ctime/mtime from root")
		}
		err := fsys.directoryReadFromDNAx(
			parentDirInfo.inode,
			parentDirInfo.projId, parentDirInfo.projFolder,
			parentDirInfo.ctime, parentDirInfo.mtime,
			parentDir)
		if err != nil {
			return 0, nil, err
		}
	}

	// At this point, we can check if the directory exists
	retCode, dInfo := fsys.directoryLookup(dirPath)
	return retCode, dInfo, nil
}

// Add a directory with its contents to an exisiting database
func (fsys *Filesys) MetadataDbReadDirAll(
	dirFullName string) (map[string]File, map[string]Dir, error) {
	if fsys.options.Verbose {
		log.Printf("MetadataDbReadDirAll %s", dirFullName)
	}

	retCode, dInfo, err := fsys.directoryExists(dirFullName)
	if err != nil {
		log.Printf("err = %s, %s", err.Error(), dirFullName)
		return nil, nil, err
	}
	switch retCode {
	case dirDoesNotExist:
		return nil, nil, fuse.ENOENT
	case dirExistsButNotPopulated:
		// we need to read the directory from dnanexus.
		// This could take a while for large directories.
		err := fsys.directoryReadFromDNAx(
			dInfo.inode, dInfo.projId, dInfo.projFolder,
			dInfo.ctime, dInfo.mtime, dirFullName)
		if err != nil {
			return nil, nil, err
		}
	case dirExistAndPopulated:
		if fsys.options.Verbose {
			log.Printf("Directory %s is in the DB, and is populated", dirFullName)
		}
	default:
		panic(fmt.Sprintf("Bad return code %d",retCode))
	}

	// Now that the directory is in the database, we can read it with a local query.
	return fsys.directoryReadAllEntries(dirFullName)
}

// search for a file with a particular inode
func (fsys *Filesys) lookupFile(
	dirFullName string,
	fname string,
	inode int64) (*File, error) {
	// point lookup in the files table
	sqlStmt := fmt.Sprintf(`
 		        SELECT file_id,proj_id,size,ctime,mtime
                        FROM files
			WHERE inode = '%d';`,
		inode)
	rows, err := fsys.db.Query(sqlStmt)
	if err != nil {
		log.Printf(err.Error())
		panic(fmt.Sprintf("could not file file inode=%d dir=%s name=%s",
			inode, dirFullName, fname))
	}

	var f File
	f.Fsys = fsys
	f.Name = fname
	f.Inode = inode
	numRows := 0
	for rows.Next() {
		var ctime int64
		var mtime int64
		rows.Scan(&f.FileId, &f.ProjId, &f.Size, &ctime, &mtime)
		f.Ctime = millisecToTime(ctime)
		f.Mtime = millisecToTime(mtime)
		numRows++
	}
	rows.Close()

	switch numRows {
	case 0:
		// file not found
		panic(fmt.Sprintf(
			"File (inode=%d, dir=%s, name=%s) should exist in the files table, but doesn't exist",
			inode, dirFullName, fname))
	case 1:
		// correct, there is exactly one such file
		return &f, nil
	default:
		panic(fmt.Sprintf("Found %d files of the form %s/%s",
			numRows, dirFullName, fname))
	}
}


// search for a directory with a particular inode
func (fsys *Filesys) lookupDir(
	dirFullName string,
	dname string,
	dinode int64) (*Dir, error) {
	// point lookup in the directories table
	sqlStmt := fmt.Sprintf(`
 		        SELECT proj_id, ctime, mtime
                        FROM directories
			WHERE inode = '%d';`, dinode)
	rows, err := fsys.db.Query(sqlStmt)
	if err != nil {
		log.Printf(err.Error())
		panic(fmt.Sprintf("could not find directory inode=%d dir=%s name=%s",
			dinode, dirFullName, dname))
	}

	numRows := 0
	var projId string
	var ctime int64
	var mtime int64
	for rows.Next() {
		rows.Scan(&projId, &ctime, &mtime)
		numRows++
	}
	rows.Close()

	switch numRows {
	case 0:
		// file not found
		panic(fmt.Sprintf(
			"Directory (inode=%d, dir=%s, name=%s) should exist in the directories table, but doesn't exist",
			dinode, dirFullName, dname))
	case 1:
		// correct, there is exactly one directory
		return &Dir{
			Fsys : fsys,
			Parent : dirFullName,
			Dname : dname,
			FullPath : filepath.Join(dirFullName, dname),
			Inode : dinode,
			Ctime : millisecToTime(ctime),
			Mtime : millisecToTime(mtime),
		}, nil
	default:
		panic(fmt.Sprintf("Found %d directories of the form %s/%s",
			numRows, dirFullName, dname))
	}
}

// Search for a file/subdir in a directory
func (fsys *Filesys) fastLookup(
	dirFullName string,
	dirOrFileName string) (fs.Node, error) {
	// point lookup in the namespace
	sqlStmt := fmt.Sprintf(`
 		        SELECT obj_type,inode
                        FROM namespace
			WHERE parent = '%s' AND name = '%s';`,
		dirFullName, dirOrFileName)
	rows, err := fsys.db.Query(sqlStmt)
	if err != nil {
		return nil, err
	}

	var objType int
	var inode int64
	numRows := 0
	for rows.Next() {
		rows.Scan(&objType, &inode)
		numRows++
	}
	rows.Close()
	if numRows == 0 {
		return nil, fuse.ENOENT
	}
	if numRows > 1 {
		panic(fmt.Sprintf("Found %d files of the form %s/%s",
			numRows, dirFullName, dirOrFileName))
	}

	// There is exactly one answer
	switch objType {
	case nsDirType:
		return fsys.lookupDir(dirFullName, dirOrFileName, inode)
	case nsFileType:
		return fsys.lookupFile(dirFullName, dirOrFileName, inode)
	default:
		panic(fmt.Sprintf("Invalid object type %d", objType))
	}
}

// Look for file [filename] in directory [parent]/[dname].
//
// 1. Look if the directory has already been downloaded and placed in the DB
// 2. If not, populate it
// 3. Do a lookup in the directory.
//
// Note: the file might not exist.
func (fsys *Filesys) MetadataDbLookupInDir(
	parentDir string,
	dirOrFileName string) (fs.Node, error) {

	retCode, _, err := fsys.directoryExists(parentDir)
	if err != nil {
		log.Printf("err = %s, %s", err.Error(), parentDir)
		return nil, err
	}
	switch retCode {
	case dirDoesNotExist:
		return nil, fuse.ENOENT
	case dirExistsButNotPopulated:
		// The directory exists, but has not been populated yet.
		_, _, err := fsys.MetadataDbReadDirAll(parentDir)
		if err != nil {
			return nil, err
		}
	case dirExistAndPopulated:
		// The directory exists, and has already been populated.
		// I think this is the normal path. There is nothing to do here.
	default:
		panic(fmt.Sprintf("Bad return code %d",retCode))
	}

	return fsys.fastLookup(parentDir, dirOrFileName)
}

// Return the root directory
func (fsys *Filesys) MetadataDbRoot() (*Dir, error) {
	sqlStmt := fmt.Sprintf(`
 		        SELECT parent, name, obj_type
                        FROM namespace
			WHERE inode='%d';`,
		InodeRoot)
	rows, err := fsys.db.Query(sqlStmt)
	if err != nil {
		return nil, printErrorStack(err)
	}

	numRows := 0
	var parent string
	var dname string
	var objType int
	for rows.Next() {
		rows.Scan(&parent, &dname, &objType)
		numRows++
	}
	rows.Close()

	if fsys.options.Verbose {
		log.Printf("Read root dir, inode=%d", InodeRoot)
	}

	switch numRows {
	case 0:
		return nil, fmt.Errorf("Could not find root directory")
	case 1:
		if objType != nsDirType {
			panic(fmt.Sprintf("root node has the wrong type %d", objType))
		}
		return fsys.lookupDir("/", "/", InodeRoot)
	default:
		return nil, fmt.Errorf("Found more than one root directory")
	}
}

// Build a toplevel directory for each project.
func (fsys *Filesys) MetadataDbPopulateRoot(manifest Manifest) error {
	log.Printf("Populating root directory")

	dirSkel := manifest.DirSkeleton()
	log.Printf("dirSkeleton = %v", dirSkel)

	txn, err := fsys.db.Begin()
	if err != nil {
		return printErrorStack(err)
	}

	// build the supporting directory structure
	for _, d := range dirSkel {
		_, err := fsys.createEmptyDir(
			txn,
			"", "",   // There is no backing project/folder
			time.Now().Unix(), time.Now().Unix(),
			d, false)
		if err != nil {
			txn.Rollback()
			return printErrorStack(err)
		}
	}

	// create individual files
	for _, fl := range manifest.Files {
		_, err := fsys.createRemoteFile(
			txn, fl.ProjId, fl.FileId,
			fl.Size, fl.CtimeMillisec, fl.MtimeMillisec,
			fl.Parent, fl.Fname)
		if err != nil {
			txn.Rollback()
			return printErrorStack(err)
		}
	}

	for _, d := range manifest.Directories {
		// Local directory [d.Dirname] represents
		// folder [d.Folder] on project [d.ProjId].
		_, err := fsys.createEmptyDir(
			txn,
			d.ProjId, d.Folder,
			d.CtimeMillisec, d.MtimeMillisec,
			d.Dirname, false)
		if err != nil {
			txn.Rollback()
			return printErrorStack(err)
		}
	}

	// set the root to be populated
	if err := fsys.setDirectoryToPopulated(txn, InodeRoot); err != nil {
		txn.Rollback()
		return printErrorStack(err)
	}

	return txn.Commit()
}
