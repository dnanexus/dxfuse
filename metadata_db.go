package dxfs2

import (
	"filepath"
	"fmt"
	"log"
	"strings"
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
	inodeInvalid   = 0
	inodeRootDir   = 1
	inodeInitial   = 10
)

// Construct a local sql database that holds metadata for
// a large number of dx:files. This metadata_db will be consulted
// when performing dxfs2 operations. For example, a read-dir is
// translated into a query for all the files inside a directory.

func makeFullPath(parent string, name string) string {
	fullPath := parent + "/" + name
	return strings.ReplaceAll(fullPath, "//", "/")
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
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
		return err
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
                type smallint,
                inode bigint,
                PRIMARY KEY (parent,name)
	);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		return err
	}

	sqlStmt = `
	CREATE INDEX parent_index
	ON namespace (parent);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		return err
	}

	// we need to be able to get from the files/tables, back to the namespace
	// with an inode ID.
	sqlStmt = `
	CREATE INDEX inode_rev_index
	ON namespace (inode);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		return err
	}

	// A separate table for directories.
	//
	// If the inode is -1, then, the directory does not exist on the platform.
	// If poplated is zero, we haven't described the directory yet.
	sqlStmt = `
	CREATE TABLE directories (
                inode bigint,
                populated smallint,
                PRIMARY KEY (inode)
	);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		return err
	}

	// Adding a root directory
	sqlStmt = fmt.Sprintf(`
 		        INSERT INTO directories
			VALUES ('%d', '%d');`,
		inodeRootDir, boolToInt(false))
	if _, err := txn.Exec(sqlStmt); err != nil {
		return err
	}
	sqlStmt = fmt.Sprintf(`
 		        INSERT INTO namespace
			VALUES ('%s', '%s', '%d', '%d');`,
		"", "/", nsDirType, inodeRootDir)
	if _, err := txn.Exec(sqlStmt); err != nil {
		return err
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
		log.Printf(err.Error())
		return err
	}

	if err := fsys.metadataDbInitCore(txn); err != nil {
		txn.Rollback()
		return err
	}

	if err := txn.Commit(); err != nil {
		txn.Rollback()
		return err
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
 		        SELECT directories.inode, namespace.name
                        FROM directories
                        JOIN namespace
                        ON directories.inode = namespace.inode
			WHERE namespace.parent = '%s' AND namespace.type = '%d';
			`, dirFullName, nsDirType)
	rows, err := fsys.db.Query(sqlStmt)
	if err != nil {
		log.Printf(err.Error())
		return nil, nil, err
	}

	subdirs := make(map[string]Dir)
	for rows.Next() {
		var inode int64
		var dname string
		rows.Scan(&inode, &dname)

		subdirs[d.Dname] = Dir{
			Fsys : fsys,
			Parent : dirFullName,
			Dname : dname,
			FullPath : makeFullPath(dirFullName, dname),
			Inode : inode,
		}
	}
	rows.Close()
	log.Printf("#subdirs %d", len(subdirs))

	// Extract information for all the files
	sqlStmt := fmt.Sprintf(`
 		        SELECT files.file_id,files.proj_id,files.inode,files.size,files.ctime,files.mtime,namespace.name
                        FROM files
                        JOIN namespace
                        ON files.inode = namespace.inode
			WHERE namespace.parent = '%s' AND namespace.type = '%d';
			`, dirFullName, nsFileType)
	rows, err := fsys.db.Query(sqlStmt)
	if err != nil {
		log.Printf(err.Error())
		return nil, nil, err
	}

	// Find the files in the directory
	files := make(map[string]File)
	for rows.Next() {
		var f File
		var ctime int64
		var mtime int64
		rows.Scan(&f.FileId, &f.ProjId, &f.Inode, &f.Size, &ctime, &mtime, &f.Name)
		f.Fsys = fsys
		f.Ctime = time.Unix(ctime, 0)
		f.Mtime = time.Unix(mtime, 0)
		files[f.Name] = f
	}

	//log.Printf("  #files=%d", len(files))
	//log.Printf("]")
	return files, subdirs, nil
}

func (fsys *Filesys) createEmptyDir(txn *sql.Tx, dirPath string, populated bool) (int, error) {
	inode := fsys.allocInodeNum()

	sqlStmt := fmt.Sprintf(`
 		        INSERT INTO namespace
			VALUES ('%s', '%s', '%d', '%d');`,
		filepath.Dir(dirPath),
		filepath.Base(dirPath),
		nsDirType,
		inode)
	if _, err = txn.Exec(sqlStmt); err != nil {
		return 0, err
	}

	// Create an entry for the subdirectory
	sqlStmt = fmt.Sprintf(`
                       INSERT INTO directories
                       VALUES ('%d', '%d');`,
		inode, boolToInt(poplated))
	if _, err = fsys.db.Exec(sqlStmt); err != nil {
		return 0, err
	}
	return inode, nil
}


// Create a directory with: an i-node, files, and empty unpopulated subdirectories.
func (fsys *Filesys) constructDir(
	txn *sql.Tx,
	dinode int64,
	dirPath string,
	files []DxDescribeDataObject,
	subdirs []string) error {
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
		if _, err = txn.Exec(sqlStmt); err != nil {
			return err
		}

		//log.Printf("fInode = %d", fInode)
		sqlStmt := fmt.Sprintf(`
 		        INSERT INTO files
			VALUES ('%s', '%s', '%d', '%d', '%d', '%d');`,
			f.FileId, f.ProjId, fInode, f.Size, d.Ctime.Unix(), d.Mtime.Unix())
		_, err = txn.Exec(sqlStmt)
		if err != nil {
			return err
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
		_, err := fsys.createEmptyDir(txn, dirPath + "/" + subDirName, false)
		if err != nil {
			return err
		}
	}

	if fsys.options.Verbose {
		log.Printf("setting populated for directory %s", dirPath)
	}

	// Update the directory populated flag to TRUE
	sqlStmt := fmt.Sprintf(`
		UPDATE directories
                SET populated = '1'
                WHERE inode = '%d'`,
		dinode)
	if _, err = txn.Exec(sqlStmt); err != nil {
		return err
	}
	return nil
}

// Query DNAx about a folder, and encode all the information in the database.
//
// assumptions:
// 1. the directory has not been queried yet.
// 2. the global lock is held
func (fsys *Filesys) directoryReadFromDNAx(dinode int64, dirFullName string) error {
	if fsys.options.Verbose {
		log.Printf("describe folder %s", dirFullName)
	}

	// describe all the files
	httpClient := <- fsys.httpClientPool
	dxDir, err := DxDescribeFolder(httpClient, &fsys.dxEnv, fsys.project.Id, dirFullName)
	fsys.httpClientPool <- httpClient
	if err != nil {
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

	// The DNAx storage system does not adhere to POSIX. Try
	// to fix the elements in the directory, so they would comply. This
	// comes at the cost of renaming the original files, which can
	// very well mislead the user.
	posixDir, err = PosixFixDir(fsys, dxDir)
	if err != nil {
		return err
	}

	txn, err := fsys.db.Begin()
	if err != nil {
		return err
	}

	// build the top level directory
	err = fsys.constructDir(txn, dinode, posixDir.fullName, posixDir.files, posixDir.subdirs)
	if err != nil {
		txn.Rollback()
		log.Printf(err.Error())
		return err
	}

	// create the faux sub directories. These have no additional depth, and are fully
	// populated. They contains all the files with multiple versions.
	for fauxDirName, fauxFiles = range posixDir.fauxSubdirs {
		fauxDirPath := posixDir.fullName + "/" + dName

		// create the directory in the namespace, as if it is unpopulated.
		fauxDirInode := fsys.createEmptyDir(txn, fauxDirPath, true)

		err := fsys.constructDir(txn, fauxDirInode, fauxDirPath, fauxFiles, [])
		if err != nil {
			txn.Rollback()
			log.Printf(err.Error())
			return err
		}
	}

	txn.Commit()
	return nil
}

// Look for a directory. Return:
//  1. Directory exists or not
//  2. inode
//  3. populated flag
func (fsys *Filesys) directoryLookup(dirPath string) (bool, int, bool) {
	sqlStmt := fmt.Sprintf(`
 		        SELECT directories.inode
                        FROM namespace
			WHERE namespace.parent = '%s' AND namespace.name = '%s' AND namespace.type = '%d';`,
		filepath.Dir(dirPath), filepath.Base(dirPath), nsDirType)
	rows, err := fsys.db.Query(sqlStmt)
	if err != nil {
		panic(err)
	}

	// There could be at most one such entry
	var inode
	numRows := 0
	for rows.Next() {
		rows.Scan(&parentInode, &parentPopulated)
		numRows++
	}
	rows.Close()
	if numRows == 0 {
		log.Printf("directory %s not found", dirPath)
		return false, 0, false
	} else if numRows > 1 {
		panic(fmt.Sprintf("Found %d entries for directory %s", numRows, dirPath))
	}

	// There is exactly one entry
	// Extract the populated flag
	sqlStmt = fmt.Sprintf(`
 		        SELECT populated
                        FROM directories
			WHERE inode = '%d';`, inode)
	rows, err = fsys.db.Query(sqlStmt)
	if err != nil {
		panic(err)
	}

	populated := 0
	numRows = 0
	for rows.Next() {
		rows.Scan(&populated)
		numRows++
	}
	rows.Close()

	if numRows == 0 {
		panic(fmt.Sprintf("directory %s found in namespace but not in table", dirPath))
	} else if numRows > 1 {
		panic(fmt.Sprintf("%d entries found for directory %s in table", numRows, dirPath))
	}

	return true, inode, populated
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
func (fsys *Filesys) directoryExists(dirFullName string) (int, error) {
	parentDir := filepath.Dir(dirFullName)

	// Get information on the parent
	parentExists, parentInode, parentPopulated := fsys.directoryLookup(parentDir)
	if !parentExists {
		panic(fmt.Sprintf(
			"Accessing directory (%s) even though parent (%s) has not been accessed",
			dirFullName, parentDir))
	}
	if parentPopulated == 0 {
		// Parent exists, but it has not been populated yet
		if fsys.options.Verbose {
			log.Printf("parent directory (%s) has not been populated yet", parentDir)
		}
		err := fsys.directoryReadFromDNAx(parentInode, parentDir)
		if err != nil {
			return 0, err
		}
	}

	// At this point, we can check if the directory exists
	exists, dinode, populated := fsys.directoryLookup(dirPath)

	if !exists {
		return dirDoesNotExist, nil
	}
	if populated == 0 {
		return dirExistsButNotPopulated, nil
	}
	return dirExistAndPopulated, nil
}

// Add a directory with its contents to an exisiting database
func (fsys *Filesys) MetadataDbReadDirAll(
	dirFullName string) (map[string]File, map[string]Dir, error) {
	if fsys.options.Verbose {
		log.Printf("MetadataDbReadDirAll %s", dirFullName)
	}

	retCode, err := fsys.directoryExists(dirFullName)
	if err != nil {
		log.Printf("err = %s, %s", err.Error(), dirFullName)
		return nil, nil, err
	}
	switch retCode {
	case dirDoesNotExist:
		return nil, nil, fuse.ENOENT
	case dirExistsButNotPopulated:
		// we need to read the directory from dnanexus
		// Get all the directory from dnanexus. This could take a while
		// for large directories.
		if err := fsys.directoryReadFromDNAx(dirFullName); err != nil {
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
	inode int64) (fs.File, error) {
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
		f.Ctime = time.Unix(ctime, 0)
		f.Mtime = time.Unix(mtime, 0)
		numRows++
	}
	rows.Close()

	switch numRows {
	case 0:
		// file not found
		panic("File (inode=%d, dir=%s, name=%s) should exist in the files table, but doesn't exist",
			inode, dirFullName, fname)
	case 1:
		// correct, there is exactly one such file
		return &f, nil
	default:
		panic(fmt.Sprintf("Found %d files of the form %s/%s",
			numRows, dirFullName, dirOrFileName))
	}
}

// Search for a file/subdir in a directory
func (fsys *Filesys) fastLookup(
	dirFullName string,
	dirOrFileName string) (fs.Node, error) {
	// point lookup in the namespace
	sqlStmt := fmt.Sprintf(`
 		        SELECT type,inode
                        FROM namespace
			WHERE parent = '%s' AND name = '%s';`,
		dirFullName, dirOrFileName)
	rows, err := fsys.db.Query(sqlStmt)
	if err != nil {
		log.Printf(err.Error())
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
		return &Dir{
			Fsys : fsys,
			Parent : dirFullName,
			Dname : dirOrFileName,
			FullPath : makeFullPath(dirFullName, dirOrFileName),
			inode : inode
		}
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

	retCode, populated, err := fsys.directoryExists(parentDir)
	if err != nil {
		log.Printf("err = %s, %s", err.Error(), parentDir)
		return nil, nil, err
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
	return &Dir{
		Fsys : fsys,
		Parent : "/",
		Dname = "/",
		FullPath = "/",
		Inode : inodeRootDir,
	}
}
