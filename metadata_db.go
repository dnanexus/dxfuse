package dxfs2

import (
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
)

// Construct a local sql database that holds metadata for
// a large number of dx:files. This metadata_db will be consulted
// when performing dxfs2 operations. For example, a read-dir is
// translated into a query for all the files inside a directory.

func makeFullPath(parent string, name string) string {
	fullPath := parent + "/" + name
	return strings.ReplaceAll(fullPath, "//", "/")
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
		proj_id text,
		parent text,
		name text,
                fullName text,
                type smallint,
                inode bigint,
                PRIMARY KEY (parent,dname)
	);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		return err
	}

	sqlStmt = `
	CREATE INDEX parent_dir_lookup
	ON namespace (parent);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		return err
	}

	// we need to be able to get from the files/tables, back to the namespace
	// with an inode ID.
	sqlStmt = `
	CREATE INDEX inode_rev_lookup
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
		proj_id text,
		dirFullName text,
                inode bigint,
                populated smallint,
                PRIMARY KEY (inode)
	);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		return err
	}

	// Sometimes we want to search by full directory name.
	sqlStmt = `
	CREATE INDEX dirFullName_lookup
	ON directories (dirFullName);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		return err
	}

	// Adding a root directory
	sqlStmt = fmt.Sprintf(`
 		        INSERT INTO directories
			VALUES ('%s', '%s', '%d', '%d');
			`,
		fsys.project.Id, "/", INODE_ROOT_DIR, 0)
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
func allocInodeNum(fsys *Filesys) int64 {
	fsys.inodeCnt += 1
	return fsys.inodeCnt
}

// Three options:
// 1. Directory has been read from DNAx
// 2. Directory does not exist on DNAx
//      error = ENOENT
// 3. Directory has been fully read, and is in the database
//     exists=true  populated=true
func (fsys *Filesys) directoryExists(dirFullName string) (bool, bool, error) {
	// split the directory into parent into last part:
	// "A/B/C" ---> "A/B", "C"
	sqlStmt := fmt.Sprintf(`
 		        SELECT inode,populated FROM directories
			WHERE dirFullName = '%s';
			`, dirFullName)
	rows, err := fsys.db.Query(sqlStmt);
	if err != nil {
		return false, false, err
	}

	// There could be at most one such entry
	var inode int64
	var populatedRawVal int
	numRows := 0
	for rows.Next() {
		rows.Scan(&inode, &populatedRawVal)
		numRows++
	}
	rows.Close()

	// The directory has already been populated, if the flag is non zero
	populated := populatedRawVal != 0

	switch numRows {
	case 0:
		// The directory has not been read from from DNAx
		return false, false, nil
	case 1:
		if inode == 0 {
			// There is no such directory on the platform,
			// we know because we queried it.
			return true, false, fuse.ENOENT
		}
		// The directory exists on DNAx
		return true, populated, nil
	default:
		err = fmt.Errorf(
			"Too many values returned from db query, zero or one are expected, received %d",
			numRows)
		return true, false, err
	}
}

// The directory is in the database, read it in its entirety.
func (fsys *Filesys) directoryReadAllEntries(
	dirFullName string) (map[string]File, map[string]Dir, error) {
	if fsys.options.Verbose {
		log.Printf("directoryReadAllEntries %s", dirFullName)
	}

	// Extract information for all the subdirectories
	sqlStmt := fmt.Sprintf(`
 		        SELECT directories.inode
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
		var d Dir
		d.Fsys = fsys
		d.Dname = subDir
		d.Parent = dirFullName

		// create a normalize full path. The edge case occurs for
		// a root directory.
		d.FullPath = makeFullPath(d.Parent, d.Dname)

		rows.Scan(&d.Inode)
		subdirs[d.Dname] = d
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


func (fsys *Filesys) constructDir(txn *sql.Tx, dirFullName string, dxDir *DxFolder) error {
	// Create a database entry for each file
	if fsys.options.Verbose {
		log.Printf("inserting files")
	}
	for _, f := range dxDir.files {
		fInode := allocInodeNum(fsys)

		sqlStmt := fmt.Sprintf(`
 		        INSERT INTO namespace
			VALUES ('%s', '%s', '%s', '%s', '%d', '%d');
			`,
			f.ProjId,
			dirFullNAme,
			f.Name,
			makeFullPath(f.Folder, f.Name),
			nsFileType,
			fInode)
		if _, err = txn.Exec(sqlStmt); err != nil {
			log.Printf(err.Error())
			return err
		}

		//log.Printf("fInode = %d", fInode)
		sqlStmt := fmt.Sprintf(`
 		        INSERT INTO files
			VALUES ('%s', '%s', '%d', '%d', '%d', '%d');
			`,
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
	for _, subDirName := range dxDir.subdirs {
		// DNAx stores directories as full paths. For example: /A/B has
		// as subdirectories  "A/B/C", "A/B/D", "A/B/KKK". A POSIX
		// filesystem represents these as: "A/B", {"C", "D", "KKK"}
		//
		dinode := allocInodeNum(fsys)
		subDirLastPart := strings.TrimPrefix(subDirName, dirFullName)
		subDirLastPart = strings.TrimPrefix(subDirLastPart,"/")
		//log.Printf("dirFullNAme=%s sub=%s lastPart=%s",
		//dirFullName, subDirName, subDirLastPart)

		// TODO: the [subDirLastPart] cannot include a slash, that is a POSIX
		// violation.
		sqlStmt := fmt.Sprintf(`
 		        INSERT INTO namespace
			VALUES ('%s', '%s', '%s', '%s', '%d', '%d');
			`,
			fsys.project.Id,
			dirFullName,
			subDirLastPart,
			subDirName,
			nsDirType,
			dinode)
		if _, err = txn.Exec(sqlStmt); err != nil {
			log.Printf(err.Error())
			return err
		}

		// Create an entry for the subdirectory
		// We haven't described it yet from DNAx, so the populate flag
		// is false.
		sqlStmt = fmt.Sprintf(`
                       INSERT INTO directories
                       VALUES ('%s', '%s', '%d', '%d');
                       `,
			fsys.project.Id, subDirName, dinode, 0)
		if _, err = fsys.db.Exec(sqlStmt); err != nil {
			return err
		}
	}

	if fsys.options.Verbose {
		log.Printf("setting populated for directory %s", dirFullName)
	}

	// Update the directory populated flag to TRUE
	sqlStmt := fmt.Sprintf(`
		UPDATE directories SET populated = '1'
                WHERE dirFullName = '%s'
                `,
		dirFullName)
	if _, err = txn.Exec(sqlStmt); err != nil {
		return err
	}
	return nil
}

func (fsys *Filesys) directoryReadFromDNAx(dirFullName string) error {
	if fsys.options.Verbose {
		log.Printf("describe folder %s", dirFullName)
	}

	// The directory has not been queried yet.
	//
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

	// limit the number of files
	numElementsInDir := len(dxDir.files) + len(dxDir.subdirs)
	if numElementsInDir > MAX_DIR_SIZE {
		return fmt.Errorf(
			"Too many elements (%d) in a directory, the limit is %d",
			numElementsInDir, MAX_DIR_SIZE)
	}

	// The DNAx storage system does not adhere to POSIX. Try
	// to fix the elements in the directory, so they would comply. This
	// comes at the cost of renaming the original files, which can
	// very well mislead the user.
	dxDir, err = PosixFixDir(fsys, dxDir)
	if err != nil {
		return err
	}
	if fsys.options.Verbose {
		log.Printf("unique file names [")
		for _, fDesc := range dxDir.files {
			log.Printf("  %s", fDesc.Name)
		}
		log.Printf("]")
	}

	txn, err := fsys.db.Begin()
	if err != nil {
		return err
	}

	if err := fsys.constructDir(txn, dirFullNAme, dxDir); err != nil {
		txn.Rollback()
		log.Printf(err.Error())
		return err
	}
	txn.Commit()
	return nil
}


// Add a directory with its contents to an exisiting database
func (fsys *Filesys) MetadataDbReadDirAll(
	dirFullName string) (map[string]File, map[string]Dir, error) {
	if fsys.options.Verbose {
		log.Printf("MetadataDbReadDirAll %s", dirFullName)
	}

	retval, populated, err := fsys.directoryExists(dirFullName)
	if err != nil {
		log.Printf("err = %s, %s", err.Error(), dirFullName)
		return nil, nil, err
	}
	if retval {
		if fsys.options.Verbose {
			log.Printf("Directory %s exists in the database", dirFullName)
		}
		// the directory already exists. read it, and return
		// all the entries.
		//
		if !populated {
			// we need to read the directory from dnanexus

			// Get all the directory from dnanexus. This could take a while
			// for large directories.
			if err := fsys.directoryReadFromDNAx(dirFullName); err != nil {
				return nil, nil, err
			}
		}
	}

	// Now that the directory is in the database, we can read it with a local query.
	return fsys.directoryReadAllEntries(dirFullName)
}

func (fsys *Filesys) fastLookup(
	dirFullName string,
	dirOrFileName string) (fs.Node, error) {
	// point lookup in the files table
	sqlStmt := fmt.Sprintf(`
 		        SELECT file_id,proj_id,fname,size,inode,ctime,mtime FROM files
			WHERE folder = '%s' AND fname = '%s';
			`,
		dirFullName, dirOrFileName)
	rows, err := fsys.db.Query(sqlStmt)
	if err != nil {
		log.Printf(err.Error())
		return nil, err
	}

	var f File
	f.Fsys = fsys
	numRows := 0
	for rows.Next() {
		var ctime int64
		var mtime int64
		rows.Scan(&f.FileId, &f.ProjId, &f.Name, &f.Size, &f.Inode, &ctime, &mtime)
		f.Ctime = time.Unix(ctime, 0)
		f.Mtime = time.Unix(mtime, 0)
		numRows++
	}
	rows.Close()
	if numRows == 1 {
		return &f, nil
	}
	if numRows > 1 {
		err = fmt.Errorf("Found %d files of the form %s/%s",
			numRows, dirFullName, dirOrFileName)
		return nil, err
	}

	// do a point lookup in the directory table
	fullPath := makeFullPath(dirFullName, dirOrFileName)
	sqlStmt = fmt.Sprintf(`
 		        SELECT inode FROM directories
			WHERE dirFullName = '%s';
			`,
		fullPath)

	rows, err = fsys.db.Query(sqlStmt)
	if err != nil {
		return nil, err
	}

	var d Dir
	d.Fsys = fsys
	d.Parent = dirFullName
	d.Dname = dirOrFileName
	d.FullPath = fullPath
	numRows = 0
	for rows.Next() {
		rows.Scan(&d.Inode)
		numRows++
	}
	rows.Close()

	if numRows == 1 {
		return &d, nil
	}
	if numRows > 1 {
		return nil, fmt.Errorf("Found %d > 1 entries for directory %s", numRows, d.FullPath)
	}

	// no such entry
	return nil, fuse.ENOENT
}

// Look for file [filename] in directory [parent]/[dname].
//
// 1. Look if the directory has already been downloaded and placed in the DB
// 2. If not, populate it
// 3. Do a lookup in the directory.
//
// Note: the file might not exist.
func (fsys *Filesys) MetadataDbLookupInDir(
	dirFullName string,
	dirOrFileName string) (fs.Node, error) {

	retval, populated, err := fsys.directoryExists(dirFullName)
	if retval && populated {
		// The directory exists, and has already been populated.
		// I think this is the normal path.
		return fsys.fastLookup(dirFullName, dirOrFileName)
	}

	// Slow path,
	files, subdirs, err := fsys.MetadataDbReadDirAll(dirFullName)
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
func (fsys *Filesys) MetadataDbRoot() (*Dir, error) {
	sqlStmt := fmt.Sprintf(`
 		        SELECT inode FROM directories
			WHERE dirFullName = "/";
			`)
	rows, err := fsys.db.Query(sqlStmt)
	if err != nil {
		return nil, err
	}

	var d Dir
	d.Fsys = fsys
	d.Parent = ""
	d.Dname = "/"
	d.FullPath = "/"
	numRows := 0
	for rows.Next() {
		rows.Scan(&d.Inode)
		numRows++
	}
	rows.Close()

	if fsys.options.Verbose {
		log.Printf("Read root dir, inode=%d", d.Inode)
	}

	switch numRows {
	case 0:
		return nil, fmt.Errorf("Could not find root directory")
	case 1:
		return &d, nil
	default:
		return nil, fmt.Errorf("Found more than one root directory")
	}
}
