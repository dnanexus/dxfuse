package dxfs2

import (
	"fmt"
	"log"
	"strings"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)


// Construct a local sql database that holds metadata for
// a large number of dx:files. This metadata_db will be consulted
// when performing dxfs2 operations. For example, a read-dir is
// translated into a query for all the files inside a directory.

func makeFullPath(parent string, name string) string {
	fullPath := parent + "/" + name
	return strings.ReplaceAll(fullPath, "//", "/")
}

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
	//
	// mtime and ctime are measured in seconds since 1st of January 1970
	// (Unix time).
	sqlStmt := `
	CREATE TABLE files (
		file_id text,
		proj_id text,
		fname text,
		folder text,
		size integer,
                inode integer,
                ctime integer,
                mtime integer,
                PRIMARY KEY (folder,fname)
	);
	`
	if _, err := fsys.db.Exec(sqlStmt); err != nil {
		return err
	}

	// Create a table for subdirectories.
	// For example, directory /A/B/C will be represented with record:
	//    dname="C"
	//    folder="/A/B"
	//
	sqlStmt = `
	CREATE TABLE subdirs (
		proj_id text,
		parent text,
		dname text,
                PRIMARY KEY (parent,dname)
	);
	`
	if _, err := fsys.db.Exec(sqlStmt); err != nil {
		return err
	}

	// A separate table for directories.
	//
	// If the inode is -1, then, the directory does not exist on the platform.
	// If poplated is zero, we haven't described the directory yet.
	sqlStmt = `
	CREATE TABLE directories (
		proj_id text,
		dirFullName text PRIMARY KEY,
                inode integer,
                populated int
	);
	`
	if _, err := fsys.db.Exec(sqlStmt); err != nil {
		return err
	}

	// Adding a root directory
	sqlStmt = fmt.Sprintf(`
 		        INSERT INTO directories
			VALUES ('%s', '%s', '%d', '%d');
			`,
		fsys.projectId, "/", INODE_ROOT_DIR, 0)
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
	fsys.inodeCnt += 1
	return fsys.inodeCnt
}

// Three options:
// 1. Directory has been read from DNAx
// 2. Directory does not exist on DNAx
//      error = ENOENT
// 3. Directory has been fully read, and is in the database
//     exists=true  populated=true
func directoryExists(fsys *Filesys, dirFullName string) (bool, bool, error) {
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
			"Two many values returned from db query, zero or one are expected, received %d",
			numRows)
		return true, false, err
	}
}

func queryDirInode(
	fsys *Filesys,
	dirFullName string) (int64, error) {
	sqlStmt := fmt.Sprintf(`
 		        SELECT inode FROM directories
			WHERE dirFullName = '%s';
			`,
		dirFullName)

	rows, err := fsys.db.Query(sqlStmt)
	if err != nil {
		return 0, err
	}
	var inode int64
	numRows := 0
	for rows.Next() {
		rows.Scan(&inode)
		numRows++
	}
	rows.Close()
	if numRows != 1 {
		return 0, fmt.Errorf("Found %d != 1 entries for directory %s", numRows, dirFullName)
	}
	return inode, nil
}


func queryDirSubdirs(
	fsys *Filesys,
	dirFullName string) ([]string, error) {
	// Find the subdirectories
	sqlStmt := fmt.Sprintf(`
 		        SELECT dname FROM subdirs
			WHERE parent = '%s';
			`,
		dirFullName)
	rows, err := fsys.db.Query(sqlStmt)
	if err != nil {
		return nil, err
	}

	// get all the sub-directory names
	subdirList := make([]string, 0)
	for rows.Next() {
		var dname string
		rows.Scan(&dname)
		subdirList = append(subdirList, dname)
	}
	rows.Close()

	return subdirList, nil
}


// The directory is in the database, read it in its entirety.
func directoryReadAllEntries(
	fsys * Filesys,
	dirFullName string) (map[string]File, map[string]Dir, error) {
	if fsys.options.Debug {
		log.Printf("directoryReadAllEntries %s", dirFullName)
	}

	subdirList, err := queryDirSubdirs(fsys, dirFullName)
	if err != nil {
		return nil, nil, err
	}
	//log.Printf("queryDirSubdirs %s -- done", dirFullName)

	subdirs := make(map[string]Dir)
	for _, subDir := range subdirList {
		var d Dir
		d.Fsys = fsys
		d.Dname = subDir
		d.Parent = dirFullName

		// create a normalize full path. The edge case occurs for
		// a root directory.
		d.FullPath = makeFullPath(d.Parent, d.Dname)

		// We need to query the directories table, to get the inode
		subdInode, err := queryDirInode(fsys, d.FullPath)
		if err != nil {
			return nil, nil, err
		}
		d.Inode = subdInode
		subdirs[d.Dname] = d
	}
	//log.Printf("#subdirs %d", len(subdirs))

	// Find the files in the directory
	//
	// TODO: something is going wrong with the ctime and mtime. Not sure
	// what exactly. The order matters, for some reason. Which is why I
	// query inode first.
	sqlStmt := fmt.Sprintf(`
 		        SELECT file_id,proj_id,fname,size,inode,ctime,mtime FROM files
			WHERE folder = '%s';
			`,
		dirFullName)
	rows, err := fsys.db.Query(sqlStmt)
	if err != nil {
		log.Printf(err.Error())
		return nil, nil, err
	}

	//log.Printf("creating files structure [")
	files := make(map[string]File)
	for rows.Next() {
		var f File
		var ctime int64
		var mtime int64
		rows.Scan(&f.FileId, &f.ProjId, &f.Name, &f.Size, &f.Inode, &ctime, &mtime)

		f.Fsys = fsys
		f.Ctime = time.Unix(ctime, 0)
		f.Mtime = time.Unix(mtime, 0)

		files[f.Name] = f
	}
	rows.Close()
	//log.Printf("  #files=%d", len(files))
	//log.Printf("]")
	return files, subdirs, nil
}

func directoryReadFromDNAx(fsys *Filesys, dirFullName string) error {
	if fsys.options.Debug {
		log.Printf("describe folder %s", dirFullName)
	}

	// The directory has not been queried yet.
	//
	// describe all the files
	dxDir, err := DxDescribeFolder(&fsys.dxEnv, fsys.projectId, dirFullName)
	if err != nil {
		return err
	}

	if fsys.options.Debug {
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
	dxDir, err = PosixFixDir(fsys, dirFullName, dxDir)
	if err != nil {
		return err
	}
	if fsys.options.Debug {
		log.Printf("unique file names [")
		for _, fDesc := range dxDir.files {
			log.Printf("  %s", fDesc.Name)
		}
		log.Printf("]")
	}
	if _, err = fsys.db.Exec("BEGIN TRANSACTION"); err != nil {
		return err
	}

	// Create a database entry for each file
	if fsys.options.Debug {
		log.Printf("inserting files")
	}
	for _, d := range dxDir.files {
		fInode := allocInodeNum(fsys)
		//log.Printf("fInode = %d", fInode)
		sqlStmt := fmt.Sprintf(`
 		        INSERT INTO files
			VALUES ('%s', '%s', '%s', '%s', '%d', '%d', '%d', '%d');
			`,
			d.FileId, d.ProjId, d.Name, d.Folder, d.Size, fInode, d.Ctime.Unix(), d.Mtime.Unix())
		_, err = fsys.db.Exec(sqlStmt)
		if err != nil {
			log.Printf(err.Error())
			return err
		}
	}

	// Create a database entry for each sub-directory
	if fsys.options.Debug {
		log.Printf("inserting subdirs")
	}
	for _, subDirName := range dxDir.subdirs {
		// DNAx stores directories as full paths. For example: /A/B has
		// as subdirectories  "A/B/C", "A/B/D", "A/B/KKK". A POSIX
		// filesystem represents these as: "A/B", {"C", "D", "KKK"}
		//
		subDirLastPart := strings.TrimPrefix(subDirName, dirFullName)
		subDirLastPart = strings.TrimPrefix(subDirLastPart,"/")
		//log.Printf("dirFullNAme=%s sub=%s lastPart=%s",
		//dirFullName, subDirName, subDirLastPart)

		// TODO: the [subDirLatPart] cannot include a slash, that is a POSIX
		// violation.

		sqlStmt := fmt.Sprintf(`
 		        INSERT INTO subdirs
			VALUES ('%s', '%s', '%s');
			`,
			fsys.projectId,
			dirFullName,
			subDirLastPart)
		if _, err = fsys.db.Exec(sqlStmt); err != nil {
			log.Printf(err.Error())
			return err
		}

		// Create an entry for the subdirectory
		// We haven't described it yet from DNAx, so the populate flag
		// is false.
		subdirInode := allocInodeNum(fsys)
		sqlStmt = fmt.Sprintf(`
                       INSERT INTO directories
                       VALUES ('%s', '%s', '%d', '%d');
                       `,
			fsys.projectId, subDirName, subdirInode, 0)
		if _, err = fsys.db.Exec(sqlStmt); err != nil {
			log.Printf(err.Error())
			return err
		}
	}

	if fsys.options.Debug {
		log.Printf("setting populated for directory %s", dirFullName)
	}

	// Update the directory populated flag to TRUE
	sqlStmt := fmt.Sprintf(`
		UPDATE directories SET populated = '1'
                WHERE dirFullName = '%s'
                `,
		dirFullName)
	if _, err = fsys.db.Exec(sqlStmt); err != nil {
		return err
	}

	if _, err = fsys.db.Exec("END TRANSACTION"); err != nil {
		return err
	}

	return nil
}


// Add a directory with its contents to an exisiting database
func MetadataDbReadDirAll(
	fsys *Filesys,
	dirFullName string) (map[string]File, map[string]Dir, error) {
	if fsys.options.Debug {
		log.Printf("MetadataDbReadDirAll %s", dirFullName)
	}

	retval, populated, err := directoryExists(fsys, dirFullName)
	if err != nil {
		log.Printf("err = %s", dirFullName)
		return nil, nil, err
	}
	if retval {
		if fsys.options.Debug {
			log.Printf("Directory %s exists in the database", dirFullName)
		}
		// the directory already exists. read it, and return
		// all the entries.
		//
		if !populated {
			// we need to read the directory from dnanexus

			// Get all the directory from dnanexus. This could take a while
			// for large directories.
			if err := directoryReadFromDNAx(fsys, dirFullName); err != nil {
				return nil, nil, err
			}
		}
	}

	// Now that the directory is in the database, we can read it with a local query.
	return directoryReadAllEntries(fsys, dirFullName)
}

func fastLookup(
	fsys *Filesys,
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
func MetadataDbLookupInDir(
	fsys *Filesys,
	dirFullName string,
	dirOrFileName string) (fs.Node, error) {

	retval, populated, err := directoryExists(fsys, dirFullName)
	if retval && populated {
		// The directory exists, and has already been populated.
		// I think this is the normal path.
		return fastLookup(fsys, dirFullName, dirOrFileName)
	}

	// Slow path,
	files, subdirs, err := MetadataDbReadDirAll(fsys, dirFullName)
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

	if fsys.options.Debug {
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
