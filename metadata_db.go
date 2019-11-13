package dxfuse

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
	"os"
	"strings"
	"time"

	"github.com/dnanexus/dxda"
	"github.com/hashicorp/go-retryablehttp" // use http libraries from hashicorp for implement retry logic
)


const (
	nsDirType = 1
	nsDataObjType = 2
)

type MetadataDb struct {
	// an open handle to the database
	db               *sql.DB
	dbFullPath        string

	// a pool of http clients, for short requests, such as file creation,
	// or file describe.
	httpClientPool    chan(*retryablehttp.Client)

	// configuration information for accessing dnanexus servers
	dxEnv             dxda.DXEnvironment

	// mapping from mounted directory to project ID
	baseDir2ProjectId map[string]string

	inodeCnt          int64
	options           Options
}

func NewMetadataDb(
	dbFullPath string,
	dxEnv dxda.DXEnvironment,
	httpClientPool chan(*retryablehttp.Client),
	options Options) (*MetadataDb, error) {
	// create a connection to the database, that will be kept open
	db, err := sql.Open("sqlite3", dbFullPath + "?mode=rwc")
	if err != nil {
		return nil, fmt.Errorf("Could not open the database %s", dbFullPath)
	}

	return &MetadataDb{
		db : db,
		dbFullPath : dbFullPath,
		httpClientPool : httpClientPool,
		dxEnv : dxEnv,
		baseDir2ProjectId: make(map[string]string),
		inodeCnt : InodeRoot + 1,
		options : options,
	}, nil
}

// Construct a local sql database that holds metadata for
// a large number of dx:files. This metadata_db will be consulted
// when performing dxfuse operations. For example, a read-dir is
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

func intToBool(x int) bool {
	if x > 0 {
		return true
	}
	return false
}

func (mdb *MetadataDb) init2(txn *sql.Tx) error {
	// Create table for files.
	//
	// mtime and ctime are measured in seconds since 1st of January 1970
	// (Unix time).
	sqlStmt := `
	CREATE TABLE data_objects (
                kind int,
		id text,
		proj_id text,
                inode bigint,
		size bigint,
                ctime bigint,
                mtime bigint,
                mode int,
                nlink int,
                inline_data  string,
                PRIMARY KEY (inode)
	);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		log.Printf(err.Error())
		return fmt.Errorf("Could not create table data_objects")
	}

	sqlStmt = `
	CREATE INDEX id_index
	ON data_objects (id);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		log.Printf(err.Error())
		return fmt.Errorf("Could not create index id_index on table data_objects")
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
		log.Printf(err.Error())
		return fmt.Errorf("Could not create table namespace")
	}

	sqlStmt = `
	CREATE INDEX parent_index
	ON namespace (parent);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		log.Printf(err.Error())
		return fmt.Errorf("Could not create index parent_index on table namespace")
	}

	// we need to be able to get from the files/tables, back to the namespace
	// with an inode ID.
	sqlStmt = `
	CREATE INDEX inode_rev_index
	ON namespace (inode);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		log.Printf(err.Error())
		return fmt.Errorf("Could not create index inode_rev_index on table namespace")
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
                mode int,
                PRIMARY KEY (inode)
	);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		log.Printf(err.Error())
		return fmt.Errorf("Could not create table directories")
	}

	// Adding a root directory. The root directory does
	// not belong to any one project. This allows mounting
	// several projects from the same root. This is denoted
	// by marking the project as the empty string.
	nowSeconds := time.Now().Unix()
	sqlStmt = fmt.Sprintf(`
 		        INSERT INTO directories
			VALUES ('%d', '%s', '%s', '%d', '%d', '%d', '%d');`,
		InodeRoot, "", "", boolToInt(false),
		nowSeconds, nowSeconds, dirReadOnlyMode)
	if _, err := txn.Exec(sqlStmt); err != nil {
		log.Printf(err.Error())
		return fmt.Errorf("Could not create root directory")
	}

	// We want the root path to match the results of splitPath("/")
	rParent, rBase := splitPath("/")
	sqlStmt = fmt.Sprintf(`
 		        INSERT INTO namespace
			VALUES ('%s', '%s', '%d', '%d');`,
		rParent, rBase, nsDirType, InodeRoot)
	if _, err := txn.Exec(sqlStmt); err != nil {
		log.Printf(err.Error())
		return fmt.Errorf("Could not create root inode (%d)", InodeRoot)
	}

	return nil
}

// construct an initial empty database, representing an entire project.
func (mdb *MetadataDb) Init() error {
	if mdb.options.Verbose {
		log.Printf("Initializing metadata database\n")
	}

	txn, err := mdb.db.Begin()
	if err != nil {
		log.Printf(err.Error())
		return fmt.Errorf("Could not open transaction")
	}

	if err := mdb.init2(txn); err != nil {
		txn.Rollback()
		log.Printf(err.Error())
		return fmt.Errorf("Could not initialize database")
	}

	if err := txn.Commit(); err != nil {
		txn.Rollback()
		log.Printf(err.Error())
		return fmt.Errorf("Error during commit")
	}

	if mdb.options.Verbose {
		log.Printf("Completed creating files and directories tables\n")
	}
	return nil
}

// Allocate an inode number. These must remain stable during the
// lifetime of the mount.
//
// Note: this call should perform while holding the mutex
func (mdb *MetadataDb) allocInodeNum() int64 {
	mdb.inodeCnt += 1
	return mdb.inodeCnt
}

// search for a file by Id. If the file exists, return its inode and link-count. Otherwise,
// return 0, 0.
func (mdb *MetadataDb) lookupDataObjectById(txn *sql.Tx, fId string) (int64, int, bool, error) {
	// point lookup in the files table
	sqlStmt := fmt.Sprintf(`
 		        SELECT inode,nlink
                        FROM data_objects
			WHERE id = '%s';`,
		fId)
	rows, err := txn.Query(sqlStmt)
	if err != nil {
		log.Printf(err.Error())
		return InodeInvalid, 0, false, fmt.Errorf("lookupDataObjectById fId=%s", fId)
	}

	var nlink int
	var inode int64
	numRows := 0
	for rows.Next() {
		rows.Scan(&inode, &nlink)
		numRows++
	}
	rows.Close()

	switch numRows {
	case 0:
		// this file doesn't exist in the database
		return InodeInvalid, 0, false, nil
	case 1:
		// correct, there is exactly one such file
		return inode, nlink, true, nil
	default:
		panic(fmt.Sprintf("Found %d data-objects with Id %s", numRows, fId))
	}
}

// search for a file with a particular inode
func (mdb *MetadataDb) lookupDataObjectByInode(oname string, inode int64) (File, bool, error) {
	// point lookup in the files table
	sqlStmt := fmt.Sprintf(`
 		        SELECT kind,id,proj_id,size,ctime,mtime,mode,nlink,inline_data
                        FROM data_objects
			WHERE inode = '%d';`,
		inode)
	rows, err := mdb.db.Query(sqlStmt)
	if err != nil {
		log.Printf(err.Error())
		return File{}, false, fmt.Errorf("lookupDataObjectByInode %s inode=%d", oname, inode)
	}

	var f File
	f.Name = oname
	f.Inode = inode
	numRows := 0
	for rows.Next() {
		var ctime int64
		var mtime int64
		rows.Scan(&f.Kind,&f.Id, &f.ProjId, &f.Size, &ctime, &mtime, &f.Mode, &f.Nlink, &f.InlineData)
		f.Ctime = SecondsToTime(ctime)
		f.Mtime = SecondsToTime(mtime)
		numRows++
	}
	rows.Close()

	switch numRows {
	case 0:
		// no file found
		return File{}, false, nil
	case 1:
		// found exactly one file
		return f, true, nil
	default:
		panic(fmt.Sprintf("Found %d data-objects with name %s",	numRows, oname))
	}
}

func (mdb *MetadataDb) lookupDirByInode(parent string, dname string, inode int64) (Dir, bool, error) {
	var d Dir
	d.Parent = parent
	d.Dname = dname
	d.FullPath = filepath.Clean(filepath.Join(parent, dname))
	d.Inode = inode
	d.Uid = mdb.options.Uid
	d.Gid = mdb.options.Gid

	// Extract information from the directories table
	sqlStmt := fmt.Sprintf(`
 		        SELECT proj_id, proj_folder, populated, ctime, mtime, mode
                        FROM directories
			WHERE inode = '%d';`,
		inode)
	rows, err := mdb.db.Query(sqlStmt)
	if err != nil {
		log.Print(err.Error())
		return Dir{}, false, fmt.Errorf("lookupDirByInode inode=%d", inode)
	}

	numRows := 0
	for rows.Next() {
		var populated int
		var ctime int64
		var mtime int64
		var mode  int

		rows.Scan(&d.ProjId, &d.ProjFolder, &populated, &ctime, &mtime, &mode)

		d.Ctime = SecondsToTime(ctime)
		d.Mtime = SecondsToTime(mtime)
		d.Mode = os.FileMode(mode)
		d.Populated = intToBool(populated)
		numRows++
	}
	rows.Close()

	switch numRows {
	case 0:
		return Dir{}, false, nil
	case 1:
		// correct, just one version
	default:
		panic(fmt.Sprintf("found %d directory with inode=%d in the directories table",
			numRows, inode))
	}

	return d, true, nil
}

// search for a file with a particular inode
func (mdb *MetadataDb) LookupByInode(ctx context.Context, inode int64) (Node, bool, error) {
	// point lookup in the namespace table
	sqlStmt := fmt.Sprintf(`
 		        SELECT parent,name,obj_type
                        FROM namespace
			WHERE inode = '%d';`,
		inode)
	rows, err := mdb.db.Query(sqlStmt)
	if err != nil {
		log.Printf(err.Error())
		return nil, false, fmt.Errorf("LookupByInode: error in query")
	}
	var parent string
	var name string
	var obj_type int
	numRows := 0
	for rows.Next() {
		rows.Scan(&parent, &name, &obj_type)
		numRows++
	}
	rows.Close()

	switch numRows {
	case 0:
		return nil, false, nil
	case 1:
		// correct, there is exactly one such file
	default:
		panic(fmt.Sprintf("Found %d data-objects with inode %d", numRows, inode))
	}

	switch obj_type {
	case nsDirType:
		return mdb.lookupDirByInode(parent, name, inode)
	case nsDataObjType:
		return mdb.lookupDataObjectByInode(name, inode)
	default:
		panic(fmt.Sprintf("Invalid type %d in namespace table", obj_type))
	}
}


// The directory is in the database, read it in its entirety.
func (mdb *MetadataDb) directoryReadAllEntries(
	dirFullName string) (map[string]File, map[string]Dir, error) {
	if mdb.options.Verbose {
		log.Printf("directoryReadAllEntries %s", dirFullName)
	}

	// Extract information for all the subdirectories
	sqlStmt := fmt.Sprintf(`
 		        SELECT directories.inode, directories.proj_id, namespace.name, directories.ctime, directories.mtime, directories.mode
                        FROM directories
                        JOIN namespace
                        ON directories.inode = namespace.inode
			WHERE namespace.parent = '%s' AND namespace.obj_type = '%d';
			`, dirFullName, nsDirType)
	rows, err := mdb.db.Query(sqlStmt)
	if err != nil {
		log.Print(err.Error())
		log.Printf("Error in directories query")
		return nil, nil, err
	}

	subdirs := make(map[string]Dir)
	for rows.Next() {
		var inode int64
		var dname string
		var projId string
		var ctime int64
		var mtime int64
		var mode int
		rows.Scan(&inode, &projId, &dname, &ctime, &mtime, &mode)

		subdirs[dname] = Dir{
			Parent : dirFullName,
			Dname : dname,
			FullPath : filepath.Clean(filepath.Join(dirFullName, dname)),
			Inode : inode,
			Ctime : SecondsToTime(ctime),
			Mtime : SecondsToTime(mtime),
			Mode : os.FileMode(mode),
		}
	}
	rows.Close()

	// Extract information for all the files
	sqlStmt = fmt.Sprintf(`
 		        SELECT dos.kind, dos.id, dos.proj_id, dos.inode, dos.size, dos.ctime, dos.mtime, dos.mode, dos.nlink, dos.inline_data, namespace.name
                        FROM data_objects as dos
                        JOIN namespace
                        ON dos.inode = namespace.inode
			WHERE namespace.parent = '%s' AND namespace.obj_type = '%d';
			`, dirFullName, nsDataObjType)
	rows, err = mdb.db.Query(sqlStmt)
	if err != nil {
		log.Print(err.Error())
		log.Printf("Error in data object query")
		return nil, nil, err
	}

	// Find the files in the directory
	files := make(map[string]File)
	for rows.Next() {
		var f File

		var ctime int64
		var mtime int64
		var mode int
		rows.Scan(&f.Kind,&f.Id, &f.ProjId, &f.Inode, &f.Size, &ctime, &mtime, &mode, &f.Nlink, &f.InlineData,&f.Name)
		f.Ctime = SecondsToTime(ctime)
		f.Mtime = SecondsToTime(mtime)
		f.Mode = os.FileMode(mode)

		files[f.Name] = f
	}

	//log.Printf("  #files=%d", len(files))
	//log.Printf("]")
	return files, subdirs, nil
}

// Create an entry representing one remote file. This has
// several use cases:
//  1) Create a singleton file from the manifest
//  2) Create a new file, and upload it later to the platform
//  3) Discover a file in a directory, which may actually be a link to another file.
func (mdb *MetadataDb) createDataObject(
	mustBeNewObject bool,
	txn *sql.Tx,
	kind int,
	projId string,
	objId string,
	size int64,
	ctime int64,
	mtime int64,
	mode os.FileMode,
	parentDir string,
	fname string,
	inlineData string) (int64, error) {
	if mdb.options.VerboseLevel > 1 {
		log.Printf("createDataObject %s:%s %s", projId, objId,
			filepath.Clean(parentDir + "/" + fname))
	}

	inode, nlink, ok, err := mdb.lookupDataObjectById(txn, objId)
	if err != nil {
		return 0, err
	}

	if !ok {
		// File doesn't exist, we need to choose a new inode number.
		// NOte: it is on stable storage, and will not change.
		inode = mdb.allocInodeNum()

		// Create an entry for the file
		sqlStmt := fmt.Sprintf(`
 		        INSERT INTO data_objects
			VALUES ('%d', '%s', '%s', '%d', '%d', '%d', '%d', '%d', '%d', '%s');`,
			kind, objId, projId, inode, size, ctime, mtime, int(mode), 1, inlineData)
		if _, err := txn.Exec(sqlStmt); err != nil {
			log.Printf(err.Error())
			log.Printf("Error inserting into data objects table")
			return 0, err
		}
	} else {
		// File already exists, we need to increase the link count
		if mustBeNewObject {
			panic(fmt.Sprintf("Object %s:%s must not be already in the database. We are trying to create it again", projId, objId))
		}

		sqlStmt := fmt.Sprintf(`
 		        UPDATE data_objects
                        SET nlink = '%d'
			WHERE id = '%s';`,
			nlink + 1, objId)
		if _, err := txn.Exec(sqlStmt); err != nil {
			log.Printf("Error updating data_object table, incrementing the link number")
			return 0, err
		}
	}

	sqlStmt := fmt.Sprintf(`
 		        INSERT INTO namespace
			VALUES ('%s', '%s', '%d', '%d');`,
		parentDir, fname, nsDataObjType, inode)
	if _, err := txn.Exec(sqlStmt); err != nil {
		log.Printf("Error inserting %s/%s into the namespace table", parentDir, fname)
		return 0, err
	}

	return inode, nil
}

// Create an empty directory, and return the inode
//
// Assumption: the directory does not already exist in the database.
func (mdb *MetadataDb) createEmptyDir(
	txn *sql.Tx,
	projId string,
	projFolder string,
	ctime int64,
	mtime int64,
	mode os.FileMode,
	dirPath string,
	populated bool) (int64, error) {
	if dirPath[0] != '/' {
		panic("directory must start with a slash")
	}

	// choose unused inode number. It is on stable stoage, and will not change.
	inode := mdb.allocInodeNum()
	parentDir, basename := splitPath(dirPath)
	if mdb.options.VerboseLevel > 1 {
		log.Printf("createEmptyDir %s:%s %s populated=%t",
			projId, projFolder, dirPath, populated)
	}

	sqlStmt := fmt.Sprintf(`
 		        INSERT INTO namespace
			VALUES ('%s', '%s', '%d', '%d');`,
		parentDir, basename, nsDirType,	inode)
	if _, err := txn.Exec(sqlStmt); err != nil {
		log.Printf("createEmptyDir: error inserting into namespace table %s/%s",
			parentDir, basename)
		return 0, err
	}

	// Create an entry for the subdirectory
	mode = mode | os.ModeDir
	sqlStmt = fmt.Sprintf(`
                       INSERT INTO directories
                       VALUES ('%d', '%s', '%s', '%d', '%d', '%d', '%d');`,
		inode, projId, projFolder, boolToInt(populated), ctime, mtime, int(mode))
	if _, err := txn.Exec(sqlStmt); err != nil {
		log.Printf("createEmptyDir: error inserting into directories table %d",
			inode)
		return 0, err
	}
	return inode, nil
}

// Assumption: the directory does not already exist in the database.
func (mdb *MetadataDb) CreateDir(
	projId string,
	projFolder string,
	ctime int64,
	mtime int64,
	mode os.FileMode,
	dirPath string) (int64, error) {
	txn, err := mdb.db.Begin()
	if err != nil {
		log.Printf(err.Error())
		return 0, fmt.Errorf("CreateDir: error opening transaction")
	}
	dnode, err := mdb.createEmptyDir(txn, projId, projFolder, ctime, mtime, mode, dirPath, true)
	if err != nil {
		txn.Rollback()
		log.Printf(err.Error())
		return 0, fmt.Errorf("error in create dir, rolling back transaction")
	}
	if err := txn.Commit(); err != nil {
		log.Printf(err.Error())
		return 0, fmt.Errorf("CreateDir: error in commit")
	}
	return dnode, nil
}

// Remove a directory from the database
func (mdb *MetadataDb) RemoveEmptyDir(inode int64) error {
	txn, err := mdb.db.Begin()
	if err != nil {
		return err
	}

	sqlStmt := fmt.Sprintf(`
                DELETE FROM directories
                WHERE inode='%d';`,
		inode)
	if _, err := txn.Exec(sqlStmt); err != nil {
		txn.Rollback()
		log.Printf("RemoveEmptyDir(%d): error in directories table removal", inode)
		return err
	}

	sqlStmt = fmt.Sprintf(`
                DELETE FROM namespace
                WHERE inode='%d';`,
		inode)
	if _, err := txn.Exec(sqlStmt); err != nil {
		txn.Rollback()
		log.Printf("RemoveEmptyDir(%d): error in namespace table removal", inode)
		return err
	}

	return txn.Commit()
}

// Update the directory populated flag to TRUE
func (mdb *MetadataDb) setDirectoryToPopulated(txn *sql.Tx, dinode int64) error {
	sqlStmt := fmt.Sprintf(`
		UPDATE directories
                SET populated = '1'
                WHERE inode = '%d'`,
		dinode)
	if _, err := txn.Exec(sqlStmt); err != nil {
		log.Printf("Error set directory %d to populated", dinode)
		return err
	}
	return nil
}

func kindOfFile(o DxDescribeDataObject) int {
	kind := 0
	if strings.HasPrefix(o.Id, "file-") {
		kind = FK_Regular
	} else if strings.HasPrefix(o.Id, "applet-") {
		kind = FK_Applet
	} else if strings.HasPrefix(o.Id, "workflow-") {
		kind = FK_Workflow
	} else if strings.HasPrefix(o.Id, "record-") {
		kind = FK_Record
	} else if strings.HasPrefix(o.Id, "database-") {
		kind = FK_Database
	}
	if kind == 0 {
		log.Printf("A data object has an unknown prefix (%s)", o.Id)
		kind = FK_Other
	}

	// A symbolic link is a special kind of regular file
	if kind == FK_Regular &&
		len(o.SymlinkPath) > 0 {
		kind = FK_Symlink
	}
	return kind
}

func inlineDataOfFile(kind int, o DxDescribeDataObject) string {
	if kind == FK_Regular && len(o.SymlinkPath) > 0 {
		// A symbolic link
		kind = FK_Symlink
	}

	switch (kind) {
	case FK_Symlink:
		return o.SymlinkPath
	default:
		return ""
	}
}

// Create a directory with: an i-node, files, and empty unpopulated subdirectories.
func (mdb *MetadataDb) populateDir(
	txn *sql.Tx,
	dinode int64,
	projId string,
	projFolder string,
	ctime int64,
	mtime int64,
	dirPath string,
	dxObjs []DxDescribeDataObject,
	subdirs []string) error {
	if mdb.options.VerboseLevel > 1 {
		var objNames []string
		for _, oDesc := range dxObjs {
			objNames = append(objNames, oDesc.Name)
		}
		log.Printf("populateDir(%s)  data-objects=%v  subdirs=%v", dirPath, objNames, subdirs)
	}

	// Create a database entry for each file
	if mdb.options.VerboseLevel > 1 {
		log.Printf("inserting files")
	}

	for _, o := range dxObjs {
		kind := kindOfFile(o)
		inlineData := inlineDataOfFile(kind, o)

		_, err := mdb.createDataObject(
			false,
			txn,
			kind,
			o.ProjId,
			o.Id,
			o.Size,
			o.CtimeSeconds,
			o.MtimeSeconds,
			fileReadOnlyMode,
			dirPath,
			o.Name,
			inlineData)
		if err != nil {
			return err
		}
	}

	// Create a database entry for each sub-directory
	if mdb.options.VerboseLevel > 1 {
		log.Printf("inserting subdirs")
	}
	for _, subDirName := range subdirs {
		// Create an entry for the subdirectory.
		// We haven't described it yet from DNAx, so the populate flag
		// is false.
		_, err := mdb.createEmptyDir(
			txn,
			projId, filepath.Clean(projFolder + "/" + subDirName),
			ctime, mtime,
			dirReadWriteMode,
			filepath.Clean(dirPath + "/" + subDirName),
			false)
		if err != nil {
			log.Printf("Error creating empty directory %s while populating directory %s",
				filepath.Clean(projFolder + "/" + subDirName), dirPath)
			return err
		}
	}

	if mdb.options.VerboseLevel > 1 {
		log.Printf("setting populated for directory %s", dirPath)
	}

	// Update the directory populated flag to TRUE
	mdb.setDirectoryToPopulated(txn, dinode)
	return nil
}

// Query DNAx about a folder, and encode all the information in the database.
//
// assumptions:
// 1. An empty directory has been created on the database.
// 1. The directory has not been queried yet.
// 2. The global lock is held
func (mdb *MetadataDb) directoryReadFromDNAx(
	ctx context.Context,
	dinode int64,
	projId string,
	projFolder string,
	ctime int64,
	mtime int64,
	dirFullName string) error {

	if mdb.options.Verbose {
		log.Printf("directoryReadFromDNAx: describe folder %s:%s", projId, projFolder)
	}

	// describe all (closed) files
	httpClient := <- mdb.httpClientPool
	dxDir, err := DxDescribeFolder(ctx, httpClient, &mdb.dxEnv, projId, projFolder, true)
	mdb.httpClientPool <- httpClient
	if err != nil {
		fmt.Printf(err.Error())
		fmt.Printf("reading directory frmo DNAx error")
		return err
	}

	if mdb.options.Verbose {
		log.Printf("read dir from DNAx #data_objects=%d #subdirs=%d",
			len(dxDir.dataObjects),
			len(dxDir.subdirs))
	}

	// Approximate the ctime/mtime using the file timestamps.
	// - The directory creation time is the minimum of all file creates.
	// - The directory modification time is the maximum across all file modifications.
	ctimeApprox := ctime
	mtimeApprox := mtime
	for _, f := range dxDir.dataObjects {
		ctimeApprox = MinInt64(ctimeApprox, f.CtimeSeconds)
		mtimeApprox = MaxInt64(mtimeApprox, f.MtimeSeconds)
	}

	// The DNAx storage system does not adhere to POSIX. Try
	// to fix the elements in the directory, so they would comply. This
	// comes at the cost of renaming the original files, which can
	// very well mislead the user.
	posixDir, err := PosixFixDir(mdb.options, dxDir)
	if err != nil {
		return err
	}

	txn, err := mdb.db.Begin()
	if err != nil {
		log.Printf(err.Error())
		return fmt.Errorf("directoryReadFromDNAx: error opening transaction")
	}

	// build the top level directory
	err = mdb.populateDir(
		txn, dinode,
		projId, projFolder,
		ctimeApprox, mtimeApprox,
		dirFullName, posixDir.dataObjects, posixDir.subdirs)
	if err != nil {
		txn.Rollback()
		log.Printf(err.Error())
		return fmt.Errorf("directoryReadFromDNAx: Error populating directory")
	}

	// create the faux sub directories. These have no additional depth, and are fully
	// populated. They contains all the files with multiple versions.
	//
	// Note: these directories DO NOT have a matching project folder.
	for dName, fauxFiles := range posixDir.fauxSubdirs {
		fauxDirPath := filepath.Clean(dirFullName + "/" + dName)

		// create the directory in the namespace, as if it is unpopulated.
		fauxDirInode, err := mdb.createEmptyDir(
			txn, projId, "",
			ctimeApprox, mtimeApprox,
			dirReadOnlyMode,    // faux directories are read only
			fauxDirPath, true)
		if err != nil {
			txn.Rollback()
			log.Printf(err.Error())
			return fmt.Errorf("directoryReadFromDNAx: creating faux directory %s", fauxDirPath)
		}

		var no_subdirs []string
		err = mdb.populateDir(
			txn, fauxDirInode,
			projId, "",
			ctimeApprox, mtimeApprox,
			fauxDirPath, fauxFiles, no_subdirs)
		if err != nil {
			txn.Rollback()
			log.Printf(err.Error())
			return fmt.Errorf("directoryReadFromDNAx: populating faux directory %s", fauxDirPath)
		}
	}

	return txn.Commit()
}


// Add a directory with its contents to an exisiting database
func (mdb *MetadataDb) ReadDirAll(ctx context.Context, dir *Dir) (map[string]File, map[string]Dir, error) {
	if mdb.options.Verbose {
		log.Printf("ReadDirAll %s", dir.FullPath)
	}

	if !dir.Populated {
		err := mdb.directoryReadFromDNAx(
			ctx,
			dir.Inode,
			dir.ProjId,
			dir.ProjFolder,
			int64(dir.Ctime.Second()),
			int64(dir.Mtime.Second()),
			dir.FullPath)
		if err != nil {
			return nil, nil, err
		}
		dir.Populated = true
	}

	// Now that the directory is in the database, we can read it with a local query.
	return mdb.directoryReadAllEntries(dir.FullPath)
}


// Search for a file/subdir in a directory
// Look for file [filename] in directory [parent]/[dname].
//
// 1. Look if the directory has already been downloaded and placed in the DB
// 2. If not, populate it
// 3. Do a lookup in the directory.
//
// Note: the file might not exist.
func (mdb *MetadataDb) LookupInDir(ctx context.Context, dir *Dir, dirOrFileName string) (Node, bool, error) {
	if !dir.Populated {
		mdb.ReadDirAll(ctx, dir)
	}

	// point lookup in the namespace
	sqlStmt := fmt.Sprintf(`
 		        SELECT obj_type,inode
                        FROM namespace
			WHERE parent = '%s' AND name = '%s';`,
		dir.FullPath, dirOrFileName)
	rows, err := mdb.db.Query(sqlStmt)
	if err != nil {
		return nil, false, err
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
		return nil, false, nil
	}
	if numRows > 1 {
		panic(fmt.Sprintf("Found %d files of the form %s/%s",
			numRows, dir.FullPath, dirOrFileName))
	}

	// There is exactly one answer
	switch objType {
	case nsDirType:
		return mdb.lookupDirByInode(dir.FullPath, dirOrFileName, inode)
	case nsDataObjType:
		file, ok, err := mdb.lookupDataObjectByInode(dirOrFileName, inode)
		if ok && err == nil {
			log.Printf("lookupDataObjectByInode: %v", file)
			log.Printf("size: %d", file.Size)
		}
		return file, ok, err
	default:
		panic(fmt.Sprintf("Invalid object type %d", objType))
	}
}

// Build a toplevel directory for each project.
func (mdb *MetadataDb) PopulateRoot(ctx context.Context, manifest Manifest) error {
	log.Printf("Populating root directory")

	for _, d := range manifest.Directories {
		mdb.baseDir2ProjectId[d.Dirname] = d.ProjId
	}

	dirSkel, err := manifest.DirSkeleton()
	if err != nil {
		log.Printf("PopulateRoot: Error creating a manifest skeleton")
		return err
	}
	if mdb.options.Verbose {
		log.Printf("dirSkeleton = %v", dirSkel)
	}

	txn, err := mdb.db.Begin()
	if err != nil {
		log.Printf("PopulateRoot: Error opening transaction")
		return err
	}

	// build the supporting directory structure.
	// We mark each directory as populated, so that the platform would not
	// be queries.
	nowSeconds := time.Now().Unix()
	for _, d := range dirSkel {
		_, err := mdb.createEmptyDir(
			txn,
			"", "",   // There is no backing project/folder
			nowSeconds, nowSeconds,
			dirReadOnlyMode, // skeleton directories are scaffolding, they cannot be modified.
			d, true)
		if err != nil {
			txn.Rollback()
			log.Printf("PopulateRoot: Error creating empty dir")
			return err
		}
	}

	// create individual files
	for _, fl := range manifest.Files {
		_, err := mdb.createDataObject(
			false,
			txn, FK_Regular, fl.ProjId, fl.FileId,
			fl.Size,
			fl.CtimeSeconds, fl.MtimeSeconds,
			fileReadOnlyMode,
			fl.Parent, fl.Fname, "")
		if err != nil {
			txn.Rollback()
			log.Printf("PopulateRoot: error creating singleton file")
			return err
		}
	}

	for _, d := range manifest.Directories {
		// Local directory [d.Dirname] represents
		// folder [d.Folder] on project [d.ProjId].
		_, err := mdb.createEmptyDir(
			txn,
			d.ProjId, d.Folder,
			d.CtimeSeconds, d.MtimeSeconds,
			dirReadWriteMode,
			d.Dirname, false)
		if err != nil {
			txn.Rollback()
			log.Printf("PopulateRoot: error creating empty manifest directory")
			return err
		}
	}

	// set the root to be populated
	if err := mdb.setDirectoryToPopulated(txn, InodeRoot); err != nil {
		txn.Rollback()
		log.Printf("PopulateRoot: error setting root directory to populated")
		return err
	}

	return txn.Commit()
}

// We know that the parent directory exists, is populated, and the file does not exist
func (mdb *MetadataDb) CreateFile(
	ctx context.Context,
	dir *Dir,
	fileId string,
	fname string,
	mode os.FileMode,
	localPath string) (File, error) {
	if mdb.options.Verbose {
		log.Printf("CreateFile %s/%s  localPath=%s proj=%s",
			dir.FullPath, fname, localPath, dir.ProjId)
	}

	// insert into the database
	txn, err := mdb.db.Begin()
	if err != nil {
		log.Printf(err.Error())
		return File{}, fmt.Errorf("CreateFile error opening transaction")
	}
	nowSeconds := time.Now().Unix()
	inode, err := mdb.createDataObject(
		true,
		txn,
		FK_Regular,
		dir.ProjId,
		fileId,
		0,    /* the file is empty */
		nowSeconds,
		nowSeconds,
		mode,
		dir.FullPath,
		fname,
		localPath)
	if err != nil {
		txn.Rollback()
		log.Printf(err.Error())
		return File{}, fmt.Errorf("CreateFile error creating data object")
	}
	if err := txn.Commit(); err != nil {
		log.Printf(err.Error())
		return File{}, fmt.Errorf("CreateFile commit failed")
	}

	// 3. return a File structure
	return File{
		Kind: FK_Regular,
		Id : fileId,
		ProjId : dir.ProjId,
		Name : fname,
		Size : 0,
		Inode : inode,
		Ctime : SecondsToTime(nowSeconds),
		Mtime : SecondsToTime(nowSeconds),
		Mode : mode,
		Nlink : 1,
		InlineData : localPath,
	}, nil
}

// reduce link count by one. If it reaches zero, delete the file.
//
// TODO: take into account the case of ForgetInode, and files that are open, but unlinked.
func (mdb *MetadataDb) Unlink(ctx context.Context, file File) error {
	txn, err := mdb.db.Begin()
	if err != nil {
		log.Printf(err.Error())
		return fmt.Errorf("Unlink error opening transaction")
	}

	nlink := file.Nlink - 1
	if nlink > 0 {
		// reduce one from the link count. It is still positive,
		// so there is nothing else to do
		sqlStmt := fmt.Sprintf(`
  		           UPDATE data_objects
                           SET nlink = '%d'
                           WHERE inode = '%d'`,
			nlink, file.Inode)
		if _, err := txn.Exec(sqlStmt); err != nil {
			log.Printf(err.Error())
			return fmt.Errorf("could not reduce the link count for inode=%d to %d",
				file.Inode, nlink)
		}
	} else {
		// the link hit zero, we can remove the file
		sqlStmt := fmt.Sprintf(`
                           DELETE FROM namespace
                           WHERE inode='%d';`,
			file.Inode)
		if _, err := txn.Exec(sqlStmt); err != nil {
			log.Printf(err.Error())
			return fmt.Errorf("could not delete row for inode=%d from the namespace table",
				file.Inode, nlink)
		}

		sqlStmt = fmt.Sprintf(`
                           DELETE FROM data_objects
                           WHERE inode='%d';`,
			file.Inode)
		if _, err := txn.Exec(sqlStmt); err != nil {
			log.Printf(err.Error())
			return fmt.Errorf("could not delete row for inode=%d from the data_objects table",
				file.Inode)
		}

	}

	if err := txn.Commit(); err != nil {
		log.Printf(err.Error())
		return fmt.Errorf("Unlink inode=%dcommit failed", file.Inode)
	}

	return nil
}

func (mdb *MetadataDb) UpdateFile(
	ctx context.Context,
	f File,
	fileSize int64,
	modTime time.Time,
	mode os.FileMode) error {
	if mdb.options.Verbose {
		log.Printf("Update file=%v size=%d mode=%d", f, fileSize, mode)
	}

	txn, err := mdb.db.Begin()
	if err != nil {
		log.Printf(err.Error())
		return fmt.Errorf("UpdateFile error opening transaction")
	}

	modTimeSec := modTime.Unix()
	sqlStmt := fmt.Sprintf(`
 		        UPDATE data_objects
                        SET size = '%d', mtime='%d', mode='%d'
			WHERE inode = '%d';`,
		fileSize, modTimeSec, int(mode), f.Inode)

	if _, err := txn.Exec(sqlStmt); err != nil {
		txn.Rollback()
		log.Printf(err.Error())
		return fmt.Errorf("UpdateFile error executing transaction")
	}
	return txn.Commit()
}

func (mdb *MetadataDb) Shutdown() {
	if err := mdb.db.Close(); err != nil {
		log.Printf(err.Error())
		log.Printf("Error closing the sqlite database %s", mdb.dbFullPath)
	}
}
