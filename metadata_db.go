package dxfuse

import (
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
	"os"
	"runtime/debug"
	"strings"
	"time"
)


const (
	nsDirType = 1
	nsDataObjType = 2

	// return code from a directoryExists call
	dirDoesNotExist = 1
	dirExistsButNotPopulated = 2
	dirExistsAndPopulated = 3
)

type DirInfo struct {
	inode int64
	projId string
	projFolder string
	ctime int64
	mtime int64
}

type MetadataDb struct {
	// an open handle to the database
	db  *sql.DB

	// a pool of http clients, for short requests, such as file creation,
	// or file describe.
	httpClientPool chan(*retryablehttp.Client)

	// configuration information for accessing dnanexus servers
	dxEnv dxda.DXEnvironment

	// mapping from mounted directory to project ID
	baseDir2ProjectId map[string]string

	inodeCnt int64
	options Options
}

func NewMetadataDb(dbFullPath string, dxEnv dxda.DXEnvironment, options Options) (*MetadataDb, error) {
	// initialize a pool of http-clients.
	httpClientPool := make(chan *retryablehttp.Client, HttpClientPoolSize)
	for i:=0; i < HttpClientPoolSize; i++ {
		httpClientPool <- dxda.NewHttpClient(true)
	}

	// create a connection to the database, that will be kept open
	db, err := sql.Open("sqlite3", dbFullPath + "?mode=rwc")
	if err != nil {
		return nil, errors.Newf("Could not open the database %s", dbFullPath)
	}

	return &MetadataDb{
		db : db,
		httpClientPool : httpClientPool,
		dxEnv : dxEnv,
		baseDir2ProjectId: make(map[string]string)
		inodeCnt : InodeRoot + 1,
		options : Options
	}
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
                nlink int,
                inline_data  string,
                PRIMARY KEY (inode)
	);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		log.Printf(err.Error())
		return errors.Newf("Could not create table data_objects", dbFullPath)
	}

	sqlStmt = `
	CREATE INDEX id_index
	ON data_objects (id);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		log.Printf(err.Error())
		return errors.Newf("Could not create index id_index on table data_objects")
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
		return errors.Newf("Could not create table namespace")
	}

	sqlStmt = `
	CREATE INDEX parent_index
	ON namespace (parent);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		log.Printf(err.Error())
		return errors.Newf("Could not create index parent_index on table namespace")
	}

	// we need to be able to get from the files/tables, back to the namespace
	// with an inode ID.
	sqlStmt = `
	CREATE INDEX inode_rev_index
	ON namespace (inode);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		log.Printf(err.Error())
		return errors.Newf("Could not create index inode_rev_index on table namespace")
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
		log.Printf(err.Error())
		return errors.Newf("Could not create table directories")
	}

	// Adding a root directory. The root directory does
	// not belong to any one project. This allows mounting
	// several projects from the same root. This is denoted
	// by marking the project as the empty string.
	nowSeconds := time.Now().Unix()
	sqlStmt = fmt.Sprintf(`
 		        INSERT INTO directories
			VALUES ('%d', '%s', '%s', '%d', '%d', '%d');`,
		InodeRoot, "", "", boolToInt(false),
		nowSeconds, nowSeconds)
	if _, err := txn.Exec(sqlStmt); err != nil {
		log.Printf(err.Error())
		return errors.Newf("Could not create root directory")
	}

	// We want the root path to match the results of splitPath("/")
	rParent, rBase := splitPath("/")
	sqlStmt = fmt.Sprintf(`
 		        INSERT INTO namespace
			VALUES ('%s', '%s', '%d', '%d');`,
		rParent, rBase, nsDirType, InodeRoot)
	if _, err := txn.Exec(sqlStmt); err != nil {
		log.Printf(err.Error())
		return errors.Newf("Could not create root inode (%d)", InodeRoot)
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
		return errors.Newf("Could not open transaction")
	}

	if err := mdb.init2(txn); err != nil {
		txn.Rollback()
		log.Printf(err.Error())
		return errors.Newf("Could not initialize database")
	}

	if err := txn.Commit(); err != nil {
		txn.Rollback()
		log.Printf(err.Error())
		return errors.Newf("Error during commit")
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
	return mdb.InodeCnt
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
		return InodeInvalid, 0, printErrorStack(err)
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
 		        SELECT kind,id,proj_id,size,ctime,mtime,nlink,inline_data
                        FROM data_objects
			WHERE inode = '%d';`,
		inode)
	rows, err := mdb.db.Query(sqlStmt)
	if err != nil {
		log.Printf(err.Error())
		return errors.Newf("lookupDataObjectByInode %s inode=%d", oname, inode)
	}

	var f File
	f.Name = oname
	f.Inode = inode
	numRows := 0
	for rows.Next() {
		var ctime int64
		var mtime int64
		rows.Scan(&f.Kind,&f.Id, &f.ProjId, &f.Size, &ctime, &mtime, &f.Nlink, &f.InlineData)
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
		panic(fmt.Sprintf("Found %d data-objects of the form %s/%s",
			numRows, dirFullName, oname))
	}
}

func (mdb *MetadataDb) LookupDirByInode(inode int64) (Dir, bool, error) {
	// Extract information for a particular sub directory
	sqlStmt := fmt.Sprintf(`
 		        SELECT parent,name,ctime,mtime
                        FROM directories
			WHERE inode = '%d';`,
		inode)
	rows, err := mdb.db.Query(sqlStmt)
	if err != nil {
		log.Print(err.Error())
		return Dir{}, false, errors.Newf("lookupDirByInode inode=%d", inode)
	}
	var d Dir
	d.Inode = inode
	d.uid = mdb.options.uid
	d.gid = mdb.options.gid

	numRows := 0
	var ctime int64
	var mtime int64
	for rows.Next() {
		rows.Scan(&d.parent, &d.name, &ctime, &mtime)
		numRows++
	}
	rows.Close()

	switch numRows {
	case 0:
		return Dir{}, false, nil
	case 1:
		d.Ctime = SecondsToTime(ctime)
		d.Mtime = SecondsToTime(mtime)
		d.FullPath = filepath.Clean(filepath.Join(d.parent, d.name))
		return d, true, nil
	default:
		panic(fmt.Sprintf("found %d directory with inode=%d in the directories table",
			numRows, inode))
	}
}

// search for a file with a particular inode
func (mdb *MetadataDb) LookupByInode(inode int64) (Node, bool, error) {
	// point lookup in the files table
	sqlStmt := fmt.Sprintf(`
 		        SELECT parent,name,obj\_type
                        FROM namespace
			WHERE inode = '%d';`,
		inode)
	rows, err := mdb.db.Query(sqlStmt)
	if err != nil {
		log.Printf(err.Error())
		log.Printf("Error in query")
		return nil, err
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
		return mdb.LookupDirByInode(inode)
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
 		        SELECT directories.inode, directories.proj_id, namespace.name, directories.ctime, directories.mtime
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
		rows.Scan(&inode, &projId, &dname, &ctime, &mtime)

		subdirs[dname] = Dir{
			Parent : dirFullName,
			Dname : dname,
			FullPath : filepath.Clean(filepath.Join(dirFullName, dname)),
			Inode : inode,
			Ctime : SecondsToTime(ctime),
			Mtime : SecondsToTime(mtime),
		}
	}
	rows.Close()

	// Extract information for all the files
	sqlStmt = fmt.Sprintf(`
 		        SELECT data_objects.kind,data_objects.id,data_objects.proj_id,data_objects.inode,data_objects.size,data_objects.ctime,data_objects.mtime,data_objects.nlink,data_objects.inline_data,namespace.name
                        FROM data_objects
                        JOIN namespace
                        ON data_objects.inode = namespace.inode
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
		rows.Scan(&f.Kind,&f.Id, &f.ProjId, &f.Inode, &f.Size, &ctime, &mtime, &f.Nlink, &f.InlineData,&f.Name)
		f.Ctime = SecondsToTime(ctime)
		f.Mtime = SecondsToTime(mtime)

		files[f.Name] = f
	}

	//log.Printf("  #files=%d", len(files))
	//log.Printf("]")
	return files, subdirs, nil
}

// Create an entry representing one remote file. This is used by
// dxWDL as a way to stream individual files.
func (mdb *MetadataDb) createDataObject(
	txn *sql.Tx,
	kind int,
	projId string,
	objId string,
	size int64,
	ctime int64,
	mtime int64,
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
			VALUES ('%d', '%s', '%s', '%d', '%d', '%d', '%d', '%d', '%s');`,
			kind, objId, projId, inode, size, ctime, mtime, 1, inlineData)
		if _, err := txn.Exec(sqlStmt); err != nil {
			log.Printf(err.Error())
			log.Printf("Error inserting into data objects table")
			return 0, err
		}
	} else {
		// File already exists, we need to increase the link count
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
	sqlStmt = fmt.Sprintf(`
                       INSERT INTO directories
                       VALUES ('%d', '%s', '%s', '%d', '%d', '%d');`,
		inode, projId, projFolder, boolToInt(populated), ctime, mtime)
	if _, err := txn.Exec(sqlStmt); err != nil {
		log.Printf("createEmptyDir: error inserting into directories table %d",
			inode)
		return 0, err
	}
	return inode, nil
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

		_, err := mdb.createDataObject(txn,
			kind,
			o.ProjId,
			o.Id,
			o.Size,
			o.CtimeSeconds,
			o.MtimeSeconds,
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
			ctime, mtime, filepath.Clean(dirPath + "/" + subDirName),
			false)
		if err != nil {
			log.Printf("Error creating empty directory %s while populating directory %s",
				filepath.Clean(projFolder + "/" + subDirName), dirPath)
			return err
		}
	}

	if mdb.VerboseLevel > 1 {
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
	dinode int64,
	projId string,
	projFolder string,
	ctime int64,
	mtime int64,
	dirFullName string) error {

	if mdb.options.Verbose {
		log.Printf("directoryReadFromDNAx: describe folder %s:%s", projId, projFolder)
	}

	// describe all the files
	httpClient := <- mdb.httpClientPool
	dxDir, err := DxDescribeFolder(httpClient, &mdb.dxEnv, projId, projFolder)
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
		return errors.Newf("directoryReadFromDNAx: error opening transaction")
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
		return errors.Newf("directoryReadFromDNAx: Error populating directory")
	}

	// create the faux sub directories. These have no additional depth, and are fully
	// populated. They contains all the files with multiple versions.
	//
	// Note: these directories DO NOT have a matching project folder.
	for dName, fauxFiles := range posixDir.fauxSubdirs {
		fauxDirPath := filepath.Clean(dirFullName + "/" + dName)

		// create the directory in the namespace, as if it is unpopulated.
		fauxDirInode, err := mdb.createEmptyDir(
			txn, projId, "", ctimeApprox, mtimeApprox, fauxDirPath, true)
		if err != nil {
			txn.Rollback()
			log.Printf(err.Error())
			return log.Newf("directoryReadFromDNAx: creating faux directory %s", fauxDirPath)
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
			return errors.Newf("directoryReadFromDNAx: populating faux directory %s", fauxDirPath)
		}
	}

	return txn.Commit()
}

// check if a directory is fully populated.
func (mdb *MetadataDb) dirIsPopulated(inode int64) (bool, error) {
	sqlStmt = fmt.Sprintf(`
 		        SELECT populated
                        FROM directories
			WHERE inode = '%d';`, inode)
	rows, err = mdb.db.Query(sqlStmt)
	if err != nil {
		log.Printf(err.Error())
		return false, errors.Newf("lookupDirByName: query directories, %s", dirPath)
	}

	var populated int
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

	switch populated {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		panic(fmt.Sprintf("illegal value for populated field (%d)", populated))
	}
}

// Add a directory with its contents to an exisiting database
func (mdb *MetadataDb) ReadDirAll(dir Dir) (map[string]File, map[string]Dir, error) {
	if mdb.options.Verbose {
		log.Printf("ReadDirAll %s", dir.FullPath)
	}

	populated, err := mdb.dirIsPopulated(dir.inode)
	if err != nil {
		return err
	}
	if !populated {
		err := mdb.directoryReadFromDNAx(
			parentDirInfo.inode,
			parentDirInfo.projId, parentDirInfo.projFolder,
			parentDirInfo.ctime, parentDirInfo.mtime,
			parentDir)
		if err != nil {
			return nil, nil, err
		}
	}

	// Now that the directory is in the database, we can read it with a local query.
	return mdb.directoryReadAllEntries(dirFullName)
}


// Search for a file/subdir in a directory
func (mdb *MetadataDb) fastLookup(
	dirFullName string,
	dirOrFileName string) (Node, error) {
	// point lookup in the namespace
	sqlStmt := fmt.Sprintf(`
 		        SELECT obj_type,inode
                        FROM namespace
			WHERE parent = '%s' AND name = '%s';`,
		dirFullName, dirOrFileName)
	rows, err := mdb.db.Query(sqlStmt)
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
		return mdb.lookupDirByInode(inode)
	case nsDataObjType:
		return mdb.lookupDataObjectByInode(dirOrFileName, inode)
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
func (mdb *MetadataDb) LookupInDir(
	parentInode int64,
	dirOrFileName string) (Node, bool, error) {

	retCode, _, err := mdb.directoryExists(parentDir)
	if err != nil {
		log.Printf("LookupInDir errow while figuring out if %s exists", parentDir)
		return nil, err
	}
	switch retCode {
	case dirDoesNotExist:
		return nil, fuse.ENOENT
	case dirExistsButNotPopulated:
		// The directory exists, but has not been populated yet.
		_, _, err := mdb.MetadataDbReadDirAll(parentDir)
		if err != nil {
			return nil, err
		}
	case dirExistsAndPopulated:
		// The directory exists, and has already been populated.
		// I think this is the normal path. There is nothing to do here.
	default:
		panic(fmt.Sprintf("Bad return code %d",retCode))
	}

	return mdb.fastLookup(parentDir, dirOrFileName)
}

// Build a toplevel directory for each project.
func (mdb *MetadataDb) PopulateRoot(manifest Manifest) error {
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
			txn, FK_Regular, fl.ProjId, fl.FileId,
			fl.Size, fl.CtimeSeconds, fl.MtimeSeconds,
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

// Figure out which project this folder belongs to.
// For example,
//  "/dxWDL_playground/A/B" -> "project-xxxx", "/A/B"
func (mdb *MetadataDb) projectIdAndFolder(dirname string) (string, string) {
	for baseDir, projId := range mdb.baseDir2ProjectId {
		if strings.HasPrefix(dirname, baseDir) {
			folderInProject := dirname[len(baseDir) : ]
			if !strings.HasPrefix(folderInProject, "/") {
				// folders in DNAx have to start with a slash
				folderInProject = "/" + folderInProject
			}
			return projId, folderInProject
		}
	}
	panic(fmt.Sprintf("directory %s does not belong to any project", dirname))
}

func (mdb *MetadataDb) CreateFile(dir *Dir, fname string, localPath string) (*File, error) {
	if mdb.options.Verbose {
		log.Printf("CreateFile %s/%s  localPath=%s", dir.FullPath, fname, localPath)
	}

	// Check if the directory already contains [name].
	_, err := mdb.LookupInDir(dir.FullPath, fname)
	if err == nil {
		// file already exists
		return nil, fuse.EEXIST
	}
	if err != fuse.ENOENT {
		// An error occured. We are expecting the file to -not- exist.
		return nil, err
	}

	projId,folder := fsys.projectIdAndFolder(dir.FullPath)
	if fsys.options.Verbose {
		log.Printf("projId = %s", projId)
	}

	// now we know this is a new file
	// 1. create it on the platform
	httpClient := <- fsys.httpClientPool
	fileId, err := DxFileNew(
		httpClient, &fsys.dxEnv,
		fsys.nonce.String(),
		projId, fname, folder)
	fsys.httpClientPool <- httpClient
	if err != nil {
		return nil, err
	}

	// 2. insert into the database
	txn, err := fsys.db.Begin()
	if err != nil {
		return nil, printErrorStack(err)
	}
	nowSeconds := time.Now().Unix()
	inode, err := fsys.createDataObject(
		txn,
		FK_Regular,
		projId,
		fileId,
		0,    /* the file is empty */
		nowSeconds,
		nowSeconds,
		dir.FullPath,
		fname,
		localPath)
	if err != nil {
		txn.Rollback()
		return nil, printErrorStack(err)
	}
	if err := txn.Commit(); err != nil {
		return nil, err
	}

	// 3. return a File structure
	file := &File{
		Kind: FK_Regular,
		Id : fileId,
		ProjId : projId,
		Name : fname,
		Size : 0,
		Inode : inode,
		Ctime : SecondsToTime(nowSeconds),
		Mtime : SecondsToTime(nowSeconds),
		Nlink : 1,
		InlineData : localPath,
	}
	return file, nil
}

func (fsys *Filesys) MetadataDbUpdateFile(f File, fInfo os.FileInfo) error {
	txn, err := fsys.db.Begin()
	if err != nil {
		return printErrorStack(err)
	}

	modTimeSec := fInfo.ModTime().Unix()
	sqlStmt := fmt.Sprintf(`
 		        UPDATE data_objects
                        SET size = '%d', mtime='%d'
			WHERE inode = '%d';`,
		fInfo.Size(), modTimeSec, f.Inode)

	if _, err := txn.Exec(sqlStmt); err != nil {
		txn.Rollback()
		return printErrorStack(err)
	}
	return txn.Commit()
}
