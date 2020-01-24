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
)


const (
	nsDirType = 1
	nsDataObjType = 2
)

type MetadataDb struct {
	// an open handle to the database
	db               *sql.DB
	dbFullPath        string

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
	options Options) (*MetadataDb, error) {
	// create a connection to the database, that will be kept open
	db, err := sql.Open("sqlite3", dbFullPath + "?mode=rwc")
	if err != nil {
		return nil, fmt.Errorf("Could not open the database %s", dbFullPath)
	}

	return &MetadataDb{
		db : db,
		dbFullPath : dbFullPath,
		dxEnv : dxEnv,
		baseDir2ProjectId: make(map[string]string),
		inodeCnt : InodeRoot + 1,
		options : options,
	}, nil
}

// write a log message, and add a header
func (mdb *MetadataDb) log(a string, args ...interface{}) {
	LogMsg("metadata_db", a, args...)
}

// open a transaction
func (mdb *MetadataDb) BeginTxn() (*sql.Tx, error) {
	return mdb.db.Begin()
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
                archival_state text,
                inode bigint,
		size bigint,
                ctime bigint,
                mtime bigint,
                mode int,
                nlink int,
                inline_data text,
                PRIMARY KEY (inode)
	);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		mdb.log(err.Error())
		return fmt.Errorf("Could not create table data_objects")
	}

	sqlStmt = `
	CREATE INDEX id_index
	ON data_objects (id);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		mdb.log(err.Error())
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
		mdb.log(err.Error())
		return fmt.Errorf("Could not create table namespace")
	}

	sqlStmt = `
	CREATE INDEX parent_index
	ON namespace (parent);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		mdb.log(err.Error())
		return fmt.Errorf("Could not create index parent_index on table namespace")
	}

	// we need to be able to get from the files/tables, back to the namespace
	// with an inode ID. Due to hardlinks, a single inode can have multiple namespace entries.
	sqlStmt = `
	CREATE INDEX inode_rev_index
	ON namespace (inode);
	`
	if _, err := txn.Exec(sqlStmt); err != nil {
		mdb.log(err.Error())
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
		mdb.log(err.Error())
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
		mdb.log(err.Error())
		return fmt.Errorf("Could not create root directory")
	}

	// We want the root path to match the results of splitPath("/")
	rParent, rBase := splitPath("/")
	sqlStmt = fmt.Sprintf(`
 		        INSERT INTO namespace
			VALUES ('%s', '%s', '%d', '%d');`,
		rParent, rBase, nsDirType, InodeRoot)
	if _, err := txn.Exec(sqlStmt); err != nil {
		mdb.log(err.Error())
		return fmt.Errorf("Could not create root inode (%d)", InodeRoot)
	}

	return nil
}

// construct an initial empty database, representing an entire project.
func (mdb *MetadataDb) Init() error {
	if mdb.options.Verbose {
		mdb.log("Initializing metadata database\n")
	}

	txn, err := mdb.db.Begin()
	if err != nil {
		mdb.log(err.Error())
		return fmt.Errorf("Could not open transaction")
	}

	if err := mdb.init2(txn); err != nil {
		txn.Rollback()
		mdb.log(err.Error())
		return fmt.Errorf("Could not initialize database")
	}

	if err := txn.Commit(); err != nil {
		txn.Rollback()
		mdb.log(err.Error())
		return fmt.Errorf("Error during commit")
	}

	if mdb.options.Verbose {
		mdb.log("Completed creating files and directories tables\n")
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
func (mdb *MetadataDb) lookupDataObjectById(oph *OpHandle, fId string) (int64, int, bool, error) {
	// point lookup in the files table
	sqlStmt := fmt.Sprintf(`
 		        SELECT inode,nlink
                        FROM data_objects
			WHERE id = '%s';`,
		fId)
	rows, err := oph.txn.Query(sqlStmt)
	if err != nil {
		mdb.log("lookupDataObjectById fId=%s err=%s", fId, err.Error())
		return InodeInvalid, 0, false, oph.RecordError(err)
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
		log.Panicf("Found %d data-objects with Id %s", numRows, fId)
		return 0, 0, false, nil
	}
}

// search for a file with a particular inode
//
// This is important for a file with multiple hard links. The
// parent directory determines which project the file belongs to.
// This is why we set the project-id instead of reading it from the file
func (mdb *MetadataDb) lookupDataObjectByInode(oph *OpHandle, oname string, inode int64) (File, bool, error) {
	// point lookup in the files table
	sqlStmt := fmt.Sprintf(`
 		        SELECT kind,id,proj_id,archival_state,size,ctime,mtime,mode,nlink,inline_data
                        FROM data_objects
			WHERE inode = '%d';`,
		inode)
	rows, err := oph.txn.Query(sqlStmt)
	if err != nil {
		mdb.log("lookupDataObjectByInode %s inode=%d err=%s", oname, inode, err.Error())
		return File{}, false, oph.RecordError(err)
	}

	var f File
	f.Name = oname
	f.Inode = inode
	f.Uid = mdb.options.Uid
	f.Gid = mdb.options.Gid

	numRows := 0
	for rows.Next() {
		var ctime int64
		var mtime int64
		rows.Scan(&f.Kind, &f.Id, &f.ProjId, &f.ArchivalState, &f.Size, &ctime, &mtime,	&f.Mode, &f.Nlink, &f.InlineData)
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
		log.Panicf("Found %d data-objects with name %s", numRows, oname)
		return File{}, false, nil
	}
}

func (mdb *MetadataDb) lookupDirByInode(oph *OpHandle, parent string, dname string, inode int64) (Dir, bool, error) {
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
	rows, err := oph.txn.Query(sqlStmt)
	if err != nil {
		mdb.log("lookupDirByInode inode=%d err=%s", inode, err.Error())
		return Dir{}, false, oph.RecordError(err)
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
		log.Panicf("found %d directory with inode=%d in the directories table",
			numRows, inode)
	}

	// is this a faux directory? These don't exist on the platform,
	// but are used for files with multiple versions.
	if d.ProjFolder == "" {
		d.faux = true
	} else {
		d.faux = false
	}

	return d, true, nil
}


// search for a file with a particular inode.
//
// Note: a file with multiple hard links will appear several times.
func (mdb *MetadataDb) LookupByInodeAll(ctx context.Context, oph *OpHandle, inode int64) ([]Node, error) {
	// point lookup in the namespace table
	sqlStmt := fmt.Sprintf(`
 		        SELECT parent,name,obj_type
                        FROM namespace
			WHERE inode = '%d';`,
		inode)
	rows, err := oph.txn.Query(sqlStmt)
	if err != nil {
		mdb.log("LookupByInodeAll: error in query  err=%s", err.Error())
		return nil, oph.RecordError(err)
	}
	var parents []string
	var names []string
	var obj_types []int
	for rows.Next() {
		var p string
		var n string
		var o int
		rows.Scan(&p, &n, &o)

		parents = append(parents, p)
		names = append(names, n)
		obj_types = append(obj_types, o)
	}
	rows.Close()

	if len(parents) == 0 {
		return nil, nil
	}

	var nodes []Node
	for i, obj_type := range(obj_types) {
		var node Node
		var ok bool
		var err error

		switch obj_type {
		case nsDirType:
			node, ok, err = mdb.lookupDirByInode(oph, parents[i], names[i], inode)
		case nsDataObjType:
			// This is important for a file with multiple hard links. The
			// parent directory determines which project the file belongs to.
			node, ok, err = mdb.lookupDataObjectByInode(oph, names[i], inode)
		default:
			log.Panicf("Invalid type %d in namespace table", obj_type)
		}

		if err != nil {
			return nil, err
		}
		if ok {
			nodes = append(nodes, node)
		}
	}
	return nodes, nil
}

func (mdb *MetadataDb) LookupDirByInode(ctx context.Context, oph *OpHandle, inode int64) (Dir, bool, error) {
	nodes,err := mdb.LookupByInodeAll(ctx, oph, inode)
	if err != nil {
		return Dir{}, false, err
	}

	switch len(nodes) {
	case 0:
		return Dir{}, false, nil
	case 1:
		dir := nodes[0].(Dir)
		return dir, true, nil
	default:
		log.Panicf("found multiple directories for inode=%d", inode)
		return Dir{}, false, nil
	}
}

// Return one of the hits for an inode. A file that has multiple hard links will appear
// multiple times, and we return only the first hit.
func (mdb *MetadataDb) LookupByInode(ctx context.Context, oph *OpHandle, inode int64) (Node, bool, error) {
	nodes, err := mdb.LookupByInodeAll(ctx, oph, inode)
	if err != nil {
		return File{}, false, oph.RecordError(err)
	}
	if len(nodes) == 0 {
		return File{}, false, nil
	}
	return nodes[0], true, nil
}

// The directory is in the database, read it in its entirety.
func (mdb *MetadataDb) directoryReadAllEntries(
	oph *OpHandle,
	dirFullName string) (map[string]File, map[string]Dir, error) {
	if mdb.options.Verbose {
		mdb.log("directoryReadAllEntries %s", dirFullName)
	}

	// Extract information for all the subdirectories
	sqlStmt := fmt.Sprintf(`
 		        SELECT directories.inode, directories.proj_id, namespace.name, directories.ctime, directories.mtime, directories.mode
                        FROM directories
                        JOIN namespace
                        ON directories.inode = namespace.inode
			WHERE namespace.parent = '%s' AND namespace.obj_type = '%d';
			`, dirFullName, nsDirType)
	rows, err := oph.txn.Query(sqlStmt)
	if err != nil {
		mdb.log("Error in directories query, err=%s", err.Error())
		return nil, nil, oph.RecordError(err)
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
 		        SELECT dos.kind, dos.id, dos.proj_id, dos.archival_state, dos.inode, dos.size, dos.ctime, dos.mtime, dos.mode, dos.nlink, dos.inline_data, namespace.name
                        FROM data_objects as dos
                        JOIN namespace
                        ON dos.inode = namespace.inode
			WHERE namespace.parent = '%s' AND namespace.obj_type = '%d';
			`, dirFullName, nsDataObjType)
	rows, err = oph.txn.Query(sqlStmt)
	if err != nil {
		mdb.log("Error in data object query, err=%s", err.Error())
		return nil, nil, oph.RecordError(err)
	}

	// Find the files in the directory
	files := make(map[string]File)
	for rows.Next() {
		var f File

		var ctime int64
		var mtime int64
		var mode int
		rows.Scan(&f.Kind,&f.Id, &f.ProjId, &f.ArchivalState, &f.Inode, &f.Size, &ctime, &mtime, &mode, &f.Nlink, &f.InlineData,&f.Name)
		f.Ctime = SecondsToTime(ctime)
		f.Mtime = SecondsToTime(mtime)
		f.Mode = os.FileMode(mode)

		files[f.Name] = f
	}

	//mdb.log("  #files=%d", len(files))
	//mdb.log("]")
	return files, subdirs, nil
}

// Create an entry representing one remote file. This has
// several use cases:
//  1) Create a singleton file from the manifest
//  2) Create a new file, and upload it later to the platform
//  3) Discover a file in a directory, which may actually be a link to another file.
const (
	CDO_MUST_BE_NEW = 1
	CDO_ALREADY_EXISTS = 2
	CDO_NEUTRAL = 3
)
func (mdb *MetadataDb) createDataObject(
	oph *OpHandle,
	flag int,
	kind int,
	projId string,
	archivalState string,
	objId string,
	size int64,
	ctime int64,
	mtime int64,
	mode os.FileMode,
	parentDir string,
	fname string,
	inlineData string) (int64, error) {
	if mdb.options.VerboseLevel > 1 {
		mdb.log("createDataObject %s:%s %s", projId, objId,
			filepath.Clean(parentDir + "/" + fname))
	}

	inode, nlink, ok, err := mdb.lookupDataObjectById(oph, objId)
	if err != nil {
		return 0, err
	}

	if !ok {
		if flag == CDO_ALREADY_EXISTS {
			log.Panicf("Object %s:%s should already exists, but does not",
				projId, objId)
		}

		// File doesn't exist, we need to choose a new inode number.
		// NOte: it is on stable storage, and will not change.
		inode = mdb.allocInodeNum()

		// Create an entry for the file
		sqlStmt := fmt.Sprintf(`
 		        INSERT INTO data_objects
			VALUES ('%d', '%s', '%s', '%s', '%d', '%d', '%d', '%d', '%d', '%d', '%s');`,
			kind, objId, projId, archivalState, inode, size, ctime, mtime, int(mode), 1, inlineData)
		if _, err := oph.txn.Exec(sqlStmt); err != nil {
			mdb.log(err.Error())
			mdb.log("Error inserting into data objects table")
			return 0, oph.RecordError(err)
		}
	} else {
		if flag == CDO_MUST_BE_NEW {
			log.Panicf("Object %s:%s must not be already in the database",
				projId, objId)
		}

		// File already exists, we need to increase the link count
		sqlStmt := fmt.Sprintf(`
 		        UPDATE data_objects
                        SET nlink = '%d'
			WHERE id = '%s';`,
			nlink + 1, objId)
		if _, err := oph.txn.Exec(sqlStmt); err != nil {
			mdb.log("Error updating data_object table, incrementing the link number err=%s",
				err.Error())
			return 0, oph.RecordError(err)
		}
	}

	sqlStmt := fmt.Sprintf(`
 		        INSERT INTO namespace
			VALUES ('%s', '%s', '%d', '%d');`,
		parentDir, fname, nsDataObjType, inode)
	if _, err := oph.txn.Exec(sqlStmt); err != nil {
		mdb.log("Error inserting %s/%s into the namespace table  err=%s", parentDir, fname, err.Error())
		return 0, oph.RecordError(err)
	}

	return inode, nil
}

// Create an empty directory, and return the inode
//
// Assumption: the directory does not already exist in the database.
func (mdb *MetadataDb) createEmptyDir(
	oph *OpHandle,
	projId string,
	projFolder string,
	ctime int64,
	mtime int64,
	mode os.FileMode,
	dirPath string,
	populated bool) (int64, error) {
	if dirPath[0] != '/' {
		log.Panicf("directory must start with a slash")
	}

	// choose unused inode number. It is on stable stoage, and will not change.
	inode := mdb.allocInodeNum()
	parentDir, basename := splitPath(dirPath)
	if mdb.options.VerboseLevel > 1 {
		mdb.log("createEmptyDir %s:%s %s populated=%t",
			projId, projFolder, dirPath, populated)
	}

	sqlStmt := fmt.Sprintf(`
 		        INSERT INTO namespace
			VALUES ('%s', '%s', '%d', '%d');`,
		parentDir, basename, nsDirType,	inode)
	if _, err := oph.txn.Exec(sqlStmt); err != nil {
		mdb.log("createEmptyDir: error inserting into namespace table %s/%s, err=%s",
			parentDir, basename, err.Error())
		return 0, oph.RecordError(err)
	}

	// Create an entry for the subdirectory
	mode = mode | os.ModeDir
	sqlStmt = fmt.Sprintf(`
                       INSERT INTO directories
                       VALUES ('%d', '%s', '%s', '%d', '%d', '%d', '%d');`,
		inode, projId, projFolder, boolToInt(populated), ctime, mtime, int(mode))
	if _, err := oph.txn.Exec(sqlStmt); err != nil {
		mdb.log(err.Error())
		mdb.log("createEmptyDir: error inserting into directories table %d",
			inode)
		return 0, oph.RecordError(err)
	}
	return inode, nil
}

// Assumption: the directory does not already exist in the database.
func (mdb *MetadataDb) CreateDir(
	oph *OpHandle,
	projId string,
	projFolder string,
	ctime int64,
	mtime int64,
	mode os.FileMode,
	dirPath string) (int64, error) {
	dnode, err := mdb.createEmptyDir(oph, projId, projFolder, ctime, mtime, mode, dirPath, true)
	if err != nil {
		mdb.log("error in create dir")
		return 0, oph.RecordError(err)
	}
	return dnode, nil
}

// Remove a directory from the database
func (mdb *MetadataDb) RemoveEmptyDir(oph *OpHandle, inode int64) error {
	sqlStmt := fmt.Sprintf(`
                DELETE FROM directories
                WHERE inode='%d';`,
		inode)
	if _, err := oph.txn.Exec(sqlStmt); err != nil {
		mdb.log("RemoveEmptyDir(%d): error in directories table removal", inode)
		return oph.RecordError(err)
	}

	sqlStmt = fmt.Sprintf(`
                DELETE FROM namespace
                WHERE inode='%d';`,
		inode)
	if _, err := oph.txn.Exec(sqlStmt); err != nil {
		mdb.log("RemoveEmptyDir(%d): error in namespace table removal", inode)
		return oph.RecordError(err)
	}

	return nil
}

// Update the directory populated flag to TRUE
func (mdb *MetadataDb) setDirectoryToPopulated(oph *OpHandle, dinode int64) error {
	sqlStmt := fmt.Sprintf(`
		UPDATE directories
                SET populated = '1'
                WHERE inode = '%d'`,
		dinode)
	if _, err := oph.txn.Exec(sqlStmt); err != nil {
		mdb.log("Error set directory %d to populated", dinode)
		return oph.RecordError(err)
	}
	return nil
}

func (mdb *MetadataDb) kindOfFile(o DxDescribeDataObject) int {
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
		mdb.log("A data object has an unknown prefix (%s)", o.Id)
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
	oph *OpHandle,
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
		mdb.log("populateDir(%s)  data-objects=%v  subdirs=%v", dirPath, objNames, subdirs)
	}

	// Create a database entry for each file
	if mdb.options.VerboseLevel > 1 {
		mdb.log("inserting files")
	}

	for _, o := range dxObjs {
		kind := mdb.kindOfFile(o)
		inlineData := inlineDataOfFile(kind, o)

		_, err := mdb.createDataObject(
			oph,
			CDO_NEUTRAL,
			kind,
			o.ProjId,
			o.ArchivalState,
			o.Id,
			o.Size,
			o.CtimeSeconds,
			o.MtimeSeconds,
			fileReadOnlyMode,
			dirPath,
			o.Name,
			inlineData)
		if err != nil {
			return oph.RecordError(err)
		}
	}

	// Create a database entry for each sub-directory
	if mdb.options.VerboseLevel > 1 {
		mdb.log("inserting subdirs")
	}
	for _, subDirName := range subdirs {
		// Create an entry for the subdirectory.
		// We haven't described it yet from DNAx, so the populate flag
		// is false.
		_, err := mdb.createEmptyDir(
			oph,
			projId, filepath.Clean(projFolder + "/" + subDirName),
			ctime, mtime,
			dirReadWriteMode,
			filepath.Clean(dirPath + "/" + subDirName),
			false)
		if err != nil {
			mdb.log("Error creating empty directory %s while populating directory %s",
				filepath.Clean(projFolder + "/" + subDirName), dirPath)
			return err
		}
	}

	if mdb.options.VerboseLevel > 1 {
		mdb.log("setting populated for directory %s", dirPath)
	}

	// Update the directory populated flag to TRUE
	mdb.setDirectoryToPopulated(oph, dinode)
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
	oph *OpHandle,
	dinode int64,
	projId string,
	projFolder string,
	ctime int64,
	mtime int64,
	dirFullName string) error {

	if mdb.options.Verbose {
		mdb.log("directoryReadFromDNAx: describe folder %s:%s", projId, projFolder)
	}

	// describe all (closed) files
	dxDir, err := DxDescribeFolder(ctx, oph.httpClient, &mdb.dxEnv, projId, projFolder, true)
	if err != nil {
		fmt.Printf(err.Error())
		fmt.Printf("reading directory frmo DNAx error")
		return err
	}

	if mdb.options.Verbose {
		mdb.log("read dir from DNAx #data_objects=%d #subdirs=%d",
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
	px := NewPosix(mdb.options)
	posixDir, err := px.FixDir(dxDir)
	if err != nil {
		return err
	}

	// build the top level directory
	err = mdb.populateDir(
		oph, dinode,
		projId, projFolder,
		ctimeApprox, mtimeApprox,
		dirFullName, posixDir.dataObjects, posixDir.subdirs)
	if err != nil {
		mdb.log("directoryReadFromDNAx: Error populating directory, err=%s", err.Error())
		return oph.RecordError(err)
	}

	// create the faux sub directories. These have no additional depth, and are fully
	// populated. They contains all the files with multiple versions.
	//
	// Note: these directories DO NOT have a matching project folder.
	for dName, fauxFiles := range posixDir.fauxSubdirs {
		fauxDirPath := filepath.Clean(dirFullName + "/" + dName)

		// create the directory in the namespace, as if it is unpopulated.
		fauxDirInode, err := mdb.createEmptyDir(
			oph, projId, "",
			ctimeApprox, mtimeApprox,
			dirReadWriteMode,
			fauxDirPath, true)
		if err != nil {
			mdb.log("directoryReadFromDNAx: creating faux directory %s, err=%s", fauxDirPath, err.Error())
			return oph.RecordError(err)
		}

		var no_subdirs []string
		err = mdb.populateDir(
			oph, fauxDirInode,
			projId, "",
			ctimeApprox, mtimeApprox,
			fauxDirPath, fauxFiles, no_subdirs)
		if err != nil {
			mdb.log("directoryReadFromDNAx: populating faux directory %s, %s", fauxDirPath, err.Error())
			return oph.RecordError(err)
		}
	}

	return nil
}


// Add a directory with its contents to an exisiting database
func (mdb *MetadataDb) ReadDirAll(ctx context.Context, oph *OpHandle, dir *Dir) (map[string]File, map[string]Dir, error) {
	if mdb.options.Verbose {
		mdb.log("ReadDirAll %s", dir.FullPath)
	}

	if !dir.Populated {
		err := mdb.directoryReadFromDNAx(
			ctx,
			oph,
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
	return mdb.directoryReadAllEntries(oph, dir.FullPath)
}


// Search for a file/subdir in a directory
// Look for file [filename] in directory [parent]/[dname].
//
// 1. Look if the directory has already been downloaded and placed in the DB
// 2. If not, populate it
// 3. Do a lookup in the directory.
//
// Note: the file might not exist.
func (mdb *MetadataDb) LookupInDir(ctx context.Context, oph *OpHandle, dir *Dir, dirOrFileName string) (Node, bool, error) {
	if !dir.Populated {
		mdb.ReadDirAll(ctx, oph, dir)
	}

	// point lookup in the namespace
	sqlStmt := fmt.Sprintf(`
 		        SELECT obj_type,inode
                        FROM namespace
			WHERE parent = '%s' AND name = '%s';`,
		dir.FullPath, dirOrFileName)
	rows, err := oph.txn.Query(sqlStmt)
	if err != nil {
		return nil, false, oph.RecordError(err)
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
		log.Panicf("Found %d files of the form %s/%s",
			numRows, dir.FullPath, dirOrFileName)
	}

	// There is exactly one answer
	switch objType {
	case nsDirType:
		return mdb.lookupDirByInode(oph, dir.FullPath, dirOrFileName, inode)
	case nsDataObjType:
		return mdb.lookupDataObjectByInode(oph, dirOrFileName, inode)
	default:
		log.Panicf("Invalid object type %d", objType)
		return nil, false, nil
	}
}

// Build a toplevel directory for each project.
func (mdb *MetadataDb) PopulateRoot(ctx context.Context, oph *OpHandle, manifest Manifest) error {
	mdb.log("Populating root directory")

	for _, d := range manifest.Directories {
		mdb.baseDir2ProjectId[d.Dirname] = d.ProjId
	}

	dirSkel, err := manifest.DirSkeleton()
	if err != nil {
		mdb.log("PopulateRoot: Error creating a manifest skeleton")
		return err
	}
	if mdb.options.Verbose {
		mdb.log("dirSkeleton = %v", dirSkel)
	}

	// build the supporting directory structure.
	// We mark each directory as populated, so that the platform would not
	// be queried.
	nowSeconds := time.Now().Unix()
	for _, d := range dirSkel {
		_, err := mdb.createEmptyDir(
			oph,
			"", "",   // There is no backing project/folder
			nowSeconds, nowSeconds,
			dirReadOnlyMode, // skeleton directories are scaffolding, they cannot be modified.
			d, true)
		if err != nil {
			mdb.log("PopulateRoot: Error creating empty dir")
			return oph.RecordError(err)
		}
	}

	// create individual files
	for _, fl := range manifest.Files {
		_, err := mdb.createDataObject(
			oph,
			CDO_NEUTRAL,
			FK_Regular,
			fl.ProjId,
			fl.ArchivalState,
			fl.FileId,
			fl.Size,
			fl.CtimeSeconds,
			fl.MtimeSeconds,
			fileReadOnlyMode,
			fl.Parent,
			fl.Fname,
			"")
		if err != nil {
			mdb.log(err.Error())
			mdb.log("PopulateRoot: error creating singleton file")
			return oph.RecordError(err)
		}
	}

	for _, d := range manifest.Directories {
		// Local directory [d.Dirname] represents
		// folder [d.Folder] on project [d.ProjId].
		_, err := mdb.createEmptyDir(
			oph,
			d.ProjId, d.Folder,
			d.CtimeSeconds, d.MtimeSeconds,
			dirReadWriteMode,
			d.Dirname, false)
		if err != nil {
			mdb.log("PopulateRoot: error creating empty manifest directory")
			return oph.RecordError(err)
		}
	}

	// set the root to be populated
	if err := mdb.setDirectoryToPopulated(oph, InodeRoot); err != nil {
		mdb.log("PopulateRoot: error setting root directory to populated")
		return oph.RecordError(err)
	}

	return nil
}

// We know that the parent directory exists, is populated, and the file does not exist
func (mdb *MetadataDb) CreateFile(
	ctx context.Context,
	oph *OpHandle,
	dir *Dir,
	fileId string,
	fname string,
	mode os.FileMode,
	localPath string) (File, error) {
	if mdb.options.Verbose {
		mdb.log("CreateFile %s/%s  localPath=%s proj=%s",
			dir.FullPath, fname, localPath, dir.ProjId)
	}

	nowSeconds := time.Now().Unix()
	inode, err := mdb.createDataObject(
		oph,
		CDO_MUST_BE_NEW,
		FK_Regular,
		dir.ProjId,
		"live",
		fileId,
		0,    /* the file is empty */
		nowSeconds,
		nowSeconds,
		mode,
		dir.FullPath,
		fname,
		localPath)
	if err != nil {
		mdb.log("CreateFile error creating data object")
		return File{}, err
	}

	// 3. return a File structure
	return File{
		Kind: FK_Regular,
		Id : fileId,
		ProjId : dir.ProjId,
		ArchivalState : "live",
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

// We know that
// 1) the parent directory exists and is populated
// 2) the target file
// 3) the source i-node exists
func (mdb *MetadataDb) CreateLink(ctx context.Context, oph *OpHandle, srcFile File, dstParent Dir, name string) (File, error) {
	// insert into the database
	_, err := mdb.createDataObject(
		oph,
		CDO_ALREADY_EXISTS,
		FK_Regular,
		dstParent.ProjId,
		srcFile.ArchivalState,
		srcFile.Id,
		srcFile.Size,
		int64(srcFile.Ctime.Second()),
		int64(srcFile.Mtime.Second()),
		srcFile.Mode,
		dstParent.FullPath,
		name,
		"")
	if err != nil {
		mdb.log("CreateLink error creating data object")
		return File{}, oph.RecordError(err)
	}

	// 3. return a File structure
	return File{
		Kind: FK_Regular,
		Id : srcFile.Id,
		ProjId : dstParent.ProjId,
		ArchivalState : srcFile.ArchivalState,
		Name : name,
		Size : srcFile.Size,
		Inode : srcFile.Inode,
		Ctime : srcFile.Ctime,
		Mtime : srcFile.Mtime,
		Mode : srcFile.Mode,
		Nlink : srcFile.Nlink + 1,
		InlineData : "",
	}, nil
}

// reduce link count by one. If it reaches zero, delete the file.
//
// TODO: take into account the case of ForgetInode, and files that are open, but unlinked.
func (mdb *MetadataDb) Unlink(ctx context.Context, oph *OpHandle, file File) error {
	nlink := file.Nlink - 1
	if nlink > 0 {
		// reduce one from the link count. It is still positive,
		// so there is nothing else to do
		sqlStmt := fmt.Sprintf(`
  		           UPDATE data_objects
                           SET nlink = '%d'
                           WHERE inode = '%d'`,
			nlink, file.Inode)
		if _, err := oph.txn.Exec(sqlStmt); err != nil {
			mdb.log(err.Error())
			mdb.log("could not reduce the link count for inode=%d to %d",
				file.Inode, nlink)
			return oph.RecordError(err)
		}
	} else {
		// the link hit zero, we can remove the file
		sqlStmt := fmt.Sprintf(`
                           DELETE FROM namespace
                           WHERE inode='%d';`,
			file.Inode)
		if _, err := oph.txn.Exec(sqlStmt); err != nil {
			mdb.log(err.Error())
			mdb.log("could not delete row for inode=%d from the namespace table",
				file.Inode)
			return oph.RecordError(err)
		}

		sqlStmt = fmt.Sprintf(`
                           DELETE FROM data_objects
                           WHERE inode='%d';`,
			file.Inode)
		if _, err := oph.txn.Exec(sqlStmt); err != nil {
			mdb.log(err.Error())
			mdb.log("could not delete row for inode=%d from the data_objects table",
				file.Inode)
			return oph.RecordError(err)
		}

		if file.Kind == RW_File || file.Kind == RO_LocalCopy {
			// remove the file data so it does not take up space on disk.
			// This might be undergoing upload at the moment. Removing the local
			// file will cause the download to fail early, which is what we
			// want.
			if err := os.Remove(file.InlineData); err != nil {
				mdb.log(err.Error())
			}
		}
	}
	return nil
}

func (mdb *MetadataDb) UpdateFile(
	ctx context.Context,
	oph *OpHandle,
	f File,
	fileSize int64,
	modTime time.Time,
	mode os.FileMode) error {
	if mdb.options.Verbose {
		mdb.log("Update file=%v size=%d mode=%d", f, fileSize, mode)
	}
	modTimeSec := modTime.Unix()
	sqlStmt := fmt.Sprintf(`
 		        UPDATE data_objects
                        SET size = '%d', mtime='%d', mode='%d'
			WHERE inode = '%d';`,
		fileSize, modTimeSec, int(mode), f.Inode)

	if _, err := oph.txn.Exec(sqlStmt); err != nil {
		mdb.log(err.Error())
		mdb.log("UpdateFile error executing transaction")
		return oph.RecordError(err)
	}
	return nil
}


func (mdb *MetadataDb) UpdateFileMakeRemote(ctx context.Context, oph *OpHandle, fileId string) error {
	if mdb.options.Verbose {
		mdb.log("Make file remote fileId=%s", fileId)
	}
	sqlStmt := fmt.Sprintf(`
 		        UPDATE data_objects
                        SET Mode = '%d', InlineData=''
			WHERE id = '%s';`,
		fileReadOnlyMode, fileId)

	if _, err := oph.txn.Exec(sqlStmt); err != nil {
		mdb.log(err.Error())
		mdb.log("UpdateFileMakeRemote error executing transaction")
		return oph.RecordError(err)
	}
	return nil
}

// Move a file
// 1) Can move a file from one directory to another,
//    or leave it in the same directory
// 2) Can change the filename.
func (mdb *MetadataDb) MoveFile(
	ctx context.Context,
	oph *OpHandle,
	inode int64,
	newParentDir Dir,
	newName string) error {
	if mdb.options.Verbose {
		mdb.log("MoveFile -> %s/%s", newParentDir.FullPath, newName)
	}
	sqlStmt := fmt.Sprintf(`
 		        UPDATE namespace
                        SET parent = '%s', name = '%s'
			WHERE inode = '%d';`,
		newParentDir.FullPath, newName, inode)

	if _, err := oph.txn.Exec(sqlStmt); err != nil {
		mdb.log(err.Error())
		mdb.log("MoveFile error executing transaction")
		return oph.RecordError(err)
	}
	return nil
}


type MoveRecord struct  {
	oldFullPath   string
	name          string
	newParent     string
	newProjFolder string
	inode         int64
	nsObjType     int
}

func (mdb *MetadataDb) execModifyRecord(oph *OpHandle, r MoveRecord) error {
	// Modify the parent fields in the namespace table.
	if mdb.options.Verbose {
		mdb.log("%s -> %s/%s", r.oldFullPath, r.newParent, r.name)
	}
	sqlStmt := fmt.Sprintf(`
 		        UPDATE namespace
                        SET parent = '%s', name = '%s'
			WHERE inode = '%d';`,
		r.newParent, r.name, r.inode)
	if _, err := oph.txn.Exec(sqlStmt); err != nil {
		mdb.log(err.Error())
		mdb.log("MoveDir error executing transaction")
		return oph.RecordError(err)
	}

	if r.nsObjType == nsDataObjType {
		return nil
	}
	//  /dxfuse_test_data/A/fruit ->  proj-xxxx:/D/K/A/fruit
	//  /dxfuse_test_data/A       ->  proj-xxxx:/D/K/A
	//
	if mdb.options.Verbose {
		mdb.log("move subdir (%s) project-folder %s", r.oldFullPath, r.newProjFolder)
	}

	sqlStmt = fmt.Sprintf(`
 		        UPDATE directories
                        SET proj_folder = '%s'
			WHERE inode = '%d';`,
		r.newProjFolder, r.inode)
	if _, err := oph.txn.Exec(sqlStmt); err != nil {
		mdb.log(err.Error())
		mdb.log("MoveDir error executing transaction")
		return oph.RecordError(err)
	}
	return nil
}

// As a running example:
//
// say we have a directory structure:
// A
// ├── fruit
// │   ├── grapes.txt
// │   └── melon.txt
// ├── X.txt
// └── Y.txt
//
// We also have:
// D
// └── K
//
// From the shell we issue the command:
// $ mv A D/K/
//
func (mdb *MetadataDb) MoveDir(
	ctx context.Context,
	oph *OpHandle,
	oldParentDir Dir,
	newParentDir Dir,
	oldDir Dir,
	newName string) error {

	// Find the sub-tree rooted at oldDir, and change
	// all the:
	// 1) namespace records olddir -> newDir
	// 2) directory records for proj_folder: olddir -> newDir

	if mdb.options.Verbose {
		mdb.log("MoveDir %s -> %s/%s", oldDir.FullPath, newParentDir.FullPath, newName)
	}

	// Find all directories and data objects in the subtree rooted at
	// the old directory. We use the SQL ability to match the prefix
	// of a string; we add % at the end.
	sqlStmt := fmt.Sprintf(`
 		        SELECT parent, name, inode, obj_type
                        FROM namespace
			WHERE parent LIKE '%s';`,
		oldDir.FullPath + "%")
	rows, err := oph.txn.Query(sqlStmt)
	if err != nil {
		return oph.RecordError(err)
	}

	// extract the records, close the query
	var records []MoveRecord = make([]MoveRecord, 0)
	for rows.Next() {
		var parent string
		var name string
		var inode int64
		var nsObjType int
		rows.Scan(&parent, &name, &inode, &nsObjType)

		// sanity check: make sure the path actually starts with the old directory
		if !strings.HasPrefix(parent, oldDir.FullPath) {
			log.Panicf("Query returned node %s that does not start with prefix %s",
				parent, oldDir.FullPath)
		}

		// For file /A/fruit/melon.txt
		//   oldFullPath : /A/fruit/melon.txt
		//   name : melon.txt
		//   midPath : /A/fruit
		//
		midPath := newName + "/" + strings.TrimPrefix(parent, oldDir.FullPath)
		var newProjFolder string
		if nsObjType == nsDirType {
			newProjFolder  = filepath.Clean(newParentDir.ProjFolder + "/" + midPath + "/" + name)
		}
		mr := MoveRecord{
			oldFullPath : parent + "/" + name,
			name : name,
			newParent : filepath.Clean(newParentDir.FullPath + "/" + midPath),
			newProjFolder : newProjFolder,
			inode : inode,
			nsObjType : nsObjType,
		}
		records = append(records, mr)
	}
	rows.Close()

	// add the top level directory (A) to be moved. Note, that the top level directory may
	// change name.
	records = append(records, MoveRecord{
		oldFullPath : oldDir.FullPath,
		name : newName,
		newParent : filepath.Clean(newParentDir.FullPath),
		newProjFolder : filepath.Clean(filepath.Join(newParentDir.ProjFolder, newName)),
		inode : oldDir.Inode,
		nsObjType: nsDirType,
	})
	if mdb.options.Verbose {
		mdb.log("found %d records under directory %s: %v", len(records), oldDir.FullPath, records)
	}

	for _, r := range records {
		if err := mdb.execModifyRecord(oph, r); err != nil {
			return err
		}
	}
	return nil
}

func (mdb *MetadataDb) Shutdown() {
	if err := mdb.db.Close(); err != nil {
		mdb.log(err.Error())
		mdb.log("Error closing the sqlite database %s", mdb.dbFullPath)
	}
}
