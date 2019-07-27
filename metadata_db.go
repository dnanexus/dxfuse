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
const DB_NUM_OBJECTS_LIMIT = 100 * 1000

// Construct a local sql database that holds metadata for
// a large number of dx:files. This metadata_db will be consulted
// when performing dxfs2 operations. For example, a read-dir is
// translated into a query for all the files inside a directory.

// build a database from all the files in folder inside a DNAx project
/*func BuildDB(
	dxEnv *dxda.DXEnvironment
	project string
	folder  string) error {
}*/

// build the database from a list of file-ids
func BuildDB(
	dxEnv *dxda.DXEnvironment,
	dbFname string,
	fileIds []string) (string, error) {

	// describe all the files
	descs, err := DescribeBulk(dxEnv, fileIds)
	if err != nil {
		return "", err
	}
	// limit the number of files
	if len(descs) > DB_NUM_OBJECTS_LIMIT {
		err := fmt.Errorf("Too many objects (%d), the limit is %d", len(descs), DB_NUM_OBJECTS_LIMIT)
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
                mtime bigint
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

	for _, d := range descs {
		sqlStmt = fmt.Sprintf(`
 		        INSERT INTO files
			VALUES ('%s', '%s', '%s', '%s', %d, '%d', '%d');
			`,
			d.FileId, d.ProjId, d.Name, d.Folder, d.Size, d.Ctime, d.Mtime)
		_, err = db.Exec(sqlStmt)
		if err != nil {
			return "", err
		}
	}
	_, err = db.Exec("END TRANSACTION")
	if err != nil {
		return "", err
	}
	return dbName, nil
}
