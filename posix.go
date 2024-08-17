package dxfuse

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Try to fix a DNAx directory, so it will adhere to POSIX.
//
//  1. If several files share the same name, make them unique by moving into an
//     extra subdirectory. For example:
//
//     src name file-id      new name
//     X.txt    file-0001    X.txt
//     X.txt    file-0005    1/X.txt
//     X.txt    file-0012    2/X.txt
//
// 2. DNAx files can include slashes. Drop these files, with a put note in the log.
//
//  3. A directory and a file can have the same name. For example:
//     ROOT/
//     zoo/      sub-directory
//     zoo       regular file
//
//     Is converted into:
//     ROOT
//     zoo/      sub-directory
//     1/        faux sub-directory
//     zoo     regular file
type PosixDir struct {
	path        string // entire directory path
	dataObjects []DxDescribeDataObject
	subdirs     []string

	// additional subdirectories holding files that have multiple versions,
	// and could not be placed in the original location.
	fauxSubdirs map[string]([]DxDescribeDataObject)
}

type Posix struct {
	options Options
}

func NewPosix(options Options) *Posix {
	return &Posix{
		options: options,
	}
}

// write a log message, and add a header
func (px *Posix) log(a string, args ...interface{}) {
	LogMsg("posix", a, args...)
}

func FilenameIsPosixCompliant(filename string) bool {
	if strings.Contains(filename, "/") {
		return false
	}
	return true
}

// Slashes cannot be included in a posix filename. Replace them with a triple underscore.
func (px *Posix) filenameNormalize(filename string) string {
	return strings.ReplaceAll(filename, "/", "___")
}

// Choose [num] directory names that are not taken by existing subdirs or files
func (px *Posix) pickFauxDirNames(subdirs []string, uniqueFileNames []string, num int) []string {
	usedNames := make(map[string]bool)
	for _, dName := range subdirs {
		usedNames[dName] = true
	}
	for _, fName := range uniqueFileNames {
		usedNames[fName] = true
	}

	i := 1
	maxNumIter := (len(subdirs) + len(uniqueFileNames) + num) * 2
	var dirNames []string
	for len(dirNames) < num {
		tentativeName := strconv.Itoa(i)

		_, ok := usedNames[tentativeName]
		if !ok {
			// name has not been used yet
			usedNames[tentativeName] = true
			dirNames = append(dirNames, tentativeName)
		}

		// already used, we need another name
		i++
		if i > maxNumIter {
			panic(fmt.Sprintf("Too many iterations choosing faux directory names %d > %d",
				i, maxNumIter))
		}
	}

	return dirNames
}

// Find all the unique file names.
func (px *Posix) uniqueFileNames(dxObjs []DxDescribeDataObject) []string {
	usedNames := make(map[string]bool)

	var firstTimers []string
	for _, oDesc := range dxObjs {
		_, ok := usedNames[oDesc.Name]
		if !ok {
			// first time for this name
			firstTimers = append(firstTimers, oDesc.Name)
			usedNames[oDesc.Name] = true
		}
	}
	return firstTimers
}

// pick all the objects with "name" from the list. Return an empty array
// if none exist. Sort them from newest to oldest.
func (px *Posix) SortObjectsByCtime(dxObjs []DxDescribeDataObject) []DxDescribeDataObject {
	// sort by date
	sort.Slice(dxObjs, func(i, j int) bool { return dxObjs[i].CtimeSeconds > dxObjs[j].CtimeSeconds })
	return dxObjs
}

// main entry point
//
// 1. Keep directory names fixed
// 2. Change file names to not collide with directories, or with each other.
func (px *Posix) FixDir(dxFolder *DxFolder) (*PosixDir, error) {
	if px.options.VerboseLevel > 1 {
		px.log("PosixFixDir %s #objects=%d #subdirs=%d",
			dxFolder.path,
			len(dxFolder.dataObjects),
			len(dxFolder.subdirs))
	}

	// The subdirectories are specified in long paths ("/A/B/C"). Leave just
	// the last part of the name ("C").
	//
	// Remove all subdirectories that contain a slash
	subdirs := make([]string, 0)
	for _, subDirName := range dxFolder.subdirs {
		// Make SURE that the subdirectory does not contain a slash.
		lastPart := strings.TrimPrefix(subDirName, dxFolder.path)
		lastPart = strings.TrimPrefix(lastPart, "/")
		if strings.Contains(lastPart, "/") {
			px.log("Dropping subdirectory %s, it contains a slash", lastPart)
			continue
		}
		if lastPart != filepath.Base(subDirName) {
			px.log("Dropping subdirectory %s, it isn't the same as Base(d)=%s",
				lastPart, filepath.Base(subDirName))
			continue
		}

		subdirs = append(subdirs, filepath.Base(subDirName))
	}
	if px.options.VerboseLevel > 1 {
		px.log("subdirs = %v", subdirs)
	}

	// convert the map into an array. Normalize any non Posix names.
	var allDxObjs []DxDescribeDataObject
	for _, dxObj := range dxFolder.dataObjects {
		var objNorm DxDescribeDataObject = dxObj

		if !FilenameIsPosixCompliant(objNorm.Name) {
			// we need to normalize the name
			objNorm.Name = px.filenameNormalize(objNorm.Name)
		}
		allDxObjs = append(allDxObjs, objNorm)
	}

	// TODO: choose the latest object for each name. For example, if we
	// have two files named zoo, the toplevel one will be the most recent.
	uniqueFileNames := px.uniqueFileNames(allDxObjs)
	if px.options.VerboseLevel > 1 {
		px.log("unique file names=%v", uniqueFileNames)
	}

	dxObjsPerUniqueFilename := make(map[string][]DxDescribeDataObject)

	// Create a map of object names to obj describes
	// To be used in spreading objects across faux subdirs
	// objName: [obj1Desc, obj2Desc]
	for _, dxObj := range allDxObjs {
		dxObjsPerUniqueFilename[dxObj.Name] = append(dxObjsPerUniqueFilename[dxObj.Name], dxObj)
	}

	// Iteratively, take unique files from the remaining objects, and place them in
	// subdirectories 1, 2, 3, ... Be careful to create unused directory names
	fauxSubDirs := make(map[string][]DxDescribeDataObject)

	// choose names for faux subdirs for the worst case where all objects
	// have the same name.
	var fauxDirNames []string
	maxNumDirs := len(allDxObjs)
	fauxDirNames = px.pickFauxDirNames(subdirs, uniqueFileNames, maxNumDirs)
	if px.options.Verbose {
		px.log("pickFauxDirNames %v", fauxDirNames)
	}

	subdirSet := make(map[string]bool)
	for _, dName := range subdirs {
		subdirSet[dName] = true
	}

	// Take all the data-objects that have names that aren't already taken
	// up by subdirs. They go in the top level
	var topLevelObjs []DxDescribeDataObject
	for _, oName := range uniqueFileNames {
		dxObjs := px.SortObjectsByCtime(dxObjsPerUniqueFilename[oName])
		if px.options.VerboseLevel > 1 {
			px.log("name=%s len(objs)=%d", oName, len(dxObjs))
		}

		_, ok := subdirSet[oName]
		if !ok {
			// There is no directory with this name, we
			// place the object at the toplevel
			topLevelObjs = append(topLevelObjs, dxObjs[0])
			dxObjs = dxObjs[1:]
		}

		// spread the remaining copies across the faux subdirectories
		for i, obj := range dxObjs {
			dName := fauxDirNames[i]
			vec, ok := fauxSubDirs[dName]
			if !ok {
				// need to start a new faux subdir called "dName"
				v := make([]DxDescribeDataObject, 1)
				v[0] = obj
				fauxSubDirs[dName] = v
			} else {
				fauxSubDirs[dName] = append(vec, obj)
			}
		}
	}

	posixDxFolder := &PosixDir{
		path:        dxFolder.path,
		dataObjects: topLevelObjs,
		subdirs:     subdirs,
		fauxSubdirs: fauxSubDirs,
	}
	if px.options.VerboseLevel > 1 {
		px.log("%v", posixDxFolder)
	}
	return posixDxFolder, nil
}
