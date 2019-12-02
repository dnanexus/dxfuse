package dxfuse

import (
	"path/filepath"
	"strconv"
	"strings"
)

// Try to fix a DNAx directory, so it will adhere to POSIX.
//
// 1. If several files share the same name, make them unique by moving into an
//    extra subdirectory. For example:
//
//    src name file-id      new name
//    X.txt    file-0001    X.txt
//    X.txt    file-0005    1/X.txt
//    X.txt    file-0012    2/X.txt
//
// 2. DNAx files can include slashes. Drop these files, with a put note in the log.
//
// 3. A directory and a file can have the same name. This is not handled right now.
//
type PosixDir struct {
	path          string   // entire directory path
	dataObjects []DxDescribeDataObject
	subdirs     []string

	// additional subdirectories holding files that have multiple versions,
	// and could not be placed in the original location.
	fauxSubdirs  map[string]([]DxDescribeDataObject)
}

// write a log message, and add a header
func posixLog(a string, args ...interface{}) {
	LogMsg("posix", a, args...)
}

func FilenameIsPosixCompliant(filename string) bool {
	if strings.Contains(filename, "/") {
		return false
	}
	return true
}

// Slashes cannot be included in a posix filename. Replace them with a triple underscore.
func filenameNormalize(filename string) string {
	return strings.ReplaceAll(filename, "/", "___")
}

// Choose a directory name that is unused.
func chooseFauxDirName(usedNames map[string]bool, counter *int) string {
	posixLog("chooseFauxDirName used=%v counter=%d", usedNames, *counter)
	maxNumIter := len(usedNames) + 1

	for i := 0; i <= maxNumIter; i++ {
		tentativeName := strconv.Itoa(*counter)
		posixLog("choose dir name=%s", tentativeName)
		_, ok := usedNames[tentativeName]
		if !ok {
			// name has not been used yet
			usedNames[tentativeName] = true
			return tentativeName
		}

		// already used, we need another name
		*counter++
	}

	panic("could not choose a directory name after " + string(maxNumIter) + " iterations")
}


// Choose each name once. Return the remaining data-objects, those that are multiply named,
// and were not chosen.
func makeCut(dxObjs []DxDescribeDataObject) ([]DxDescribeDataObject, []DxDescribeDataObject) {
	used := make(map[string]bool)
	remaining := make([]DxDescribeDataObject, 0)
	firstTimers := make([]DxDescribeDataObject, 0)

	for _, oDesc := range dxObjs {
		_, ok := used[oDesc.Name]
		if ok {
			// We have already seen a file with this name
			remaining = append(remaining, oDesc)
		} else {
			// first time for this file name
			firstTimers = append(firstTimers, oDesc)
			used[oDesc.Name] = true
		}
	}
	return remaining, firstTimers
}

// main entry point
//
// 1. Keep directory names fixed
// 2. Change file names to not collide with directories, or with each other.
func PosixFixDir(options Options, dxFolder *DxFolder) (*PosixDir, error) {
	if options.VerboseLevel > 1 {
		posixLog("PosixFixDir %s #objects=%d #subdirs=%d",
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
		lastPart = strings.TrimPrefix(lastPart,"/")
		if strings.Contains(lastPart, "/") {
			posixLog("Dropping subdirectory %s, it contains a slash", lastPart)
			continue
		}
		if lastPart != filepath.Base(subDirName) {
			posixLog("Dropping subdirectory %s, it isn't the same as Base(d)=%s",
				lastPart, filepath.Base(subDirName))
			continue
		}

		subdirs = append(subdirs, filepath.Base(subDirName))
	}
	if options.VerboseLevel > 1 {
		posixLog("subdirs = %v", subdirs)
	}

	// convert the map into an array. Normalize any non Posix names.
	var allDxObjs []DxDescribeDataObject
	for _, dxObj := range dxFolder.dataObjects {
		var objNorm DxDescribeDataObject = dxObj

		if !FilenameIsPosixCompliant(objNorm.Name) {
			// we need to normalize the name
			objNorm.Name = filenameNormalize(objNorm.Name)
		}
		allDxObjs = append(allDxObjs, objNorm)
	}

	// Take all the data-objects that appear just once. There will be placed
	// at the toplevel.
	nonUniqueNamedObjs, topLevelObjs := makeCut(allDxObjs)
	if options.VerboseLevel > 1 {
		posixLog("make cut: nonUniqueObj=%v topLevelObjs=%v", nonUniqueNamedObjs, topLevelObjs)
	}

	// Iteratively, take unique files from the remaining objects, and place them in
	// subdirectories 1, 2, 3, ... Be careful to create unused directory names
	fauxDirCounter := 1
	fauxSubdirs := make(map[string][]DxDescribeDataObject)
	usedSubdirNames := make(map[string]bool)
	for _, dname := range subdirs {
		usedSubdirNames[dname] = true
	}

	remaining := nonUniqueNamedObjs
	for len(remaining) > 0 {
		notChosenThisTime, uniqueObjs := makeCut(remaining)
		fauxDir := chooseFauxDirName(usedSubdirNames, &fauxDirCounter)
		fauxSubdirs[fauxDir] = uniqueObjs

		if options.VerboseLevel > 1 {
			posixLog("fauxDir=%s  len(remainingObjs)=%d  len(uniqueObjs)=%d",
				fauxDir, len(notChosenThisTime), len(uniqueObjs))
		}

		if len(remaining) <= len(notChosenThisTime) {
			// The number of non unique objects must drop
			// monotonically
			panic("not making progress")
		}
		remaining = notChosenThisTime
	}

	posixDxFolder := &PosixDir{
		path: dxFolder.path,
		dataObjects: topLevelObjs,
		subdirs: subdirs,
		fauxSubdirs: fauxSubdirs,
	}
	posixLog("posix folder: %v", posixDxFolder)
	return posixDxFolder, nil
}
