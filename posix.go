package dxfs2

import (
	"fmt"
	"log"
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
	path      string   // entire directory path
	files     []DxDescribeDataObject
	subdirs   []string

	// additional subdirectories holding files that have multiple versions,
	// and could not be placed in the original location.
	fauxSubdirs map[string]([]DxDescribeDataObject)
}

func FilenameIsPosixCompliant(filename string) bool {
	if strings.Contains(filename, "/") {
		return false
	}
	return true
}


// Choose each filename once. Return the remaining files, those that are multiply named,
// and were not chosen.
func makeCut(files []DxDescribeDataObject) ([]DxDescribeDataObject, []DxDescribeDataObject) {
	used := make(map[string]bool)
	remaining := make([]DxDescribeDataObject, 0)
	firstTimers := make([]DxDescribeDataObject, 0)

	for _, fDesc := range files {
		_, ok := used[fDesc.Name]
		if ok {
			// We have already seen a file with this name
			remaining = append(remaining, fDesc)
		} else {
			// first time for this file name
			firstTimers = append(firstTimers, fDesc)
			used[fDesc.Name] = true
		}
	}
	return remaining, firstTimers
}

// main entry point
//
// 1. Keep directory names fixed
// 2. Change file names to not collide with directories, or with each other.
func PosixFixDir(fsys *Filesys, dxFolder *DxFolder) (*PosixDir, error) {
	if fsys.options.Verbose {
		log.Printf("PosixFixDir %s #files=%d #subdirs=%d",
			dxFolder.path,
			len(dxFolder.files),
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
			log.Printf("Dropping subdirectory %s, it contains a slash", lastPart)
			continue
		}
		if lastPart != filepath.Base(subDirName) {
			log.Printf("Dropping subdirectory %s, it isn't the same as Base(d)=%s",
				lastPart, filepath.Base(subDirName))
			continue
		}

		subdirs = append(subdirs, filepath.Base(subDirName))
	}
	if fsys.options.Verbose {
		log.Printf("subdirs = %v", subdirs)
	}

	// Take all the files that appear just once. There will be placed
	// at the toplevel.
	var allFiles []DxDescribeDataObject
	for _, file := range dxFolder.files {
		allFiles = append(allFiles, file)
	}

	nonUniqueNamedFiles, topLevelFiles := makeCut(allFiles)

	// Iteratively, take unique files from the remaining files, and place them in
	// subdirectories 1, 2, 3, ...
	fauxDir := 1
	fauxSubdirs := make(map[string][]DxDescribeDataObject)

	for remaining := nonUniqueNamedFiles; len(remaining) > 0; {
		notChosenThisTime, uniqueFiles := makeCut(remaining)
		fauxSubdirs[strconv.Itoa(fauxDir)] = uniqueFiles
		fauxDir++

		if fsys.options.Verbose {
			log.Printf(fmt.Sprintf("fauxDir=%d  len(remainingFiles)=%d  len(uniqueFiles)=%d",
				fauxDir, len(notChosenThisTime), len(uniqueFiles)))
		}

		if len(remaining) <= len(notChosenThisTime) {
			// The number of non unique files must drop
			// monotonically
			panic("not making progress")
		}
		remaining = notChosenThisTime
	}

	posixDxFolder := &PosixDir{
		path: dxFolder.path,
		files: topLevelFiles,
		subdirs: subdirs,
		fauxSubdirs: fauxSubdirs,
	}
	return posixDxFolder, nil
}
