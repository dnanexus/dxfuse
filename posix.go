package dxfs2

import (
	"fmt"
	"log"
	"strconv"
	"strings"
)

// Try to fix a DNAx directory, so it will adhere to POSIX.
//
// 1. If several files share the same name, make the unique by adding a prefix.
//    For example:
//
//    src name file-id      new name
//    X.txt    file-0001    X.txt
//    X.txt    file-0005    X_1.txt
//    X.txt    file-0012    X_2.txt
//
// 2. DNAx files can include slashes. Replace these with underscores, while keeping
//    filenames unique.
//
// 3. A directory and a file can have the same name


// If a file is of the form AAA.txt, return AAA_SUFF.txt.
//
// Examples:
//    source filename     result
//    foobar              foobar_SUFFIX
//    foobar.gz           foobar_SUFFIX.gz
//    foobar.tar.gz       foobar_SUFFIX.tar.gz
func addToBasename(name string, suffix string) string {
	parts := strings.Split(name, ".")
	if len(parts) == 0 {
		// The file contains only dots. Is that even possible?
		return name + "_" + suffix
	}

	parts[0] = parts[0] + "_" + suffix
	return strings.Join(parts, ".")
}

func chooseUniqueName(
	fnameOrg string,
	used map[string]bool,
	choiceLimit int,
	removeSlashes bool) string {

	// get rid of slashes, if there are any
	fnameNormal := fnameOrg
	if removeSlashes {
		fnameNormal = strings.ReplaceAll(fnameOrg, "/", "_")
	}

	if !used[fnameNormal] {
		return fnameNormal
	}

	// Name is already used. Try adding _NUMBER to the core
	// name, until you find a name that isn't already used
	cnt := 1
	fnameUnq := fnameNormal
	for used[fnameUnq] {
		s := strconv.Itoa(cnt)
		fnameUnq = addToBasename(fnameNormal, s)
		cnt += 1
		if cnt >= choiceLimit {
			panic(fmt.Errorf("Too many iterations, %d > %d", cnt, choiceLimit))
		}
	}
	return fnameUnq
}

// Rename files such that they are unique, and do not contain slashes.
func fixFileNames(files map[string]string) map[string]string {
	// This is really a set that keeps track
	// of the unique names used.
	used := make(map[string]bool)
	totNumFiles := len(files)

	// new maping from file-id to filename.
	translation := make(map[string]string)

	for fid, fname := range files {
		fnameUnq := chooseUniqueName(fname, used, totNumFiles + 2, true)
		used[fnameUnq] = true
		translation[fid] = fnameUnq
	}

	return translation
}

// rename directory names, if needed, so they do not overlap with filenames
func fixSubdirNames(dirnames []string, filenameTranslation map[string]string) []string {
	// This is really a set that keeps track
	// of the unique names used.
	used := make(map[string]bool)

	// mark all the names already used
	for _, fname := range filenameTranslation {
		used[fname] = true
	}
	log.Printf("used=%#v", used)

	totNumElements := len(filenameTranslation) + len(dirnames)
	var result []string

	for _, dName := range dirnames {
		// a directory may have slashes. Note that these are full pathes (/A/B/C).
		subdirUnq := chooseUniqueName(dName, used, totNumElements + 2, false)
		if dName != subdirUnq {
			log.Printf("dirname %s -> %s", dName, subdirUnq)
		}
		used[subdirUnq] = true
		result = append(result, subdirUnq)
	}
	return result
}

// main entry point
func PosixFixDir(fsys *Filesys, dirFullName string, dxFolder *DxFolder) (*DxFolder, error) {
	if fsys.options.Debug {
		log.Printf("PosixFixDir %s #files=%d #subdirs=%d",
			dirFullName,
			len(dxFolder.files),
			len(dxFolder.subdirs))
	}

	// Make the filenames POSIX

	// strip the descriptions, leaving a mapping from file-id, to file-name.
	// Make the filenames unique, and adhering to posix.
	filenames := make(map[string]string)
	for fid, fDesc := range dxFolder.files {
		filenames[fid] = fDesc.Name
	}
	filenameTranslation := fixFileNames(filenames)
	log.Printf("filenameTranslation=%#v", filenameTranslation)

	// Go over the per file descriptions, and rename the file names
	posixFiles := make(map[string]DxDescribe)
	for fid, fDesc := range dxFolder.files {
		fnameUnq, ok := filenameTranslation[fid]
		if !ok {
			panic(fmt.Errorf("Sanity, could not find %s in file translations", fid))
		}
		// Create a new structure because we can't modify the original
		posixDesc := fDesc
		posixDesc.Name = fnameUnq
		posixFiles[fid] = posixDesc
	}

	// Make sure there is no overlap between subdirs and file names
	posixSubdirs := fixSubdirNames(dxFolder.subdirs, filenameTranslation)
	log.Printf("posixSubdirs=%v", posixSubdirs)

	posixDxFolder := &DxFolder{
		files: posixFiles,
		subdirs: posixSubdirs,
	}
	return posixDxFolder, nil
}
