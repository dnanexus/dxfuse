package dxfs2

import (
	"fmt"
	"log"
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

// Rename files such that they are unique, and do not contain slashes.
func fixFileNames(files []string) map[string]string {
	// This is really a set that keeps track
	// of the unique names used.
	used := make(map[string]bool)
	totNumFiles := len(files)

	// conversion from old to new file names
	translation := make(map[string]string)

	for _, fnameOrg := range files {
		// get rid of slashes, if there are any
		fnameNormal := strings.ReplaceAll(fnameOrg, "/", "_")
		if !used[fnameNormal] {
			used[fnameNormal] = true
			translation[fnameOrg] = fnameNormal
			continue
		}

		// Name is already used. Try adding _NUMBER to the core
		// name, until you find a name that isn't already used
		var cnt int = 1
		var fnameUnq string = fnameNormal
		for used[fnameUnq] {
			fnameUnq = addToBasename(fnameNormal, string(cnt))
			cnt += 1
			if cnt >= totNumFiles + 2 {
				panic(fmt.Errorf("Too many iterations, %d > %d", cnt, totNumFiles))
			}
		}

		used[fnameUnq] = true
		translation[fnameOrg] = fnameUnq
	}

	return translation
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
	var filenames []string
	for _, fDesc := range dxFolder.files {
		filenames = append(filenames, fDesc.Name)
	}
	filenameTranslation := fixFileNames(filenames)

	// Go over the per file descriptions, and rename the file names
	for _, fDesc := range dxFolder.files {
		fnameUnq, ok := filenameTranslation[fDesc.Name]
		if !ok {
			panic(fmt.Errorf("Sanity, could not find %s in file translations", fDesc.Name))
		}
		fDesc.Name = fnameUnq
	}

	return dxFolder, nil
}
