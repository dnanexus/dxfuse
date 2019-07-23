package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	// The dxda package has the get-environment code
	"github.com/dnanexus/dxda"
	"github.com/dnanexus/dxfs2"
)

var progName = filepath.Base(os.Args[0])

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", progName)
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT FILE_IDS\n", progName)
	flag.PrintDefaults()
}

// Read a JSON manifest file of the form:
//  [ "file-0001",
//    "file-1122",
//    "file-77337",
//  ]
//
func readManifest(path string) ([]string, error) {
	jsonFile, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer jsonFile.Close()

	bytes, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return nil, err
	}
	var fileIds []string
	json.Unmarshal(bytes, &fileIds)
	return fileIds, nil
}


func main() {
	log.SetFlags(0)
	log.SetPrefix(progName + ": ")

	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 2 {
		usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)
	manifest := flag.Arg(1)

	dxEnv, _, err := dxda.GetDxEnvironment()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fileIds = readManifest(manifest)

	if err := dxfs2.Mount(mountpoint, dxEnv, fileIds); err != nil {
		log.Fatal(err)
	}
}
