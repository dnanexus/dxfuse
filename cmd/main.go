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
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT MANIFEST\n", progName)
	flag.PrintDefaults()
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

	fileIds,err = utils.ReadManifest(manifest)

	// See that we can describe a file
//	testFileDescribe(&dxEnv, projId, fileId)

	if err := dxfs2.Mount(mountpoint, dxEnv, fileIds); err != nil {
		log.Fatal(err)
	}
}

/*
func testFileDescribe(dxEnv *dxda.DXEnvironment, projId string, fileId string) {
	desc, err := dxfs2.Describe(dxEnv, projId, fileId)
	if desc == nil {
		fmt.Printf("The description is empty\n")
		fmt.Printf(err.Error() + "\n")
	} else {
		fmt.Printf("%v\n", *desc)
	}
}
*/
