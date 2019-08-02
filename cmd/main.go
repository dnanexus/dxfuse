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
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT PROJECT_ID", progName)
	flag.PrintDefaults()
}

var (
	debugFlag = flag.Bool("debug", false, "enable verbose debugging")
)

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
	projectId := flag.Arg(1)

	options := dxfs2.Options {
		Debug : *debugFlag,
	}

	dxEnv, _, err := dxda.GetDxEnvironment()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if err := dxfs2.Mount(mountpoint, dxEnv, projectId, options); err != nil {
		log.Fatal(err)
	}
}
