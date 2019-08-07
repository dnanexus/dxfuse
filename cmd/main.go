package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	// The dxda package has the get-environment code
	"github.com/dnanexus/dxda"
	"github.com/dnanexus/dxfs2"
)

var progName = filepath.Base(os.Args[0])

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", progName)
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT PROJECT_ID_OR_NAME", progName)
	fmt.Fprintf(os.Stderr, "Optional flags\n")
	fmt.Fprintf(os.Stderr, "  -debug : write debugging output to stdout")
	fmt.Fprintf(os.Stderr, "  -debugFuse : redirect FUSE debugging outputs to stdout")
	flag.PrintDefaults()
}

var (
	debugFlag = flag.Bool("debug", false, "enable verbose debugging")
	debugFuseFlag = flag.Bool("debugFuse", false, "tap into FUSE debugging information")
)

func lookupProject(projectIdOrName string) string {
	if strings.HasPrefix(projectIdOrName, "project-") {
		// This is a project ID
		return projectIdOrName
	}

	// This is a project name, describe it, and
	// return the project-id.
	DxDescribeProject(projectIdOrName)
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
	projectIdOrName := flag.Arg(1)

	options := dxfs2.Options {
		Debug : *debugFlag,
		DebugFuse: *debugFuseFlag,
	}

	projectId := lookupProject(projectIdOrName)

	dxEnv, _, err := dxda.GetDxEnvironment()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if err := dxfs2.Mount(mountpoint, dxEnv, projectId, options); err != nil {
		log.Fatal(err)
	}
}
