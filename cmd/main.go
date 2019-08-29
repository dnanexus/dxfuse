package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	//"runtime"
	"strings"

	// The dxda package has the get-environment code
	"github.com/dnanexus/dxda"
	"github.com/dnanexus/dxfs2"
)

var progName = filepath.Base(os.Args[0])

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", progName)
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT PROJECT_ID_OR_NAME\n", progName)
	flag.PrintDefaults()
}

var (
	metadataDbPath = flag.String("dbPath", "/var/dxfs2", "Directory where to place metadata database")
	debugFuseFlag = flag.Bool("debugFuse", false, "Tap into FUSE debugging information")
	verbose = flag.Int("verbose", 0, "Enable verbose debugging")
)

func lookupProject(dxEnv *dxda.DXEnvironment, projectIdOrName string) (string, error) {
	if strings.HasPrefix(projectIdOrName, "project-") {
		// This is a project ID
		return projectIdOrName, nil
	}

	// This is a project name, describe it, and
	// return the project-id.
	return dxfs2.DxFindProject(dxEnv, projectIdOrName)
}

func main() {
	log.SetFlags(0)
	log.SetPrefix(progName + ": ")

	flag.Usage = usage
	flag.Parse()

	// Limit the number of spare OS threads
	//runtime.GOMAXPROCS(64)

	if flag.NArg() != 2 {
		usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)
	projectIdOrName := flag.Arg(1)

	options := dxfs2.Options {
		DebugFuse: *debugFuseFlag,
		MetadataDbPath: *metadataDbPath,
		Verbose : *verbose > 0,
		VerboseLevel : *verbose,
	}

	dxEnv, _, err := dxda.GetDxEnvironment()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	projectId, err := lookupProject(&dxEnv, projectIdOrName)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if projectId == "" {
		fmt.Printf("Error: no project with name %s\n", projectIdOrName)
		os.Exit(1)
	}

	if err := dxfs2.Mount(mountpoint, dxEnv, projectId, options); err != nil {
		fmt.Printf(err.Error())
	}
}
