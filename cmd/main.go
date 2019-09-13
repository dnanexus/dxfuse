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
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT PROJECT1 PROJECT2 ...\n", progName)
	fmt.Fprintf(os.Stderr, "Note:\n")
	fmt.Fprintf(os.Stderr, "  Multiple projects can be specified\n")
	fmt.Fprintf(os.Stderr, "  A project can be specified by its ID or name\n")
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
	numArgs := flag.NArg()
	if numArgs < 2 {
		usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)

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

	// process the project inputs, and convert to an array of verified
	// project IDs
	log.Printf("processing projects to mount")
	var projectIds []string
	for i := 1; i < numArgs; i++ {
		projectIdOrName := flag.Arg(i)
		projId, err := lookupProject(&dxEnv, projectIdOrName)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if projId == "" {
			fmt.Printf("Error: no project with name %s\n", projectIdOrName)
			os.Exit(1)
		}
		projectIds = append(projectIds, projId)
	}

	if err := dxfs2.Mount(mountpoint, dxEnv, projectIds, options); err != nil {
		//fmt.Println(err.(*errors.Error).ErrorStack())
		fmt.Printf(err.Error())
	}
}
