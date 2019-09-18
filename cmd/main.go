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
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT manifest.json\n", progName)
	fmt.Fprintf(os.Stderr, "A project can be specified by its ID or name. The manifest is a JSON\n")
	fmt.Fprintf(os.Stderr, "file describing the initial filesystem structure.\n")
	flag.PrintDefaults()
}

var (
	debugFuseFlag = flag.Bool("debugFuse", false, "Tap into FUSE debugging information")
	metadataDbPath = flag.String("dbPath", "/var/dxfs2", "Directory where to place metadata database")
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

	// distinguish between the case of a manifest, and a list of projects.
	var manifest *dxfs2.Manifest
	if numArgs == 2 && strings.HasSuffix(flag.Arg(1), ".json") {
		p := flag.Arg(1)
		log.Printf("Provided with a manifest, reading from %s", p)
		manifest, err = dxfs2.ReadManifest(p)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if err := manifest.FillInMissingFields(dxEnv); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	} else {
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

		manifest, err = dxfs2.MakeManifestFromProjectIds(dxEnv, projectIds)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	if err := dxfs2.Mount(mountpoint, dxEnv, *manifest, options); err != nil {
		fmt.Printf(err.Error())
	}
}
