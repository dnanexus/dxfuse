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
	"github.com/dnanexus/dxfuse"
)

var progName = filepath.Base(os.Args[0])

func usage() {
	fmt.Fprintf(os.Stderr, "usage:\n")
	fmt.Fprintf(os.Stderr, "    %s MOUNTPOINT [options] PROJECT1 PROJECT2 ...\n", progName)
	fmt.Fprintf(os.Stderr, "    %s MOUNTPOINT [options] manifest.json\n", progName)
	fmt.Fprintf(os.Stderr, "options:\n")
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "A project can be specified by its ID or name. The manifest is a JSON\n")
	fmt.Fprintf(os.Stderr, "file describing the initial filesystem structure.\n")
}

var (
	debugFuseFlag = flag.Bool("debugFuse", false, "Tap into FUSE debugging information")
	gid = flag.Int("gid", -1, "User group id (gid)")
	help = flag.Bool("help", false, "display program options")
	uid = flag.Int("uid", -1, "User id (uid)")
	verbose = flag.Int("verbose", 0, "Enable verbose debugging")
	version = flag.Bool("version", false, "Print the version and exit")
)

func lookupProject(dxEnv *dxda.DXEnvironment, projectIdOrName string) (string, error) {
	if strings.HasPrefix(projectIdOrName, "project-") {
		// This is a project ID
		return projectIdOrName, nil
	}

	// This is a project name, describe it, and
	// return the project-id.
	return dxfuse.DxFindProject(dxEnv, projectIdOrName)
}

func initLog() *os.File {
	// Redirect the log output to a file
	f, err := os.OpenFile(dxfuse.LogFile, os.O_RDWR | os.O_CREATE | os.O_APPEND | os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	log.SetOutput(f)
	log.SetFlags(0)
	log.SetPrefix(progName + ": ")

	return f
}

func main() {
	flag.Usage = usage
	flag.Parse()

	if *version {
		// print the version and exit
		fmt.Println(dxfuse.Version)
		os.Exit(0)
	}
	if *help {
		usage()
		os.Exit(0)
	}

	numArgs := flag.NArg()
	if numArgs < 2 {
		usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)

	options := dxfuse.Options {
		DebugFuse: *debugFuseFlag,
		Verbose : *verbose > 0,
		VerboseLevel : *verbose,
		Uid : *uid,
		Gid : *gid,
	}

	dxEnv, _, err := dxda.GetDxEnvironment()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if dxEnv.DxJobId == "" {
		fmt.Println(`
Warning: running outside a worker. Dxfuse is currently engineered to
operate inside a cloud worker. The system depends on a good network
connection to the DNAnexus servers, and to the backing store, which is
S3 or Azure. Without such connectivity, some operations may take a
long time, causing operating system timeouts to expire. This can
result in the filesystem freezing, or being unmounted.`)
	}

	// initialize the log file
	logf := initLog()
	defer logf.Close()

	// distinguish between the case of a manifest, and a list of projects.
	var manifest *dxfuse.Manifest
	if numArgs == 2 && strings.HasSuffix(flag.Arg(1), ".json") {
		p := flag.Arg(1)
		log.Printf("Provided with a manifest, reading from %s", p)
		manifest, err = dxfuse.ReadManifest(p)
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

		manifest, err = dxfuse.MakeManifestFromProjectIds(dxEnv, projectIds)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	if err := dxfuse.Mount(mountpoint, dxEnv, *manifest, options); err != nil {
		fmt.Println("Error: " + err.Error())
	}
}
