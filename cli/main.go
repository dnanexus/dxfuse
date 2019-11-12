package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"

	// The dxda package has the get-environment code
	"github.com/dnanexus/dxda"
	"github.com/dnanexus/dxfuse"
)

var progName = filepath.Base(os.Args[0])

func usage() {
	fmt.Fprintf(os.Stderr, "usage:\n")
	fmt.Fprintf(os.Stderr, "    %s [options] MOUNTPOINT PROJECT1 PROJECT2 ...\n", progName)
	fmt.Fprintf(os.Stderr, "    %s [options] MOUNTPOINT manifest.json\n", progName)
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
	readOnly = flag.Bool("readOnly", false, "mount the filesystem in read-only mode")
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
	return dxfuse.DxFindProject(context.TODO(), dxEnv, projectIdOrName)
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

func initUidGid(uid int, gid int) (uint32,uint32) {
	// This is current the root user, because the program is run under
	// sudo privileges. The "user" variable is used only if we don't
	// get command line uid/gid.
	user, err := user.Current()
	if err != nil {
		panic(err)
	}

	// get the user ID
	if uid == -1 {
		var err error
		uid, err = strconv.Atoi(user.Uid)
		if err != nil {
			panic(err)
		}
	}

	// get the group ID
	if gid == -1 {
		var err error
		gid, err = strconv.Atoi(user.Gid)
		if err != nil {
			panic(err)
		}
	}
	return uint32(uid),uint32(gid)
}

// Mount the filesystem:
//  - setup the debug log to the FUSE kernel log (I think)
//  - mount as read-only
func mount(
	mountpoint string,
	dxEnv dxda.DXEnvironment,
	manifest dxfuse.Manifest,
	options dxfuse.Options) error {

	fsys, err := dxfuse.NewDxfuse(dxEnv, manifest, options)
	if err != nil {
		return err
	}
	server := fuseutil.NewFileSystemServer(fsys)

	// initialize the log file
	logf := initLog()
	defer logf.Close()
	logger := log.New(logf, "dxfuse: ", log.Flags())

	mountOptions := make(map[string]string)

	// Allow users other than root access the filesystem
	mountOptions["allow_other"] = ""

	// capture debug output from the FUSE subsystem
	var fuse_logger *log.Logger
	if *debugFuseFlag {
		fuse_logger = log.New(logf, "fuse_debug: ", log.Flags())
	}

	// Fuse mount
	cfg := &fuse.MountConfig{
		FSName : "dxfuse",
		ErrorLogger : logger,
		DebugLogger : fuse_logger,
		DisableWritebackCaching : true,
		Options : mountOptions,
	}

	log.Printf("mounting dxfuse")
	mfs, err := fuse.Mount(mountpoint, server, cfg)
	if err != nil {
		log.Fatalf("Mount: %v", err)
	}

	// Wait for it to be unmounted. This happens only after
	// all requests have been served.
	if err = mfs.Join(context.Background()); err != nil {
		log.Fatalf("Join: %v", err)
	}

	// shutdown the filesystem
	fsys.Shutdown()

	return nil
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

	uid,gid := initUidGid(*uid, *gid)

	options := dxfuse.Options {
		ReadOnly: *readOnly,
		Verbose : *verbose > 0,
		VerboseLevel : *verbose,
		Uid : uid,
		Gid : gid,
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
		if err := manifest.FillInMissingFields(context.TODO(), dxEnv); err != nil {
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

		manifest, err = dxfuse.MakeManifestFromProjectIds(context.TODO(), dxEnv, projectIds)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	err = mount(mountpoint, dxEnv, *manifest, options)
	if err != nil {
		fmt.Println("Error: " + err.Error())
	}
}
