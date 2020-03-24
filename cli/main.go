package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"

	// The dxda package has the get-environment code
	"github.com/dnanexus/dxda"
	"github.com/dnanexus/dxfuse"
)

type Config struct {
	mountpoint string
	dxEnv dxda.DXEnvironment
	options dxfuse.Options
}

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
	fsSync = flag.Bool("sync", false, "Sychronize the filesystem and exit")
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
	if strings.HasPrefix(projectIdOrName, "container-") {
		// This is a container ID
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
func fsDaemon(
	mountpoint string,
	dxEnv dxda.DXEnvironment,
	manifest dxfuse.Manifest,
	options dxfuse.Options,
	logf *os.File,
	logger *log.Logger) error {

	fsys, err := dxfuse.NewDxfuse(dxEnv, manifest, options)
	if err != nil {
		return err
	}
	server := fuseutil.NewFileSystemServer(fsys)

	logger.Printf("starting fsDaemon")
	mountOptions := make(map[string]string)

	// Allow users other than root access the filesystem
	mountOptions["allow_other"] = ""

	// capture debug output from the FUSE subsystem
	var fuse_logger *log.Logger
	if *debugFuseFlag {
		fuse_logger = log.New(logf, "fuse_debug: ", log.Flags())
	}

	logger.Printf("building config")

	// Fuse mount
	cfg := &fuse.MountConfig{
		FSName : "dxfuse",
		ErrorLogger : logger,
		DebugLogger : fuse_logger,

		// This option makes writes accumulate in the kernel
		// buffers before being handed over to dxfuse for processing.
		// It significantly improves file-write performance. Still,
		// what you get isn't great.
		//
		// Currently, instead of dxfuse receiving every 4KB synchronously,
		// it can get 128KB.
		DisableWritebackCaching : false,
		Options : mountOptions,
	}

	logger.Printf("mounting-dxfuse")
	mfs, err := fuse.Mount(mountpoint, server, cfg)
	if err != nil {
		logger.Printf(err.Error())
	}

	// Wait for it to be unmounted. This happens only after
	// all requests have been served.
	if err = mfs.Join(context.Background()); err != nil {
		logger.Fatalf("Join: %v", err)
	}

	logger.Printf("done")

	// shutdown the filesystem
	fsys.Shutdown()

	return nil
}

func waitForReady() string {
	for i := 0; i < 10; i++ {
		time.Sleep(1 * time.Second)

		// read the log file and look for either "ready" or "error"
		data, err := ioutil.ReadFile(dxfuse.LogFile)
		if err != nil {
			continue
		}
		content := string(data)
		if (strings.Contains(content, "mounting-dxfuse")) {
			return "ready"
		}
		if (strings.Contains(content, "error")) {
			return "error"
		}
	}

	// The filesystem failed to start for some reason.
	return "error"
}

func parseCmdLineArgs() Config {
	if *version {
		// print the version and exit
		fmt.Println(dxfuse.Version)
		os.Exit(0)
	}
	if *fsSync {
		cmdClient := dxfuse.NewCmdClient()
		cmdClient.Sync()
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

	options := dxfuse.Options{
		ReadOnly: *readOnly,
		Verbose : *verbose > 0,
		VerboseLevel : *verbose,
		Uid : uint32(*uid),
		Gid : uint32(*gid),
	}

	dxEnv, _, err := dxda.GetDxEnvironment()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	return Config{
		mountpoint : mountpoint,
		dxEnv : dxEnv,
		options : options,
	}
}

func validateConfig(cfg Config) {
	fileInfo, err := os.Stat(cfg.mountpoint)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("error, mountpoint %s does not exist\n", cfg.mountpoint)
			os.Exit(1)
		}
		fmt.Printf("error opening mountpoint %s %s", cfg.mountpoint, err.Error())
	}
	if !fileInfo.IsDir() {
		fmt.Printf("error, mountpoint %s is not a directory\n", cfg.mountpoint)
		os.Exit(1)
	}
}

func parseManifest(cfg Config) (*dxfuse.Manifest, error) {
	numArgs := flag.NArg()

	// distinguish between the case of a manifest, and a list of projects.
	if numArgs == 2 && strings.HasSuffix(flag.Arg(1), ".json") {
		p := flag.Arg(1)
		fmt.Printf("Provided with a manifest, reading from %s\n", p)
		manifest, err := dxfuse.ReadManifest(p)
		if err != nil {
			return nil, err
		}
		if err := manifest.FillInMissingFields(context.TODO(), cfg.dxEnv); err != nil {
			return nil, err
		}
		return manifest, nil
	} else {
		// process the project inputs, and convert to an array of verified
		// project IDs
		var projectIds []string
		for i := 1; i < numArgs; i++ {
			projectIdOrName := flag.Arg(i)
			projId, err := lookupProject(&cfg.dxEnv, projectIdOrName)
			if err != nil {
				return nil, err
			}
			if projId == "" {
				return nil, fmt.Errorf("no project with name %s", projectIdOrName)
			}
			projectIds = append(projectIds, projId)
		}

		manifest, err := dxfuse.MakeManifestFromProjectIds(context.TODO(), cfg.dxEnv, projectIds)
		if err != nil {
			return nil, err
		}
		return manifest, nil
	}
}


func startDaemon(cfg Config) {
	// initialize the log file
	logf := initLog()
	logger := log.New(logf, "dxfuse: ", log.Flags())

	// Read the manifest from disk. It should already have all the fields
	// filled in. There is no need to perform API calls.
	p := flag.Arg(1)
	manifest, err := dxfuse.ReadManifest(p)
	if err != nil {
		logger.Printf("Error reading manifest (%s)", err.Error())
		os.Exit(1)
	}

	err = fsDaemon(cfg.mountpoint, cfg.dxEnv, *manifest, cfg.options, logf, logger)
	if err != nil {
		logger.Printf(err.Error())
		os.Exit(1)
	}
}

// check if the variable ACTUAL is set to 1.
// This means that we need to run the filesystem inside this process
func isActual() bool {
	environment := os.Environ()
	for _,kv := range(environment) {
		if kv == "ACTUAL=1" {
			return true
		}
	}
	return false
}

// build the command line for the daemon process
func buildDaemonCommandLine(cfg Config, fullManifestPath string) []string {
	daemonArgs := []string{ cfg.mountpoint, fullManifestPath }

	if (*debugFuseFlag) {
		daemonArgs = append(daemonArgs, "-debugFuse")
	}
	if (*fsSync) {
		daemonArgs = append(daemonArgs, "-sync")
	}

	// add the user's Unix id and group
	uidX,gidX := initUidGid(*uid, *gid)
	uidArgs := []string{ "-uid", string(uidX), "-gid", string(gidX) }
	daemonArgs = append(daemonArgs, uidArgs...)

	if (*readOnly) {
		daemonArgs = append(daemonArgs, "-readOnly")
	}
	if (*verbose > 0) {
		verboseArgs := []string { "-verbose", string(*verbose) }
		daemonArgs = append(daemonArgs, verboseArgs...)
	}
	fmt.Printf("args=%v\n", daemonArgs)
	return daemonArgs
}


func startDaemonAndWaitForInitializationToComplete(cfg Config) {
	if isActual() {
		// This will be true -only- in the child sub-process
		startDaemon(cfg)
		return
	}

	// We are in the parent process.
	//
	manifest, err := parseManifest(cfg)
	if err != nil {
		fmt.Printf(err.Error())
		os.Exit(1)
	}

	// write the manifest to a new file
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		fmt.Printf("Error marshalling new manifest (%s)", err.Error())
		os.Exit(1)
	}

	// This could be converted into a random temporary file to avoid collisions
	fullManifestPath := "/tmp/dxfuse_manifest.json"
	err = ioutil.WriteFile(fullManifestPath, manifestJSON, 0644)
	if err != nil {
		fmt.Printf("Error writing out fully elaborated manifest to %s (%s)",
			fullManifestPath, err.Error())
		os.Exit(1)
	}

	// Mount in a subprocess, and wait for the filesystem to start.
	// If there is an error, report it. Otherwise, return after the filesystem
	// is mounted and useable.
	//
	progPath, err := exec.LookPath(os.Args[0])
	if err != nil {
		fmt.Printf("Error: couldn't find program %s", os.Args[0])
		os.Exit(1)
	}

	// build the command line arguments for the daemon
	daemonArgs := buildDaemonCommandLine(cfg, fullManifestPath)
	mountCmd := exec.Command(progPath, daemonArgs...)
	mountCmd.Env = append(os.Environ(), "ACTUAL=1")

	// Start the command.
	fmt.Println("starting fs daemon")
	if err := mountCmd.Start(); err != nil {
		fmt.Printf("failed to start filesystem daemon: %v\n", err)
		os.Exit(1)
	}

	// Wait for the tool to say the file system is ready.
	fmt.Println("wait for ready")
	status := waitForReady()

	if status == "error" {
		fmt.Printf(
			"There was an error starting the daemon, take a look at the log file (%s) for more information\n",
			dxfuse.LogFile)
		os.Exit(1)
	}
	fmt.Println("Daemon started successfully")
}

func main() {
	// parse command line options
	flag.Usage = usage
	flag.Parse()
	cfg := parseCmdLineArgs()
	validateConfig(cfg)

	startDaemonAndWaitForInitializationToComplete(cfg)
}
