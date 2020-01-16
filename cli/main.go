package main

import (
	"context"
	"flag"
	"fmt"
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
		DisableWritebackCaching : true,
		Options : mountOptions,
	}

	logger.Printf("mounting dxfuse")
	os.Stdout.WriteString("Ready")
	os.Stdout.Close()
	mfs, err := fuse.Mount(mountpoint, server, cfg)
	if err != nil {
		logger.Printf(err.Error())
	}

	// Wait for it to be unmounted. This happens only after
	// all requests have been served.
	if err = mfs.Join(context.Background()); err != nil {
		logger.Fatalf("Join: %v", err)
	}

	// shutdown the filesystem
	//fsys.Shutdown()

	return nil
}

func waitForReady(readyReader *os.File, c chan string) {
	status := make([]byte, 100)
	_, err := readyReader.Read(status)
	if err != nil {
		log.Printf("Reading from ready pipe: %v", err)
		return
	}
	c <- string(status)
}

func parseCmdLineArgs() Config {
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

	options := dxfuse.Options{
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

func parseManifest(cfg Config, logger *log.Logger) (*dxfuse.Manifest, error) {
	numArgs := flag.NArg()

	// distinguish between the case of a manifest, and a list of projects.
	if numArgs == 2 && strings.HasSuffix(flag.Arg(1), ".json") {
		p := flag.Arg(1)
		logger.Printf("Provided with a manifest, reading from %s", p)
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
	defer logf.Close()
	logger := log.New(logf, "dxfuse: ", log.Flags())

	manifest, err := parseManifest(cfg, logger)
	if err != nil {
		logger.Printf(err.Error())
		os.Exit(1)
	}
	logger.Printf("manifest = %v", manifest)
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


func startDaemonAndWaitForInitializationToComplete(cfg Config) {
	if isActual() {
		// This will be true -only- in the child sub-process
		startDaemon(cfg)
		return
	}

	// Set up a pipe for the "ready" status.
	errorReader, errorWriter, err := os.Pipe()
	if err != nil {
		fmt.Printf("Pipe: %v", err)
		os.Exit(1)
	}
	defer errorWriter.Close()
	defer errorReader.Close()

	// Mount in a subprocess, and wait for the filesystem to start.
	// If there is an error, report it. Otherwise, return after the filesystem
	// is mounted and useable.
	//
	progPath, err := exec.LookPath(os.Args[0])
	if err != nil {
		fmt.Printf("Error: couldn't find program %s", os.Args[0])
		os.Exit(1)
	}
	mountCmd := exec.Command(progPath, os.Args[1:]...)
	mountCmd.Stdout = errorWriter
	mountCmd.Env = append(os.Environ(), "ACTUAL=1")

	// Start the command.
	fmt.Println("starting fs daemon")
	if err := mountCmd.Start(); err != nil {
		fmt.Printf("failed to start filesystem daemon: %v\n", err)
		os.Exit(1)
	}

	// Wait for the tool to say the file system is ready.
	fmt.Println("wait for ready")

	// This is needed for older versions of mac?
	time.Sleep(1 * time.Second)

	readyChan := make(chan string, 1)
	go waitForReady(errorReader, readyChan)

	status := <-readyChan
	status = strings.ToLower(status)
	if !strings.HasPrefix(status, "ready") {
		fmt.Println("There was an error starting the daemon")
		fmt.Println(status)
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
