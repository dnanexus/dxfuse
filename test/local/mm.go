package main

import (
	"flag"
	"fmt"
	"golang.org/x/exp/mmap"
	"os"
	"path/filepath"
)

type Config struct {
	filename string
	verbose  bool
}

var progName = filepath.Base(os.Args[0])

func usage() {
	fmt.Fprintf(os.Stderr, "usage:\n")
	fmt.Fprintf(os.Stderr, "    %s filename\n", progName)
	flag.PrintDefaults()
}

var (
	help    = flag.Bool("help", false, "display program options")
	verbose = flag.Bool("verbose", false, "Enable verbose debugging")
)

func parseCmdLineArgs() Config {
	if *help {
		usage()
		os.Exit(0)
	}

	numArgs := flag.NArg()
	if numArgs > 1 {
		usage()
		os.Exit(2)
	}

	return Config{
		filename: flag.Arg(0),
		verbose:  *verbose,
	}
}

func main() {
	// parse command line options
	flag.Usage = usage
	flag.Parse()
	cfg := parseCmdLineArgs()

	reader, err := mmap.Open(cfg.filename)
	if err != nil {
		fmt.Printf("Error mmaping file %s (%s)\n", cfg.filename, err.Error())
		os.Exit(1)
	}
	defer reader.Close()

	// read the data
	dataLen := reader.Len()
	buf := make([]byte, dataLen)
	recvLen, err := reader.ReadAt(buf, 0)
	if err != nil {
		fmt.Printf("Error reading mmaped file %s (%s)\n", cfg.filename, err.Error())
		os.Exit(1)
	}
	if recvLen != dataLen {
		fmt.Printf("recvLen != dataLen (%d, %d)\n", recvLen, dataLen)
		os.Exit(1)
	}
	fmt.Printf("content=%s\n", buf)
}
