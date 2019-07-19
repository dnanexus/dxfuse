package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	// The dxda package has the get-environment code
	"github.com/dnanexus/dxda"
	"github.com/dnanexus/dxfuse"
)

var progName = filepath.Base(os.Args[0])

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", progName)
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", progName)
	flag.PrintDefaults()
}

func main() {
	log.SetFlags(0)
	log.SetPrefix(progName + ": ")

	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 1 {
		usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)

	dxEnv, _, err := dxda.GetDxEnvironment()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// See that we can describe a file
	//testFileDescribe(&dxEnv)

	if err := dxfuse.Mount(mountpoint, dxEnv); err != nil {
		log.Fatal(err)
	}
}

func testFileDescribe(dxEnv *dxda.DXEnvironment) {
	desc, err := dxfuse.Describe(dxEnv, "project-FGpfqjQ0ffPF1Q106JYP2j3v", "file-FJ1qyg80ffP9v6gVPxKz9pQ7")
	if desc == nil {
		fmt.Printf("The description is empty\n")
		fmt.Printf(err.Error() + "\n")
	} else {
		fmt.Printf("%v\n", *desc)
	}
}
