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
	//mountpoint := flag.Arg(0)

	dxEnv, _, err := dxda.GetDxEnvironment()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	desc, err := dxfuse.Describe(&dxEnv, "project-FGpfqjQ0ffPF1Q106JYP2j3v", "file-FJ1qyg80ffP9v6gVPxKz9pQ7")
	/*fmt.Printf("proj=%s  fileId=%s  name=%s  folder=%s size=%d created=%s\n", desc->ProjId,
		desc->FileId,
		desc->Name,
		desc->Folder,
		desc->Size,
		desc->Create)*/
	if desc == nil {
		fmt.Printf("The description is empty\n")
		fmt.Printf(err.Error() + "\n")
	} else {
		fmt.Printf("%v\n", *desc)
	}
		/*
	if err := Mount(mountpoint, dxEnv); err != nil {
		log.Fatal(err)
	}*/
}
