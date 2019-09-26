package dxfuse_test

import (
	"fmt"
	"testing"

	"github.com/dnanexus/dxfuse"
)

func TestDirSkel(t *testing.T) {
	t.Log("starting test")
	fmt.Fprintln(os.Stdout, "hello")

	mf1 := dxfuse.ManifestFile{
		ProjId  : "project-1000",
		FileId  : "file-1000",
		Parent  : "/A/B/C",
		Fname   : "foo.txt",
	}
	mf2 := dxfuse.ManifestFile{
		ProjId  : "project-1000",
		FileId  : "file-1000",
		Parent  : "/Cards/Poker",
		Fname   : "A.txt",
	}
	md1 := ManifestDir {
		ProjId  : "project-2001",
		Folder  : "/hello",
		Dirname : "/Words",
		MtimeMillisec : 0,
		CtimeMillisec : 0,
	}
	files := []ManifestFile {mf1, mf2}
	dirs := []MainfestDir {md1}
	m := Manifest{
		Files :       files,
		Directories : dirs,
	}

	t.Log("before dir skeleton")
	skel := m.DirSkeleton()

	fmt.Fprintln(os.Stdout, "%v", skel)
}
