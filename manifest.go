package dxfs2

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sort"
	"strings"

	"github.com/dnanexus/dxda"
)

type ManifestFile struct {
	ProjId  string `json:"proj_id"`
	FileId  string `json:"file_id"`
	Parent  string `json:"parent"`
	Fname   string `json:"fname"`
}

type ManifestDir struct {
	ProjId        string `json:"proj_id"`
	Folder        string `json:"folder"`
	Dirname       string `json:"dirname"`
	MtimeMillisec int64  `json:"-"`
	CtimeMillisec int64  `json:"-"`
}

type Manifest struct {
	Files        []ManifestFile  `json:"files"`
	Directories  []ManifestDir   `json:"directories"`
}


func validate(manifest Manifest) error {
	for _, d := range manifest.Directories {
		if !strings.HasPrefix(d.ProjId, "project-") {
			return fmt.Errorf("project has invalid ID %s", d.ProjId)
		}
		dirNameLen := len(d.Dirname)
		switch dirNameLen {
		case 0:
			return fmt.Errorf("the directory cannot be empty %v", d)
		default:
			if d.Dirname[0] != '/' {
				return fmt.Errorf("the directory name must start with a slash %v", d)
			}
		}
	}

	return nil
}

// read the manifest from a file into a memory structure
func ReadManifest(fname string) (*Manifest, error) {
	srcData, err := ioutil.ReadFile(fname)
	if err != nil {
		panic(err)
	}
	br := bytes.NewReader(srcData)
	data, err := ioutil.ReadAll(br)
	if err != nil {
		panic(err)
	}

	var m Manifest
	json.Unmarshal(data, &m)
	if err := validate(m); err != nil {
		return nil, err
	}
	return &m, nil
}


func MakeManifestFromProjectIds(
	dxEnv dxda.DXEnvironment,
	projectIds []string) (*Manifest, error) {
	// describe the projects, retrieve metadata for them
	tmpHttpClient := dxda.NewHttpClient(false)
	projDescs := make(map[string]DxDescribePrj)
	for _, pid := range projectIds {
		pDesc, err := DxDescribeProject(tmpHttpClient, &dxEnv, pid)
		if err != nil {
			return nil, err
		}
		projDescs[pDesc.Id] = *pDesc
	}

	// validate that the projects have good names
	for _, pDesc := range projDescs {
		if !FilenameIsPosixCompliant(pDesc.Name) {
			err := errors.New(
				fmt.Sprintf("Project %s has a non posix compliant name (%s)",
					pDesc.Id, pDesc.Name))
			return nil, err
		}
	}

	dirs := make([]ManifestDir, 0)
	for _, pDesc := range projDescs {
		mstDir := ManifestDir{
			ProjId : pDesc.Id,
			Folder : "/",
			Dirname : filepath.Clean("/" + pDesc.Name),
			CtimeMillisec : pDesc.CtimeMillisec,
			MtimeMillisec : pDesc.MtimeMillisec,
		}
		dirs = append(dirs, mstDir)
	}

	var emptyFiles []ManifestFile
	manifest := &Manifest{
		Files : emptyFiles,
		Directories : dirs,
	}

	if err := validate(*manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}


// return all the parents of a directory.
// For example:
//   "/A/B/C"      ["/", "/A", "/A/B"]
//   "/foo/bar"   ["/", "/foo"]
func ancestors(p string) []string {
	if p == "" || p == "/" {
		return []string{"/"}
	}
	parent, dirname := filepath.Split(p)
	if parent == "/" {
		return []string{"/", p}
	}
	ators := ancestors(parent)
	if len(ators) == 0 {
		panic(fmt.Sprintf("cannot create ancestor list for path %s", p))
	}
	longest := ators[len(ators) - 1]
	return append(ators, longest + "/" + dirname)
}

// We need all of this, to be able to sort the list of directories according
// to the number of parts they have.
// for example
//    ["/Alpha/Beta", "/A/B/C", "/D", "/E", "/Alpha"]
// Will be sorted into:
//    ["/Alpha", "/D", "/E", "/Alpha/Beta", "/A/B/C"]

type Dirs struct {
	elems []*string
}
func (d Dirs) Len() int { return len(d.elems) }
func (d Dirs) Swap(i, j int) { d.elems[i], d.elems[j] = d.elems[j], d.elems[i] }
func (d Dirs) Less(i, j int) bool {
	nI := strings.Count(*d.elems[i], "/")
	nJ := strings.Count(*d.elems[j], "/")
	return nI < nJ
}


// Figure out the directory structure needed to support
// the leaf nodes. For example, if we need to create:
//     ["/A/B/C", "/D", "/D/E"]
// then the skeleton is:
//     ["/A", "/A/B", "/D"]
//
func (m *Manifest) DirSkeleton() []string {
	tree := make(map[string]bool)

	// record all the parents
	for _, file := range m.Files {
		dirpath := file.Parent
		for _, p := range (ancestors(dirpath)) {
			tree[p] = true
		}
	}
	for _, file := range m.Directories {
		dirParent, _ := filepath.Split(file.Dirname)
		for _, p := range (ancestors(dirParent)) {
			tree[p] = true
		}
	}

	// sort the elements from the bottom of the tree, to its branches.
	//
	// for example, ["/A/B/C/D", "/A/B", "/A/B/C", "/D", "/E", "/A"] ->
	// ["/A", "/D", "/E", "/A/B", "/A/B/C", "/A/B/C/D"]
	//
	var emptyStringArray []*string
	de := Dirs{
		elems : emptyStringArray,
	}
	for p, _ := range tree {
		de.elems = append(de.elems, &p)
	}
	sort.Sort(de)

	// return plain strings, instead of pointers to strings
	// do not include the root, because it already exists.
	var retval []string
	for _, e := range(de.elems) {
		if *e != "/" {
			retval = append(retval, *e)
		}
	}
	return retval
}
