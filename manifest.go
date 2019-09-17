package dxfs2

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
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
		if strings.HasPrefix(d.ProjId, "project-") {
			return fmt.Errorf("project has invalid ID %s", d.ProjId)
		}
		dirNameLen := len(d.Dirname)
		switch dirNameLen {
		case 0:
			return fmt.Errorf("the directory cannot be empty %v", d)
		default:
			if strings.Contains(d.Dirname, "/") {
				return fmt.Errorf("the directory name cannot contain a slash %v", d)
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
			Dirname : pDesc.Name,
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
