package dxfuse

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	// The dxda package has the get-environment code
	"github.com/dnanexus/dxda"
)

// Limit on the number of objects that the bulk-describe API can take
const (
	maxNumObjectsInDescribe = 1000
)

// -------------------------------------------------------------------
// Description of a DNAx data object
type DxDataObjectDescription struct {
	Id            string
	ProjId        string
	Name          string
	State         string
	ArchivalState string
	Folder        string
	Size          int64
	CtimeSeconds  int64
	MtimeSeconds  int64
	Tags          []string
	Properties    map[string]string
}

// https://documentation.dnanexus.com/developer/api/data-containers/projects#api-method-project-xxxx-describe
type FileUploadParameters struct {
	MinimumPartSize      int64 `json:"minimumPartSize"`
	MaximumPartSize      int64 `json:"maximumPartSize"`
	EmptyLastPartAllowed bool  `json:"emptyLastPartAllowed"`
	MaximumNumParts      int64 `json:"maximumNumParts"`
	MaximumFileSize      int64 `json:"maximumFileSize"`
}

type DxProjectDescription struct {
	Id           string
	Name         string
	Region       string
	Version      int
	DataUsageGiB float64
	CtimeSeconds int64
	MtimeSeconds int64
	UploadParams FileUploadParameters
	Level        int // one of VIEW, UPLOAD, CONTRIBUTE, ADMINISTER
}

// a DNAx directory. It holds files and sub-directories.
type DxFolder struct {
	path        string // Full directory name, for example: { "/A/B/C", "foo/bar/baz" }
	dataObjects map[string]DxDataObjectDescription
	subdirs     []string
}

// -------------------------------------------------------------------

type RequestWithScope struct {
	Objects         []string                   `json:"id"`
	Scope           map[string]string          `json:"scope"`
	DescribeOptions map[string]map[string]bool `json:"describe"`
}

type DxFindDataObjectsRequest struct {
	Objects         []string                   `json:"id"`
	DescribeOptions map[string]map[string]bool `json:"describe"`
}

type DxFindDataObjectsReply struct {
	Results []DxDescribeRawTop `json:"results"`
}

type DxDescribeRawTop struct {
	Describe DxDescribeRaw `json:"describe"`
}

type DxDescribeRaw struct {
	Id               string            `json:"id"`
	ProjId           string            `json:"project"`
	Name             string            `json:"name"`
	State            string            `json:"state"`
	ArchivalState    string            `json:"archivalState"`
	Folder           string            `json:"folder"`
	CreatedMillisec  int64             `json:"created"`
	ModifiedMillisec int64             `json:"modified"`
	Size             int64             `json:"size"`
	Tags             []string          `json:"tags"`
	Properties       map[string]string `json:"properties"`
}

// Describe a large number of file-ids in one API call.
func submit(
	ctx context.Context,
	httpClient *http.Client,
	dxEnv *dxda.DXEnvironment,
	projectId string,
	fileIds []string) (map[string]DxDataObjectDescription, error) {

	// Limit the number of fields returned, because by default we
	// get too much information, which is a burden on the server side.

	describeOptions := map[string]map[string]bool{
		"fields": {
			"id":            true,
			"project":       true,
			"name":          true,
			"state":         true,
			"archivalState": true,
			"folder":        true,
			"created":       true,
			"modified":      true,
			"size":          true,
			"tags":          true,
			"properties":    true,
		},
	}

	var payload []byte
	var err error

	// If given a valid project or container provide the scope parameter to reduce load on the backend
	if validProject(projectId) {
		scope := map[string]string{
			"project": projectId,
		}
		request := RequestWithScope{
			Objects:         fileIds,
			Scope:           scope,
			DescribeOptions: describeOptions,
		}
		payload, err = json.Marshal(request)
		if err != nil {
			return nil, err
		}
	} else {
		request := DxFindDataObjectsRequest{
			Objects:         fileIds,
			DescribeOptions: describeOptions,
		}
		payload, err = json.Marshal(request)
		if err != nil {
			return nil, err
		}
	}

	//log.Printf("payload = %s", string(payload))

	repJs, err := dxda.DxAPI(ctx, httpClient, NumRetriesDefault, dxEnv, "system/findDataObjects", string(payload))
	if err != nil {
		return nil, err
	}
	var reply DxFindDataObjectsReply
	err = json.Unmarshal(repJs, &reply)
	if err != nil {
		return nil, err
	}

	var files = make(map[string]DxDataObjectDescription)
	for _, descRawTop := range reply.Results {
		descRaw := descRawTop.Describe

		desc := DxDataObjectDescription{
			Id:            descRaw.Id,
			ProjId:        descRaw.ProjId,
			Name:          descRaw.Name,
			State:         descRaw.State,
			ArchivalState: descRaw.ArchivalState,
			Folder:        descRaw.Folder,
			Size:          descRaw.Size,
			CtimeSeconds:  descRaw.CreatedMillisec / 1000,
			MtimeSeconds:  descRaw.ModifiedMillisec / 1000,
			Tags:          descRaw.Tags,
			Properties:    descRaw.Properties,
		}
		//fmt.Printf("%v\n", desc)
		files[desc.Id] = desc
	}
	return files, nil
}

func DxDescribeBulkObjects(
	ctx context.Context,
	httpClient *http.Client,
	dxEnv *dxda.DXEnvironment,
	projectId string,
	objIds []string) (map[string]DxDataObjectDescription, error) {

	var gMap = make(map[string]DxDataObjectDescription)
	if len(objIds) == 0 {
		return gMap, nil
	}

	// split into limited batches
	batchSize := maxNumObjectsInDescribe
	var batches [][]string

	for batchSize < len(objIds) {
		head := objIds[0:batchSize:batchSize]
		objIds = objIds[batchSize:]
		batches = append(batches, head)
	}
	// Don't forget the tail of the requests, that is smaller than the batch size
	batches = append(batches, objIds)
	for _, objIdBatch := range batches {
		m, err := submit(ctx, httpClient, dxEnv, projectId, objIdBatch)
		if err != nil {
			return nil, err
		}

		// add the results to the total result map
		for key, value := range m {
			gMap[key] = value
		}
	}
	return gMap, nil
}

type ListFolderRequest struct {
	Folder        string `json:"folder"`
	Only          string `json:"only"`
	IncludeHidden bool   `json:"includeHidden"`
}

type ListFolderResponse struct {
	Objects []ObjInfo `json:"objects"`
	Folders []string  `json:"folders"`
}

type ObjInfo struct {
	Id string `json:"id"`
}

type DxListFolder struct {
	objIds  []string
	subdirs []string
}

// Issue a /project-xxxx/listFolder API call. Get
// back a list of object-ids and sub-directories.
func listFolder(
	ctx context.Context,
	httpClient *http.Client,
	dxEnv *dxda.DXEnvironment,
	projectId string,
	dir string) (*DxListFolder, error) {

	request := ListFolderRequest{
		Folder:        dir,
		Only:          "all",
		IncludeHidden: false,
	}
	var payload []byte
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	dxRequest := fmt.Sprintf("%s/listFolder", projectId)
	repJs, err := dxda.DxAPI(ctx, httpClient, NumRetriesDefault, dxEnv, dxRequest, string(payload))
	if err != nil {
		return nil, err
	}
	var reply ListFolderResponse
	if err := json.Unmarshal(repJs, &reply); err != nil {
		return nil, err
	}
	var objIds []string
	for _, objInfo := range reply.Objects {
		objIds = append(objIds, objInfo.Id)
	}
	retval := DxListFolder{
		objIds:  objIds,
		subdirs: reply.Folders,
	}
	return &retval, nil
}

func DxDescribeFolder(
	ctx context.Context,
	httpClient *http.Client,
	dxEnv *dxda.DXEnvironment,
	projectId string,
	folder string) (*DxFolder, error) {
	// The listFolder API call returns a list of object ids and folders.
	// We could describe the objects right here, but we do that separately.
	folderInfo, err := listFolder(ctx, httpClient, dxEnv, projectId, folder)
	if err != nil {
		log.Printf("listFolder(%s) error %s", folder, err.Error())
		return nil, err
	}
	// limit the number of directory elements
	numElementsInDir := len(folderInfo.objIds)
	if numElementsInDir > MaxDirSize {
		return nil, fmt.Errorf(
			"Too many elements (%d) in a directory, the limit is %d",
			numElementsInDir, MaxDirSize)
	}
	dxObjs, err := DxDescribeBulkObjects(ctx, httpClient, dxEnv, projectId, folderInfo.objIds)
	if err != nil {
		log.Printf("describeBulkObjects(%v) error %s", folderInfo.objIds, err.Error())
		return nil, err
	}
	dataObjects := make(map[string]DxDataObjectDescription)
	for _, oDesc := range dxObjs {
		dataObjects[oDesc.Id] = oDesc
	}
	return &DxFolder{
		path:        folder,
		dataObjects: dataObjects,
		subdirs:     folderInfo.subdirs,
	}, nil
}

type DxDescribeProjectRequest struct {
	Fields map[string]bool `json:"fields"`
}

type DxDescribeProjectReply struct {
	Id               string               `json:"id"`
	Name             string               `json:"name"`
	Region           string               `json:"region"`
	Version          int                  `json:"version"`
	DataUsage        float64              `json:"dataUsage"`
	CreatedMillisec  int64                `json:"created"`
	ModifiedMillisec int64                `json:"modified"`
	UploadParams     FileUploadParameters `json:"fileUploadParameters"`
	Level            string               `json:"level"`
}

func projectPermissionsToInt(perm string) int {
	switch perm {
	case "VIEW":
		return PERM_VIEW
	case "UPLOAD":
		return PERM_UPLOAD
	case "CONTRIBUTE":
		return PERM_CONTRIBUTE
	case "ADMINISTER":
		return PERM_ADMINISTER
	}

	log.Panicf("Unknown project permission %s", perm)
	return 0
}

func DxDescribeProject(
	ctx context.Context,
	httpClient *http.Client,
	dxEnv *dxda.DXEnvironment,
	projectId string) (*DxProjectDescription, error) {

	var request DxDescribeProjectRequest
	request.Fields = map[string]bool{
		"id":                   true,
		"name":                 true,
		"region":               true,
		"version":              true,
		"dataUsage":            true,
		"created":              true,
		"modified":             true,
		"fileUploadParameters": true,
		"level":                true,
	}
	var payload []byte
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	dxRequest := fmt.Sprintf("%s/describe", projectId)
	repJs, err := dxda.DxAPI(ctx, httpClient, NumRetriesDefault, dxEnv, dxRequest, string(payload))
	if err != nil {
		return nil, err
	}

	var reply DxDescribeProjectReply
	if err := json.Unmarshal(repJs, &reply); err != nil {
		return nil, err
	}

	prj := DxProjectDescription{
		Id:           reply.Id,
		Name:         reply.Name,
		Region:       reply.Region,
		Version:      reply.Version,
		DataUsageGiB: reply.DataUsage,
		CtimeSeconds: reply.CreatedMillisec / 1000,
		MtimeSeconds: reply.ModifiedMillisec / 1000,
		UploadParams: reply.UploadParams,
		Level:        projectPermissionsToInt(reply.Level),
	}
	return &prj, nil
}

// Describe just one object. Retrieve state even if the object is not closed.
func DxDescribe(
	ctx context.Context,
	httpClient *http.Client,
	dxEnv *dxda.DXEnvironment,
	projectId string,
	objId string) (DxDataObjectDescription, error) {
	var objectIds []string
	objectIds = append(objectIds, objId)
	m, err := DxDescribeBulkObjects(ctx, httpClient, dxEnv, projectId, objectIds)
	if err != nil {
		return DxDataObjectDescription{}, err
	}
	oDesc, ok := m[objId]
	if !ok {
		return DxDataObjectDescription{}, fmt.Errorf("Object %s not found", objId)
	}
	return oDesc, nil
}
