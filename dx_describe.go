package dxfs2

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	// The dxda package has the get-environment code
	"github.com/dnanexus/dxda"
	"github.com/hashicorp/go-retryablehttp" // use http libraries from hashicorp for implement retry logic
)

// Limit on the number of objects that the bulk-describe API can take
const (
	maxNumObjectsInDescribe = 1000
)

// -------------------------------------------------------------------
// Description of a DNAx data object
type DxDescribeDataObject struct {
	FileId    string
	ProjId    string
	Name      string
	Folder    string
	Size      uint64
	Ctime     time.Time
	Mtime     time.Time
}

type DxDescribePrj struct {
	Id           string
	Name         string
	Region       string
	Version      int
	DataUsageGiB float64
	Ctime        time.Time
	Mtime        time.Time
}

// a DNAx directory. It holds files and sub-directories.
type DxFolder struct {
	path  string  // Full directory name, for example: { "/A/B/C", "foo/bar/baz" }
	files map[string]DxDescribeDataObject
	subdirs []string
}

// -------------------------------------------------------------------

type Request struct {
	Objects []string `json:"objects"`
	ClassDescribeOptions map[string]map[string]map[string]bool `json:"classDescribeOptions"`
}

type Reply struct {
	Results []DxDescribeRawTop `json:"results"`
}

type DxDescribeRawTop struct {
	Describe DxDescribeRaw `json:"describe"`
}

type DxDescribeRaw struct {
	FileId           string `json:"id"`
	ProjId           string `json:"project"`
	Name             string `json:"name"`
	State            string `json:"state"`
	Folder           string `json:"folder"`
	CreatedMillisec  int64 `json:"created"`
	ModifiedMillisec int64 `json:"modified"`
	Size             uint64 `json:"size"`
}

// convert time in milliseconds since 1970, in the equivalent
// golang structure
func dxTimeToUnixTime(dxTime int64) time.Time {
	sec := int64(dxTime/1000)
	millisec := int64(dxTime % 1000)
	return time.Unix(sec, millisec)
}


// Describe a large number of file-ids in one API call.
func submit(
	httpClient *retryablehttp.Client,
	dxEnv *dxda.DXEnvironment,
	fileIds []string) (map[string]DxDescribeDataObject, error) {

	// Limit the number of fields returned, because by default we
	// get too much information, which is a burden on the server side.
	describeOptions := map[string]map[string]map[string]bool {
		"file" : map[string]map[string]bool {
			"fields" : map[string]bool {
				"id" : true,
				"project" : true,
				"name" : true,
				"state" : true,
				"folder" : true,
				"created" : true,
				"modified" : true,
				"size" : true,
			},
		},
	}
	request := Request{
		Objects : fileIds,
		ClassDescribeOptions : describeOptions,
	}
	var payload []byte
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	//fmt.Printf("payload = %s", string(payload))

	repJs, err := dxda.DxAPI(httpClient, dxEnv, "system/describeDataObjects", string(payload))
	if err != nil {
		return nil, err
	}
	var reply Reply
	err = json.Unmarshal(repJs, &reply)
	if err != nil {
		return nil, err
	}

	var files = make(map[string]DxDescribeDataObject)
	for _, descRawTop := range(reply.Results) {
		descRaw := descRawTop.Describe
		if descRaw.State != "closed" {
			log.Printf("File %s is not closed, it is [" + descRaw.State + "], dropping")
			continue
		}
		desc := DxDescribeDataObject{
			FileId : descRaw.FileId,
			ProjId : descRaw.ProjId,
			Name : descRaw.Name,
			Folder : descRaw.Folder,
			Size : descRaw.Size,
			Ctime : dxTimeToUnixTime(descRaw.CreatedMillisec),
			Mtime : dxTimeToUnixTime(descRaw.ModifiedMillisec),
		}
		//fmt.Printf("%v\n", desc)
		files[desc.FileId] = desc
	}
	return files, nil
}

func DxDescribeBulkObjects(
	httpClient *retryablehttp.Client,
	dxEnv *dxda.DXEnvironment,
	fileIds []string) (map[string]DxDescribeDataObject, error) {
	var gMap = make(map[string]DxDescribeDataObject)
	if len(fileIds) == 0 {
		return gMap, nil
	}

	// split into limited batchs
	batchSize := maxNumObjectsInDescribe
	var batches [][]string

	for batchSize < len(fileIds) {
		head := fileIds[0:batchSize:batchSize]
		fileIds = fileIds[batchSize:]
		batches = append(batches, head)
	}
	// Don't forget the tail of the requests, that is smaller than the batch size
	batches = append(batches, fileIds)

	for _, fileIdBatch := range(batches) {
		m, err := submit(httpClient, dxEnv, fileIdBatch)
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
	Folder string `json:"folder"`
	Only   string `json:"only"`
	IncludeHidden bool `json:"includeHidden"`
}

type ListFolderResponse struct {
	Objects []ObjInfo  `json:"objects"`
	Folders []string   `json:"folders"`
}

type ObjInfo struct {
	Id string  `json:"id"`
}

type DxListFolder struct {
	fileIds  []string
	otherIds []string
	subdirs  []string
}

// Issue a /project-xxxx/listFolder API call. Get
// back a list of object-ids and sub-directories.
func listFolder(
	httpClient *retryablehttp.Client,
	dxEnv *dxda.DXEnvironment,
	projectId string,
	dir string) (*DxListFolder, error) {

	request := ListFolderRequest{
		Folder : dir,
		Only : "all",
		IncludeHidden : false,
	}
	var payload []byte
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	dxRequest := fmt.Sprintf("%s/listFolder", projectId)
	repJs, err := dxda.DxAPI(httpClient, dxEnv, dxRequest , string(payload))
	if err != nil {
		return nil, err
	}
	var reply ListFolderResponse
	if err := json.Unmarshal(repJs, &reply); err != nil {
		return nil, err
	}
	var objectIds []string
	var otherIds []string
	for _, objInfo := range reply.Objects {
		if strings.HasPrefix(objInfo.Id, "file-") {
			objectIds = append(objectIds, objInfo.Id)
		} else {
			otherIds = append(otherIds, objInfo.Id)
		}
	}
	retval := DxListFolder{
		fileIds : objectIds,
		otherIds : otherIds,
		subdirs : reply.Folders,
	}
	return &retval, nil
}


func DxDescribeFolder(
	httpClient *retryablehttp.Client,
	dxEnv *dxda.DXEnvironment,
	projectId string,
	folder string) (*DxFolder, error) {

	// The listFolder API call returns a list of object ids and folders.
	// We could describe the objects right here, but we do that separately.
	folderInfo, err := listFolder(httpClient, dxEnv, projectId, folder)
	if err != nil {
		log.Printf("listFolder(%s) error %s", folder, err.Error())
		return nil, err
	}
	files, err := DxDescribeBulkObjects(httpClient, dxEnv, folderInfo.fileIds)
	if err != nil {
		log.Printf("describeBulkObjects(%v) error %s", folderInfo.fileIds, err.Error())
		return nil, err
	}

	return &DxFolder{
		path : folder,
		files : files,
		subdirs : folderInfo.subdirs,
	}, nil
}

type RequestDescribeProject struct {
	Fields map[string]bool `json:"fields"`
}

type ReplyDescribeProject struct {
	Id               string `json:"id"`
	Name             string `json:"name"`
	Region           string `json:"region"`
	Version          int    `json:"version"`
	DataUsage        float64 `jdon:"dataUsage"`
	CreatedMillisec  int64 `json:"created"`
	ModifiedMillisec int64 `json:"modified"`
}

func DxDescribeProject(
	httpClient *retryablehttp.Client,
	dxEnv *dxda.DXEnvironment,
	projectId string) (*DxDescribePrj, error) {

	var request RequestDescribeProject
	request.Fields = map[string]bool {
		"id" : true,
		"name" : true,
		"region" : true,
		"version" : true,
		"dataUsage" : true,
		"created" : true,
		"modified" : true,
	}
	var payload []byte
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	dxRequest := fmt.Sprintf("%s/describe", projectId)
	repJs, err := dxda.DxAPI(httpClient, dxEnv, dxRequest, string(payload))
	if err != nil {
		return nil, err
	}

	var reply ReplyDescribeProject
	if err := json.Unmarshal(repJs, &reply); err != nil {
		return nil, err
	}

	prj := DxDescribePrj {
		Id :      reply.Id,
		Name :    reply.Name,
		Region :  reply.Region,
		Version : reply.Version,
		DataUsageGiB : reply.DataUsage,
		Ctime : dxTimeToUnixTime(reply.CreatedMillisec),
		Mtime : dxTimeToUnixTime(reply.ModifiedMillisec),
	}
	return &prj, nil
}
