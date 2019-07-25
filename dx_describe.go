package dxfs2

import (
	"encoding/json"
	"errors"
	"time"


	// The dxda package has the get-environment code
	"github.com/dnanexus/dxda"
)

// Limit on the number of objects that the bulk-describe API can take
const MAX_NUM_OBJECTS_IN_DESCRIBE = 1000

type Request struct {
	Objects []string `json:"objects"`
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

type DxDescribe struct {
	FileId    string
	ProjId    string
	Name      string
	Folder    string
	Size      uint64
	Ctime     time.Time
	Mtime     time.Time
}

// convert time in milliseconds since 1970, in the equivalent
// golang structure
func dxTimeToUnixTime(dxTime int64) time.Time {
	sec := int64(dxTime/1000)
	millisec := int64(dxTime % 1000)
	return time.Unix(sec, millisec)
}


// Describe a large number of file-ids in one API call.
func submit(dxEnv *dxda.DXEnvironment, fileIds []string) (map[string]DxDescribe, error) {
	request := Request{
		Objects : fileIds,
	}
	var payload []byte
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	//fmt.Printf("payload = %s", string(payload))

	repJs, err := DxAPI(dxEnv, "system/describeDataObjects", string(payload))
	if err != nil {
		return nil, err
	}
	var reply Reply
	json.Unmarshal(repJs, &reply)

	var files = make(map[string]DxDescribe)
	for _, descRawTop := range(reply.Results) {
		descRaw := descRawTop.Describe
		if descRaw.State != "closed" {
			err := errors.New("The file is not in the closed state, it is [" + descRaw.State + "]")
			return nil, err
		}
		desc := DxDescribe{
			ProjId : descRaw.ProjId,
			FileId : descRaw.FileId,
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


func DescribeBulk(dxEnv *dxda.DXEnvironment, fileIds []string) (map[string]DxDescribe, error) {
	// split into limited batchs
	batchSize := MAX_NUM_OBJECTS_IN_DESCRIBE
	var batches [][]string

	for batchSize < len(fileIds) {
		head := fileIds[0:batchSize:batchSize]
		fileIds = fileIds[batchSize:]
		batches = append(batches, head)
	}
	// Don't forget the tail of the requests, that is smaller than the batch size
	batches = append(batches, fileIds)

	var gMap = make(map[string]DxDescribe)
	for _, fileIdBatch := range(batches) {
		m, err := submit(dxEnv, fileIdBatch)
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
