package dxfs2

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"time"


	// The dxda package has the get-environment code
	"github.com/dnanexus/dxda"

	"github.com/hashicorp/go-cleanhttp"     // required by go-retryablehttp
	"github.com/hashicorp/go-retryablehttp" // use http libraries from hashicorp for implement retry logic
)

const minRetryTime = 1   // seconds
const maxRetryTime = 120 // seconds
const maxRetryCount = 10
const userAgent = "dxfs2: DNAnexus FUSE filesystem"
const reqTimeout = 15  // seconds
const maxNumAttempts = 3

func DxHttpRequest(requestType string, url string, headers map[string]string, data []byte) (body []byte, err error) {
	var client *retryablehttp.Client
	client = &retryablehttp.Client{
		HTTPClient:   cleanhttp.DefaultClient(),
		Logger:       log.New(ioutil.Discard, "", 0), // Throw away retryablehttp internal logging
		RetryWaitMin: minRetryTime * time.Second,
		RetryWaitMax: maxRetryTime * time.Second,
		RetryMax:     maxRetryCount,
		CheckRetry:   retryablehttp.DefaultRetryPolicy,
		Backoff:      retryablehttp.DefaultBackoff,
	}

	// Safety procedure to force timeout to prevent hanging
	ctx, cancel := context.WithCancel(context.TODO())
	timer := time.AfterFunc(reqTimeout * time.Second, func() {
		cancel()
	})
	req, err := retryablehttp.NewRequest(requestType, url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	for header, value := range headers {
		req.Header.Set(header, value)
	}
	resp, err := client.Do(req)
	timer.Stop()
	if err != nil {
		return nil, err
	}
	status := resp.Status

	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	// TODO: Investigate more sophsticated handling of these error codes ala
	// https://github.com/dnanexus/dx-toolkit/blob/3f34b723170e698a594ccbea16a82419eb06c28b/src/python/dxpy/__init__.py#L655
	if !strings.HasPrefix(status, "2") {
		log.Fatalln(fmt.Errorf("%s request to '%s' failed with status %s",
			requestType, url, status))
		return nil, fmt.Errorf("http error, status %s", status)
	}
	return body, nil
}

// DxAPI - Function to wrap a generic API call to DNAnexus
func DxAPI(dxEnv *dxda.DXEnvironment, api string, payload string) (body []byte, err error) {
	if (dxEnv.Token == "") {
		err := errors.New("The token is not set. This may be because the environment isn't set.")
		return nil, err
	}
	headers := map[string]string{
		"User-Agent":   userAgent,
		"Authorization": fmt.Sprintf("Bearer %s", dxEnv.Token),
		"Content-Type":  "application/json",
	}
	url := fmt.Sprintf("%s://%s:%d/%s",
		dxEnv.ApiServerProtocol,
		dxEnv.ApiServerHost,
		dxEnv.ApiServerPort,
		api)
	return DxHttpRequest("POST", url, headers, []byte(payload))
}




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
func DescribeBulk(dxEnv *dxda.DXEnvironment, fileIds []string) (map[string]DxDescribe, error) {
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
		files[desc.FileId] = desc
	}
	return files, nil
}
