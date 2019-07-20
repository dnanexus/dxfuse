package dxfuse

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
const userAgent = "DNAnexus FUSE filesystem"
const reqTimeout = 15  // seconds
const maxNumAttempts = 3

// the total time to service a request cannot go above this number of seconds
const maxTotalReqTime = 120 // seconds


func MakeRequest(requestType string, url string, headers map[string]string, data []byte) (body []byte, err error) {
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

//	var numAttempts int = 0
//	var totalTimeNanoSec int64 = 0

//	for  ((totalTimeNanoSec / 1000*1000*1000) < maxTotalReqTime) &&
//		(numAttempts < maxNumAttempts) {
//		startNanoSec := time.Now().UnixNano()

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

//		deltaNanoSec := time.Now().UnixNano() - startNanoSec
//		totalTimeNanoSec += deltaNanoSec
//	}
	return body, nil
}

// DXAPI - Function to wrap a generic API call to DNAnexus
func DXAPI(dxEnv *dxda.DXEnvironment, api string, payload string) (body []byte, err error) {
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
	return MakeRequest("POST", url, headers, []byte(payload))
}


type DXDescribeFileRawJSON struct {
	ProjectId        string `json:"project"`
	FileId           string `json:"id"`
	CreatedMillisec  uint64 `json:"created"`
	State            string `json:"state"`
	Name             string `json:"name"`
	Folder           string `json:"folder"`
	Size             uint64 `json:"size"`
}

type DXDescribeFile struct {
	ProjId    string
	FileId    string
	Name      string
	Folder    string
	Size      uint64
	Created   time.Time
}

// make an API call that describes a file
//
// If the file isn't closed, or closing, return an error.
func Describe(dxEnv *dxda.DXEnvironment, projId string, fileId string) (*DXDescribeFile, error) {
	payload := fmt.Sprintf("{\"project\": \"%s\"}", projId)
	apiCall := fmt.Sprintf("%s/describe", fileId)
	body, err := DXAPI(dxEnv, apiCall, payload)
	if err != nil {
		return nil, err
	}

	// unmarshal the response
	var descRaw DXDescribeFileRawJSON
	json.Unmarshal(body, &descRaw)

	if descRaw.State != "closed" {
		err := errors.New("The file is not in the closed state, it is [" + descRaw.State + "]")
		return nil, err
	}

	// convert time in milliseconds since 1970, in the equivalent
	// golang structure
	sec := int64(descRaw.CreatedMillisec/1000)
	millisec := int64(descRaw.CreatedMillisec % 1000)
	crtTime := time.Unix(sec, millisec)

	desc := &DXDescribeFile{
		ProjId : descRaw.ProjectId,
		FileId : descRaw.FileId,
		Name : descRaw.Name,
		Folder : descRaw.Folder,
		Created : crtTime,
		Size : descRaw.Size,
	}
	return desc, nil
}
