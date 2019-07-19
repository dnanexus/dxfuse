package dxfuse

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

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


func makeRequest(
	requestType string,
	url string,
	headers map[string]string,
	data []byte) (status string, body []byte) {

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

	var numAttempts int = 0
	var totalTimeNanoSec uint64 = 0

	while ( (totalTimeNanoSec / 1000*1000*1000) < maxTotalReqTime
		&& numAttempts < maxNumAttempts ) {
		startNanoSec := time.Now().UnixNano()

		// Safety procedure to force timeout to prevent hanging
		ctx, cancel := context.WithCancel(context.TODO())
		timer := time.AfterFunc(reqTimeout * time.Second, func() {
			cancel()
		})
		req, err := retryablehttp.NewRequest(requestType, url, bytes.NewReader(data))
		if err != nil {
			// Precondition failed error. This is really a case
			// where we couldn't allocate a new request.
			return "412" , nil
		}
		req = req.WithContext(ctx)
		for header, value := range headers {
			req.Header.Set(header, value)
		}
		resp, err := client.Do(req)
		timer.Stop()
		if err != nil {
			// We need a better error code here.
			return "500", nil
		}
		status = resp.Status

		body, _ = ioutil.ReadAll(resp.Body)
		resp.Body.Close()

		// TODO: Investigate more sophsticated handling of these error codes ala
		// https://github.com/dnanexus/dx-toolkit/blob/3f34b723170e698a594ccbea16a82419eb06c28b/src/python/dxpy/__init__.py#L655
		if !strings.HasPrefix(status, "2") {
			log.Fatalln(fmt.Errorf("%s request to '%s' failed with status %s",
				requestType, url, status))
			return status, nil
		}

		deltaNanoSec := time.Now().UnixNano() - startNanoSec
		totalTimeNanoSec += deltaNanoSec
	}
	return status, body
}

// DXAPI - Function to wrap a generic API call to DNAnexus
func DXAPI(dxEnv dxda.DXEnvironment, api string, payload string) (status string, body []byte, err error) {
	if (dxEnv.Token == "") {
		err := errors.New("The token is not set. This may be because the environment isn't set.")
		return nil, nil, err
	}
	headers := map[string]string{
		"User-Agent":   userAgent,
		"Authorization": fmt.Sprintf("Bearer %s", dxEnv.Token),
		"Content-Type":  "application/json",
	}
	url := fmt.Sprintf("%s://%s:%d/%s", dxEnv.ApiServerProtocol, dxEnv.ApiServerHost, dxEnv.ApiServerPort, api)
	status, body = makeRequest("POST", url, headers, []byte(payload))
	return status, body, nil
}


type DXDescribeFileRawJSON struct {
	projectId string `json:"project"`
	fileId    string `json:"id"`
	class     string `json:"class"`
	created   uint64 `json:"created"`
	state     string `json:"state"`
	name      string `json:"name"`
	folder    string `json:"folder"`
	size      uint64 `json:"size"`
}

type DXDescribeFile struct {
	ProjId    string
	FileId    string
	Name      string
	Folder    string
	Size      uint64
	Created   timeTime
}

// make an API call that describes a file
//
// If the file isn't closed, or closing, return an error.
func Describe(dxEnv dxda.DXEnvironment, projId string, fileId string) (DXDescribeFile, error) {
	// make the call
	payload := fmt.Sprintf("{\"project\": \"%s\"}", projId)
	apiCall := fmt.Sprint("/%s/describe", fileId)
	status, body, err = DXAPI(dxEnv, apiCall, payload)
	if err != nil {
		return nil, err
	}

	// unmarshal the response
	var descRaw DXDescribeFileRawJSON
	json.Unmarshal(body, &descRaw)

	if descRaw.class != "file" {
		err := errors.New("This is not a file, it is a " + descRaw.class)
		return nil, err
	}
	if descRaw.state != "closed" {
		err := errors.New("The file is not in the closed state, it is" + descRaw.state)
		return nil, err
	}
	desc := &DXDescribeFile{
		ProjId : descRaw.projectId,
		fileId : descRaw.fileId,
		Name : descRaw.name,
		Folder : descRaw.folder
		Created : time.Unix(descRaw.created/1000, descRaw.created % 1000),
		Size : descRaw.size
	}
	return desc, nil
}
