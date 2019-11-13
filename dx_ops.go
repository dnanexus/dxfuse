package dxfuse

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/dnanexus/dxda"
	"github.com/hashicorp/go-retryablehttp"
)

type RequestFolderNew struct {
	ProjId   string `json:"project"`
	Folder   string `json:"folder"`
	Parents  bool   `json:"parents"`
}

type ReplyFolderNew struct {
	Id string `json:"id"`
}

func DxFolderNew(
	ctx context.Context,
	httpClient *retryablehttp.Client,
	dxEnv *dxda.DXEnvironment,
	projId string,
	folder string) error {

	var request RequestFolderNew
	request.ProjId = projId
	request.Folder = folder
	request.Parents = false

	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	repJs, err := dxda.DxAPI(
		ctx,
		httpClient,
		NumRetriesDefault,
		dxEnv,
		fmt.Sprintf("%s/newFolder", projId),
		string(payload))
	if err != nil {
		return err
	}

	var reply ReplyFolderNew
	if err := json.Unmarshal(repJs, &reply); err != nil {
		return err
	}

	return nil
}


type RequestFolderRemove struct {
	ProjId   string `json:"project"`
	Folder   string `json:"folder"`
}

type ReplyFolderRemove struct {
	Id string `json:"id"`
}

func DxFolderRemove(
	ctx context.Context,
	httpClient *retryablehttp.Client,
	dxEnv *dxda.DXEnvironment,
	projId string,
	folder string) error {

	var request RequestFolderRemove
	request.ProjId = projId
	request.Folder = folder

	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	repJs, err := dxda.DxAPI(
		ctx,
		httpClient,
		NumRetriesDefault,
		dxEnv,
		fmt.Sprintf("%s/removeFolder", projId),
		string(payload))
	if err != nil {
		return err
	}

	var reply ReplyFolderRemove
	if err := json.Unmarshal(repJs, &reply); err != nil {
		return err
	}

	return nil
}


type RequestRemoveObjects struct {
	Objects   []string `json:"objects"`
	Force     bool   `json:"force"`
}

type ReplyRemoveObjects struct {
	Id string `json:"id"`
}


func DxRemoveObjects(
	ctx context.Context,
	httpClient *retryablehttp.Client,
	dxEnv *dxda.DXEnvironment,
	projId string,
	objectIds []string) error {

	var request RequestRemoveObjects
	request.Objects = objectIds
	request.Force = false

	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	repJs, err := dxda.DxAPI(
		ctx,
		httpClient,
		NumRetriesDefault,
		dxEnv,
		fmt.Sprintf("%s/removeObjects", projId),
		string(payload))
	if err != nil {
		return err
	}

	var reply ReplyFolderRemove
	if err := json.Unmarshal(repJs, &reply); err != nil {
		return err
	}

	return nil
}

type RequestNewFile struct {
	ProjId   string `json:"project"`
	Name     string `json:"name"`
	Folder   string `json:"folder"`
	Parents  bool   `json:"parents"`
	Nonce    string `json:"nonce"`
}

type ReplyNewFile struct {
	Id string `json:"id"`
}

func DxFileNew(
	ctx context.Context,
	httpClient *retryablehttp.Client,
	dxEnv *dxda.DXEnvironment,
	nonceStr string,
	projId string,
	fname string,
	folder string) (string, error) {

	var request RequestNewFile
	request.ProjId = projId
	request.Name = fname
	request.Folder = folder
	request.Parents = false
	request.Nonce = nonceStr

	payload, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	repJs, err := dxda.DxAPI(ctx, httpClient, NumRetriesDefault, dxEnv, "file/new", string(payload))
	if err != nil {
		return "", err
	}

	var reply ReplyNewFile
	if err := json.Unmarshal(repJs, &reply); err != nil {
		return "", err
	}

	// got a file ID back
	return reply.Id, nil
}

func DxFileCloseAndWait(
	ctx context.Context,
	httpClient *retryablehttp.Client,
	dxEnv *dxda.DXEnvironment,
	fid string,
	verbose bool) error {

	_, err := dxda.DxAPI(
		ctx,
		httpClient,
		NumRetriesDefault,
		dxEnv,
		fmt.Sprintf("%s/close", fid),
		"{}")
	if err != nil {
		return err
	}

        // wait for file to achieve closed state
	start := time.Now()
	deadline := start.Add(fileCloseMaxWaitTime)
        for true {
		fDesc, err := DxDescribe(ctx, httpClient, dxEnv, fid, false)
		if err != nil {
			return err
		}
		switch fDesc.State {
		case "closed":
			// done. File is closed.
			return nil
		case "closing":
			// not done yet.
			if verbose {
				elapsed := time.Now().Sub(start)
				log.Printf("Waited %s for file %s to close", elapsed.String(), fid)
			}
			time.Sleep(fileCloseWaitTime)

			// don't wait too long
			if time.Now().After(deadline) {
				return fmt.Errorf("Waited %s for file %s to close, stopping the wait",
					fileCloseMaxWaitTime.String(), fid)
			}
			continue
		default:
			return fmt.Errorf("data object %s has bad state %s", fid, fDesc.State)
		}
	}
	return nil
}
