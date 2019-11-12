package dxfuse

import (
	"context"
	"encoding/json"
	"fmt"

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
