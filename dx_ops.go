package dxfuse

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"encoding/hex"
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

type RequestUploadChunk struct {
	Size  int     `json:"size"`
	Index int     `json:"index"`
	Md5   string  `json:"md5"`
}

type ReplyUploadChunk struct {
	Url     string            `json:"url"`
	Expires int64             `json:"expires"`
	Headers map[string]string `json:"headers"`
}

func dxFileUploadPart(
	ctx context.Context,
	httpClient *retryablehttp.Client,
	dxEnv *dxda.DXEnvironment,
	fileId string,
	index int,
	data []byte) error {

	md5Sum := md5.Sum(data)
	uploadReq := RequestUploadChunk{
		Size: len(data),
		Index: index,
		Md5: hex.EncodeToString(md5Sum[:]),
	}
	log.Printf("%v", uploadReq)

	reqJson, err := json.Marshal(uploadReq)
	if err != nil {
		return err
	}
	replyJs, err := dxda.DxAPI(
		ctx,
		httpClient,
		NumRetriesDefault,
		dxEnv,
		fmt.Sprintf("%s/upload", fileId),
		string(reqJson))
	if err != nil {
		log.Printf(err.Error())
		return err
	}

	var reply ReplyUploadChunk
	if err = json.Unmarshal(replyJs, &reply); err != nil {
		return err
	}

	_, err = dxda.DxHttpRequest(ctx, httpClient, NumRetriesDefault, "PUT", reply.Url, reply.Headers, data)
	return err
}


type RequestRename struct {
	ProjId string  `json:"project"`
	Name   string  `json:"name"`
}

type ReplyRename struct {
	Id string `json:"id"`
}

//  API method: /class-xxxx/rename
//
//  rename a data object
func DxRename(
	ctx context.Context,
	httpClient *retryablehttp.Client,
	dxEnv *dxda.DXEnvironment,
	projId string,
	fileId string,
	newName string) error {

	var request RequestRename
	request.ProjId = projId
	request.Name = newName

	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	repJs, err := dxda.DxAPI(
		ctx, httpClient, NumRetriesDefault, dxEnv,
		fmt.Sprintf("%s/rename", fileId),
		string(payload))
	if err != nil {
		return err
	}

	var reply ReplyRename
	if err := json.Unmarshal(repJs, &reply); err != nil {
		return err
	}

	return nil
}


type RequestMove struct {
	Objects    []string `json:"objects"`
	Folders    []string `json:"folders"`
	Destination  string `json:"destination"`
}

type ReplyMove struct {
	Id string `json:"id"`
}

//  API method: /class-xxxx/move
//
// Moves the specified data objects and folders to a destination folder in the same container.
func DxMove(
	ctx context.Context,
	httpClient *retryablehttp.Client,
	dxEnv *dxda.DXEnvironment,
	projId      string,
	objectIds []string,
	folders   []string,
	destination string) error {

	var request RequestMove
	request.Objects = objectIds
	request.Folders = folders

	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	repJs, err := dxda.DxAPI(
		ctx, httpClient, NumRetriesDefault, dxEnv,
		fmt.Sprintf("%s/move", projId),
		string(payload))
	if err != nil {
		return err
	}

	var reply ReplyMove
	if err := json.Unmarshal(repJs, &reply); err != nil {
		return err
	}

	return nil
}
