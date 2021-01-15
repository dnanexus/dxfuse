package dxfuse

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"github.com/dnanexus/dxda"
)

const (
	fileCloseWaitTime    = 5 * time.Second
	fileCloseMaxWaitTime = 10 * time.Minute
)

type DxOps struct {
	dxEnv   dxda.DXEnvironment
	options Options

	// http error that occurs when an upload has taken too long
	timeoutExpirationErrorRe *regexp.Regexp
}

func NewDxOps(dxEnv dxda.DXEnvironment, options Options) *DxOps {
	timeoutRe := regexp.MustCompile(`<Message>Request has expired</Message>`)
	return &DxOps{
		dxEnv:                    dxEnv,
		options:                  options,
		timeoutExpirationErrorRe: timeoutRe,
	}
}

func (ops *DxOps) log(a string, args ...interface{}) {
	LogMsg("dx_ops", a, args...)
}

type RequestFolderNew struct {
	ProjId  string `json:"project"`
	Folder  string `json:"folder"`
	Parents bool   `json:"parents"`
}

type ReplyFolderNew struct {
	Id string `json:"id"`
}

func (ops *DxOps) DxFolderNew(
	ctx context.Context,
	httpClient *http.Client,
	projId string,
	folder string) error {
	if ops.options.Verbose {
		ops.log("new-folder %s:%s", projId, folder)
	}

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
		&ops.dxEnv,
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
	ProjId string `json:"project"`
	Folder string `json:"folder"`
}

type ReplyFolderRemove struct {
	Id string `json:"id"`
}

func (ops *DxOps) DxFolderRemove(
	ctx context.Context,
	httpClient *http.Client,
	projId string,
	folder string) error {
	if ops.options.Verbose {
		ops.log("remove-folder %s:%s", projId, folder)
	}

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
		&ops.dxEnv,
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
	Objects []string `json:"objects"`
	Force   bool     `json:"force"`
}

type ReplyRemoveObjects struct {
	Id string `json:"id"`
}

func (ops *DxOps) DxRemoveObjects(
	ctx context.Context,
	httpClient *http.Client,
	projId string,
	objectIds []string) error {
	if ops.options.Verbose {
		ops.log("Removing %d objects from project %s", len(objectIds), projId)
	}

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
		&ops.dxEnv,
		fmt.Sprintf("%s/removeObjects", projId),
		string(payload))
	if err != nil {
		return err
	}

	var reply ReplyRemoveObjects
	if err := json.Unmarshal(repJs, &reply); err != nil {
		return err
	}

	return nil
}

type RequestNewFile struct {
	ProjId  string `json:"project"`
	Name    string `json:"name"`
	Folder  string `json:"folder"`
	Parents bool   `json:"parents"`
	Nonce   string `json:"nonce"`
}

type ReplyNewFile struct {
	Id string `json:"id"`
}

func (ops *DxOps) DxFileNew(
	ctx context.Context,
	httpClient *http.Client,
	nonceStr string,
	projId string,
	fname string,
	folder string) (string, error) {
	if ops.options.Verbose {
		ops.log("file-new %s:%s/%s", projId, folder, fname)
	}

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
	repJs, err := dxda.DxAPI(ctx, httpClient, NumRetriesDefault, &ops.dxEnv, "file/new", string(payload))
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

func (ops *DxOps) DxFileCloseAndWait(
	ctx context.Context,
	httpClient *http.Client,
	projectId string,
	fid string) error {
	if ops.options.Verbose {
		ops.log("file close-and-wait %s", fid)
	}

	_, err := dxda.DxAPI(
		ctx,
		httpClient,
		NumRetriesDefault,
		&ops.dxEnv,
		fmt.Sprintf("%s/close", fid),
		"{}")
	if err != nil {
		return err
	}

	// wait for file to achieve closed state
	start := time.Now()
	deadline := start.Add(fileCloseMaxWaitTime)
	for true {
		fDesc, err := DxDescribe(ctx, httpClient, &ops.dxEnv, projectId, fid)
		if err != nil {
			return err
		}
		switch fDesc.State {
		case "closed":
			// done. File is closed.
			return nil
		case "closing":
			// not done yet.
			if ops.options.Verbose {
				elapsed := time.Now().Sub(start)
				ops.log("Waited %s for file %s to close", elapsed.String(), fid)
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
	Size  int    `json:"size"`
	Index int    `json:"index"`
	Md5   string `json:"md5"`
}

type ReplyUploadChunk struct {
	Url     string            `json:"url"`
	Expires int64             `json:"expires"`
	Headers map[string]string `json:"headers"`
}

// check if this is an error we can retry
func (ops *DxOps) isRetryableUploadError(err error) bool {
	// This was caused by a timeout expiring
	if ops.timeoutExpirationErrorRe.MatchString(err.Error()) {
		return true
	}

	// An error that cannot be retried
	return false
}

func (ops *DxOps) DxFileUploadPart(
	ctx context.Context,
	httpClient *http.Client,
	fileId string,
	index int,
	data []byte) error {

	md5Sum := md5.Sum(data)
	uploadReq := RequestUploadChunk{
		Size:  len(data),
		Index: index,
		Md5:   hex.EncodeToString(md5Sum[:]),
	}

	reqJson, err := json.Marshal(uploadReq)
	if err != nil {
		return err
	}

	for i := 0; i < NumRetriesDefault; i++ {
		replyJs, err := dxda.DxAPI(
			ctx,
			httpClient,
			NumRetriesDefault,
			&ops.dxEnv,
			fmt.Sprintf("%s/upload", fileId),
			string(reqJson))
		if err != nil {
			ops.log("DxFileUploadPart: error in dxapi call [%s/upload] %v",
				fileId, err.Error())
			return err
		}

		var reply ReplyUploadChunk
		if err = json.Unmarshal(replyJs, &reply); err != nil {
			return err
		}

		// bulk data upload
		_, err = dxda.DxHttpRequest(ctx, httpClient, 1, "PUT", reply.Url, reply.Headers, data)
		if err != nil {
			if ops.isRetryableUploadError(err) {
				// This is a retryable error, try again
				ops.log("Retrying part upload, timeout expired")
				continue
			}
			ops.log("DxFileUploadPart: failure in data upload %s", err.Error())
			return err
		}
	}
	return nil
}

type RequestRename struct {
	ProjId string `json:"project"`
	Name   string `json:"name"`
}

type ReplyRename struct {
	Id string `json:"id"`
}

//  API method: /class-xxxx/rename
//
//  rename a data object
func (ops *DxOps) DxRename(
	ctx context.Context,
	httpClient *http.Client,
	projId string,
	fileId string,
	newName string) error {
	if ops.options.Verbose {
		ops.log("file rename %s:%s %s", projId, fileId, newName)
	}

	var request RequestRename
	request.ProjId = projId
	request.Name = newName

	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	repJs, err := dxda.DxAPI(
		ctx, httpClient, NumRetriesDefault, &ops.dxEnv,
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
	Objects     []string `json:"objects"`
	Folders     []string `json:"folders"`
	Destination string   `json:"destination"`
}

type ReplyMove struct {
	Id string `json:"id"`
}

//  API method: /class-xxxx/move
//
// Moves the specified data objects and folders to a destination folder in the same container.
func (ops *DxOps) DxMove(
	ctx context.Context,
	httpClient *http.Client,
	projId string,
	objectIds []string,
	folders []string,
	destination string) error {

	if ops.options.Verbose {
		ops.log("%s source folders=%v  -> %s", projId, folders, destination)
	}
	var request RequestMove
	request.Objects = objectIds
	request.Folders = folders
	request.Destination = destination

	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	repJs, err := dxda.DxAPI(
		ctx, httpClient, NumRetriesDefault, &ops.dxEnv,
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

type RequestRenameFolder struct {
	Folder string `json:"folder"`
	Name   string `json:"name"`
}

type ReplyRenameFolder struct {
	Id string `json:"id"`
}

func (ops *DxOps) DxRenameFolder(
	ctx context.Context,
	httpClient *http.Client,
	projId string,
	folder string,
	newName string) error {

	var request RequestRenameFolder
	request.Folder = folder
	request.Name = newName

	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}

	repJs, err := dxda.DxAPI(
		ctx, httpClient, NumRetriesDefault, &ops.dxEnv,
		fmt.Sprintf("%s/renameFolder", projId),
		string(payload))
	if err != nil {
		return err
	}

	var reply ReplyRenameFolder
	if err := json.Unmarshal(repJs, &reply); err != nil {
		return err
	}

	return nil
}

type RequestClone struct {
	Objects     []string `json:"objects"`
	Folders     []string `json:"folders"`
	Project     string   `json:"project"`
	Destination string   `json:"destination"`
	Parents     bool     `json:"parents"`
}

type ReplyClone struct {
	Id      string   `json:"id"`
	Project string   `json:"project"`
	Exists  []string `json:"exists"`
}

func (ops *DxOps) DxClone(
	ctx context.Context,
	httpClient *http.Client,
	srcProjId string,
	srcId string,
	destProjId string,
	destProjFolder string) (bool, error) {

	var request RequestClone
	objs := make([]string, 1)
	objs[0] = srcId
	request.Objects = objs
	request.Folders = make([]string, 0)
	request.Project = destProjId
	request.Destination = destProjFolder
	request.Parents = false

	payload, err := json.Marshal(request)
	if err != nil {
		return false, err
	}

	repJs, err := dxda.DxAPI(
		ctx, httpClient, NumRetriesDefault, &ops.dxEnv,
		fmt.Sprintf("%s/clone", srcProjId),
		string(payload))
	if err != nil {
		return false, err
	}

	var reply ReplyClone
	if err := json.Unmarshal(repJs, &reply); err != nil {
		return false, err
	}

	for _, id := range reply.Exists {
		if id == srcId {
			// was not copied, because there is an existing
			// copy in the destination project.
			return false, nil
		}
	}

	return true, nil
}

type RequestSetProperties struct {
	ProjId     string               `json:"project"`
	Properties map[string](*string) `json:"properties"`
}

type ReplySetProperties struct {
	Id string `json:"id"`
}

func (ops *DxOps) DxSetProperties(
	ctx context.Context,
	httpClient *http.Client,
	projId string,
	objId string,
	props map[string](*string)) error {

	var request RequestSetProperties
	request.ProjId = projId
	request.Properties = props

	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}

	repJs, err := dxda.DxAPI(
		ctx, httpClient, NumRetriesDefault, &ops.dxEnv,
		fmt.Sprintf("%s/setProperties", objId),
		string(payload))
	if err != nil {
		return err
	}

	var reply ReplySetProperties
	if err := json.Unmarshal(repJs, &reply); err != nil {
		return err
	}

	return nil
}

type RequestAddTags struct {
	ProjId string   `json:"project"`
	Tags   []string `json:"tags"`
}

type ReplyAddTags struct {
	Id string `json:"id"`
}

func (ops *DxOps) DxAddTags(
	ctx context.Context,
	httpClient *http.Client,
	projId string,
	objId string,
	tags []string) error {

	var request RequestAddTags
	request.ProjId = projId
	request.Tags = tags

	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}

	repJs, err := dxda.DxAPI(
		ctx, httpClient, NumRetriesDefault, &ops.dxEnv,
		fmt.Sprintf("%s/addTags", objId),
		string(payload))
	if err != nil {
		return err
	}

	var reply ReplyAddTags
	if err := json.Unmarshal(repJs, &reply); err != nil {
		return err
	}

	return nil
}

type RequestRemoveTags struct {
	ProjId string   `json:"project"`
	Tags   []string `json:"tags"`
}

type ReplyRemoveTags struct {
	Id string `json:"id"`
}

func (ops *DxOps) DxRemoveTags(
	ctx context.Context,
	httpClient *http.Client,
	projId string,
	objId string,
	tags []string) error {

	var request RequestRemoveTags
	request.ProjId = projId
	request.Tags = tags

	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}

	repJs, err := dxda.DxAPI(
		ctx, httpClient, NumRetriesDefault, &ops.dxEnv,
		fmt.Sprintf("%s/removeTags", objId),
		string(payload))
	if err != nil {
		return err
	}

	var reply ReplyRemoveTags
	if err := json.Unmarshal(repJs, &reply); err != nil {
		return err
	}

	return nil
}
