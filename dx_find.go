package dxfuse

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/dnanexus/dxda"
)

type FindProjectRequest struct {
	Name            string                              `json:"name"`
	Level           string                              `json:"level"`
	DescribeOptions map[string]DxDescribeProjectRequest `json:"describe,omitempty"`
}

type FindResult struct {
	Id                string                 `json:"id"`
	Level             string                 `json:"level"`
	PermissionSources []string               `json:"permissionSources"`
	Public            bool                   `json:"public"`
	Describe          DxDescribeProjectReply `json:"describe"`
}

type FindProjectReply struct {
	Results []FindResult `json:"results"`
}

// Find the project-id for a project name. Return nil if
// the project does not exist
func DxFindProject(
	ctx context.Context,
	httpClient *http.Client,
	dxEnv *dxda.DXEnvironment,
	projName string) (*DxProjectDescription, error) {
	var describeRequest DxDescribeProjectRequest
	describeRequest.Fields = map[string]bool{
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
	var describeOptions = map[string]DxDescribeProjectRequest{
		"describe": describeRequest,
	}

	findRequest := FindProjectRequest{
		Name:            projName,
		Level:           "VIEW",
		DescribeOptions: describeOptions,
	}

	var payload []byte
	payload, err := json.Marshal(findRequest)
	if err != nil {
		return nil, err
	}

	repJs, err := dxda.DxAPI(ctx, httpClient, NumRetriesDefault, dxEnv, "system/findProjects", string(payload))
	if err != nil {
		return nil, err
	}
	var reply FindProjectReply
	if err = json.Unmarshal(repJs, &reply); err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}

	if len(reply.Results) == 0 {
		// project not found
		return nil, nil
	} else if len(reply.Results) > 1 {
		err := fmt.Errorf("Found more than one project with the name %s", projName)
		return nil, err
	} else {
		projDesc := reply.Results[0].Describe
		prj := DxProjectDescription{
			Id:           projDesc.Id,
			Name:         projDesc.Name,
			Region:       projDesc.Region,
			Version:      projDesc.Version,
			DataUsageGiB: projDesc.DataUsage,
			CtimeSeconds: projDesc.CreatedMillisec / 1000,
			MtimeSeconds: projDesc.ModifiedMillisec / 1000,
			UploadParams: projDesc.UploadParams,
			Level:        projectPermissionsToInt(projDesc.Level),
		}
		return &prj, nil
	}
}
