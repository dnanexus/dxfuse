package dxfuse

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dnanexus/dxda"
)

type FindProjectRequest struct {
	Name  string `json:"name"`
	Level string `json:"level"`
}

type FindResult struct {
	Id string `json:"id"`
}

type FindProjectReply struct {
	Results []FindResult `json:"results"`
}

// Find the project-id for a project name. Return nil if
// the project does not exist
func DxFindProject(
	ctx context.Context,
	dxEnv *dxda.DXEnvironment,
	projName string) (string, error) {

	request := FindProjectRequest{
		Name : projName,
		Level : "VIEW",
	}
	var payload []byte
	payload, err := json.Marshal(request)
	if err != nil {
		return "", err
	}

	httpClient := dxda.NewHttpClient(false)
	repJs, err := dxda.DxAPI(ctx, httpClient, dxEnv, "system/findProjects", string(payload))
	if err != nil {
		return "", err
	}
	var reply FindProjectReply
	if err = json.Unmarshal(repJs, &reply); err != nil {
		return "", err
	}

	if len(reply.Results) == 0 {
		// project not found
		return "", nil
	} else if len(reply.Results) == 1 {
		return reply.Results[0].Id, nil
	} else {
		err := fmt.Errorf("Found more than one project with the name %s", projName)
		return "", err
	}
}
