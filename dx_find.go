type FindProjectRequest struct {
	Class string `json:"class"`
	Name  string `json:"name"`
}

type FindResult struct {
	id string `json:"id"`
}

type FindProjectReply struct {
	Results []FindResult `json:"results"`
}

// Find the project-id for a project name. Return nil if
// the project does not exist
func DxFindProject(
	dxEnv *dxda.DXEnvironment,
	projName string) (string, error) {

	request := FindProjectRequest{
		Class : "project",
		Name : projName,
	}
	var payload []byte
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	repJs, err := DxAPI(dxEnv, "/system/findDataObjects", string(payload))
	if err != nil {
		return nil, err
	}

}
