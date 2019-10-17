package dxfuse

import (
	"encoding/json"
	"log"

	"github.com/dnanexus/dxda"
)

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
	fsys *Filesys,
	projId string,
	fname string,
	folder string) (string, error) {
	httpClient := <- fsys.httpClientPool
	defer func() {
		// return the http client to the pool, even if an error occurs
		fsys.httpClientPool <- httpClient
	} ()

	var request RequestNewFile
	request.ProjId = projId
	request.Name = fname
	request.Folder = folder
	request.Parents = false
	request.Nonce = fsys.nonce.String()
	if fsys.options.Verbose {
		log.Printf("%v", request)
	}

	payload, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	repJs, err := dxda.DxAPI(httpClient, &fsys.dxEnv, "file/new", string(payload))
	if err != nil {
		return "", err
	}

	var reply ReplyNewFile
	if err := json.Unmarshal(repJs, &reply); err != nil {
		// TODO: triage the errors
		/*
		InvalidInput
		A nonce was reused in a request but some of the other inputs had changed signifying a new and different request

		PermissionDenied
		    UPLOAD access required
		InvalidType
		    project is not a project ID
		ResourceNotFound
		    The specified project is not found
		    The route in folder does not exist, and parents is false
	*/
		return "", err
	}

	// got a file ID back
	return reply.Id, nil
}

func DxFileClose() error {
}
