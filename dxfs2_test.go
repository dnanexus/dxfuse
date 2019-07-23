package dxfs2_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/dnanexus/dxfs2"
)

func TestReadManifest(t *testing.T) {
	mExample := `[
  {
    "id": "file-0001",
    "proj" : "project-1001",
    "ctime" : 100000011,
    "mtime" : 100029992,
    "size" : 8102
   }
]`
	tmpFile := "/tmp/dat1"
	os.Remove(tmpFile)

	err := ioutil.WriteFile(tmpFile, []byte(mExample), 0644)
	if err != nil {
		t.Errorf(err.Error())
	}
	t.Log("testing manifest\n")

	descs, err := dxfs2.ReadManifest(tmpFile)
	if err != nil {
		t.Errorf(err.Error())
	}
	t.Logf("print %d entries\n", len(descs))
	for _, value := range(descs) {
		t.Logf("%v\n", value)
	}
}



func testFileDescribe(dxEnv *dxda.DXEnvironment, projId string, fileId string) {
	desc, err := dxfs2.Describe(dxEnv, projId, fileId)
	if desc == nil {
		fmt.Printf("The description is empty\n")
		fmt.Printf(err.Error() + "\n")
	} else {
		fmt.Printf("%v\n", *desc)
	}
}
