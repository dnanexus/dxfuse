package dxfs2_test

import (
	"testing"

	"github.com/dnanexus/dxda"
	"github.com/dnanexus/dxfs2"
)

func TestDescribeBulk(t *testing.T) {
	fileIds := []string {"file-FJ1qyg80ffP9v6gVPxKz9pQ7","file-FYjp6Yj0ffP9KFPGPF3FfVzk"}
	t.Log("testing bulk describe\n")

	dxEnv, _, err := dxda.GetDxEnvironment()
	if err != nil {
		t.Errorf(err.Error())
	}

	descs, err := dxfs2.DescribeBulk(&dxEnv, fileIds)
	if err != nil {
		t.Errorf(err.Error())
	}
	t.Logf("print %d entries\n", len(descs))
	for key, value := range(descs) {
		t.Logf("%s %v\n", key, value)
	}
}
