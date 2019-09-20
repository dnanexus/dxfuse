SRC = dx_describe.go dx_find.go dxfs2.go dxfs2_test.go manifest.go metadata_db.go posix.go prefetch.go

all : ${SRC}
	go build -o /go/bin/dxfs2 /go/src/github.com/dnanexus/dxfs2/cmd/main.go
