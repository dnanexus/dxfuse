SRC = dx_describe.go dx_find.go dxfuse.go dxfuse_test.go manifest.go metadata_db.go posix.go prefetch.go

all : ${SRC}
	go build -o /go/bin/dxfuse /go/src/github.com/dnanexus/dxfuse/cmd/main.go
