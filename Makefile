/go/bin/dxfuse : $(wildcard *.go)
	go build -o /go/bin/dxfuse /go/src/github.com/dnanexus/dxfuse/cli/main.go
