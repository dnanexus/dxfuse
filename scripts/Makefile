all : dxfs2 docker_image

dxfs2 :
	go build -o /go/bin/dxfs2 /go/src/github.com/dnanexus/dxfs2/cmd/main.go

docker_image :
	docker build -t dxfs2_test_box .
