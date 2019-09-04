all : dxfs2 copy_all
	dx build benchmark -f --destination dxfs2_test_data:/applets/benchmark

dxfs2 :
	go build -o dxfs2 /go/src/github.com/dnanexus/dxfs2/cmd/main.go

copy_all : dxfs2
	mkdir -p benchmark/resources/usr/bin
	cp dxfs2 benchmark/resources/usr/bin/
