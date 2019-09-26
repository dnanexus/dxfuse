all : dxfuse copy_all
	dx build dxfuse_benchmark -f --destination dxfs2_test_data:/applets/dxfuse_benchmark
	dx build dxfuse_correctness -f --destination dxfs2_test_data:/applets/dxfuse_correctness
	dx build dxfuse_download_only -f --destination dxfs2_test_data:/applets/dxfuse_download_only

dxfuse : ../*.go
	go build -o dxfuse /go/src/github.com/dnanexus/dxfuse/cmd/main.go

copy_all : dxfuse
	mkdir -p dxfuse_benchmark/resources/usr/bin
	cp -f dxfuse dxfuse_benchmark/resources/usr/bin/
	mkdir -p dxfuse_correctness/resources/usr/bin
	cp -f dxfuse dxfuse_correctness/resources/usr/bin/
	mkdir -p dxfuse_download_only/resources/usr/bin
	cp -f dxfuse dxfuse_download_only/resources/usr/bin/

clean :
	rm -f dxfuse
	dx rm -f dxfs2_test_data:/applets/dxfuse_* || true
