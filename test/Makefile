all : aws

aws : copy_all
	dx build benchmark -f --destination dxfuse_test_data:/applets/benchmark
	dx build correctness -f --destination dxfuse_test_data:/applets/correctness
	dx build correctness_downloads -f --destination dxfuse_test_data:/applets/correctness_downloads
	dx build bam_diff -f --destination dxfuse_test_data:/applets/bam_diff

azure: dxfuse copy_all
	dx build benchmark -f --destination dxfuse_azure_westus:/applets/benchmark
	dx build correctness -f --destination dxfuse_azure_westus:/applets/correctness

dxfuse : $(wildcard ../**/*)
	go build -o /go/bin/dxfuse /go/src/github.com/dnanexus/dxfuse/cli/main.go

copy_all : dxfuse bench correct bio correct_downloads

bench : dxfuse
	rm -rf benchmark/resources
	mkdir -p benchmark/resources/usr/bin
	cp /go/bin/dxfuse benchmark/resources/usr/bin/

correct: dxfuse
	rm -rf correctness/resources
	mkdir -p correctness/resources/usr/bin
	cp /go/bin/dxfuse correctness/resources/usr/bin/
	cp test_routines.sh correctness/resources/

correct_downloads: dxfuse
	rm -rf correctness_downloads/resources
	mkdir -p correctness_downloads/resources/usr/bin
	cp /go/bin/dxfuse correctness_downloads/resources/usr/bin/

bio : dxfuse
	cp -f /go/bin/dxfuse bam_diff/resources/usr/bin/

clean :
	rm -f dxfuse
	dx rm -f dxfuse_test_data:/applets/* || true
	dx rm -f dxfuse_azure_westus:/applets/* || true
