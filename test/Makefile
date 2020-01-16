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
	mkdir -p benchmark/resources/usr/bin
	cp -f /go/bin/dxfuse benchmark/resources/usr/bin/

correct: dxfuse
	mkdir -p correctness/resources/usr/bin
	cp -f /go/bin/dxfuse correctness/resources/usr/bin/

correct_downloads: dxfuse
	mkdir -p correctness_downloads/resources/usr/bin
	cp -f /go/bin/dxfuse correctness_downloads/resources/usr/bin/

bio : dxfuse
	cp -f /go/bin/dxfuse bam_diff/resources/usr/bin/

clean :
	rm -f dxfuse
	dx rm -f dxfuse_test_data:/applets/* || true
	dx rm -f dxfuse_azure_westus:/applets/* || true
