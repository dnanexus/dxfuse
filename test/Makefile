all : aws

aws : copy_all
	dx build dxfuse_benchmark -f --destination dxfuse_test_data:/applets/dxfuse_benchmark
	dx build dxfuse_correctness -f --destination dxfuse_test_data:/applets/dxfuse_correctness
	dx build dxfuse_bam_diff -f --destination dxfuse_test_data:/applets/dxfuse_bam_diff

azure: dxfuse copy_all
	dx build dxfuse_benchmark -f --destination dxfuse_azure_westus:/applets/dxfuse_benchmark
	dx build dxfuse_correctness -f --destination dxfuse_azure_westus:/applets/dxfuse_correctness

dxfuse : $(wildcard ../**/*)
	go build -o /go/bin/dxfuse /go/src/github.com/dnanexus/dxfuse/cli/main.go

copy_all : dxfuse mk_bench mk_correct mk_bio

mk_bench : dxfuse
	rm -rf dxfuse_benchmark/resources
	mkdir -p dxfuse_benchmark/resources/usr/bin
	cp /go/bin/dxfuse dxfuse_benchmark/resources/usr/bin/

mk_correct: dxfuse
	rm -rf dxfuse_correctness/resources
	mkdir -p dxfuse_correctness/resources/usr/bin
	cp /go/bin/dxfuse dxfuse_correctness/resources/usr/bin/
	cp test_routines.sh dxfuse_correctness/resources/

mk_bio : dxfuse
	cp -f /go/bin/dxfuse dxfuse_bam_diff/resources/usr/bin/

clean :
	rm -f dxfuse
	dx rm -f dxfuse_test_data:/applets/dxfuse_* || true
	dx rm -f dxfuse_azure_westus:/applets/dxfuse_* || true
