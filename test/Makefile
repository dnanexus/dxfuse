all : dxfs2 copy_all
	dx build dxfs2_benchmark -f --destination dxfs2_test_data:/applets/dxfs2_benchmark
	dx build dxfs2_correctness -f --destination dxfs2_test_data:/applets/dxfs2_correctness
	dx build dxfs2_download_only -f --destination dxfs2_test_data:/applets/dxfs2_download_only

dxfs2 :
	go build -o dxfs2 /go/src/github.com/dnanexus/dxfs2/cmd/main.go

copy_all : dxfs2
	mkdir -p dxfs2_benchmark/resources/usr/bin
	cp -f dxfs2 dxfs2_benchmark/resources/usr/bin/
	mkdir -p dxfs2_correctness/resources/usr/bin
	cp -f dxfs2 dxfs2_correctness/resources/usr/bin/
	mkdir -p dxfs2_download_only/resources/usr/bin
	cp -f dxfs2 dxfs2_download_only/resources/usr/bin/

clean :
	rm -f dxfs2
	dx rm -f dxfs2_test_data:/applets/dxfs2_* || true
