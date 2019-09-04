all : dxfs2 copy_all
	dx build benchmark -f --destination dxfs2_test_data:/applets/benchmark
	dx build correctness -f --destination dxfs2_test_data:/applets/correctness

dxfs2 :
	rm -f dxfs2
	go build -o dxfs2 /go/src/github.com/dnanexus/dxfs2/cmd/main.go

copy_all : dxfs2
	mkdir -p benchmark/resources/usr/bin
	cp -f dxfs2 benchmark/resources/usr/bin/
	mkdir -p correctness/resources/usr/bin
	cp -f dxfs2 correctness/resources/usr/bin/


exec :
	dx run applets/correctness --instance-type=mem1_ssd1_x4 -y --watch
