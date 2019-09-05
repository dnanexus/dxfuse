all : dxfs2 copy_all
	dx build dxfs2_benchmark -f --destination dxfs2_test_data:/applets/dxfs2_benchmark
	dx build dxfs2_correctness -f --destination dxfs2_test_data:/applets/dxfs2_correctness

dxfs2 :
	rm -f dxfs2
	go build -o dxfs2 /go/src/github.com/dnanexus/dxfs2/cmd/main.go

copy_all : dxfs2
	mkdir -p dxfs2_benchmark/resources/usr/bin
	cp -f dxfs2 dxfs2_benchmark/resources/usr/bin/
	mkdir -p dxfs2_correctness/resources/usr/bin
	cp -f dxfs2 dxfs2_correctness/resources/usr/bin/


exec :
	dx run dxfs2_test_data:/applets/dxfs2_correctness --instance-type=mem1_ssd1_x4 -y --watch


exec_bench :
#	dx run dxfs2_test_data:/applets/dxfs2_benchmark --instance-type=mem1_ssd1_x16 -y --watch
	dx run dxfs2_test_data:/applets/dxfs2_benchmark --instance-type=mem3_ssd1_x32 -y --watch
