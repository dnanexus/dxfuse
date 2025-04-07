all: aws

aws: copy_all
	dx build benchmark -f --destination dxfuse_test_data:/applets/benchmark
	dx build correctness -f --destination dxfuse_test_data:/applets/correctness
	dx build correctness_downloads -f --destination dxfuse_test_data:/applets/correctness_downloads
	dx build bio_tools -f --destination dxfuse_test_data:/applets/bio_tools

azure: copy_all
	dx build benchmark -f --destination dxfuse_azure_westus:/applets/benchmark
	dx build correctness -f --destination dxfuse_azure_westus:/applets/correctness

copy_all: bench correct bio correct_downloads

bench:
	mkdir -p benchmark/resources/usr/bin
	cp -f ../dxfuse benchmark/resources/usr/bin/

correct:
	mkdir -p correctness/resources/usr/bin
	cp -f ../dxfuse correctness/resources/usr/bin/

correct_downloads:
	mkdir -p correctness_downloads/resources/usr/bin
	cp -f ../dxfuse correctness_downloads/resources/usr/bin/

bio:
	cp -f ../dxfuse bio_tools/resources/usr/bin/

clean:
	dx rm -f dxfuse_test_data:/applets/* || true
	dx rm -f dxfuse_azure_westus:/applets/* || true
