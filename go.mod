module github.com/dnanexus/dxfuse

go 1.15

require (
	github.com/dnanexus/dxda v0.5.5
	github.com/jacobsa/fuse v0.0.0-20210330112455-9677d0392291
	github.com/mattn/go-sqlite3 v1.14.7
	github.com/pbnjay/memory v0.0.0-20201129165224-b12e5d931931 // indirect
	golang.org/x/exp v0.0.0-20200513190911-00229845015e
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
)

replace github.com/jacobsa/fuse => /home/kjensen/repos/fuse-1
