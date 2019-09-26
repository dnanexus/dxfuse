# Procedures

## Release check list
- Make sure regression tests pass
- Update release notes and README.md
- Make sure the version number in `defs.go` is correct. It is used
when building the release.
- Merge onto master branch, make sure [travis tests](https://travis-ci.org/dnanexus/dxfuse) pass
- Build new externally visible release.
On Linux and Mac:
```
go build -o dxfuse /go/src/github.com/dnanexus/dxfuse/cmd/main.go
```
- Update [releases](https://github.com/dnanexus/dxfuse/releases) github page,
  use the `Draft a new release` button, and upload a dxWDL.jar file.
