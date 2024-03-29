# Procedures

## Release check list
- Make sure regression tests pass
- Update release notes and README.md
- Make sure the version number in `utils.go` is correct. It is used
when building the release.
- Merge onto master branch, make sure [travis tests](https://travis-ci.org/dnanexus/dxfuse) pass.
- Tag release with new version:
```
git tag $version
git push origin $version
```
- Update [releases](https://github.com/dnanexus/dxfuse/releases) github page, use the `Draft a new release` button, and upload executables.
