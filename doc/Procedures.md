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

## Track usage with Splunk logs

Launch information is automatically recorded in the `user-agent` field of `/system/greet` API call when creating a new dxFUSE session, with the following fields included:
- dxFUSE version and platform information:`OS=Ubuntu 24.04`
- Command-line options used at launch: 
    - `Mode=ReadOnly, MountProjects=[project-xxxx, project-yyyy], Manifest=None` when using project names/ids as mount targets
    - `Mode=ReadOnly, MountProjects=[], Manifest=my_manifest.json` when using a manifest file
- Job environment details (when launched within a DNAnexus job): `JobId=job-J38vzkQ04J9GG9BY4vGFZ74Z, ExecutableName=app_test, Project=project-1111, BillTo=org-AAA, Workspace=container-2222, LaunchedBy=user-BBB`

### Common troubleshooting queries

To locate specific dxFUSE launch events, use the following sample Splunk queries. 

**Find the `billTo`s of all dxFUSE sessions**

```splunk
index="dxstaging" user-agent="dxfuse*" /system/greet 
| rex field=user-agent "(?P<billTo>(?<=BillTo=)[^,\)]+)"
```

**Analyze usage by `mode`**
```splunk
index="dxstaging" user-agent="dxfuse*" "/system/greet"
| rex field=user-agent "(?<=Mode=)(?P<mode>[^\),]+)"
| stats count by mode
```
