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

Launch information is automatically recorded in the `user-agent` field of `/system/greet` API call when creating a new dxFUSE session:
```
dxfuse/v1.6.0 (OS=Ubuntu 22.04, Project=project-1111, LaunchedBy=user-BBB, MountProjects=[project-xxxx, project-yyyy], Manifest=None, Mode=AllowOverwrite, JobId=job-1234, ExecutableName=test_app, BillTo=org-AAA)
```
with following information are included:
- dxFUSE version: `dxfuse/v1.6.0`
- Platform information:`OS=Ubuntu 24.04`
- Command-line options used at launch: 
    - `Mode=ReadOnly`: one of the valid modes: ReadOnly, LimitedWrite, AllowOverwrite
    - `MountProjects=[project-xxxx, project-yyyy]`: list of projects IDs mounted if the mounting targets was specified by project IDs/names, `[]` otherwise
    - `Manifest=my_manifest.json`: path of manifest JSON file if launching dxfuse with a manifest file, `None` if not specified
- Job environment details (when launched within a DNAnexus job): 
    - `JobId=job-1234`: job ID
    - `ExecutableName=test_app`: the executable name that the job is running
    - `Project=project-1111`: project in which the job is launched
    - `BillTo=org-AAA`: billTo of the job
    - `LaunchedBy=user-BBB`: user who launched the job

### Common lookup and analysis queries

To locate specific dxFUSE launch events, use the following sample Splunk queries. 

**Find the `billTo`s of all dxFUSE sessions**

```splunk
index="dxstaging" user-agent="dxfuse*" url="/system/greet"
| rex field=user-agent "(?P<billTo>(?<=BillTo=)[^,\)]+)"
```

**Analyze usage by `mode`**
```splunk
index="dxstaging" user-agent="dxfuse*" url="/system/greet"
| rex field=user-agent "(?<=Mode=)(?P<mode>[^\),]+)"
| stats count by mode
```
