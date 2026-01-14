# Release Notes
## v1.6.1 Track dxFUSE launch information
- Track dxFUSE launch information (dxFUSE version, platform, launch options, job execution context) in the `user-agent` field of `/system/greet` API call when creating a new session

## v1.6.0 -allowOverwrite mode
- New flag `-allowOverwrite` for use with -limitedWrite mode enabled. Supports opening a file with O_TRUNC to allow overwriting dxfuse backed files. This mode is in beta similar to -limitedWrite.

## v1.5.0 -stateFolder argument
- Add a `-stateFolder` argument which takes a string directory path where dxfuse will write its logfile and database. Default location if not provided is $HOME/.dxfuse, matching the behavior of previous versions of dxfuse.

## v1.4.1 Fix crash when read offset precedes cache offset
- Fix dxfuse crash for a non sequential read with an offset prior to the current sequential read cache.

## v1.4.0 Fix truncated reads from cache
- Fixed bug where dxfuse returned truncated data when it mistakenly expected data to be in its prefetch cache.
- Revert /listFolder changes from e3f92d8 as the DNAnexus API was not optimized for this.

## v1.3.0 List folder objects and describe content in a single request
- List folder objects and describe content in a single /listFolder request instead of /listFolder and then iterating with /system/findDataObjects.
- Update MacOS runner version in GHA.

## v1.2.0 Fix dnanexus files that are symlinks
- Fix downloading dnanexus files that are backed by symlinks. Previously this only worked for symlinks on a public s3 bucket.
- Update Linux build from Ubuntu 18.04 --> 20.04 github action runner as 18.04 was deprecated.

## v1.1.1 Remove command server
- Remove the command server functionality that was previously used in dxfuse -sync which was deprecated in v1.0.0. This will allow multiple dxfuse processes to run on the same machine without port conflicts.

## v1.1.0 Write temporary manifest to $HOME/.dxfuse/
- Write temporary manifest to `$HOME/.dxfuse/` instead of `/tmp/` to allow multiple users to run an instance dxfuse concurrently on the same machine.

## v1.0.1 Fix crash when file-xxxx/download returns error
- Switch back to go-sqlite3 with CGO_ENABLED build for sqlite dependency.
- Fix crash when file-xxxx/download returns error

## v1.0.0 -limitedWrite mode
- Backwards incompatible release v1.0.0.
- New flag `-limitedWrite` for supporting append only writes to DNAnexus files. See README for supported operations and limitations of this mode.
- Remove dxfuse -sync command previously used for syncing writeable files to DNAnexus.
- Enable FUSE options for kernel readahead and pagecache. Enabling pagecache adds support for shared read-only mmap which is used by many applications, e.g. gatk, tidyverse readr. Kernel readahead fixes some dxfuse crashes with prefetching and may be more stable.

## v0.25.0 Fix folders with quotes, noatime
- Mount with noatime option. Fix sql statements to handle folder names with quotes.

## v0.24.3 Fix manifest input
- Do not throw an error if a manifest file is provided as input and the file contains all of the describe information.

## v0.24.2 Support single quote ' in filename
- Utilize prepared SQL statements for queries with filenames. Allows filenames with ' single quotes.

## v0.24.1 Increase startup wait time
- Increase wait for dxfuse to initialize from 10 seconds to 90 seconds.

## v0.24.0 Optimize first read of remote directory
- Reduce time to list folder contents with thousands of objects upon first read of directory by optimizing the posix conversion for objects.
Bump to golang 1.15.7

## v0.23.3 Log go panics to logfile
- Log go panics to the dxfuse logfile by redirecting the daemon subprocess's stderr to the logfile.

## v0.23.2
- Increase the default number of retries of http requests from 3 --> 10.

## v0.23.1
- Bump dxda dependency from v0.5.1 --> v0.5.4. 

## v0.23.0
- Default to read-only mode. `-readOnly` flag is deprecated.
- Experimental `-readWrite` flag, but not guarantees are made about uploads
- Raise object limit from 10k --> 255k objects in a directory
- Continue prefetching sequential data from a file if half of the data from the previous range is read, not the entire chunk. Heuristic changed for running `plink` with dxfuse.

## v0.22.4
- Migrate to go modules.  No longer require cloning and building from within `$GOPATH`

## v0.22.3
- Rebuild with dxda update of https://github.com/dnanexus/dxda/commit/1827fc34f1cdf648a672fd75ad317c2acb6d10b3 to retry 502 errors inside a DNAnexus job

## v0.22.2
- Use `/system/findDataObjects` route for bulk object describe calls, this significantly improves performance
 when the user has access to many projects.

## v0.22.1
- Re-adding support for the `uid` and `gid` command line flags.
- Allowing the older use of mounting with `sudo`, although, this is discouraged. The new command:
```
dxfuse MNT your_project
```

The older method:
```
sudo dxfuse -uid $(id -u) -gid $(id -g) MNT your_project
```

## v0.22
- Use github actions for continuous integration (CI/CD)
- Eliminate the use of `sudo` for starting up the filesystem. Normal user permissions are now sufficient to start and stop the filesystem. This assumes that the fuse device (`/dev/fuse`) is open for read/write access to regular users.

## v0.21
- Fixed bug that occurs when a data object has properties or tags that include the apostrophe (\`) character.
- Simplified the filesystem start-up mechanism.

## v0.20
- Upgrade to golang version 1.14
- Fixed use of `defer` when there are several such statements inside one function.
- Fixed documentation for `manifest` mode, which is in the expert-options.
- Improved upload performance and added a benchmark for it. The [implementation section](README.md#Implementation) includes a table comparing `dxfuse` upload to `dx upload`. The main bottleneck for FUSE is that IOs are passing through the kernel in small synchronous chunks (128KB). This means that large file copies are slow.


## v0.19
- *Experimental support for overwriting files*. There is a limit of 16 MiB on a file that is undergoes modification. This is because it needs to first be downloaded in its entirety, before allowing any changes. It will then be uploaded to the platform. This is an expensive operation that is required because DNAnexus files are immutable.

- Removed support for hard links. The combination of hard-links, cloning on DNAx, and writable files does not work at the moment.

- Improvements to extended attributes (xattrs). The testing tool we use is `xattr`, which is native on MacOS (OSX), and can be installed with `sudo apt-get install xattr` on Linux. Xattrs can be written and removed.

Tags and properties are namespaced. For example, if `zebra.txt` is a normal text file with no DNAx tags or properties then `xattr -l` will print out all the tags, properties, and extra attributes that have no POSIX equivalent. This is split into three namespaces: _base_, _prop_, and _tag_.

```
$ xattr -l zebra.txt

base.state: closed
base.archivalState: live
base.id: file-Fjj89YQ04J96Yg3K51FKf9f7
```

Add a property named `family` with value `mammal`
```
$ xattr -w prop.family mammal zebra.txt
```

Add a tag `africa`
```
$ xattr -w tag.africa XXX zebra.txt
```

## v0.18
- Showing archived and open files; these were previously hidden. Trying to read or write from an archived
or non-closed file will cause an EACCES error.
- Presenting file properties and tags as extended attributes. To list all attributes for a file you can do:
```
$ getfattr -d -m - FILENAME
```

The `getattr` utility is part of the ubuntu attr apt package. It can be installed with:
```
$ sudo apt-get install attr
```

## v0.17
- Fixed bugs having to do with manifest mode, used in conjunction with dxWDL.

## v0.16
- Fixed bug when a file and a directory have the same name.
- Improvements to prefetch. If a file-descriptor stops accessing data sequentially, the algorithm will
give it a second chance. If access pattern becomes sequential again, then it will resume prefetching.
- Added regression test for `bam` and `samtools` bioinformatics utilities.
- Fix bug that disallowed creating empty files.
- Ignore archived files; these cannot be read without unarchival.

## v0.15
- The prefetch algorithm recognizes cases where a file is read from the middle. This
allows sambamba to work with sequential prefetch.

## v0.14
Initializing the filesystem with a subprocess. The dxfuse program spawns a subprocess that runs
the actual filesystem. The main program waits for the subprocess to start and then returns the status.
If an initialization error occurs, it is reported synchronously. This obviates the need for shell scripts like:

```
$ /go/bin/dxfuse ... &
sleep 2
```

You can just write:
```
$ /go/bin/dxfuse
```

Rename works. There is one important limitation, which is, that you cannot rename onto an existing file.
This is because dnanexus does not guaranty that remove + rename is atomic. For example, if you have
directory `A` with files `foo` and `bar`, then this works:
```
$ mv A/foo A/X
```

But this does not:
```
$ mv A/foo A/bar
```

A less important limitation is that faux directories cannot be moved. Such directories arise when a platform
directory holds files with the same name. For example:

```
$ dx ls proj-xxxx:/A

Chicago.txt : file-FgXbQ2j04J92g9b87V2zPF59
Chicago.txt : file-FgXbPG004J90ZB7q62p191vZ
NewYork.txt : file-FgXbPb004J980yX164B4KPFy
NewYork.txt : file-FgXbPJQ04J94B2xg637jQJqz
```

In the filesystem, these are represented like this:
```
A
├── 1
│   ├── Chicago.txt
│   └── NewYork.txt
├── Chicago.txt
└── NewYork.txt
```

The directory `1` is not real, it does not have a backend folder on the platform. Therefore, it cannot be moved or
modified. It may be erased, if all of its files are removed.

Hard links are supported. There are backend limitations to their use. You can make a link to a file
in project `A` from a file in project `B`. However, you will get an error if you link to a file in the same project. This is not allowed with the dnanexus `clone` operation.

Streaming speed on a cloud worker is quite good, generally on par with `dx cat`. On a remote machine streaming still works, but uses much smaller buffer sizes. Large HTTP requests tend to incur long timeouts causing streaming to slow down to a crawl.

## v0.13 21-Nov-2019
- Additional operations supported: create directory, remove directory, file unlink

## v0.12  12-Nov-2019
- Migrated to the [jacobsa](https://github.com/jacobsa/fuse) golang fuse filesystem package
- File creation supported. Such files are uploaded to the platform once they are closed. They also
become read-only on close. This mimics the DNAx behavior, which is that files are immutable.

## v0.11  26-Sep-2019
- Initial release
