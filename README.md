# dxfuse: a FUSE filesystem for dnanexus

A filesystem that allows users access to the DNAnexus storage system.

[![Build Status](https://travis-ci.org/dnanexus/dxfuse.svg?branch=master)](https://travis-ci.org/dnanexus/dxfuse)

**NOTE: This is a project in its beta stage. We are using it on cloud workers, however, it may be run from any machine with a network connection and a DNAx account.**

The code uses the [FUSE](https://bazil.org/fuse/)
library, implemented in [golang](https://golang.org). The DNAnexus
storage system is not POSIX compilant. It holds not just files and
directories, but also records, databases, applets, and workflows. It
allows things that are not POSIX, for example:
1. Files in a directory can have the same name
2. A filename can include slashes
3. A file and a directory may share a name

To fit these names into a POSIX compliant filesystem, as FUSE and
Linux require, files are moved to avoid name collisions. For example,
if file `foo.txt` has three versions in directory `bar`, dxfuse will
present the following Unix directory structure:

```
bar/
    foo.txt
    1/foo.txt
    2/foo.txt
```

The directories `1` and `2` are new, and do not exist in the
project. If a file contains a slash, it is replaced with a triple
underscore. As a rule, directories are not moved, nor are their
characters modified. However, if a directory name contains a slash, it
is dropped, and a warning is emitted to the log.

dxfuse approximates a normal POSIX filesystem, but does not always have the same semantics. For example:
1. Metadata like last access time are not supported
2. Directories have approximate create/modify times. This is because DNAx does not keep such attributes for directories.

There are several limitations currently:
- Primarily intended for Linux, but can be used on OSX
- Limits directories to 255,000 elements
- Updates to the project emanating from other machines are not reflected locally
- Rename does not allow removing the target file or directory. This is because this cannot be
  done automatically by dnanexus.
- Does not support hard links

# Implementation

The implementation uses an [sqlite](https://www.sqlite.org/index.html)
database, located on `/var/dxfuse/metadata.db`. It stores files and
directories in tables, indexed to speed up common queries.

Load on the DNAx API servers and the cloud object system is carefully controlled. Bulk calls
are used to describe data objects, and the number of parallel IO requests is bounded.

dxfuse operations can sometimes be slow, for example, if the server is
slow to respond, or has been temporarily shut down (503 mode). This
may cause the filesystem to lose its interactive feel. Running it on a
cloud worker reduces network latency significantly, and is the way it
is used in the product. Running on a local, non cloud machine, runs
the risk of network choppiness.

Bandwidth when streaming a file is close to the dx-toolkit, but may be a
little bit lower. The following table shows performance across several
instance types. The benchmark was *how many seconds does it take to
download a file of size X?* The lower the number, the better. The two
download methods were (1) `dx cat`, and (2) `cat` from a dxfuse mount point.

| instance type   | dx cat (seconds) | dxfuse cat (seconds) | file size |
| ----            | ----             | ---                  |  ----     |
| mem1\_ssd1\_v2\_x4|	         207 |	219                 |       17G |
| mem1\_ssd1\_v2\_x4|	          66 |      	         77 |      5.9G |
| mem1\_ssd1\_v2\_x4|		6|	4 | 705M|
| mem1\_ssd1\_v2\_x4|		3|	3 | 285M|
| | | | |
| mem1\_ssd1\_v2\_x16|	57|	49 | 17G|
| mem1\_ssd1\_v2\_x16|		22|	24 | 5.9G|
| mem1\_ssd1\_v2\_x16|		3|	3 | 705M|
| mem1\_ssd1\_v2\_x16|		2|	1 | 285M|
| | | | |
| mem3\_ssd1\_v2\_x32|		52|	51 | 17G|
| mem3\_ssd1\_v2\_x32|		20	| 15 | 5.9G |
| mem3\_ssd1\_v2\_x32|		4|	2 | 705M|
| mem3\_ssd1\_v2\_x32|		2|	1 | 285M|

# Writeable mode (with limitations)

Creating new files and uploading them to the platform is allowed when dxfuse is mounted with the `-limitedWrite` flag. Writing to files is **append only**, and any non-sequential writes will return `ENOTSUP`. Seeking or reading from is not permitted while a file is being written.

**NOTE `dxfuse -limitedWrite` mode was primarly designed and written to support spark file output**

## Supported operations

`-limitedWrite` mode also enables the following operations: rename (mv), unlink (rm), mkdir (see below), and rmdir. Rewriting existing files is not permitted, nor is truncating existing files. 

### mkdir behavior

All `mkdir` operations via dxfuse are treated as `mkdir -p`. This is because dxfuse does not present the realtime state of the project. Folders can be created outside of dxfuse (or in another dxfuse process), and therefore not be visible to the current running dxfuse. A subsequent `mkdir` --> `project-xxxx/newFolder` would return a 422 error, because dxfuse did not know the directory already exists. This design is due to spark behavior where multiple worker nodes sometimes attempt to create the same output directory. 

## File upload and closing

Each file open for writing is allocated a 96MiB write buffer in memory, which is uploaded as a file part when full. Currently dxfuse will upload up to 4 parts in parallel. 
The last part upload and `file-xxxx/close` DNAx operation is called only when a file descriptor is closed and the `FlushFile` fuse operation is triggered.

### File descriptor duplication and empty files

Applications which immediately duplicate a file descriptor after opening are supported, but writing and then subsequently duplicating is not supported, as this triggers the `FlushFile` fuse operation. See the below syscall access pattern which is supported, as the `FlushFile` op triggered by `close(3)` is ignored because no data has been written to the file yet.
```
# Supported
openat(AT_FDCWD, "MNT/project/writefile", O_WRONLY|O_CREAT|O_TRUNC, 0666) = 3
dup2(3, 1)                              = 1
# Triggers a FlushFile ignored by dxfuse since the file is empty
close(3)                                = 0
read(0, "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"..., 1024) = 1024
write(1, "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"..., 1024) = 1024
```
The example below closes the file descriptor after writing, which closes the dnanexus file, causing subsequent writes to fail.
```
# Not supported
openat(AT_FDCWD, "MNT/project/writefile", O_WRONLY|O_CREAT|O_TRUNC, 0666) = 3
read(0, "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"..., 1024) = 1024
write(3, "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"..., 1024) = 1024
dup2(3, 1)                              = 1
# Triggers a FlushFile --> file-xxxx/close by dxfuse since the file size is greater than 0
close(3)                                = 0
# Returns EPERM since the file has been closed already
write(1, "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"..., 1024) = -1
```

Ignoring the `FlushFile` op creates an edge case for creating empty files. For empty files the empty part upload and `file-xxxx/close` are not called until the `ReleaseFileHandle` fuse op is triggered by the kernel. This is only triggered when all open file descriptors have been closed. The downside of this behavior is that the application which is writing does not wait for this operation to return as it does for regular files closed via `FlushFile`. 

## Spark output

dxfuse in `-limitedWrite` mode supports output from the `file:///` protocol in spark. 

## Upload benchmarks

Upload benchmarks are from an AWS m5n.xlarge instance running Ubuntu 18.04 with kernel 5.4.0-1048-aws. 
`dx` and `dxfuse` benchmark commands were run like so. These benchmarks are not exact because they include the wait time until the uploaded file is transitioned to the `closed` state. 

`time dd if=/dev/zero bs=1M count=$SIZE | dx upload -`

`time dd if=/dev/zero bs=1M count=$SIZE of=MNT/project/$SIZE` 
| dx upload --wait (seconds) | dxfuse upload(seconds) | file size |
| ---                        |  ----                  | ----      |
|	4.4 |	5.6| 100M |
|	9  |	6.1 | 200M |
|	8.8 |	6.7 | 400M |
|	8.8 |	12.8 | 800M |
|	18 |	19 | 1600M |
|	32 |	24 | 3200M |
|	79  |	65 | 10000M |

# Building

To build the code from source, you'll need, at the very least, the `go` and `git` tools.
```
git clone git@github.com:dnanexus/dxfuse.git
cd dxfuse
go build -o dxfuse cli/main.go
```

# Usage

Allow regular users access to the fuse device on the local machine:
```
chmod u+rw /dev/fuse
```

In theory, it should be possible to use `suid` to achive this instead, but that does
not currently work.

To mount a dnanexus project `mammals` on local directory `/home/jonas/foo` do:
```
dxfuse /home/jonas/foo mammals
```

The bootstrap process has some asynchrony, so it could take it a
second two to start up. It spawns a separate process for the filesystem
server, waits for it to start, and exits. To get more information, use
the `verbose` flag. Debugging output is written to the log, which is
placed at `$HOME/.dxfuse/dxfuse.log`. The maximal verbosity level is 2.

```
dxfuse -verbose 1 MOUNT-POINT PROJECT-NAME
```

Project ids can be used instead of project names. To mount several projects, say, `mammals`, `fish`, and `birds`, do:
```
dxfuse /home/jonas/foo mammals fish birds
```

This will create the directory hierarchy:
```
/home/jonas/foo
              |_ mammals
              |_ fish
              |_ birds
```

Note that files may be hard linked from several projects. These will appear as a single inode with
a link count greater than one.

To stop the dxfuse process do:
```
fusermount -u MOUNT-POINT
```

## Extended attributes (xattrs)

DNXa data objects have properties and tags, these are exposed as POSIX extended attributes. Xattrs can be read, written, and removed. The package we use here is `attr`, it can installed with `sudo apt-get install attr` on Linux. On OSX the `xattr` package comes packaged with the base operating system, and can be used to the same effect.

DNAx tags and properties are prefixed. For example, if `zebra.txt` is a file then `attr -l zebra.txt` will print out all the tags, properties, and attributes that have no POSIX equivalent. These are split into three correspnding prefixes _tag_, _prop_, and _base_ all under the `user` Linux namespace.

Here `zebra.txt` has no properties or tags.
```
$ attr -l zebra.txt

base.state: closed
base.archivalState: live
base.id: file-xxxx
```

Add a property named `family` with value `mammal`
```
$ attr -s prop.family -V mammal zebra.txt
```

Add a tag `africa`
```
$ attr -s tag.africa -V XXX zebra.txt
```

Remove the `family` property:
```
$ attr -r prop.family zebra.txt
```

You cannot modify _base.*_ attributes, these are read-only. Currently, setting and deleting xattrs can be done only for files that are closed on the platform.

## macOS

For OSX you will need to install [OSXFUSE](http://osxfuse.github.com/). Note that Your Milage May Vary (YMMV) on this platform, we are mostly focused on Linux.

Feaures such as kernel read-ahead, pagecache, mmap, and PID tracking may not work on macOS.

## mmap

dxfuse supports shared read-only mmap for remote files. This is only possible with FUSE when both the kernel pagecache and kernel readahead options are both enabled for the FUSE mount, which may have other side effects of increased memory usage (pagecache) and more remote read requests (readahead).

```
>>> import mmap
>>> fd = open('MNT/dxfuse_test_data/README.md', 'r')
mmap.mmap(fd.fileno(), 0, flags=mmap.MAP_SHARED, prot=mmap.PROT_READ)
<mmap.mmap object at 0x7f9cadd87770>
```

# Common problems

If a project appears empty, or is missing files, it could be that the dnanexus token does not have permissions for it. Try to see if you can do `dx ls YOUR_PROJECT:`.

There is no natural match for DNAnexus applets and workflows, so they are presented as block devices. They do not behave like block devices, but the shell colors them differently from files and directories.
