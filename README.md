# dxfuse: a FUSE filesystem for dnanexus

A filesystem that allows users read-only access to the DNAnexus
storage system.

[![Build Status](https://travis-ci.org/dnanexus/dxfuse.svg?branch=master)](https://travis-ci.org/dnanexus/dxfuse)

**NOTE: This is a project in its early stage. We are using in carefully controlled situations on the cloud, however, it may be run from any machine with a network connection. At this point, it requires good connectivity to the DNAnexus platform. Using it elsewhere may result in sluggish behavior, or the OS unmounting the system.**

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
3. Files are immutable

There are several limitations currently:
- Assumes a Linux operating system
- Intended to operate on platform workers
- Mounted read only
- Limits directories to 10,000 elements

## Implementation

The implementation uses an [sqlite](https://www.sqlite.org/index.html)
database, located on `/var/dxfs/metadata.db`. It stores files and
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

| instance type   | method | seconds | file size |
| ----            | ----   | ----    |  ----     |
| mem1\_ssd1\_x4  | dx-cat | 73| 5.9G |
| mem1\_ssd1\_x4  | dxfuse  | 74| 5.9G |
| mem1\_ssd1\_x4  | dx-cat | 7| 705M |
| mem1\_ssd1\_x4  | dxfuse  | 8| 705M |
| mem1\_ssd1\_x4  | dx-cat | 3| 285M |
| mem1\_ssd1\_x4  | dxfuse  | 4| 285M |
| mem1\_ssd1\_x16 | dx-cat | 27| 5.9G |
| mem1\_ssd1\_x16 | dxfuse  | 28| 5.9G |
| mem1\_ssd1\_x16 | dx-cat | 4| 705M |
| mem1\_ssd1\_x16 | dxfuse  | 5| 705M |
| mem1\_ssd1\_x16 | dx-cat | 2| 285M |
| mem1\_ssd1\_x16 | dxfuse  | 2| 285M |
| mem3\_ssd1\_x32 | dx-cat | 25| 5.9G |
| mem3\_ssd1\_x32 | dxfuse  | 30| 5.9G |
| mem3\_ssd1\_x32 | dx-cat | 5| 705M |
| mem3\_ssd1\_x32 | dxfuse  | 4| 705M |
| mem3\_ssd1\_x32 | dx-cat | 2| 285M |
| mem3\_ssd1\_x32 | dxfuse  | 2| 285M |

# Building

To build the code from source, you'll need, at the very least, the `go` and `git` tools.
Assuming the go directory is `/go`, then, clone the code with:
```
git clone git@github.com:dnanexus/dxfuse.git
```

Build the code:
```
go build -o /go/bin/dxfuse /go/src/github.com/dnanexus/cmd/main.go
```

# Usage

To mount a dnanexus project `mammals` on local directory `/home/jonas/foo` do:
```
sudo dxfuse /home/jonas/foo mammals &
```

The bootstrap process has some asynchrony, so it could take it a
second two to start up. To get more information, use the `verbose`
flag. Debugging output is written to the log, which is placed in
`/var/log/dxfuse.log`. The maximal verbosity level is 2.

```
sudo dxfuse -verbose 1 MOUNT-POINT PROJECT-NAME &
```

Project ids can be used instead of project names. To mount several projects, say, `mammals`, `fish`, and `birds`, do:
```
sudo dxfuse /home/jonas/foo mammals fish birds &
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


# Common problems

If a project appears empty, or is missing files, it could be that the dnanexus token does not have permissions for it. Try to see if you can do `dx ls YOUR_PROJECT:`.
