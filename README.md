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
- Limits directories to 10,000 elements
- Updates to the project emanating from other machines are not reflected locally
- Rename does not allow removing the target file or directory. This is because this cannot be
  done automatically by dnanexus.
- Does not support hard links

Updates to files are batched and asynchronously applied to the cloud
object system. For example, if `foo.txt` is updated, the changes will
not be immediately visible to another user looking at the platform
object directly. Because platform files are immutable, even a minor
modification requires rewriting the entire file, creating a new
version. This is an inherent limitation, making file update
inefficient.

## Implementation

The implementation uses an [sqlite](https://www.sqlite.org/index.html)
database, located on `/var/dxuse/metadata.db`. It stores files and
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

Creating new files, and uploading them to the platform is
significantly slower when using dxfuse. This is mostly a FUSE problem,
where IOs are handed over from user-space, to the kernel, and then to
the dxfuse server in 128KB chunks. This is fine for small files, but
becomes slow for large files. Here is a comparison between (1) `dx
upload FILE`, and (2) `cp FILE to fuse, sync fuse`.

| instance type      | dx upload (seconds) | dxfuse upload (seconds) | file size |
| ----               | ----                | ---                  |  ----     |
| mem1\_ssd1\_v2\_x4 |	11 |	99 | 705M |
| mem1_ssd1\_v2\_x4  |	5  |	24 | 285M |
| mem1\_ssd1\_v2\_x16 |	10 |	67 | 705M |
| mem1\_ssd1\_v2\_x16 |	5  |	17 | 285M |
| mem3\_ssd1\_v2\_x32 |	9  |	34 | 705M |
| mem3\_ssd1\_v2\_x32 |	6  |	12 | 285M |

# Building

To build the code from source, you'll need, at the very least, the `go` and `git` tools.
```
git clone https://github.com/dnanexus/dxfuse.git
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

There are situations where you want the background process to
synchronously update all modified and newly created files. For example, before shutting down a machine,
or unmounting the filesystem. This can be done by issuing the command:
```
$ dxfuse -sync
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

## Mac OS (OSX)

For OSX you will need to install [OSXFUSE](http://osxfuse.github.com/). Note that Your Milage May Vary (YMMV) on this platform, we are mostly focused on Linux.

# Common problems

If a project appears empty, or is missing files, it could be that the dnanexus token does not have permissions for it. Try to see if you can do `dx ls YOUR_PROJECT:`.

There is no natural match for DNAnexus applets and workflows, so they are presented as block devices. They do not behave like block devices, but the shell colors them differently from files and directories.

FUSE does not support memory mappings shared between processes ([explanation](https://github.com/jacobsa/fuse/issues/82)). This is the location in the [Linux kernel](https://elixir.bootlin.com/linux/v5.6/source/fs/fuse/file.c#L2306) where this is not allowed. For example, trying to memory-map (mmap) a file with python causes an error.

```
>>> import mmap
>>> fd = open('/home/orodeh/MNT/dxfuse_test_data/README.md', 'r')
>>> mmap.mmap(fp.fileno(), 0, mmap.PROT_READ)
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  OSError: [Errno 19] No such device
```

If the mapping is private to a single process then it does work. For example:

```
>>> import mmap
>>> fd = open('/home/orodeh/MNT/dxfuse_test_data/README.md', 'r')
>>> mmap.mmap(fd.fileno(), 0, prot=mmap.PROT_READ, flags=mmap.MAP_PRIVATE, offset=0)
>>> fd.readline()
```
