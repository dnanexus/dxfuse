# dxfuse: a FUSE filesystem for DNAnexus

dxfuse is a FUSE (Filesystem in Userspace) implementation that allows users to mount DNAnexus cloud storage as a local POSIX-compliant filesystem. The code uses the [FUSE](https://github.com/jacobsa/fuse)
library, implemented in [golang](https://golang.org). It bridges the gap between DNAnexus's cloud storage system and traditional Unix filesystem expectations.

**For support issues please contact support@dnanexus.com**

**NOTE: This project is designed for read-only use in the DNAnexus worker environment. Use in any other environment (such as macOS or Linux clients) or use of `-limitedWrite` is in beta**

# Usage

To allow regular users access to the fuse device on the local machine:
```
chmod u+rw /dev/fuse
```
In theory, it should be possible to use `suid` to achieve this instead, but that does
not currently work.

## mount a single project

To mount a DNAnexus project `mammals` in local directory `/home/jonas/foo` do:
```
dxfuse /home/jonas/foo mammals
```
Project ids can be used instead of project names. 
```
dxfuse /home/jonas/foo project-12345678
```

Note that dxfuse will hide any existing content of the mount point (e.g. `/home/jonas/foo` directory in the example above) 
until the dxfuse process is stopped.

The bootstrap process is done asynchronously, so it could take a
second or two to start up. It spawns a separate process for the filesystem
server, waits for it to start, and exits. 

## set debug level
To get more information, use
the `verbose` flag. Debugging output is written to the log, which is
placed at `$HOME/.dxfuse/dxfuse.log`. The maximal verbosity level is 2.

```
dxfuse -verbose 1 MOUNT-POINT PROJECT-NAME
```
## mount multiple projects
To mount several projects, say, `mammals`, `fish`, and `birds`, do:
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

Note that same DNAnexus file (with the same fileID) may be resided in several projects, each link will be assigned to a distinct inode. And hard linking mounted files is not currently supported.

## mount with manifest
The manifest option specifies the initial snapshot of the filesystem
tree as a JSON file. The database is initialized from this snapshot,
and the filesystem starts its life from that point. This is useful
for WDL, where we want to mount remote files and directories on a
pre-specified tree.

The manifest contains two tables, one for individual files to be mounted, and the other for directories. To learn how to write the manifest, please refer to the [manifest schema](doc/Internals.md#manifest).

## limitedWrite and allowOverwrite mode

dxfuse provides three modes for mounting:
- (default) read-only
- `-limitedWrite` : allows creating new file
- `-limitedWrite` + `-allowOverwrite`: allows creating new file and overwrite existing file.
See detailed behaviors [here](./doc/Implementation.md#semantics)

## stop mounting
To stop the dxfuse process do:
```
fusermount -u MOUNT-POINT
```

# Supported Filesystem Operations and corresponding DNAnexus API

See [supported operations](doc/Implementation.md#filesystem-operations)

# POSIX approximation
The DNAnexus storage system is not POSIX compliant. It holds not just files and
directories, but also records, databases, applets, and workflows. It allows things that are not POSIX, for example:
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
- Does not support hard links
- limitedWrite mode has additional limitations. See details in the [Limited Write Mode](doc/Implementation.md#limitedwrite-mode) section


# Building

To build the code from source, you'll need, at the very least, the `go` and `git` tools.
```
git clone git@github.com:dnanexus/dxfuse.git
cd dxfuse
go build -o dxfuse cli/main.go
```


## macOS

For OSX you will need to install [macFUSE](https://osxfuse.github.io/). Note that Your Milage May Vary (YMMV) on this platform, we are mostly focused on Linux.

Features such as kernel read-ahead, pagecache, mmap, and PID tracking may not work on macOS.

## mmap

dxfuse supports shared read-only mmap for remote files. This is only possible with FUSE when both the kernel pagecache and kernel readahead options are both enabled for the FUSE mount, which may have other side effects of increased memory usage (pagecache) and more remote read requests (readahead).

```
>>> import mmap
>>> fd = open('MNT/dxfuse_test_data/README.md', 'r')
mmap.mmap(fd.fileno(), 0, flags=mmap.MAP_SHARED, prot=mmap.PROT_READ)
<mmap.mmap object at 0x7f9cadd87770>
```

# Common problems

If a project appears empty, or is missing files, it could be that the DNAnexus token does not have permissions for it. Try to see if you can do `dx ls YOUR_PROJECT:`.

There is no natural match for DNAnexus applets and workflows, so they are presented as block devices. They do not behave like block devices, but the shell colors them differently from files and directories.
