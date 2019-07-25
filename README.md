# dxfs2: a FUSE filesystem for dnanexus

A filesystem that allows users read-only access to files on the
DNAnexus storage system. It is built with a
[FUSE](https://bazil.org/fuse/) library, implemented in
[go](https://golang.org). The dnanexus storage system subsumes
POSIX. It holds not just files and directories, but also records,
databases, applets, and workflows.


There are several key discrepencies with POSIX:

1. A file can have multiple versions, all of which have the same name.
2. A filename can include slashes.
3. The storage system holds not just files and directories, but also records, databases, applets and workflows.
4. A file and a directory may share a name.

Furthermore, directories can be very large, holding tens of thousands of files.


## Limitations

- Assumes a Linux operating system
- Operate on platform workers
- Mounted read only

The current version requires specifying a list of platform files to
export. It takes a JSON manifest file of the form:

```json
[ "file-xxxx", "file-yyyy", "file-zzzz" ]
```

## Performance

Bandwidth when streaming a file is lower than `dx cat`, or `dx
download`. This is because the Linux kernel currently limits FUSE IOs
to 128KB. This limitation has been lifted in
[kernel 4.20](https://github.com/torvalds/linux/commit/5da784cce4308ae10a79e3c8c41b13fb9568e4e0#diff-e3d21d11c912d0845d7a8fc1f678d4a6), which is still a ways away from running on workers.


## Special considerations

dxfs2 operations can sometimes be slow, for example, if the
server has been temporarily shut down (503 mode). For an
interactive filesystem, this would be a big problem, because it would
freeze, causing the entire OS to freeze with it. However, in this
case, it is used for batch jobs, so we can afford to wait.

The code cannot use the `panic` function, because it is effectively
inside the kernel. It must methodically propagate all error codes, and
handle all error conditions.


# Fixed

The **fixed** variant represent all the dx:files as members in a root
directory. This makes it look like this:

```
MOUNTPOINT/
           file-xxxx
           file-yyyy
           file-zzzz
           ...
```

The directory hierarchy is one level deep.


# One project

The **one project* variants mounts an entire platform project. In preparation for
the mount, metadata for all dx:files in the project is download, and
stored in a local database. The database is scanned, checking for POSIX
violations. If any are found, they are reported, and the mount
fails. Otherwise, the project is mounted with a directory
structure similar mimicking the one on the platform.
