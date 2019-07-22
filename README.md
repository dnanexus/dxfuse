# fuse filesystem for dnanexus

A filesystem that allows users to access platform files as if they
were regular files. It is built with a [FUSE](https://bazil.org/fuse/)
library, implemented in [go](https://golang.org). The dnanexus storage
system subsumes POSIX. It holds not just files and directories, but
also records, databases, applets, and workflows. To get around this
problem, we implement a filesystem that is really just an object
disk. It's members are files of the form : `MOUNTPOINT/file-xxxx`.


# Limitations

- Works only on Linux.
- Is intended to operate on platform workers
- Read only

The current version requires specifying a list of platform files to
export. It takes a JSON manifest file of the form:

```json
{
  [{
    "id": "file-0001",
    "proj" : "project-1001",
    "ctime" : 100000011,
    "mtime" : 100029992,
    "size" : 8102
   },
  ...
  ]
}
```

# List of discrepencies with POSIX

1. A file can have multiple versions, all of which have the same name.
2. A filename can include slashes.
3. The storage system holds not just files and directories, but also records, databases, applets and workflows.
4. A file and a directory may share a name.

Other issues: there are gigantic directories, the may hold tens of thousands of files.


# Special considerations

dxfs2 operations can sometimes be slow, for example, if the
server has been temporarily shut down (503 mode). For an
interactive filesystem, this would be a big problem, because it would
freeze, causing the entire OS to freeze with it. However, in this
case, it is used for batch jobs, so we can afford to wait. Still, we
probably need to tweek the worker Linux FS timeout, which is outside of the
immediate control of dxfs2.

The code cannot use the `panic` function, because it is effectively
inside the kernel. It must methodically propagate all error codes, and
handle all error conditions.
