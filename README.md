# fuse filesystem for dnanexus

A filesystem that allows users to access platform files as if they
were regular files. It is built with a [FUSE](https://bazil.org/fuse/)
library, implemented in [go](https://golang.org). The dnanexus storage
system subsumes POSIX. It holds not just files and directories, but
also records, databases, applets, and workflows. To get around this
problem, we implement a filesystem that is really just an object
disk. It's members are files of the form : `project-xxxx:file-yyyy`,
mounted on the path `/mnt/dnanexus`. To access a dx:file, the user
much resolve it into a project-id, file-id pair. Then, he can read and
seek on the file `/mnt/dnanexus/project-<hash>:file-<hash>`.

# Limitations

- Works only on Linux, and on workers.
- The first release will be read only

# List of discrepencies with POSIX

1. A file can have multiple versions, all of which have the same name.
2. A filename can include slashes.
3. The storage system holds not just files and directories, but also records, databases, applets and workflows.
4. A file and a directory may share a name.

Other issues: there gigantic directories, the may hold tens of thousands of files.


# Special considerations

Operations of dxfuse could be very slow at times, for example, if the
server is slow, or has been temporarily shut down (503 mode). For a user
facing filesystem, this would be a big problem, because it would
freeze, causing the entire OS to freeze with it. However, in this
case, it is used for batch jobs, so we can afford to wait. Still, we
probably need to tweek the worker Linux FS timeout, which is outside of the
immediate control of dxfuse.

The code cannot use the `panic` function, because it is effectively
inside the kernel. It must methodically propagate all error codes, and
deal with them.
