# Implementations

## Filesystem Operations
### **Main Entry Point**
`main.go`: 
  - Parse command line arguments to create mount configuration
  - Load or create manifest
  - Setup work directory (log, manifest, metadata file)
  - Create the dxfuse filesystem, mount it using the FUSE kernel interface, and start the daemon process.

### **Operations**
The `Filesys` struct in `dxfuse.go` implements the FUSE filesystem interface with these key methods:
| Operation Category | Supported | Mode | Operation Name | Function Explanation | DNAnexus API Endpoint | dx CLI Command |
|-------------------|-----------|------|----------------|---------------------|----------------------|----------------|
| **Directory Operations** | ✅ | All | `LookUpInode()` | Find files/directories by name | `/describe` | `dx describe` |
| | ✅ | All | `OpenDir()` | Open directory for reading | | |
| | ✅ | All | `ReadDir()` | List directory contents | `/listFolder` | `dx ls` |
| | ✅ | limitedWrite, allowOverwrite | `MkDir()` | Create directories | `/newFolder` | `dx mkdir` |
| | ✅ | limitedWrite, allowOverwrite | `RmDir()` | Remove directories | `/removeFolder` | `dx rmdir` |
| **File Operations** | ✅ | All | `OpenFile()` | Open files for reading/writing | | |
| | ✅ | All | `ReadFile()` | Read file content from DNAnexus | `/download` | `dx download` |
| | ✅ | limitedWrite, allowOverwrite | `WriteFile()` | Write data to new files | `/upload` | `dx upload` |
| | ✅ | limitedWrite, allowOverwrite | `CreateFile()` | Create new files | `/file/new` | `dx upload` |
| | ✅ | limitedWrite, allowOverwrite | `FlushFile()` | Flush file data to storage | `/closeFile` | |
| | ✅ | limitedWrite, allowOverwrite | `ReleaseFileHandle()` | Release file handle and upload | `/closeFile` | |
| **Inode Operations** | ✅ | limitedWrite, allowOverwrite | `Rename()` | Move/rename files and directories | `/move` | `dx mv` |
| | ✅ | limitedWrite, allowOverwrite | `Unlink()` | Delete files | `/removeObjects` | `dx rm` |
| | ✅ | limitedWrite (limited) | `SetInodeAttributes()` | Modify file metadata(limited) | | |
| | ✅ | All | `GetInodeAttributes()` | Get file/directory metadata | `/describe` | `dx describe` |
| **Extended Attributes** | ✅ | All | `GetXattr()` | Get extended attribute values | `/describe` | `dx describe` |
| | ✅ | limitedWrite, allowOverwrite | `SetXattr()` | Set extended attribute values | `/setProperties`, `/addTags` | `dx set_properties`, `dx tag` |
| | ✅ | All | `ListXattr()` | List all extended attributes | `/describe` | `dx describe` |
| | ✅ | limitedWrite, allowOverwrite | `RemoveXattr()` | Remove extended attributes | `/setProperties`, `/removeTags` | `dx unset_properties`, `dx untag` |
| **Hard/Symbolic Links** | ❌ | None | `CreateLink()` | Creating hard links | | |
| | ❌ | None | `CreateSymlink()` | Creating symbolic links | | |
| | ❌ | None | `ReadSymlink()` | Reading symbolic link targets | | |
| **Advanced Attributes** | ❌ | None | `SetInodeAttributes()` (timestamps) | File timestamps (atime, ctime) | | |
| | ❌ | None | `SetInodeAttributes()` (ownership) | File ownership (uid, gid) | | |
| | ❌ | None | `SetInodeAttributes()` (truncation) | File truncation (size) | | |
| **File Locking** | ❌ | None | POSIX file locking | flock, fcntl locking operations | | |
| **Memory Mapping** | ❌ | None | Direct memory mapping | Direct memory mapping of files | | |

See [DNAnexus Documentation](https://documentation.dnanexus.com/developer) for more info on DNAnexus API and CLI commands

### Supporting Modules
  - **Database Layer (`metadata_db.go`):**  
    SQLite database for caching file/directory metadata and managing inode mapping between local and DNAnexus objects. Schemas could be found in [Internals.md](./Internals.md)
  
  - **DNAnexus Operations (`dx_ops.go`):**  
    API client for DNAnexus platform; handles file upload, download, folder operations, project and permission management.
  - **POSIX Compliance (`posix.go`):**  
    Resolves naming conflicts, creates "faux" directories for version management, normalizes non-POSIX filenames.
  - **File Upload (`upload.go`):**  
    Manages parallel multipart uploads, buffers write operations, handles streaming uploads for large files.
  - **Prefetch & Caching (`prefetch.go`):**  
    Optimizes read performance with predictive fetching and caching.

## Semantics
### Read and Sequential Prefetch
Performing prefetch for sequential streams incurs overhead and costs
memory. The goal of the prefetch module is: *if a file is read from start to finish, we want to be
able to read it in large network requests*. What follows is a simplified description of the algorithm.

In order for a file to be eligible for streaming it has to be at
8MiB. A bitmap is maintained for areas accessed. If a complete MiB
is accessed, prefetch is started. This entails sending multiple
asynchronous IO to fetch up to 16 (remote machine) or 96 (worker) MiB of data. As long as the data
is fully read, prefetch continues. If a file is not accessed for more
than five minutes, or, access is outside the prefetched area, the process halts. It will start again if sequential access is detected down the road.

### File Creation and Write

#### limitedWrite Mode

`dxfuse -limitedWrite` mode was primarily designed to support spark file output over the `file:///` protocol.

Creating and writing to new files is allowed when dxfuse is mounted with the `-limitedWrite` flag.

Under `-limitedWrite` mode, when a file is created, a remote DNAnexus file object is created. Writing to files is **append only**: sequential write operations are first written to a buffer in memory which maps to a DNAnexus file object part. Part sizes increase as file size grows. Once writing is complete and the file is flushed, the remote file object is closed immutably and can no longer be appended to.

Any non-sequential writes will return `ENOTSUP`. Seeking or reading operations are not permitted while a file is being written.

`-limitedWrite` mode also enables the following operations: 
- rename (`mv`, see [below](#rename-file-or-directory))
- remove (`rm` and `rmdir`)
- mkdir (see [below](#make-directory))
- rmdir (empty folders only). 

Rewriting existing files is not permitted, nor is truncating existing files, unless using `-allowOverwrite` flag at the same time. 

#### allowOverwrite Mode
If `-limitedWrite` and `-allowOverwrite` flags are both set when launching dxfuse, overwriting existing files (both created before or during the current session) is enabled.

When opening an existing file, the corresponding remote file will be removed and the same inode will be linked to a new open file. The following write operation could be done in the same behavior as in `limitedWrite` mode, starting from write offset = 0. 

### Make directory

All `mkdir` operations via dxfuse are treated as `mkdir -p`, which creates the parent directory if needed, and does not fail if the directory already exists. This is because dxfuse does not present the realtime state of the project. Folders can be created outside of dxfuse (or in another dxfuse process), and therefore not be visible to the current running dxfuse. A subsequent `mkdir` --> `project-xxxx/newFolder` would return a 422 error, because dxfuse did not know the directory already exists. This design is due to spark behavior where multiple worker nodes sometimes attempt to create the same output directory.

### Rename File or Directory

Rename does not allow removing the target file or directory because DNAnexus API does not support this functionality. Removal of target file or directory must be done as a separate operation before calling the rename (mv) operation.

```
$ ls -lht MNT/file*
-r--r--r-- 1 root root 6 Aug 20 21:23 file1
-r--r--r-- 1 root root 6 Aug 20 21:23 file
$ mv MNT/file MNT/file1
mv: cannot move 'file' to 'file1': Operation not permitted

# Supported via 2 distinct operations:
$ rm MNT/file1
$ mv MNT/file MNT/file1
```

### File upload and closing

Each dxfuse file open for writing is allocated a 16MiB write buffer in memory, which is uploaded as a DNAnexus file part when full. This buffer increases in size for each part `1.1^n * 16MiB` up to a maximum 700MiB. dxfuse uploads up to 4 parts in parallel across all files being uploaded.

The upload of the last DNAnexus file part and the call of `file-xxxx/close` DNAnexus API operation are performed by dxfuse only when the OS process that created the OS file descriptor closes the OS file descriptor, triggering `FlushFile` fuse operation.

### File descriptor duplication and empty files

Applications which immediately duplicate a file descriptor after opening it are supported, but writing and the subsequent file descriptor duplication  is not supported, as this triggers the `FlushFile` fuse operation. See the below supported syscall access pattern, where `FlushFile` op triggered by `close(3)` is ignored for an empty open file.
```
# Supported access pattern
openat(AT_FDCWD, "MNT/project/writefile", O_WRONLY|O_CREAT|O_TRUNC, 0666) = 3
dup2(3, 1)                              = 1
# Triggers a FlushFile ignored by dxfuse since the file is empty
close(3)                                = 0
read(0, "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"..., 1024) = 1024
write(1, "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"..., 1024) = 1024
```
The example below closes the file descriptor after writing, which closes the DNAnexus file, causing subsequent writes to fail.
```
# Unsupported access pattern
openat(AT_FDCWD, "MNT/project/writefile", O_WRONLY|O_CREAT|O_TRUNC, 0666) = 3
read(0, "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"..., 1024) = 1024
write(3, "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"..., 1024) = 1024
dup2(3, 1)                              = 1
# Triggers a FlushFile --> file-xxxx/close by dxfuse since the file size is greater than 0
close(3)                                = 0
# Returns EPERM error since the file has been closed already
write(1, "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"..., 1024) = -1
```

Ignoring the `FlushFile` op for empty files creates an edge case for creating empty files in dxfuse-mounted folders. For empty files, the empty part upload and `file-xxxx/close` are not called until the `ReleaseFileHandle` fuse operation is triggered by the kernel when the last open file descriptor for a given file has been closed. The downside of this behavior is that the dxfuse client application creating the empty file is unable to catch errors that may happen during `file-xxxxx/close` API call as it does for non-empty files closed via `FlushFile` fuse operation triggered by application's call to `close(3)`.


### File closing error checking

dxfuse clients should check errors from `close(3)` call to make sure the corresponding DNAnexus file has been transitioned out of the `open` state,
as DNAnexus files left in open state are eventually removed by the DNAnexus cleanup daemon.

### Spark output artifacts

Spark output through dxfuse uses the spark `file://` protocol. Due to this each output produced by spark will have a corresponding `.crc` file. These files can be removed. 

## Extended attributes (xattrs)

DNAx data objects have properties and tags, these are exposed as POSIX extended attributes. Xattrs can be read, written, and removed. The package we use here is `attr`, it can installed with `sudo apt-get install attr` on Linux. On OSX the `xattr` package comes packaged with the base operating system, and can be used to the same effect.

DNAx tags and properties are prefixed. For example, if `zebra.txt` is a file then `attr -l zebra.txt` will print out all the tags, properties, and attributes that have no POSIX equivalent. These are split into three corresponding prefixes _tag_, _prop_, and _base_ all under the `user` Linux namespace.

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

You cannot modify _base.*_ attributes, these are read-only. Setting and deleting xattrs can be done only for files that are closed on the platform.
