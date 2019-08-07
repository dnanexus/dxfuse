# dxfs2: a FUSE filesystem for dnanexus

A filesystem that allows users read-only access to the
DNAnexus storage system. It is built with a
[FUSE](https://bazil.org/fuse/) library, implemented in
[golang](https://golang.org). The dnanexus storage system subsumes
POSIX. It holds not just files and directories, but also records,
databases, applets, and workflows. It allows things that are not
POSIX:
1. A file can have multiple versions, all of which have the same name.
2. A filename can include slashes.
3. A file and a directory may share a name.
4. The storage system holds not just files and directories, but also records, databases, applets and workflows.

To fit these names into a POSIX compliant filesystem, as FUSE and
Linux require, files are renamed to (1) remove slashes, (2) avoid collisions
with subdirectories, and (3) avoid collisions between files with the same names.
Generally, slashes are replaced with underscores, and suffixes of the form _NUMBER are added
to core names. For example, if file `foo.txt` has two versions in directory `bar`,
dxfs2 will present the following Unix directory structure:

```
bar/
    foo.txt
    foo_1.txt
```

The implementation uses an [sqlite](https://www.sqlite.org/index.html)
database, located on `/var/dxfs/metadata.db`. It stores files and
directories and tables, indexed for fast queries.

## Limitations

- Assumes a Linux operating system
- Operates on platform workers
- Mounted read only
- Limits directories to 10,000 elements.

## Guardrails

It is important that the filesystem limits the load it imposes on the
DNAx API servers and the cloud object system. It should use bulk calls
to describe data objects, and limit the number of parallel IO
requests.


## Performance

Bandwidth when streaming a file is lower than `dx cat`, or `dx
download`. This is because the Linux kernel currently limits FUSE IOs
to 128KB. This restriction has been lifted in
[kernel 4.20](https://github.com/torvalds/linux/commit/5da784cce4308ae10a79e3c8c41b13fb9568e4e0#diff-e3d21d11c912d0845d7a8fc1f678d4a6), which is still a ways away from running on workers.

On a cloud workstation, downloading a 200MB file is 4x faster with `dx
download`, as compared to dxfs2. On a laptop using external download
servers, dxfs2 is 15x slower. We believe this is due to the IO size
limitation.


## Special considerations

dxfs2 operations can sometimes be slow, for example, if the
server has been temporarily shut down (503 mode). For an
interactive filesystem, this would be a big problem, because it would
freeze, causing the entire OS to freeze with it. However, in this
case, it is used for batch jobs, so we can afford to wait.

The code cannot use the `panic` function, because it is effectively
inside the kernel. It must methodically propagate all error codes, and
handle all error conditions.


# Variants

## One project

The **one project** variant mounts a single platform project. It
describes directories as the user browses them, and maintains them in
memory. The assumption is that they will not change. As such, it is appropriate for
reference data, not so much for data that changes often.


## Fixed (in progress)

The **fixed** variant is intended for streaming files in WDL. It
represents all the dx:files as `/file-id/filename`. For example:

```
MOUNTPOINT/
           file-xxxx/foo.gz
           file-yyyy/bar.vcf
           file-zzzz/mouse.fasta
           ...
```

The directory hierarchy is two levels deep. Each subdirectory holds
one file, where the file name remains unchanged, and the directory is
the file-id.



# Usage

To mount a dnanexus project `mammals` on local directory `/home/jonas/foo` do:
```
sudo dxfs2 /home/jonas/foo mammals
```

To get debugging outputs, add the `debug` flag. Debugging output
will be written to stdout.

```
sudo dxfs2 -debug MOUNT-POINT PROJECT-NAME
```

Project ids can be used instead of project names.


# Further exploration

1. Use POSIX extended attributes
[xattr](https://en.wikipedia.org/wiki/Extended_file_attributes) to
represent DNAx file tags and properties. The
[xattr](http://man7.org/linux/man-pages/man7/xattr.7.html) command
line tool can be used.

2. Add the ability to create new files to an existing
   project. This does not grant the ability to rename/delete files,
   or modify the existing directory structure.

3. Mounting several projects, not just one. A key challenge is that
   files can be linked from multiple projects. This should translate
   into hard links on a POSIX filesystem.
