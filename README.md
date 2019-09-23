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

dxfs2 approximates a normal POSIX filesystem, but does not always have the same semantics. For example:
1. Metadata like last access time are not supported
2. Directories to not have create/modify times
3. Overwriting a file is not allowed.
4. At some point, we may allow creating new files, but this will not have any concurrency control.

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

Bandwidth when streaming a file is close to `dx cat`, but
may be a little bit lower. The benchmark was the time it takes to streaming a file
from the cloud to a local file. If you perform actual CPU calculation
when processing the data, performance differences should be unnoticeable.

## Special considerations

dxfs2 operations can sometimes be slow, for example, if the
server has been temporarily shut down (503 mode). For an
interactive filesystem, this would be a big problem, because it would
freeze, causing the entire OS to freeze with it. However, in this
case, it is used for batch jobs, so we can afford to wait.


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

Project ids can be used instead of project names. To mount several projects, say, `mammals`, `fish`, and `birds`, do:
```
sudo dxfs2 /home/jonas/foo mammals fish birds
```

This will create the directory hierarchy:
```
/home/jonas/foo
              |_ mammals
              |_ fish
              |_ birds
```

Note that files may be hard linked from several projects. These will appear as a single inode with
a link count greater than one. The filesystem logs are written to `/var/log/dxfs2.log`. To get more information in the log, use the `-verbose` flag.

# Further exploration

* Use POSIX [extended attributes](https://en.wikipedia.org/wiki/Extended_file_attributes) to
represent DNAx file tags and properties. The
[xattr](http://man7.org/linux/man-pages/man7/xattr.7.html) command
line tool can be used.

* Add the ability to create new files in an existing project. This
   strictly excludes renaming/deleting files, or modifying the
   existing directory structure. The UPLOAD applet permission is similar,
   although, it is slightly strongly.

* Present applets/workflows/records/databases as special entities. We want them to look visualy different from files/folders with standard bash tools like `ls`.


# Related projects

- [GCS FUSE](https://cloud.google.com/storage/docs/gcs-fuse)
