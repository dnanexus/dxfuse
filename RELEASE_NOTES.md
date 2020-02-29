# Release Notes

## v0.19.1
- Upgrade to go 1.14
- Fixed use of `defer` when there are several such statements inside one function.
- Fixed documentation for `manifest` mode, which is in the expert-options.


## v0.19
- *Experimental support for overwriting files*. There is a limit of 16 MiB on a file that is undergoes modification. This is because it needs to first be downloaded in its entirety, before allowing any changes. It will then be uploaded to the platform. This is an expensive operation that is required because DNAnexus files are immutable.

- Removed support for hard links. The combination of hard-links, cloning on DNAx, and writable files does not work at the moment.

- Improvements to extended attributes (xattrs). The testing tool we use is `xattr`, which is native on MacOS (OSX), and can be installed with `sudo apt-get install xattr` on Linux. Xattrs can be written and removed.

Tags and properties are namespaced. For example, if `zebra.txt` is a normal text file with no DNAx tags or properties then `xattr -l` will print out all the tags, properties, and extra attributes that have no POSIX equivalent. This is split into three namespaces: _base_, _prop_, and _tag_.

```
$ xattr -l zebra.txt

base.state: closed
base.archivalState: live
base.id: file-Fjj89YQ04J96Yg3K51FKf9f7
```

Add a property named `family` with value `mammal`
```
$ xattr -w prop.family mammal zebra.txt
```

Add a tag `africa`
```
$ xattr -w tag.africa XXX zebra.txt
```

## v0.18
- Showing archived and open files; these were previously hidden. Trying to read or write from an archived
or non-closed file will cause an EACCES error.
- Presenting file properties and tags as extended attributes. To list all attributes for a file you can do:
```
$ getfattr -d -m - FILENAME
```

The `getattr` utility is part of the ubuntu attr apt package. It can be installed with:
```
$ sudo apt-get install attr
```

## v0.17
- Fixed bugs having to do with manifest mode, used in conjunction with dxWDL.

## v0.16
- Fixed bug when a file and a directory have the same name.
- Improvements to prefetch. If a file-descriptor stops accessing data sequentially, the algorithm will
give it a second chance. If access pattern becomes sequential again, then it will resume prefetching.
- Added regression test for `bam` and `samtools` bioinformatics utilities.
- Fix bug that disallowed creating empty files.
- Ignore archived files; these cannot be read without unarchival.

## v0.15
- The prefetch algorithm recognizes cases where a file is read from the middle. This
allows sambamba to work with sequential prefetch.

## v0.14
Initializing the filesystem with a subprocess. The dxfuse program spawns a subprocess that runs
the actual filesystem. The main program waits for the subprocess to start and then returns the status.
If an initialization error occurs, it is reported synchronously. This obviates the need for shell scripts like:

```
$ /go/bin/dxfuse ... &
sleep 2
```

You can just write:
```
$ /go/bin/dxfuse
```

Rename works. There is one important limitation, which is, that you cannot rename onto an existing file.
This is because dnanexus does not guaranty that remove + rename is atomic. For example, if you have
directory `A` with files `foo` and `bar`, then this works:
```
$ mv A/foo A/X
```

But this does not:
```
$ mv A/foo A/bar
```

A less important limitation is that faux directories cannot be moved. Such directories arise when a platform
directory holds files with the same name. For example:

```
$ dx ls proj-xxxx:/A

Chicago.txt : file-FgXbQ2j04J92g9b87V2zPF59
Chicago.txt : file-FgXbPG004J90ZB7q62p191vZ
NewYork.txt : file-FgXbPb004J980yX164B4KPFy
NewYork.txt : file-FgXbPJQ04J94B2xg637jQJqz
```

In the filesystem, these are represented like this:
```
A
├── 1
│   ├── Chicago.txt
│   └── NewYork.txt
├── Chicago.txt
└── NewYork.txt
```

The directory `1` is not real, it does not have a backend folder on the platform. Therefore, it cannot be moved or
modified. It may be erased, if all of its files are removed.

Hard links are supported. There are backend limitations to their use. You can make a link to a file
in project `A` from a file in project `B`. However, you will get an error if you link to a file in the same project. This is not allowed with the dnanexus `clone` operation.

Streaming speed on a cloud worker is quite good, generally on par with `dx cat`. On a remote machine streaming still works, but uses much smaller buffer sizes. Large HTTP requests tend to incur long timeouts causing streaming to slow down to a crawl.

## v0.13 21-Nov-2019
- Additional operations supported: create directory, remove directory, file unlink

## v0.12  12-Nov-2019
- Migrated to the [jacobsa](https://github.com/jacobsa/fuse) golang fuse filesystem package
- File creation supported. Such files are uploaded to the platform once they are closed. They also
become read-only on close. This mimics the DNAx behavior, which is that files are immutable.

## v0.11  26-Sep-2019
- Initial release
