# Release Notes

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
modified.

## v0.13 21-Nov-2019
- Additional operations supported: create directory, remove directory, file unlink

## v0.12  12-Nov-2019
- Migrated to the [jacobsa](https://github.com/jacobsa/fuse) golang fuse filesystem package
- File creation supported. Such files are uploaded to the platform once they are closed. They also
become read-only on close. This mimics the DNAx behavior, which is that files are immutable.

## v0.11  26-Sep-2019
- Initial release
