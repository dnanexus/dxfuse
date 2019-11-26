# Release Notes

## v0.14
- Initializing the filesystem with a subprocess. The dxfuse program spawns a subprocess that runs
the actual filesystem. The main program waits for the subprocess to start and then returns the status.
If an initialization error occurs, it is reported synchronously. This obviates the need for shell scripts like:

```
/go/bin/dxfuse ... &
sleep 2
```

You can just write:
```
/go/bin/dxfuse
```

## v0.13 21-Nov-2019
- Additional operations supported: create directory, remove directory, file unlink

## v0.12  12-Nov-2019
- Migrated to the [jacobsa](https://github.com/jacobsa/fuse) golang fuse filesystem package
- File creation supported. Such files are uploaded to the platform once they are closed. They also
become read-only on close. This mimics the DNAx behavior, which is that files are immutable.

## v0.11  26-Sep-2019
- Initial release
