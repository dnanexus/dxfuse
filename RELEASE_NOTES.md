# Release Notes

## v0.13 21-Nov-2019
- Additional operations supported: create directory, remove directory, file unlink

## v0.12  12-Nov-2019
- Migrated to the [jacobsa](https://github.com/jacobsa/fuse) golang fuse filesystem package
- File creation supported. Such files are uploaded to the platform once they are closed. They also
become read-only on close. This mimics the DNAx behavior, which is that files are immutable.

## v0.11  26-Sep-2019
- Initial release
