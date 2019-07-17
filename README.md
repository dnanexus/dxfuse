# fuse filesystem for dnanexus

Build a user-mode filesystem that allows users to access platform files like regular files. This is 
much like a watered down filesystem, that is mounted remotely. The platform is more like an object-disk than a POSIX filesystem, 
creating difficult corner cases. This is why functionality is limited to files only, no directories. 

# Limitations

Works only on Linux, and on workers. 

# Discrepencies with POSIX

1. A file can have multiple versions, all of which have the same name.
2. The storage system holds not just files and directories, but also records, databases, applets and workflows.
3. A filename can include slashes.
