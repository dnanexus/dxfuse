# The Database Schema

A sqlite3 database is used locally to store filesystem information
discovered by querying DNAnexus. The database type system is a derivative of
SQL.

The `files` table maintains information for individual files

| field name | type |  description |
| ---        | ---  |  --          |
| file\_id |    text | The DNAx file-id |
| proj\_id |    text | A project id this file is in |
| fname    |    text | file name |
| folder   |    text | the local folder |
| size     |    integer | size of the file in bytes |
| inode    |    integer | local inode |
| ctime    |    integer | creation time |
| mtime    |    integer | modification time |

The primary key is `(folder,fname)`. It stores `stat` information on a file, and maps a file to an inode. This is a stable mapping that is not allowed to change. The local folder is generally the same as the DNAx folder.

The `subdirs` table maintains information on the directory structure.

| field name | type | description |
| ---        | ---  | --          |
| proj\_id    | text | the project ID |
| parent     | text | the parent folder |
| dname      | text | directory name |

The primary key is `(parent,dname)`. For example, directory `/A/B/C` is represented with the record:
```
   proj_id : proj-xxxx
   parent : /A/B
   dname : C
```

This table is queries when looking for all subdirectories of `/A/B`.


The `directories` table stores information for individual directories.

| field name | type  |
| proj_id    | text  |
| dirFullName | text |
| inode      | integer |
| populated | int |

It maps a directory to a stable inode. The `populated` flag is zero
the first time the directory is encounterd. It is set to one, once the
directory is fully described. The local directory contents does not
change after the describe calls are complete. This is even if the DNAx
folder changes. In order to get updates, the filesystem needs to be
unmounted and remounted.


# Handling files with the same name -- TODO

DNAx allows ...
Expose multiple version of a file with subdirectories (1, 2, 3, ...)


# Sequential Prefetch

Performing prefetch for sequential streams incurs overhead and costs
memory. The goal of the prefetch module is: *if a file is read from start to finish, we want to be
able to read it in large network requests*. What follows is a simplified description of the algorithm.

In order for a file to be eligible for streaming it has to be at
8MiB. A bitmap is maintained for areas accessed. If the first metabyte
is accessed, prefetch is started. This entails sending multiple
asynchronous IO to fetch 4MiB of data. As long as the data
is fully read, prefetch continues. If a file is not accessed for more
than five minutes, or, access is outside the prefetched area, the process stops.


# Manifest -- TODO

Add a "manifest" version invoked by dxWDL. The dxWDL compiler
creates a manifest of the files to stream and their local locations. The filesystem then needs to support that.
