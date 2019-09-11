# The Database Schema

A local sqlite3 database is used to store filesystem information
discovered by querying DNAnexus. The database type system is a derivative of
SQL.

The `files` table maintains information for individual files.

| field name | type |  description |
| ---        | ---  |  --          |
| file\_id   | text | The DNAx file-id |
| proj\_id   | text |	A project id for the file |
| inode      | bigint  | local filesystem i-node, cannot change |
| size       | bigint  | size of the file in bytes |
| ctime      | bigint  | creation time |
| mtime      | bigint  | modification time |

It stores `stat` information on a file, and maps a file to an inode,
which is the primary key. The inode has no DNAx equivalent, however,
it cannot change once chosen. Note that a file can be hard linked from
multiple projects on DNAx, it may also be a member of a container,
instead of a project. The container field is used at download time to
inform the system which project to check for ownership. It can safely
be omitted, at the cost of additional work on the server side.

The `namespace` table stores information on the directory structure.

| field name | type | description |
| ---        | ---  | --          |
| proj\_id   | text | the project ID |
| parent     | text | the parent folder |
| name       | text | directory/file name |
| type       | smallint | directory=1, file=2 |
| inode      | bigint  | local filesystem i-node, cannot change |

For example, directory `/A/B/C` is represented with the record:
```
   proj_id : proj-xxxx
   parent : /A/B
   name : C
   fullName : /A/B/C
   type:  1
   inode: 1056
```

The primary key is `(parent,name)`. An additional index is placed on
the `parent` field, allowing an efficient query for all members of a
directory.  The DNAx object system does not adhere to POSIX. This
sometimes requires changes to file names, and directory structure.
The main exceptions are files with the same name, and
files with posix disallowed characters, such as slash (`/`).

The `directories` table stores information for individual directories.

| field name | type | description |
| ---        | ---  | --          |
| inode      | bigint |  local filesystem inode |
| populated  | smallint |  has the directory been queried? |

It maps a directory to a stable `inode`, which is the primary key. The
populated flag is zero the first time the directory is encounterd. It
is set to one, once the directory is fully described.

The local directory contents does not change after the describe calls
are complete. The only way to update the directory, in case of
changes, is to unmount and remount the filesystem.

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
