## The Database Schema

A local sqlite3 database is used to store filesystem information
discovered by querying DNAnexus.

The `files` table maintains information for individual files.

| field name | SQL type |  description |
| ---        | ---  |  --          |
| inode      | bigint  | local filesystem i-node, cannot change |
| file\_id   | text | The DNAx file-id |
| proj\_id   | text |	A project id for the file |
| size       | bigint  | size of the file in bytes |
| ctime      | bigint  | creation time |
| mtime      | bigint  | modification time |
| nlink      | int     | number of hard links to this file |

It stores `stat` information on a file, and maps a file to an inode,
which is the primary key. The inode has no DNAx equivalent, however,
it cannot change once chosen. Note that a file can be hard linked from
multiple projects on DNAx, it may also be a member of a container,
instead of a project. The container field is used at download time to
inform the system which project to check for ownership. It can safely
be omitted, at the cost of additional work on the server side.

The `namespace` table stores information on the directory structure.

| field name | SQL type | description |
| ---        | ---  | --          |
| parent     | text | the parent folder |
| name       | text | directory/file name |
| obj\_type  | int  | directory=1, file=2 |
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
directory. The DNAx object system does not adhere to POSIX. This
sometimes requires changes to file names, and directory structure.
The main difficulties are files with the same name, and
files with posix disallowed characters, such as slash (`/`).

The `directories` table stores information for individual directories.

| field name | SQL type | description |
| ---        | ---  | --          |
| inode      | bigint |  local filesystem inode |
| proj\_id   | text | Project id the directory belongs to |
| proj\_folder | text | corresponding folder on dnanexus |
| populated  | int |  has the directory been queried? |
| ctime      | bigint  | creation time |
| mtime      | bigint  | modification time |

It maps a directory to a stable `inode`, which is the primary key. The
populated flag is zero the first time the directory is encounterd. It
is set to one, once the directory is fully described. The `ctime` and `mtime`
are approximated by using the project timestamps. All directories are
associated with a project, except the root. The root can hold multiple directories,
each representing a different project. This is why the root will have an empty `proj\_id`,
and an empty `proj\_folder`.

The local directory contents does not change after the describe calls
are complete. The only way to update the directory, in case of
changes, is to unmount and remount the filesystem.

DNAx allows multiple files in a directory to have the same name. This
violates POSIX, and cannot be presented in a FUSE filesystem. It is
possible to resolve this, by mangling the original filenames, for
example, by adding `_1`, `_2` suffixes. However, this can cause name
collisions, and will certainly make it harder to understand which file
was the original. The compromise implemented here, to use fictional
subdirectories (`1`, `2`, `3`, ...) and place non unique files in
them, keeping the original file names. For example, a directory can have the files:

| name  | id   |
| --    | --   |
| X.txt | file-xxxx |
| X.txt | file-yyyy |
| X.txt | file-zzzz |

This is presented as:

| name  | subdir | id   |
| --    | --     | --   |
| X.txt | . | file-xxxx |
| X.txt | 1 | file-yyyy |
| X.txt | 2 | file-zzzz |

```
X.txt
|_ 1
   |_ X.txt
|_ 2
   |_ X.txt
```

## Sequential Prefetch

Performing prefetch for sequential streams incurs overhead and costs
memory. The goal of the prefetch module is: *if a file is read from start to finish, we want to be
able to read it in large network requests*. What follows is a simplified description of the algorithm.

In order for a file to be eligible for streaming it has to be at
8MiB. A bitmap is maintained for areas accessed. If the first metabyte
is accessed, prefetch is started. This entails sending multiple
asynchronous IO to fetch 4MiB of data. As long as the data
is fully read, prefetch continues. If a file is not accessed for more
than five minutes, or, access is outside the prefetched area, the process stops.


## Manifest

The *manifest* option specifies the initial snapshot of the filesystem
tree as a JSON file. The database is initialized from this snapshot,
and the filesystem starts its life from that point. This is useful
for WDL, where we want to mount remote files and directories on a
pre-specified tree.

The manifest is in JSON format, and it contains two tables, one for individual files,
the other for directories.

The `files` table describes individual files, and where to mount them.

| field name  | JSON type | description |
| ---         | ---       | --          |
| proj\_id    | string | project ID  |
| file\_id    | string | file ID |
| parent      | string | local directory |
| fname       | string | file name (optional) |
| size        | int64  | file length (optional) |
| ctime       | int64  | time in milliseconds since Jan-1-1970 (optional) |
| mtime       | int64  | time in milliseconds since Jan-1-1970 (optional) |

The `fname, size, ctime`, and `mtime` fields are optional. If they are unspecified, the system queries DNAx instead.

The `directories` table maps folders in projects to local mount points.

| field name   | JSON type | description |
| ---          | ---       | --          |
| proj\_id     | string    | project ID |
| folder       | string    | folder on DNAx |
| dirname      | string    | local directory |


For example, the manifest:

```
{
  {
     "proj_id" : "proj-1019001",
     "folder" : /Spade",
     "dirname" : "Cards/S"
  },
  {
     "proj_id" : "proj-1087011",
     "folder" : "/Joker",
     "dirname" : "Cards/J"
  }
}
```

will create the directory structure:

```
/
|_ Cards
      |_ S
      |_ J
```

Browsing through directory `Cards/J`, is equivalent to traversing the remote `proj-1019001:/Joker` folder.
