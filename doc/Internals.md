# The Database Schema

A local sqlite3 database is used to store filesystem information
discovered by querying DNAnexus.

The `data_objects` table maintains information for files, applets, workflows, and other data objects.

| field name      | SQL type | description |
| ---             | ---      | --          |
| kind            | int      | type of file: regular, symbolic link, other |
| id              | text     | The DNAx object-id |
| proj\_id        | text     | A project id for the file |
| state           | text     | the file state (open/closing/closed) |
| archival\_state | text     | archival state of this file |
| inode           | bigint   | local filesystem i-node, cannot change |
| size            | bigint   | size of the file in bytes |
| ctime           | bigint   | creation time |
| mtime           | bigint   | modification time |
| mode            | int      | Unix permission bits |
| tags            | text     | DNAx tags for this object, encoded as a JSON array |
| properties      | text     | DNAx properties for this object, encoded as JSON  |
| local\_path     | text     | if file has a local copy, this is the path |
| dirty\_data     | int      | has the data been modified? (only files) |
| dirty\_metadata | int      | have the tags or properties been modified? |

It stores `stat` information on a data object, and maps it to an
inode.  The inode is the primary key, and it cannot change once
chosen; it has no DNAx equivalent. Note that a data object can be hard
linked from multiple projects on DNAx, it may also be a member of a
container, instead of a project. The container field is used at
download time to inform the system which project to check for
ownership. It can safely be omitted, at the cost of additional work on
the server side.

The `archival_state` is relevant for files only. It can have one of
four values: `live`, `archival`, `archived`, `unarchiving`. A file can
be accessed only when it is in the `live` state.

The `state` can be one of `open`, `closing`, `closed`. It applies to all data objects.

The `id` will be empty when a file is first created. It will be populated when it is first
uploaded to the platform. Every update the id will change. This is because DNAx files are immutable,
and changing, even a single byte, requires rewriting the entire file, generating a new id.

The `namespace` table stores information on the directory structure.

| field name | SQL type | description |
| ---        | ---  | --          |
| parent     | text | the parent folder |
| name       | text | directory/file name |
| obj\_type  | int  | directory=1, data-object=2 |
| inode      | bigint | local filesystem i-node, cannot change |

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
| mode       | int     | Unix permission bits |

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

DNAx allows multiple data objects in a directory to have the same name. This
violates POSIX, and cannot be presented in a FUSE filesystem. It is
possible to resolve this, by mangling the original filenames, for
example, by adding `_1`, `_2` suffixes. However, this can cause name
collisions, and will certainly make it harder to understand which file
was the original. The compromise implemented here is to use fictional
subdirectories (`1`, `2`, `3`, ...) and place non unique data objects in
them, keeping the original names intact. For example, a directory can have the files:

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

A symbolic link is represented as a regular file, with the link stored in the `inner\_data` field.

A hard link is an entry in the namespace that points to an existing data object. This means that a
single i-node can have multiple namespace entries, so it cannot serve as a primary key.


# Sequential Prefetch

Performing prefetch for sequential streams incurs overhead and costs
memory. The goal of the prefetch module is: *if a file is read from start to finish, we want to be
able to read it in large network requests*. What follows is a simplified description of the algorithm.

In order for a file to be eligible for streaming it has to be at
8MiB. A bitmap is maintained for areas accessed. If a complete metabyte
is accessed, prefetch is started. This entails sending multiple
asynchronous IO to fetch 4MiB of data. As long as the data
is fully read, prefetch continues. If a file is not accessed for more
than five minutes, or, access is outside the prefetched area, the process halts. It will start again if sequential access is detected down the road.

# Manifest

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
| ctime       | int64  | time in seconds since Jan-1-1970 (optional) |
| mtime       | int64  | time in seconds since Jan-1-1970 (optional) |

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
  "files" : [],
  "directories" : [
    {
      "proj_id" : "proj-1019001",
      "folder" : "/Spade",
      "dirname" : "Cards/S"
    },
    {
      "proj_id" : "proj-1087011",
      "folder" : "/Joker",
      "dirname" : "Cards/J"
    }
  ]
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


# Experimental file creation and modification

dxfuse has an unstable experimental mode that allows creating new files and modifing existing files,
inspite of the fact that only immutable files exist on DNAx. The
mismatch between what the filesystem allows (updating a file), and
what is available natively on the platform makes the update operation
expensive.

When a file is first created it is written to the local disk and
marked dirty in the database. In order to modify an existing file it
is downloaded in its entirety to the local disk, modified locally, and
marked dirty. A background daemon scans the database periodically and
uploads dirty files to the platform. If a file `foo` already exists as
object `file-xxxx`, a new version of it is uploaded, and when done,
the database is modified to point to the new version. It is then
possible to either erase the old version, or keep it as an old
snapshot.

There are situations where you want the background process to
synchronously update all modified and newly created files. For example, before shutting down a machine,
or unmounting the filesystem. This can be done by issuing the command:
```
$ dxfuse -sync
```

Metadata such as xattrs is updated with a similar scheme. The database
is updated, and the inode is marked `dirtyMetadata`. The background daemon then
updates the attributes asynchronously.
