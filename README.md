# storage-crawler
**storcrawl**

A parallel file system crawler that manages file and directory metadata in a (postgres) database.

**Requirements**

Language:
   - Python 3.5

Python Modules:
   - configargparse
   - psycopg2

External Dependencies:
   - a postgres DB and use with create table and drop table rights

**Current State**

Currently the main program `storcrawl.py` is working to crawl a given path. It is reasonably efficient, but still has some issues with error handling and process clean-up.

**Basic Workflow**

The main process creates three tables in the DB based on the `--tag` command line argument. There are:

   - `*tag*_files` - table contains the actual file metadata and full path with filename
   - `*tag*_errors` - OSErrors encountered during the crawl with full path and filename
   - `*tag*_status` - a status log of the crawl

Once set up, directory walking processes (walkers) and file stating processes (staters) are slow-spawned up to the specified number. These processes use two queues to pass paths around to each other - a file queue and a directory queue.

Once each queue has been empty for a given time (to account for straggling processes and likely a bug in my code around joining the processes), the entire process exits.

**Theory**

The basic idea is that by treating each file atomically, the stat process will not get bogged down in a single directory or file location. No guarantees, though!

Each walker takes a directory entry off the directory queue and scans it, putting any directories found into the directory queue, and files into the file queue. Each stater takes a file entry off the file queue, stats the file, and inserts that information into the database. Inserts are committed after a given timeout or file count.

**Schemas**

The `*tag*_files` table:

                 id SERIAL,
                 insert_time timestamp with time zone DEFAULT now () NOT NULL,
                 path text NOT NULL,
                 st_mode bit(19) NOT NULL,
                 st_ino bigint NOT NULL,
                 st_dev text NOT NULL,
                 st_nlink int NOT NULL,
                 st_uid bigint NOT NULL,
                 st_gid bigint NOT NULL,
                 st_size bigint NOT NULL,
                 st_atime int NOT NULL,
                 st_mtime int NOT NULL,
                 st_ctime int NOT NULL,
                 owner text

The `*tag*_errors` table:

                 id SERIAL,
                 path text NOT NULL,
                 errno int NOT NULL,
                 strerror text NOT NULL,
                 owner text

The `*tag*_status` table:

                 id SERIAL,
                 time timestamp with time zone DEFAULT now() NOT NULL,
                 status text NOT NULL,
                 value int,
                 units text

