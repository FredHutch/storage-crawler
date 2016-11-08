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

The main process creates two tables in the DB based on the `--tag` command line argument. The `--tag` is turned into a schema, and the tables and report account are created in that schema:

   - `*tag*.files` - table contains the actual file metadata and full path with filename
   - `*tag*.status` - a status log of the crawl

Once set up, directory walking processes (walkers), file stating processes (staters) and DB writing processes (dbprocs) are spawned up to the specified number. These processes use queues to pass paths around to each other - a file queue, a directory queue, and a db queue.

Specifically, the walkers pop from the dir queue, and push onto the dir queue and the file queue. The staters pop from the file queue and push onto the db queue, and the db procs pop from the db queue and insert into the DB.

When the dir queue is empty, the walkers are stopped. Then the file queue, and finally the db queue and the crawl is complete.

**Theory**

The basic idea is that by treating each file atomically, the stat process will not get bogged down in a single directory or file location. No guarantees, though!

**Schemas**

The `*tag*.files` table:

                 id SERIAL,
                 insert_time timestamp with time zone DEFAULT now () NOT NULL,
                 path bytea NOT NULL,
                 extension bytea NOT NULL,
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

The `*tag*_status` table:

                 id SERIAL,
                 time timestamp with time zone DEFAULT now() NOT NULL,
                 status text NOT NULL,
                 value int,
                 units text

