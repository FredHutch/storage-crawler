#!/usr/bin/env python3

import configargparse
import multiprocessing
import time
import sys
import logging
import logging.handlers
import os
import psycopg2
import psycopg2.extensions
import re
#import subprocess

# config parser
p = configargparse.ArgParser(default_config_files=['/etc/storcrawlrc','~/storcrawlrc','~/.storcrawlrc'],
                             description="A utility to create a PostgreSQL DB of file system metadata.",
                  epilog="Note: arguments with [+] can be specified more than once, except as ENV VARs")

# config file location
p.add_argument('-c', '--config-file', env_var='STORCRAWL_CONFIG_FILE', help='config file path', is_config_file=True)
# config debug and verbose switches
p.add_argument('-d', '--debug', env_var='STORCRAWL_DEBUG', help='debugging', action='store_true', default=False)
p.add_argument('-v', '--verbose', env_var='STORCRAWL_VERBOSE', help='verbose', action='store_true', default=False)
# config auxilliary folder ownership file
p.add_argument('-o', '--owners', env_var='STORCRAWL_OWNERS', help='folder owners file path')
# config DB connection
p.add_argument('-H', '--dbhost', env_var='STORCRAWL_DBHOST', help='database hostname', required=True)
p.add_argument('-p', '--dbport', env_var='STORCRAWL_DBPORT', help='database port', required=True)
p.add_argument('-U', '--dbuser', env_var='STORCRAWL_DBUSER', help='database username', required=True)
p.add_argument('-P', '--dbpass', env_var='STORCRAWL_DBPASS', help='database password', required=True)
p.add_argument('-N', '--dbname', env_var='STORCRAWL_DBNAME', help='database name', required=True)
# config logfile
p.add_argument('-l', '--logdir', env_var='STORCRAWL_LOGDIR', help='logfile location [./]', default='./')
# config crawl dirs
p.add_argument('-D', '--dir', env_var='STORCRAWL_DIR', help='[+] directory to crawl', required=True, action='append')
# config exclusion list
p.add_argument('-E', '--exclude', env_var='STORCRAWL_EXCLUDE', help='[+] pattern in file or dir name to exclude from crawl', action='append', default=[''])
# config processes
p.add_argument('-w', '--walkers', env_var='STORCRAWL_WALKERS', help='number of directory walking processes', default=4)
p.add_argument('-s', '--staters', env_var='STORCRAWL_STATERS', help='number of file statting processes', default=4)
# status update interval
p.add_argument('-u', '--update', env_var='STORCRAWL_UPDATE', help='interval between status updates in seconds', type=int, default=60)
# config tag/label
p.add_argument('-t', '--tag', env_var='STORCRAWL_TAG', help='a tag to identify the crawl results', required=True)

config = p.parse_args()
print(config)

# housekeeping
crawl_stamp = time.strftime('%Y%m%d%H%M%S')
filestablename = "{}_files".format(config.tag)
errorstablename = "{}_errors".format(config.tag)
statustablename = "{}_status".format(config.tag)
logfile = "{}{}_{}.log".format(config.logdir,config.tag,crawl_stamp)
db_conn_str = "dbname= '{}' user='{}' password='{}' host='{}' port={}".format(config.dbname,config.dbuser,config.dbpass,config.dbhost,config.dbport)
num_walkers = int(config.walkers)
num_staters = int(config.staters)
exclusion_list = []
for ent in config.exclude:
  exclusion_list.append(str.encode(ent))
folder_owners = {}
update_interval = config.update

# internal variables
file_count = multiprocessing.Value('L', 0)
dir_count = multiprocessing.Value('L', 0)
file_done_count = multiprocessing.Value('L', 0)
dir_done_count = multiprocessing.Value('L', 0)
files_committed = multiprocessing.Value('L', 0)
flock = multiprocessing.Lock()
dlock = multiprocessing.Lock()
stat_time = multiprocessing.Value('L', 0)
storath_timeout = 300   # seconds to wait for queue drain
log_count = 100000   # number of records for each worker to process between log entries
commit_timeout = 60   # number of seconds for each worker to process between commits to the DB

# database setup
# turn on unicode in psycopg2
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

def database_init(db_conn_str, tag):
    sys.stderr.write(db_conn_str)
    try:
        conn = psycopg2.connect(db_conn_str)
        cur = conn.cursor()
    except psycopg2.Error as e:
        sys.stderr.write("Error connecting to databse: {}".format(e.pgerror))
        sys.exit(1)
    # drop old tables if they exist
    try:
        qry = """DROP TABLE IF EXISTS {}""".format(filestablename)
        cur.execute(qry)
    except psycopg2.Error as e:
        sys.stderr.write("error dropping table {}: {}".format(filestablename,e.pgerror))
    try:
        qry = """DROP TABLE IF EXISTS {}""".format(errorstablename)
        cur.execute(qry)
    except psycopg2.Error as e:
        sys.stderr.write("error dropping table {}: {}".format(errorstablename,e.pgerror))
    try:
        qry = """DROP TABLE IF EXISTS {}""".format(statustablename)
        cur.execute(qry)
    except psycopg2.Error as e:
        sys.stderr.write("error dropping table {}: {}".format(statustablename,e.pgerror))
   
    # create new files table for crawl - will need to be expanded to truncate/manage crawl tables
    try:
        qry = """CREATE TABLE {}(
                 id SERIAL,
                 insert_time timestamp with time zone DEFAULT now () NOT NULL,
                 path bytea NOT NULL,
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
                 owner text)
              """.format(filestablename)
        cur.execute(qry)
        conn.commit()
    except psycopg2.Error as e:
        sys.stderr.write("Unable to create table {} error: {}".format(filestablename,e.pgerror))
        sys.exit(1)
    # create new errors table for crawl
    try:
        qry = """CREATE TABLE {}(
                 id SERIAL,
                 path bytea NOT NULL,
                 errno int NOT NULL,
                 strerror text NOT NULL,
                 owner text)
              """.format(errorstablename)
        cur.execute(qry)
        conn.commit()
    except psycopg2.Error as e:
        sys.stderr.write("Unable to create table {} error: {}".format(errorstablename,e.pgerror))
        sys.exit(1)

    # create new status table for crawl
    try:
        qry = """CREATE TABLE {}(
                 id SERIAL,
                 time timestamp with time zone DEFAULT now() NOT NULL,
                 status text NOT NULL,
                 value int,
                 units text)
              """.format(statustablename)
        cur.execute(qry)
        conn.commit()
    except psycopg2.Error as e:
        sys.stderr.write("Unable to create table {} error: {}".format(statustablename,e.pgerror))
        sys.exit(1)

# ownership file handling
# ownership data structure set-up
def init_owners(folder_owner_file):
    logger = logging.getLogger()
    map_fh = open(folder_owner_file, 'r')

    SKIP_LIST = [b'Nothing',b'netapp->thorium-a.fhcrc.org']
    logger.debug("processing folder owner map file...")

    line_count = 0
    dir_count = 0
    for raw_line in map_fh:
        line_count += 1
        line = raw_line.strip()
        pi, dirs = line.split('=',2)
        logger.debug("PI: {} Dirs: {}".format(pi, dirs))
        for d in dirs.split(':'):
            dir = str.encode(d)
            dir_count += 1
            if dir in SKIP_LIST:
                continue
            if dir in folder_owners:
                logger.debug("Duplicate folder for {} in {}: {}".format(pi,folder_owner_file,dir))
            else:
                folder_owners[dir] = pi
    logger.debug("done - processed {} lines and {} directories".format(line_count,dir_count))

# find owner recursive
def find_owner(path):
    if config.owners is None:
        return(None)
    # check for presence and return value
    if path in folder_owners:
        return(folder_owners[path])
    elif path == '/':
        return(None)
    else:
        # remove the last element and retry
        newpath = os.path.dirname(path)
        return(find_owner(newpath))

# set of logging and queues
class QueueHandler(logging.Handler):
    def __init__(self, queue):
        logging.Handler.__init__(self)
        self.queue = queue

    def emit(self, record):
        try:
            ei = record.exc_info
            if ei:
                dummy = self.format(record)
                record.exc_info = None
            self.queue.put_nowait(record)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

def log_listener_process(queue):
    root = logging.getLogger()
    h = logging.handlers.RotatingFileHandler(logfile)
    f = logging.Formatter('%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s')
    h.setFormatter(f)
    root.addHandler(h)
    while True:
        try:
            record = queue.get()
            if record is None: # We send this as a sentinel to tell the listener to quit.
                break
            logger = logging.getLogger(record.name)
            logger.handle(record) # No level or filter logic applied - just do it!
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            import sys, traceback
            print >> sys.stderr, 'Whoops! Problem:'
            traceback.print_exc(file=sys.stderr)

# the function that runs `stat` on files and inserts results
def stater_process(inq, db_conn_str, file_done_count, files_committed, flock, log_queue):
    h = QueueHandler(log_queue)
    myname = "stater {}".format(multiprocessing.current_process().name)
    logger = logging.getLogger()
    logger.debug("{} started".format(myname))
    conn = psycopg2.connect(db_conn_str)
    cur = conn.cursor()
    tot_count = 0
    outstanding_trans = 0
    # initialize the stat struct as a variable
    file_info = os.lstat(b'/')
    last_commit = time.time()
    while True:
        try:
            item = inq.get()
            if tot_count % log_count == 0:
                logger.debug("{} total count {} file_queue.qsize {} file_done_count {} files_committed {}".format(myname, tot_count,inq.qsize(),file_done_count.value,files_committed.value))
            if item is None:
                logger.debug("{} got None".format(myname))
                # commit anything left uncommitted
                try:
                    conn.commit()
                    with flock:
                        files_committed.value += outstanding_trans
                    logger.debug("{} final commit {} rows".format(myname,outstanding_trans))
                    outstanding_trans = 0
                    last_commit = time.time()
                except psycopg2.Error as e:
                    logger.debug("{} database error during final commit: {}".format(myname,e.pgerror))
                    raise
                conn.close()
                inq.task_done()
                logger.debug("{} exiting: my count was {}".format(myname, tot_count))
                return
            else:
                owner = find_owner(item)
                try:
                    file_info = os.lstat(item)
                    tot_count += 1
                except OSError as e:
                    qry="""INSERT INTO {} (path,errno,strerror,owner)
                         VALUES(%(path)s,%(errno)s,%(strerror)s,%(owner)s)""".format(errorstablename)
                    data={'path': item,
                          'errno': e.errno,
                          'strerror': e.strerror,
                          'owner': owner}
                    cur.execute(qry,data)
                    logger.debug("{} File OSError on {}: {} ({})".format(myname,e.filename,e.strerror,e.errno))
                try:
                    qry="""INSERT INTO {} (
                           path,
                           st_mode,
                           st_ino,
                           st_dev,
                           st_nlink,
                           st_uid,
                           st_gid,
                           st_size,
                           st_atime,
                           st_mtime,
                           st_ctime,
                           owner) 
                           VALUES(
                           %(path)s,
                           %(st_mode)s,
                           %(st_ino)s,
                           %(st_dev)s,
                           %(st_nlink)s,
                           %(st_uid)s,
                           %(st_gid)s,
                           %(st_size)s,
                           %(st_atime)s,
                           %(st_mtime)s,
                           %(st_ctime)s,
                           %(owner)s) ;
                        """.format(filestablename)
                    data={'path': item,
                          'st_mode': "{0:019b}".format(file_info.st_mode),
                          'st_ino': file_info.st_ino,
                          'st_dev': file_info.st_dev,
                          'st_nlink': file_info.st_nlink,
                          'st_uid': file_info.st_uid,
                          'st_gid': file_info.st_gid,
                          'st_size': file_info.st_size,
                          'st_atime': int(file_info.st_atime),
                          'st_mtime': int(file_info.st_mtime),
                          'st_ctime': int(file_info.st_ctime),
                          'owner': owner}
                    cur.execute(qry,data)
                except psycopg2.Error as e:
                    logger.debug("{} database error during insert on {} {}: {}".format(myname,item,file_info,e.pgerror))
                    conn.commit()
                    logger.debug("{}: commit executed".format(myname))

                outstanding_trans += 1

                inq.task_done()
                with flock:
                    file_done_count.value += 1
        
                # commit every commit_timeout seconds if there are outstanding transactions
                if ((time.time() - last_commit) > commit_timeout) & (outstanding_trans > 0):
                    try:
                        conn.commit()
                        with flock:
                            files_committed.value += outstanding_trans
                        logger.debug("{} committed {} rows after {} sec(s)".format(myname,outstanding_trans, commit_timeout))
                        outstanding_trans = 0
                        last_commit = time.time()
                    except psycopg2.Error as e:
                        logger.debug("{} database error during final commit: {}".format(myname,e.pgerror))

        except KeyboardInterrupt:
            try:
                conn.commit()
                with flock:
                    files_committed.value += outstanding_trans
                logger.debug("{} committed {} rows on keyboard interrupt".format(myname,outstanding_trans))
                last_commit = time.time()
                outstanding_trans = 0
            except psycopg2.Error as e:
                logger.debug("{} database error during final commit: {}".format(myname,e.pgerror))

            conn.close()
            logger.debug("{} received keyboard interrupt".format(myname))
            raise

# the function that walks directories collecting additional directories and files
def walker_process(dir_queue, file_queue, dir_count, file_count, dlock, flock, log_queue, exclusion_list):
    h = QueueHandler(log_queue)
    myname = "walker {}".format(multiprocessing.current_process().name)
    logger = logging.getLogger()
    logger.debug("{} started".format(myname))
    logger.debug("{} has exclusion list: {}".format(myname,repr(exclusion_list)))
    tot_count = 0
    try:
        for new_dir in iter(dir_queue.get, None):
            if tot_count % log_count == 0:
                logger.debug("{} total count {} dir_queue.qsize {} file_queue.qsize {} dir_count {} file_count {}".format(myname, tot_count, dir_queue.qsize(), file_queue.qsize(), dir_count.value, file_count.value))
            try:
                for entry in os.scandir(new_dir):
                    if entry.is_dir(follow_symlinks=False):
                        if entry.name in exclusion_list:
                            logger.debug("{} matched exclusion list entry on {}".format(myname,entry.name))
                            pass
                        else:
                            dir_queue.put(os.path.join(new_dir, entry.name))
                            #logger.debug("DQ {1} found directory {0}".format(os.path.join(new_dir, entry.name), dir_queue.qsize()))
                            with dlock:
                                dir_count.value += 1
                    file_queue.put(os.path.join(new_dir, entry.name))
                    with flock:
                        file_count.value += 1
            except OSError as e:
                logger.debug("{} OSError on {}: {} ({})".format(myname,e.filename,e.strerror,e.errno))
            with dlock:
                dir_done_count.value += 1
            tot_count += 1
            dir_queue.task_done()
        else:
            logger.debug("{} got None".format(myname))
            dir_queue.task_done()
    except KeyboardInterrupt:
        logger.debug("{} received keyboard interrupt".format(myname))
        raise

    logger.debug("{} exiting my count was {}".format(myname, tot_count))

# status process to update status metrics during crawl
def status_process(fileq, dirq, file_done_count, file_count, dir_done_count, dir_count, files_committed):
    conn = psycopg2.connect(db_conn_str)
    cur = conn.cursor()
    fs = 0   # files stated
    ft = 0   # files total
    dw = 0   # dirs walked
    dt = 0   # dirs total
    fc = 0   # files committed
    while True:
        try:
            # calculate rates
            stat_rate = (file_done_count.value - fs) / update_interval
            found_file_rate = (file_count.value - ft) / update_interval
            walk_rate = (dir_done_count.value - dw) / update_interval
            found_dir_rate = (dir_count.value - dt) / update_interval
            commit_rate = (files_committed.value - fc) / update_interval

            # build update
            status_rows = [
                {'status': 'stated', 'value': file_done_count.value, 'units': 'files'},
                {'status': 'stat rate', 'value': stat_rate, 'units': 'files/sec'},
                {'status': 'total', 'value': file_count.value, 'units': 'files'},
                {'status': 'discovery rate', 'value': found_file_rate, 'units': 'files/sec'},
                {'status': 'committed', 'value': files_committed.value, 'units': 'files'},
                {'status': 'commit rate', 'value': commit_rate, 'units': 'commits/sec'},
                {'status': 'walked', 'value': dir_done_count.value, 'units': 'dirs'},
                {'status': 'walk rate', 'value': walk_rate, 'units': 'dirs/sec'},
                {'status': 'total', 'value': dir_count.value, 'units': 'dirs'},
                {'status': 'discovery rate', 'value': found_dir_rate, 'units': 'dirs/sec'},
                {'status': 'queue size', 'value': fileq.qsize(), 'units': 'files'},
                {'status': 'queue size', 'value': dirq.qsize(), 'units': 'dirs'},
            ]

            # run the update
            for status_msg in status_rows:
                update_status(conn,cur,status_msg)

            # remember old values
            fs = file_done_count.value
            ft = file_count.value
            dw = dir_done_count.value
            dt = dir_count.value
            fc = files_committed.value

            # sleep
            time.sleep(update_interval)
        except KeyboardInterrupt:
            print("status process received keyboard interrupt")
            raise
    return


# a check to ensure queues have been empty for a while (no delayed walkers still running) - should be removed
def wait_for_queue(qname,q):
    logger = logging.getLogger()
    logger.debug("waiting for {0} to empty".format(qname))
    time_empty = 0
    while time_empty < storath_timeout:
        time.sleep(5)
        qs = q.qsize()
        if (qs > 0):
            time_empty = 0
        else:
            logger.debug("{} empty for {} seconds".format(qname,time_empty))
            time_empty += 5
    logger.debug("{} has been empty {} seconds - returning".format(qname,storath_timeout))

# stop outstanding processes that may still be running - should also be removed
def clean_up(name,procs,q):
    logger = logging.getLogger()
    logger.debug("cleaning up {} processes".format(name))

    # send poison pill to processes
    for p in range(len(procs)):
        logger.debug("sending None to {} process".format(name))
        q.put(None)

    # join to queue (should block until all tasks are done)
    logger.debug("joining {} queue".format(name))
    q.join()
    logger.debug("joined {} queue".format(name))

    # join to the processes
    for p in procs:
        logger.debug("{} process joining".format(name))
        p.join()
        logger.debug("{} process joined".format(name))

# update status table
def update_status(conn,cur,status_msg):
    qry="""INSERT INTO {} (
           status,
           value,
           units) 
           VALUES(
           %(status)s,
           %(value)s,
           %(units)s) ;
        """.format(statustablename)
    cur.execute(qry,status_msg)
    conn.commit()

def begin_scan():

    # queues
    file_queue = multiprocessing.JoinableQueue()
    dir_queue = multiprocessing.JoinableQueue()
    log_queue = multiprocessing.Queue()

    # process pointers
    walker_procs = []
    stater_procs = []

    # create a log listener
    log_listener_p = multiprocessing.Process(target=log_listener_process, args=(log_queue,))
    log_listener_p.start()

    h = QueueHandler(log_queue)
    root = logging.getLogger()
    root.addHandler(h)
    root.setLevel(logging.DEBUG)
    logger = logging.getLogger()
    logger.debug("begin_scan started")

    # owners - if auxiliary owners file specified, set it up, otherwise return None
    if config.owners is not None:
        logger.debug("owner file {} specified, initializing...".format(config.owners))
        init_owners(config.owners)

    # build DB connection for main process
    logger.debug("connecting to database {} on {}".format(config.dbname,config.dbhost))
    try:
        conn = psycopg2.connect(db_conn_str)
        cur = conn.cursor()
    except psycopg2.Error as e:
        sys.stderr.write("Error connecting to database {}: {}".format(config.dbname,e.pgerror))
        sys.exit(1)
    logger.debug("successfully connect to database")

    # create a status printer
    logger.debug("creating status update process")
    status_p = multiprocessing.Process(target=status_process, args=(file_queue, dir_queue, file_done_count, file_count, dir_done_count, dir_count, files_committed))
    status_p.start()

    # starting crawl
    update_status(conn,cur,{'status': 'begin', 'value':1, 'units': ''})

    # feed top dir into dir_queue
    for path in config.dir:
        logger.debug("adding {} to dir_queue".format(path))
        dir_queue.put(str.encode(path))

    # create workers:
    walker_procs = []
    stater_procs = []
    while len(walker_procs) < num_walkers or len(stater_procs) < num_staters:
        if len(walker_procs) < num_walkers:
            p = multiprocessing.Process(target=walker_process, args=(dir_queue, file_queue, dir_count, file_count, dlock, flock, log_queue, exclusion_list))
            p.start()
            walker_procs.append(p)
            logger.debug("created walker {}".format(p.name))
        if len(stater_procs) < num_staters:
            p = multiprocessing.Process(target=stater_process, args=(file_queue, db_conn_str, file_done_count, files_committed, flock, log_queue))
            p.start()
            stater_procs.append(p)
            logger.debug("created stater {}".format(p.name))
        time.sleep(5)

    update_status(conn,cur,{'status': 'all processes spawned', 'value': 1, 'units': ''})

    # wait for directory queue to be empty
    wait_for_queue("directory queue",dir_queue)
    update_status(conn,cur,{'status': 'processed all dirs', 'value': 1, 'units': ''})

    # clean up walker processes
    clean_up("walker",walker_procs,dir_queue)

    # wait for file queue to be empty
    wait_for_queue("file queue",file_queue)
    update_status(conn,cur,{'status': 'processed all files', 'value': 1, 'units': ''})

    # clean up stater processes
    clean_up("stater",stater_procs,file_queue)
    update_status(conn,cur,{'status': 'done', 'value': 1, 'units': ''})

    # stop status update process
    logger.debug("stopping status updater")
    status_p.join()

    # close logging
    log_queue.put_nowait(None)
    logger.debug("stopping logger")
    log_listener_p.terminate()
    log_queue.close()

    conn.close()

if __name__ == '__main__':
    database_init(db_conn_str,config.tag)
    begin_scan()
    print("Done - processed {}/{} files with {} committed in {}/{} directories.".format(file_done_count.value, file_count.value, files_committed.value, dir_done_count.value, dir_count.value))
    exit()
