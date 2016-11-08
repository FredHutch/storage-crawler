#!/usr/bin/env python3

import configargparse
import time
import sys
import psycopg2
import psycopg2.extensions
from psycopg2.extras import DictCursor
import json
import collections
from datetime import datetime

# config parser
p = configargparse.ArgParser(default_config_files=['/etc/storcrawlrc','~/storcrawlrc','~/.storcrawlrc'],
                             description="A utility to generate reports from a storcrawl DB.",
                  epilog="Note: arguments with [+] can be specified more than once, except as ENV VARs")

# config file location
p.add_argument('-c', '--config-file', env_var='STORCRAWL_CONFIG_FILE', help='config file path', is_config_file=True)
# config debug and verbose switches
p.add_argument('-d', '--debug', env_var='STORCRAWL_DEBUG', help='debugging', action='store_true', default=False)
p.add_argument('-v', '--verbose', env_var='STORCRAWL_VERBOSE', help='verbose', action='store_true', default=False)
# config DB connection
p.add_argument('-H', '--dbhost', env_var='STORCRAWL_DBHOST', help='database hostname', required=True)
p.add_argument('-p', '--dbport', env_var='STORCRAWL_DBPORT', help='database port', required=True)
p.add_argument('-U', '--dbuser', env_var='STORCRAWL_DBUSER', help='database username', required=True)
p.add_argument('-P', '--dbpass', env_var='STORCRAWL_DBPASS', help='database password', required=True)
p.add_argument('-N', '--dbname', env_var='STORCRAWL_DBNAME', help='database name', required=True)
# config logfile
p.add_argument('-l', '--logdir', env_var='STORCRAWL_LOGDIR', help='logfile location [./]', default='./')
# config tag/label
p.add_argument('-t', '--tag', env_var='STORCRAWL_TAG', help='a tag to identify the crawl results', required=True)
# action/command
p.add_argument('action', help='action command', metavar='ACTION')

config = p.parse_args()
#print(config)

# housekeeping
crawl_stamp = time.strftime('%Y%m%d%H%M%S')
logfile = "{}{}_report_{}.log".format(config.logdir,config.tag,crawl_stamp)
db_conn_str = "dbname= '{}' user='{}' password='{}' host='{}' port={}".format(config.dbname,config.dbuser,config.dbpass,config.dbhost,config.dbport)

# database setup
# turn on unicode in psycopg2
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

def database_init(db_conn_str, tag):
    try:
        conn = psycopg2.connect(db_conn_str)
        cur = conn.cursor(cursor_factory=DictCursor)
    except psycopg2.Error as e:
        sys.stderr.write("Error connecting to databse: {}".format(e.pgerror))
        sys.exit(1)
    # verify tag exists
    try:
        qry = """SELECT schema_name FROM information_schema.schemata WHERE schema_name = %(schemaname)s"""
        data = {'schemaname': "storcrawl_{}".format(config.tag)}
        cur.execute(qry,data)
        if cur.rowcount != 1:
            sys.stderr.write("Did not find schema called {}".format(config.tag))
            sys.exit(1)
    except psycopg2.Error as e:
        sys.stderr.write("Error checking on schema: {}".format(e.pgerror))
        sys.exit(1)
    set_schema(cur)

def set_schema(cur):
    # set default schema
    try:
        qry = """SET search_path TO %(search_path)s"""
        data = {'search_path': "storcrawl_{}".format(config.tag)}
        cur.execute(qry, data)
    except psycopg2.Error as e:
        sys.stderr.write("Error setting search_path: {}".format(e.pgerror))
        sys.exit(1)

def IterResults(cursor, label, arraysize=1000):
    yield label
    while True:
        results = cur.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result

def display(data):
    header = True
    for row in iter(data):
        if header:
            print(row)
            header = False
        else:
            my_row = []
            for col in row:
                if type(col) is datetime:
                    my_row.append(col.isoformat(' '))
                else:
                    my_row.append(repr(col))
            print(','.join(my_row))

def get_schema(cur,tbl):
    label = "{} table: name, data type, default value:".format(tbl)
    try:
        qry = """SELECT column_name, data_type, column_default
                 FROM information_schema.columns
                 WHERE table_name = %(table)s"""
        cur.execute(qry,{'table':tbl})
    except psycopg2.Error as e:
        sys.stderr.write("DB Error: {}".format(e.pgerror))
    return IterResults(cur, label)

def get_status(cur, option=None):
    if option == 'full':
        label = "current status(full):"
        try:
            qry = """SELECT date_trunc('sec',time),status,value,units
                     FROM status
                     ORDER BY time ASC"""
            cur.execute(qry)
        except psycopg2.Error as e:
            sys.stderr.write("DB Error: {}".format(e.pgerror))
        return IterResults(cur, label)
    elif option == 'events':
        label = "current status(events):"
        try:
            qry = """SELECT date_trunc('sec',time), status
                     FROM status
                     WHERE units = 'event'
                     ORDER BY time ASC"""
            cur.execute(qry)
        except psycopg2.Error as e:
            sys.stderr.write("DB Error: {}".format(e.pgerror))
        return IterResults(cur, label)
    elif option == 'averages':
        label = "current status(averages):"
        try:
            qry = """SELECT av.status, av.avg, av.units
                     FROM (SELECT status, AVG(value) as avg, units
                           FROM status
                           WHERE status like '%rate'
                           GROUP BY status, units)
                     AS av
                     ORDER BY units"""
            cur.execute(qry)
        except psycopg2.Error as e:
            sys.stderr.write("DB Error: {}".format(e.pgerror))
        return IterResults(cur, label)
    else:
        label = "current status(brief):"
        try:
            qry = """SELECT date_trunc('sec',time), status, value, units
                     FROM status
                     WHERE id
                     IN (SELECT MAX(id)
                         FROM status
                         GROUP BY status)
                     ORDER BY time ASC"""
            cur.execute(qry)
        except psycopg2.Error as e:
            sys.stderr.write("DB Error: {}".format(e.pgerror))
        return IterResults(cur, label)
        
# example just returning the first 1000 entries
def first_thousand(cur):
    label = "First 1000 files metadata"
    try:
        qry = """SELECT *
                 FROM storcrawl_{}.files
                 LIMIT 1000""".format(config.tag)
        cur.execute(qry)
    except psycopg2.Error as e:
        sys.stderr.write("Error SELECTING: {}".format(e.pgerror))
    return IterResults(cur, label)

# returning files >= min_size_bytes and >= min_delta_secs ago
def large_old_files(cur, min_size_bytes, min_delta_secs):
    try:
        qry = """SELECT EXTRACT(EPOCH FROM DATE_TRUNC('sec',insert_time)) AS insert_ts,
                  ENCODE(path,'escape') AS path, ENCODE(extension,'escape') AS ext,
                  st_uid, st_gid, st_size, st_atime, st_ctime, st_mtime, owner
                 FROM storcrawl_{}.files
                 WHERE
                 st_size >= {} and
                 (((st_ctime + {}) >= EXTRACT(EPOCH FROM NOW())) or
                 (st_mtime + {}) >= EXTRACT(EPOCH FROM NOW()))
                 ORDER BY
                 GREATEST(st_mtime, st_ctime) DESC,
                 LEAST(st_mtime,st_ctime) DESC""".format(config.tag,min_size_bytes,min_delta_secs,min_delta_secs)
        cur.execute(qry)
    except psycopg2.Error as e:
        sys.stderr.write("Error SELECTING: {}".format(e.pgerror))
    rows = cur.fetchall()
    row_list = []
    for row in rows:
        d = collections.OrderedDict()
        d['insert_ts'] = row['insert_ts']
        d['path'] = row['path']
        d['ext'] = row['ext']
        d['uid'] = row['st_uid']
        d['gid'] = row['st_gid']
        d['size'] = row['st_size']
        d['atime'] = row['st_atime']
        d['ctime'] = row['st_ctime']
        d['mtime'] = row['st_mtime']
        d['owner'] = row['owner']
        row_list.append(d)
    print(json.dumps(row_list, indent=2))

if __name__ == '__main__':
    database_init(db_conn_str,config.tag)
    conn = psycopg2.connect(db_conn_str)
    cur = conn.cursor(cursor_factory=DictCursor)
    set_schema(cur)

    if config.action.lower() == 'schema-status':
        display(get_schema(cur,'status'))
    if config.action.lower() == 'schema-files':
        display(get_schema(cur,'files'))
    if config.action.lower() == 'schema-all':
        display(get_schema(cur,'status'))
        display(get_schema(cur,'files'))
    if config.action.lower() == 'status':
        display(get_status(cur, 'brief'))
    if config.action.lower() == 'status-brief':
        display(get_status(cur, 'brief'))
    if config.action.lower() == 'status-full':
        display(get_status(cur, 'full'))
    if config.action.lower() == 'status-averages':
        display(get_status(cur, 'averages'))
    if config.action.lower() == 'status-events':
        display(get_status(cur, 'events'))
    if config.action.lower() == 'large_old_files':
        large_old_files(cur, 3221225472, 608400)

    conn.close()
    exit()
