#!/usr/bin/env python3

import configargparse
import time
import sys
import psycopg2
import psycopg2.extensions

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
    sys.stderr.write(db_conn_str)
    try:
        conn = psycopg2.connect(db_conn_str)
        cur = conn.cursor()
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
    # set default schema
    try:
        qry = """SET search_path TO %(search_path)s"""
        data = {'search_path': "storcrawl_{}".format(config.tag)}
        cur.execute(qry, data)
    except psycopg2.Error as e:
        sys.stderr.write("Error setting search_path: {}".format(e.pgerror))
        sys.exit(1)

def IterResults(cursor, arraysize=1000):
    while True:
        results = cur.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result

def space_by_owner():
    try:
        qry = """SELECT * FROM storcrawl_{}.files limit 10""".format(config.tag)
        cur.execute(qry)
        return IterResults(cur)
    except psycopg2.Error as e:
        sys.stderr.write("Error SELECTING: {}".format(e.pgerror))

if __name__ == '__main__':
    database_init(db_conn_str,config.tag)
    conn = psycopg2.connect(db_conn_str)
    cur = conn.cursor()

    # see storcrawl.sql for table schemas

    for row in space_by_owner():
        print(row)
    
    exit()
