#!/usr/bin/env python
# -*- coding: utf-8 -*-

CONCURRENCY = 10

import sys
import datetime
import re
import requests
pattern = re.compile('([0-9]*)\.([0-9*])')
matches = pattern.match(requests.__version__)
if matches:
    major = int(matches.group(1))
    minor = int(matches.group(2))
    if major < 3 and minor < 5:
        print "Your version of the requests library is too old. You will want to upgrade to the 2.5.x series or later"
        print "Using pip: pip install --upgrade requests"
        sys.exit()
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

import multiprocessing

import urlparse
import MySQLdb
import time
import re
from random import shuffle
from collections import deque

import codecs
codecs.register(lambda name: codecs.lookup('utf8') if name == 'utf8mb4' else None)

socket_timeout = 7
request_headers = {'User-agent': 'Mozilla/5.0 (Windows NT 5.1; rv:31.0) Gecko/20100101 Firefox/31.0'}
working = {}
sleepers = {}
updates = []
current_table = ""

# Disable rate-limiting of requests to these domains:
whitelist = [ 'j.mp',
              'doubleclick.net',
              'ow.ly',
              'bit.ly',
              'goo.gl',
              'dld.bz',
              'tinyurl.com',
              'fp.me',
              'wp.me',
              'is.gd',
              'twitter.com',
              't.co'
            ]

db_host = 'localhost'
db_user = 'root'
db_passwd = ''
db_db = 'twittercapture'

with open('../config.php', 'r') as f:
    read_data = f.read()
    lines = read_data.split('\n')
    for line in lines:
        result = re.search('^\$dbuser *= *["\'](.*)["\']', line)
        if result:
            db_user = result.group(1)
        result = re.search('^\$dbpass *= *["\'](.*)["\']', line)
        if result:
            db_pass = result.group(1)
        result = re.search('^\$hostname *= *["\'](.*)["\']', line)
        if result:
            db_host = result.group(1)
        result = re.search('^\$database *= *["\'](.*)["\']', line)
        if result:
            db_db = result.group(1)
f.closed

db = MySQLdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_db, charset='utf8mb4')
cursor = db.cursor()

def get_twitter_tables(table = None):
    if table is not None:
        query = "SHOW TABLES LIKE '%s'" % table
    else:
        query = "SHOW TABLES LIKE '%_urls'"
    rs = cursor.execute(query)
    tables = deque()
    for t in cursor.fetchall():
        tables.append(t[0])
    shuffle(tables)
    return tables


def get_urls_from_db(table):
    print 'DATABASE -- Getting urls from %s ...' % table
    query = "SELECT DISTINCT url_expanded FROM " + table  + """
            WHERE (domain IS NULL OR domain = '')
            AND (error_code IS NULL OR error_code = '')
            AND (url_expanded != '' AND url_expanded IS NOT NULL)
            """
    rs = cursor.execute(query)
    urls = deque()
    for r in cursor.fetchall():
        urls.append(r[0])
    print 'DATABASE -- Returning %s urls from %s..' % (len(urls), table)
    shuffle(urls)
    return urls

def unshorten(url):
    global working, current_table

    status = True
    url_followed = url

    #print url

    # Use the domainname from url_expanded to rate limit requests to certain hostnames (with timings stored in sleepers dict)
    initialhost = urlparse.urlparse(url).hostname
    if initialhost.startswith('www.'):
        initialhost = initialhost[4:]

    if working.has_key(initialhost):
        time.sleep(0.25)
        status = False
    else:
        status_code = 0
        if initialhost not in whitelist:
            working[initialhost] = True
            #print "%s working" % len(working)
        try:
            if sleepers.has_key(initialhost):
                # All dictionary access here is in try/catch because keys may be removed by another thread, causing exceptions
                try:
                    timediff = int(time.time()) - sleepers[initialhost]
                    if (timediff < on_busy_wait):
                        sleepnow = on_busy_wait - timediff
                        time.sleep(sleepnow)
                    try:
                        del sleepers[initialhost]
                    except:
                        pass
                except:
                    pass

            resp = requests.get(url, headers=request_headers, timeout=socket_timeout, verify=False)
            url_followed = resp.url
            status_code = resp.status_code

            hostname = urlparse.urlparse(url_followed).hostname
            if hostname.startswith('www.'):
                hostname = hostname[4:]

            if status_code == 429:
                sleepers[hostname] = int(time.time())
                #print "%s sleepers" % len(sleepers)

            record = (url_followed, hostname, status_code, url)

        except (requests.exceptions.RequestException, requests.exceptions.ConnectionError, requests.exceptions.URLRequired, requests.exceptions.TooManyRedirects, requests.exceptions.Timeout) as e:
            #print "error %s\t%s" % (url,e)
            record = ('', '', 0, url)
        except ValueError as e:
            #print "error %s\t%s" % (url,e)
            record = ('', '', 0, url)
        except:
            record = ('', '', 0, url)

        finally:
            if initialhost not in whitelist:
                del working[initialhost]

    return record, status

def update_row(record, table):
    #print "RESULTS -- %s, %s to insert into %s" % (record[2], record[0], table)
    global updates
    updates.append(record)
    if len(updates) == 500:
        flush_db_queue(table)

def flush_db_queue(table):
    global updates
    query = "UPDATE " + table + " SET url_followed = %s, domain = %s, error_code = %s WHERE url_expanded = %s"
    print "DATABASE -- Flushing %s records to the db\t%s" % (len(updates),datetime.datetime.now())
    cursor.executemany(query, updates)
    updates[:] = []

def main(argv = None):
    total = 0
    finished = 0
    #multiprocessing.set_start_method("spawn")
    pool = multiprocessing.Pool(processes=CONCURRENCY)
    try:
        table = argv[0]
    except (TypeError, IndexError):
        print "No tablename provided"
        table = None

    for _table in get_twitter_tables(table):
        urls = get_urls_from_db(_table)
        if len(urls) == 0:
            continue
        total += len(urls)
        for record, status in pool.imap_unordered(unshorten, urls):
            if status == False:
                urls.append(url)
            #print "%s\t%s\t%s\t%s\t%s" % (status, record[2], record[3], record[1], record[0])
            update_row(record, _table)
            finished += 1

        # Flush left over updates in the queue to the db
        flush_db_queue(_table)
        print "\n"

    print ('RESULTS -- finished: %s/%s' % (finished, total))
    pool.close()

if __name__ == '__main__':
    try:
        sys.argv[1]
    except IndexError:
        main()
    else:
        main(sys.argv[1:])
