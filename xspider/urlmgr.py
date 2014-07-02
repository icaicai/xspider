#coding: utf8

import sys
import time
import re
import base64
import hashlib
import urlparse
import shelve
import sqlite3
try:
    import cPickle as pickle
except ImportError:
    import pickle
from gevent import queue
import urly

"""
urls:
    id
    umd5
    cmd5
    url
    opts
    status
    created
    updated


clean rule
    act: rm_qs, rm_fgt





"""

class Queue(queue.Queue):


    def __init__(self):
        super(Queue, self).__init__()

    def _put(self, item, left=False):
        if left:
            self.queue.appendleft(item)
        else:
            self.queue.append(item)

    def put(self, item, left=False):
        self._put(item, left)
        if self.getters:
            self._schedule_unlock()        

    def clear(self):
        self.queue.clear()


class UrlMgr(object):

    def __init__(self, cfg, spider):
        self.expire = cfg.get('expire', 24*60*60)
        self._queue = Queue()
        self._urldb = {}
        self._dirty = set()


        self._default_clean = None
        self._clean_rules = []
        self._ignore_list = []

        self._init_db(cfg['db'])
        self._init_rule(cfg, spider)


    def _init_rule(self, cfg, spider):
        crs = cfg.get('clean', [])
        for cr in crs:
            handler = spider.load(cr['handler'])
            if handler:
                if cr['match'] == '*':
                    self._default_clean = handler
                else:
                    cond = re.compile(cr)
                    self._clean_rules.append((cond, handler))

        igs = cfg.get('ignore', [])
        for ig in igs:
            i = re.compile(ig)
            self._ignore_list.append(i)



    @property      
    def qsize(self):
        return self._queue.qsize()


    def clean(self, url):
        u = urly.parse(url)
        u.defrag()

        for rule in self._clean_rules:
            if rule[0].search(url):
                u = apply(rule[1], (u, ))
                break
        else:
            if self._default_clean:
                u = apply(self._default_clean, (u, ))

        return u.utf8()


    def put_multi(self, urls):
        for url in urls:
            self.add(url)

    def put(self, url, opts=None, left=False, force=False):
        url = self.clean(url)
        umd5 = self._md5(url)
        # print '==', umd5, umd5 in self._urldb, url
        if umd5 in self._urldb:
            d = self._urldb[umd5]

            if d.get('in_queue'):
                return

            if d.get('redirect') and d['redirect'] in self._urldb:
                ddm = d['redirect']
                d = self._urldb[ddm]


            # if not force and self.expire:
            #     for i in self._ignore_list:
            #         if i.search(url):
            #             force = True
            #             break

            # print force, not self.expire , (d['updated'] > 0 and (time.time() - d['updated'] > self.expire)), self.expire
            if force or not self.expire or (d['updated'] > 0 and (time.time() - d['updated'] > self.expire)):
                d['opts'] = opts
            else:
                # print '==================================================='
                return False
        else:
            d = {}
            d['umd5'] = umd5
            d['cmd5'] = ''
            d['url'] = url
            d['opts'] = opts
            d['status'] = 0
            d['created'] = time.time()
            d['updated'] = 0
            d['is_new'] = True
            d['in_queue'] = True
            self._urldb[umd5] = d


        if d:
            self._queue.put((umd5, url, opts), left)
            self._dirty.add(umd5)


    def get(self):
        u = self._queue.get()
        umd5 = u[0]
        self._urldb[umd5].update(in_queue = False)
        return u[1:]



    def alias(self, url1, url2):
        url1 = self.clean(url1)
        url2 = self.clean(url2)
        um1 = self._md5(url1)
        um2 = self._md5(url2)
        if um1 in self._urldb:
            ud = self._urldb[um1]
            ud['redirect'] = um2


    def update(self, url, **kwargs):
        url = self.clean(url)
        umd5 = self._md5(url)
        changed = True
        if umd5 in self._urldb and kwargs:
            ud = self._urldb[umd5]
            for key in kwargs:
                if key == 'response' and kwargs[key]:
                    resp = kwargs[key]
                    m = hashlib.md5()
                    for line in resp.iter_lines():
                        m.update(line)
                    cmd5 = m.hexdigest()

                    if cmd5 == ud['cmd5']:
                        changed = False   #######################################################

                    key = 'cmd5'
                    value = cmd5
                else:
                    value = kwargs[key]

                ud[key] = value
            ud['updated'] = time.time()
            self._dirty.add(umd5)

        return changed


    def get_multi(self, params):
        pass


    def load(self):
        # lock
        self._queue.clear()
        # queue = []
        d = self._urldb
        self._urldb = {}
        del d
        self._dirty.clear()

        sql = "select * from urls order by created"
        self._cursor.execute(sql)
        for row in self._cursor.fetchall():
            umd5 = row['umd5']
            row['opts'] = pickle.loads(row['opts'])
            row['in_queue'] = True
            self._urldb[umd5] = row
            if row['updated'] == 0:
                self._queue.put((umd5, row['url'], row['opts']))
                # queue.append((row['url'], row['opts']))
        # release

    def sync(self):
        # lock
        print 'BEGIN SYNC'
        new_data = []
        upd_data = []
        failed = set()
        dirty_tmp = self._dirty
        self._dirty = set()
        for umd5 in dirty_tmp:
            if umd5 in self._urldb:
                ud = self._urldb[umd5]
                if ud.get('is_new'):
                    data = (umd5, ud.get('cmd5', ''), ud.get('url', ''), pickle.dumps(ud.get('opts', None)), ud.get('status', 0), ud.get('created', 0), ud.get('updated', 0))
                    sql = 'INSERT INTO urls (umd5, cmd5, url, opts, status, created, updated) VALUES (?, ? ,?, ?, ?, ?, ?)'
                else:
                    data = (ud.get('cmd5', ''), pickle.dumps(ud.get('opts', None)), ud.get('status', 0), ud.get('created', 0), umd5)
                    sql = 'UPDATE urls SET cmd5=?, opts=?, status=?, updated=? WHERE umd5=?'

                try:
                    # print type(ud.get('url', '')), type(pickle.dumps(ud.get('opts', None)))
                    self._cursor.execute(sql, data)
                    ud['is_new'] = False
                    # print 'SYNC --->', umd5
                except:
                    failed.add(umd5)
                    import traceback
                    traceback.print_exc()
                    raise
                    # print sys.exc_info()

        self._conn.commit()
        self._dirty |= failed

        # release


    def _md5(self, s):
        m = hashlib.md5()
        m.update(s)
        return m.hexdigest()

    def _init_db(self, dbfile):
        self._conn = sqlite3.connect(dbfile)
	self._conn.row_factory = dict_factory
        self._conn.text_factory = str 
        self._cursor = self._conn.cursor()
        sql = "SELECT * FROM sqlite_master WHERE type='table' AND name='urls';"
        n = self._cursor.execute(sql)
        n = self._cursor.fetchone()
        if not n:
            sql = """CREATE TABLE urls(
                        umd5 CHARACTER(32) PRIMARY KEY NOT NULL,
                        cmd5 CHARACTER(32),
                        url TEXT,
                        opts TEXT,
                        status INTEGER DEFAULT 0,
                        created INTEGER DEFAULT 0,
                        updated INTEGER DEFAULT 0
                )"""
            self._cursor.execute(sql)
            self._conn.commit()
        
        self._cursor.execute("select * from sqlite_master;")
        # print 'tables:' , self._cursor.rowcount
        # for row in self._cursor.fetchall():
        #     print row


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

