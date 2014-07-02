#coding=utf8

import sys
import time
from urlparse import urlparse
from collections import namedtuple
import gevent
from gevent import pool
from gevent import threadpool, queue, monkey
from gevent.greenlet import SpawnedLink
from gevent.hub import greenlet, getcurrent, get_hub
import requests
import urly
# from .log import Logger

DownloadResult = namedtuple('DownloadResult', 'successful, value, exception')


thread_get_ident = monkey.get_original('thread', 'get_ident')




# class IntervalQueue(queue.Queue):

#     # def __init__(self, maxsize=None, items=None):
#     def __init__(self, interval=0):
#         super(IntervalQueue, self).__init__()
#         self.interval = interval
#         self._last = 0

#     def get(self, block=True, timeout=None):
#         if self.interval:
#             while (self._last + self.interval) > time.time():
#                 gevent.sleep(self._last + self.interval - time.time() + 0.001)
#                 # gevent.sleep(.05)
#             self._last = time.time()

#         return super(IntervalQueue, self).get(block, timeout)






class Downloader(object):
    
    def __init__(self, cfg, spider):
        super(Downloader, self).__init__()
        # self.cfg = cfg
        self.sessions = {}
        
        self.timeout = cfg.get('timeout')
        self.headers = cfg.get('headers')

        self.interval = cfg.get('interval', None)
        if self.interval:
            self.interval = self.interval / 1000.0
        # print 'Downloader', self.interval
        self._last = 0

        # self.session.headers = self.headers


        self._log = spider.log
        self._urlmgr = spider.urlmgr

        # _log_prefix = cfg.get('log_prefix')
        # if _log_prefix:
        #     log_name = '%s.Downloader' % _log_prefix
        
        #     self._log = Logger.get_logger(log_name)
        # else:
        #     self._log = None

        self._count = 0
        self.maxsize = maxsize = cfg.get('thread', 4)

        self._callbacks = []
        self._threads = []
        # self._pool = pool.Pool(maxsize)
        # self._pool2 = pool.Pool(2)
        # self._pool = threadpool.ThreadPool(maxsize)


    def _on_download(self, rlt, opts):
        for callback in self._callbacks:
            callback(rlt, opts)

    def add_callback(self, func):
        self._callbacks.append(func)

    def start(self):
        for i in xrange(self.maxsize):
            g = gevent.spawn(self._worker)
            self._threads.append(g)

        self._count = 0

    def stop(self):
        for thread in self._threads:
            thread.kill()

    @property
    def qsize(self):
        return self._urlmgr.qsize
        return self._queue.qsize()
        return self._count
        


    def get_session(self, url):
        up = urlparse(url)
        subdomain, domain, tld = urly.split_host(up.hostname)
        fd = '%s.%s' % (domain, tld)

        if fd not in self.sessions:
            session = requests.Session()
            session.headers = self.headers
            self.sessions[fd] = session

        return self.sessions[fd]


    def make_request(self, url, method='GET', **kwargs):
        return requests.Request(method, url, **kwargs)


    # def sync_download(self, req, opts=None):
    #     # self._count += 1
    #     g = self._pool2.spawn(self._download, req, opts)
    #     gevent.sleep(0)
    #     rlt = g.get()
    #     # self._count -= 1
    #     return rlt
    #     # return self.download(req, callback, opts).get()


    # def add(self, req, cb):
    #     self._queue.put((req, cb))
    #     # print 'add', self._queue.qsize()


    # def download(self, req, opts=None):
    #     # print self._download, ' <<============='
    #     self._count += 1
    #     g = self._pool.spawn(self._download, req, opts)
    #     g.link(self._done)
    #     return g


    def _worker(self):
        while True:
            uo = self._urlmgr.get()
            print 'unfinished_tasks', self._urlmgr.qsize
            if uo:
                try:
                    url, opts = uo
                    req = self.make_request(url)
                    # self._evtmgr.fire()
                    rlt = self.download(req)
                    dr = DownloadResult(True, rlt, None)
                except:
                    ei = sys.exc_info()
                    dr = DownloadResult(False, None, ei)

                if not opts:
                    opts = {}

                opts['__url__'] = url
                
                self._on_download(dr, opts)



    # def _worker(self):
    #     # print '_worker'
    #     while True:
    #         # print 'get --000000000000000'
    #         rc = self._queue.get()
    #         # print 'get --', rc
    #         if rc:
    #             try:
    #                 req, callback = rc
    #                 rlt = self._download(req)
    #                 dr = DownloadResult(True, rlt, None)
    #             except:
    #                 ei = sys.exc_info()
    #                 dr = DownloadResult(False, None, ei)


    #             callback(dr)
    #             # print 'callback after---------'
    #             # try:
    #             #     callback(dr)
    #             # except:
    #             #     pass
    #     print '____________worker break'


    def download(self, req, opts=None):
        if isinstance(req, basestring):
            req = self.make_request(req)

        # if self.interval:
        #     while (self._last + self.interval) > time.time():
        #         gevent.sleep(self._last + self.interval - time.time() + 0.01)
        #         # gevent.sleep(.05)
        #     self._last = time.time()
        # u = urlparse(url)
        # if u.netloc not in self.sessions:
        #     sess = requests.Session()
        #     sess.headers = self.headers
        #     self.sessions[u.netloc] = sess
        # sess = self.sessions[u.netloc]
        # return sess.request('GET', url, timeout=self.timeout)
        print 'DD ==>> %s -- %s %s' % (thread_get_ident() , time.time(), req.url)
        sess = self.get_session(req.url)
        preq = sess.prepare_request(req)
        return sess.send(preq)

    # def _done(self, g):
    #     print self._count
    #     self._count -= 1

