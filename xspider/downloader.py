

import time
from urlparse import urlparse
from collections import namedtuple
import gevent
from gevent import pool
# from gevent import threadpool, queue
from gevent.greenlet import SpawnedLink
from gevent.hub import greenlet, getcurrent, get_hub
import requests
# from .log import Logger

DownloadResult = namedtuple('DownloadResult', 'successful, value, exception')


class ArgsLink(SpawnedLink):

    def __init__(self, callback, *args, **kwargs):
        super(ArgsLink, self).__init__(callback)
        self._args = args
        self._kwargs = kwargs
        #print '$$$$$$$$$$$$$$$', self, hasattr(self, 'callback')

    def __call__(self, source):
        # print '$$$$$$$$$$$$$$$', self, hasattr(self, 'callback')
        g = greenlet(self.callback, get_hub())
        self._args = (source, ) + self._args
        g.switch(*self._args, **self._kwargs)



# class IntervalQueue(queue.Queue):


#     def __init__(self, interval=0):
#         self.interval = interval
#         self._last = 0

#     def get(self, block=True, timeout=None):
#         if self.interval:
#             while (self._last + self.interval) > time.time():
#                 gevent.sleep(self._last + self.interval - time.time() + 0.001)
#                 # gevent.sleep(.05)
#             self._last = time.time()

#         super(IntervalQueue, self).get(block, timeout)






class Downloader(object):
    
    def __init__(self, cfg):
        self.cfg = cfg
        #self.sessions = {}
        self.session = requests.Session()
        self.timeout = cfg.get('timeout')
        self.headers = cfg.get('headers')

        self.interval = cfg.get('interval', None)
        if self.interval:
            self.interval = self.interval / 1000.0
        # print 'Downloader', self.interval
        self._last = 0

        self.session.headers = self.headers


        # _log_prefix = cfg.get('log_prefix')
        # if _log_prefix:
        #     log_name = '%s.Downloader' % _log_prefix
        
        #     self._log = Logger.get_logger(log_name)
        # else:
        #     self._log = None

        self._count = 0
        maxsize = cfg.get('thread', 4)
        self._pool = pool.Pool(cfg.get('thread', 4))
        # self._pool = threadpool.ThreadPool(maxsize)
        # if self.interval:
        #     self._queue = IntervalQueue(self.interval)
        # else:
        #     self._queue = queue.Queue()
        # self._queue2 = queue.Queue()


    @property
    def qsize(self):
        # return len(self.qsize)
        return len(self._pool)




    def make_request(self, url, method='GET', **kwargs):
        return requests.Request(method, url, **kwargs)


    def sync_download(self, req, callback, opts=None):
        return self.download(req, callback, opts).get()


    def download(self, req, opts=None):
        print self._download, ' <<============='
        ar = self._pool.spawn(self._download, req, opts)
        return ar

    # def download(self, req, callback=None, opts=None):
    #     ar = self._pool.spawn(self._download, req, opts)
    #     o = {'callback': callback, 'opts': opts}
    #     c = ArgsLink(self._done, o)
    #     ar.rawlink(c)
    #     return ar

    # def _worker(self):
    #     while 1:
    #         req = self._queue.get()
    #         ar = self._pool.spawn(self._download, req, opts)
    #         # self._download(req)


    def _download(self, req, opts=None):
        if isinstance(req, basestring):
            req = self.make_request(req)

        if self.interval:
            while (self._last + self.interval) > time.time():
                gevent.sleep(self._last + self.interval - time.time() + 0.01)
                # gevent.sleep(.05)
            self._last = time.time()
        # u = urlparse(url)
        # if u.netloc not in self.sessions:
        #     sess = requests.Session()
        #     sess.headers = self.headers
        #     self.sessions[u.netloc] = sess
        # sess = self.sessions[u.netloc]
        # return sess.request('GET', url, timeout=self.timeout)
        print 'DD ==>> ', time.time(), req.url
        preq = self.session.prepare_request(req)
        return self.session.send(preq)

    # def _done(self, g, o):
    #     self._count -= 1
    #     rst = {}
    #     # rst['successful'] = g.successful()
    #     # rst['value'] = g.value
    #     # rst['exception'] = g.exception
    #     rst = DownloadResult(g.successful(), g.value, g.exception)

    #     if o and 'callback' in o:
    #         if 'opts' in o and o['opts']:
    #             args = (rst, o['opts'])
    #         else:
    #             args = (rst, {})
    #         apply(o['callback'], args)

    #     return rst
