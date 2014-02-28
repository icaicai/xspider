

from urlparse import urlparse
from collections import namedtuple
from gevent import pool
from gevent.greenlet import SpawnedLink
from gevent.hub import greenlet, getcurrent, get_hub
import requests

DownloadResult = namedtuple('DownloadResult', 'successful, value, exception')


class ArgsLink(SpawnedLink):

    def __init__(self, callback, *args, **kwargs):
        super(ArgsLink, self).__init__(callback)
        self._args = args
        self._kwargs = kwargs
        #print '$$$$$$$$$$$$$$$', self, hasattr(self, 'callback')

    def __call__(self, source):
        #print '$$$$$$$$$$$$$$$', self, hasattr(self, 'callback')
        g = greenlet(self.callback, get_hub())
        self._args = (source, ) + self._args
        g.switch(*self._args, **self._kwargs)


class Downloader(object):
    
    def __init__(self, cfg):
        self.cfg = cfg
        self.sessions = {}
        self.timeout = cfg.get('timeout')
        self.headers = cfg.get('headers')

        self._count = 0

        self._pool = pool.Pool(cfg.get('thread', 4))

    @property
    def count(self):
        return self._count
    
    @property
    def length(self):
        return len(self._pool)

    def make_request(self, url, method='GET', **kwargs):
        return requests.request(method, url, **kwargs)

    def download(self, url, callback=None, opts=None):
        self._count += 1
        o = {'callback': callback, 'opts': opts}
        g = self._pool.spawn(self._download, url, opts)
        c = ArgsLink(self._done, o)
        g.rawlink(c)

        return g

    def _download(self, url, opts):
        u = urlparse(url)
        if u.netloc not in self.sessions:
            sess = requests.Session()
            sess.headers = self.headers
            self.sessions[u.netloc] = sess
        sess = self.sessions[u.netloc]
        return sess.request('GET', url, timeout=self.timeout)

    def _done(self, g, o):
        self._count -= 1
        rst = {}
        # rst['successful'] = g.successful()
        # rst['value'] = g.value
        # rst['exception'] = g.exception
        rst = DownloadResult(g.successful(), g.value, g.exception)

        if o and 'callback' in o:
            if 'opts' in o and o['opts']:
                args = (rst, o['opts'])
            else:
                args = (rst, {})
            apply(o['callback'], args)

        return rst
