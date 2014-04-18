#coding=utf8

#from base import register_filter, register_parser
import os.path
import urlparse
from gevent.greenlet import SpawnedLink
from gevent.hub import greenlet, getcurrent, get_hub
# from collections import defaultdict
from .base import EventObject
from .loader import Loader
from .downloader import Downloader
from .parser import Parser, ParseRule
from .log import Logger




class ArgsLink(SpawnedLink):

    def __init__(self, callback, *args, **kwargs):
        super(ArgsLink, self).__init__(callback)
        self._args = args
        self._kwargs = kwargs
        # print 'c$$$$$$$$$$$$$$$', self, hasattr(self, 'callback')

    def __call__(self, source):
        # print '$$$$$$$$$$$$$$$', self, hasattr(self, 'callback')
        g = greenlet(self.callback, get_hub())
        self._args = (source, ) + self._args
        g.switch(*self._args, **self._kwargs)




class Spider(EventObject):
    """docstring for Project"""
    def __init__(self):
        super(Spider, self).__init__()
        
        self.plugin_path=None
        self.start_urls = []
        self.default_rule = None
        self._env = {}
        self._rules = {}
        self._sched = None
        self._loader = None
        self._downloader = None
        self._headers = None

        # self._qcount = 0            #未完成的队列长度
        self._ccount = 0            #抓取次数

        self._cfg_file = None

        self._dld_urls = set()    #已下载或正在下载的url



    def __call__(self):
        # if self._qcount > 0:
        if self._downloader.qsize > 0:
            raise Exception('Running ......')

        self.fire('schedule_crawl', self)
        self.start_crawl()


    def init(self, cfg):
        import yaml
        import json

        if isinstance(cfg, basestring):
            if not os.path.exists(cfg):
                raise Exception('%s not exists' % cfg)


            self._cfg_file = cfg
            _, ext = os.path.splitext(cfg)
            txt = open(cfg).read()
            if ext == '.json':
                cfg = json.load(txt)
            else:
                cfg = yaml.load(txt)

            cfg['__basepath__'] = os.path.dirname(self._cfg_file)



        if type(cfg) is not dict:
            raise Exception('invalid cfg')

        self.cfg = cfg
        
        
        if 'name' in cfg:
            self.name = cfg['name']
        else:
            self.name = self.__class__.__name__ + str(id(self))

        self._log = Logger.get(self.name, logpath=cfg['__basepath__'])  #


        #self.parse_cfg()
        self.max_retry = cfg.get('max_retry', 3)
        self.max_deep = cfg.get('max_deep', 3)


        if 'plugin_path' in cfg:
            self.plugin_path = os.path.join(cfg['__basepath__'], cfg['plugin_path'])

            #self.load_plugins(plugin_path)

        if 'start_urls' in cfg:
            self.start_urls = cfg['start_urls']
            
      
        if 'sched' in cfg:
            self._sched = cfg['sched']



        # 插件
        if self.plugin_path:
            self._loader = Loader(self.plugin_path)
        
        # 插件自定义变量
        if self.cfg.get('variable'):
            self._env.update(self.cfg.get('variable'))

        # 插件环境初始化
        if self.cfg.get('initialize'):
            inits = self.cfg.get('initialize')
            for it in inits:
                m = self.load(it['name'])
                if m:
                    r = m()
                    var = it.get('var')
                    if var:
                        self._env[var] = r

        #print self._env

        # 下载
        c = {}
        c['timeout'] = 30
        c['interval'] = cfg.get('interval')
        if 'headers' in cfg:
            c['headers'] = cfg['headers']

        print 'Downloader header ------------->', c
        self._downloader = Downloader(c)

        # 解析器
        pc = {}
        if 'allow_domain' in cfg:
            pc['allow_domain'] = cfg['allow_domain']
        pc['rules'] = cfg.get('parsers')
        self._parser = Parser(pc, self)

        # 规则及事件
        # if 'parsers' in cfg:
        #     for rcfg in cfg['parsers']:
        #         name = rcfg['name']
        #         #parse_rules = rule['parsers']
        #         robj = ParseRule(rcfg, spider)
        #         self._rules[name] = robj
        #         if robj.is_default() or self.default_rule is None:
        #             self.default_rule = name
        #         # print 'rules -->>', name, robj.events
        #         if robj.events:
        #             for en in robj.events:
        #                 evt = robj.events[en]
        #                 func = self.load(evt)
        #                 if func and callable(func):
        #                     ename = '%s_%s' % (name, en)
        #                     self.add_listener(ename, func)
        #                     self._log.info(u"监听规则 %s 事件 %s" % (name, evt))

        #     for rn in self._rules:
        #         rule = self._rules[rn]

        # else:
        #     raise Exception("No Rule")


        # 事件
        if 'events' in self.cfg:
            evts = self.cfg['events']
            for evt in evts:
                func = self.load(evts[evt])
                if func and callable(func):
                    self.add_listener(evt, func)
                    self._log.info(u"监听事件 %s" % evt)






    def get_var(self, name):
        """get config"""
        return self._env.get(name, None)

    def set_var(self, name, value):
        """get config"""
        self._env[name] = value


    def get_cfg(self, name):

        val = self.cfg
        ns = name.split('.')
        for n in ns:
            if n in val:
                val = val[n]
            else:
                return None
        return val


    def load(self, name):
        """加载插件"""

        if '@' in name:
            func, mod = name.split('@', 1)
        else:
            func, mod = None, name

        if mod in self._env:
            obj = self._env[mod]
            if func:
                return getattr(obj, func, None) 
            return obj

        if self._loader is None:
            return

        env = {}
        env['get_var'] = self.get_var
        env['set_var'] = self.set_var
        env['get_cfg'] = self.get_cfg
        env['_download'] = self._download
        obj = self._loader.load(mod, env)

        if obj and func:
            return getattr(obj, func, None)
        return obj

    @property
    def is_running(self):
        return self._downloader.qsize > 0


    # def guess_rule(self, url):
    #     for rn in self._rules:
    #         rule = self._rules[rn]


    # def get_rule(self, name):
    #     return self._rules.get(name)


    def clear(self, url):
        o = urlparse.urlparse(url)

        return url


    def _download_finished(self, rlt, opts):
        # print '------------------------_download_finished----------------------------'
        resp = None
        exce = None
        url = None

        try:
            if rlt.successful(): #下载成功
                resp = rlt.value
                url = resp.url

                rule = None
                if opts and 'rule' in opts:
                    rule = self._parser.get_rule(opts['rule'])

                if not rule:
                    rule = self._parser.guess_rule(url)

                print 'GUESS RULE ==>>> ', rule and rule.name

                evt_name = []
                if rule:
                    evt_name.append('%s_after_download' % rule.name)
                evt_name.append('after_download')
                ret = self.fire(evt_name, rlt, opts)

                html = resp.text
                result, links = self._parser.parse(resp, rule)

                if links:
                    self._download_links(links, {'referer': url})
                # fp = open('urls.txt', 'w')
                # fp.write('\n'.join(links))
                # fp.close()

                evt_name = []
                if rule:
                    evt_name.append('%s_after_parsed' % rule.name)
                    evt_name.append('after_parsed')
                    self.fire(evt_name, result, opts)

            else:   #下载失败
                exce = rlt.exception
                print exce
                self._log.error(u'下载出错', exc_info=exce)
                if not opts:
                    opts = {}

                #重试
                if 'url' in opts:
                    url = opts['url']
                    if not 'retry' in opts:
                        opts['retry'] = 0

                    if opts['retry'] < self.max_retry:
                        opts['retry'] += 1

                        self._dld_urls.remove(url)    
                        self._download(url, opts)                
        except:
            self._log.exception(u'处理内容时出错 %s' % opts.get('url'))    
        # finally:
        #     self._qcount -= 1


        # print '-------------++++++++++++++++', self._qcount, self._ccount

        # if self._qcount == 0:
        if self._downloader.qsize == 0:
            self._ccount += 1
            self.fire('finished_crawl', self)
            self._log.info(u'完成抓取 %s' % self._ccount)


    def _download_links(self, urls, o):
        for url in urls:
            self._download(url, o.copy())
            # return

    def _download(self, url, o=None, immediate=False):
        #self.fire('before_download', url, o)
        
        try:
            # rule_name = o and o.get('rule') or None
            # url = self.clear(url)
            if not url:
                return

            if url in self._dld_urls:   #是否已经下载或正在下载
                ret = self.fire('reduplicated', url)            
                return

            if not o:
                o = {}
            o['url'] = url
            hds = {}
            if 'referer' in o:
                hds['referer'] = o['referer']

            # print 'Req header -------------> b4', hds
            req = self._downloader.make_request(url, headers=hds)

            # print 'Req header ------------->', req.headers

            ret = self.fire('before_request', req, o)
            if ret is not False:
                # g = self._downloader.download(req, self._download_finished, o, immediate)
                g = self._downloader.download(req)
                c = ArgsLink(self._download_finished, o)
                g.rawlink(c)
                # print g
        except:
            self._log.exception(u'新增到下载队列时出错 %s' % url)
        else:
            self._dld_urls.add(url)
            # self._qcount += 1



    def start_crawl(self, urls=None, opts=None):
        self._log.info("==========START==========")
        if urls is None:
            urls = self.start_urls

        self._dld_urls.clear()   #新的开始

        if isinstance(urls, basestring):
            urls = [urls]

        self.fire('start_crawl', urls, self)

        for url in urls:
            self._download(url)


    def stop(self):
        """"""
        self._log.info("==========STOP==========")
    
    def get_info(self):
        """"""
        return (self._downloader.qsize, self._ccount)   #队列长度，下载中，已完成次数