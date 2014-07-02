#coding=utf8

#from base import register_filter, register_parser
import os.path
import copy
import urlparse
from gevent.greenlet import Greenlet, SpawnedLink
from gevent.hub import greenlet, getcurrent, get_hub
from gevent import threadpool, monkey
# from collections import defaultdict
from .base import EventObject, OptionData, merge_dict, EventMgr
from .loader import Loader
from .downloader import Downloader
from .parser import Parser, ParseRule
from .urlmgr import UrlMgr
from .log import Logger
from .scheduler import Scheduler

thread_get_ident = monkey.get_original('thread', 'get_ident')


# class ArgsLink(SpawnedLink):

#     def __init__(self, callback, *args, **kwargs):
#         super(ArgsLink, self).__init__(callback)
#         self._args = args
#         self._kwargs = kwargs
#         # print 'c$$$$$$$$$$$$$$$', self, hasattr(self, 'callback')

#     def __call__(self, source):
#         # print '$$$$$$$$$$$$$$$', self, hasattr(self, 'callback')
#         # g = greenlet(self.callback, get_hub())
#         self._args = (source, ) + self._args
#         # g.switch(*self._args, **self._kwargs)
#         self.callback(*self._args, **self._kwargs)




class Spider(object):
    """docstring for Project"""
    def __init__(self):
        super(Spider, self).__init__()
        
        self.plugin_path=None
        self.start_urls = []
        # self.default_rule = None
        self._env = {}
        # self._rules = {}
        # self._sched = None
        # self._loader = None
        # self._downloader = None
        self._headers = None
        self.disabled = False

        self.model = ''

        #-----------------------------------
        self._log = None
        self._loader = None
        self._evtmgr = None
        self._urlmgr = None
        self._downloader = None
        self._parser = None
        self._sched = Scheduler()
        #-----------------------------------

        self._stopping = True


        # self._qcount = 0            #未完成的队列长度
        self._ccount = 0            #抓取次数

        self._cfg_file = None


        self._pool = threadpool.ThreadPool(4)


    @property
    def evtmgr(self):
        return self._evtmgr

    @property
    def urlmgr(self):
        return self._urlmgr

    @property
    def parser(self):
        return self._parser

    @property
    def log(self):
        return self._log
    


    def __call__(self):
        # if self._qcount > 0:
        if self._urlmgr.qsize > 0:
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
        self._evtmgr = EventMgr(self._log)

        self.disabled = cfg.get('disabled', False)
        if self.disabled:
            print '---------------------DISABLED-------------------------', self.name
            return
        print '---------------------DISABLED XXX-------------------------', self.name
        #self.parse_cfg()
        self.max_retry = cfg.get('max_retry', 3)
        self.max_deep = cfg.get('max_deep', 3)





        if 'start_urls' in cfg:
            self.start_urls = cfg['start_urls']
            





        # 插件
        if 'plugin_path' in cfg:
            self.plugin_path = os.path.join(cfg['__basepath__'], cfg['plugin_path'])
            self._loader = Loader(self.plugin_path, self)
        
        # 插件自定义变量
        if cfg.get('variable'):
            self._env.update(cfg.get('variable', {}))

        # 插件环境初始化
        if cfg.get('initialize'):
            inits = cfg.get('initialize')
            for it in inits:
                m = self.load(it['name'])
                if m:
                    r = m()
                    var = it.get('var')
                    if var:
                        self._env[var] = r

        #print self._env

        #链接管理
        ucfg = cfg.get('urlmgr', {})
        ucfg['db'] = os.path.join(cfg['__basepath__'], "%s_url.db" % self.name)
        self._urlmgr = UrlMgr(ucfg, self)
        self._urlmgr.load()


        # 下载
        c = {}
        c['timeout'] = 30
        c['interval'] = cfg.get('interval')
        if 'headers' in cfg:
            c['headers'] = cfg['headers']

        self._downloader = Downloader(c, self)
        self._downloader.add_callback(self._download_finished)

        # 解析器
        pc = {}
        pc['allow_domain'] = cfg.get('allow_domain')
        pc['link_filter'] = cfg.get('link_filter')
        pc['collect_links'] = cfg.get('collect_links', True)
        pc['rules'] = cfg.get('parser')
        self._parser = Parser(pc, self)


        # 事件
        if 'events' in cfg:
            evts = cfg['events']
            for evt in evts:
                func = self.load(evts[evt])
                if func and callable(func):
                    # self.add_listener(evt, func)
                    self._evtmgr.add_listener(evt, func)


        if 'sched' in cfg:
            # self._sched = cfg['sched']
            self._sched.add(self, cfg['sched'])
        else:
            self._sched.add(self.start_crawl, None)

        self._sched.add(self._urlmgr.sync, {'type': 'interval', 'seconds': 30})



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

        # self._log.info(u'加载插件 %s' % name)

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

        # env = {}
        # env['get_var'] = self.get_var
        # env['set_var'] = self.set_var
        # env['get_cfg'] = self.get_cfg
        # env['_download'] = self._sync_download
        # env['_spider'] = self
        try:
            obj = self._loader.load(mod)
        except:
            self._log.exception(u'加载 %s 异常' % name)
            return 

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


    # def clear(self, url):
    #     o = urlparse.urlparse(url)

    #     return url


    def _download_finished(self, rlt, opts):
        print '----- _download_finished', thread_get_ident(), rlt.successful
        self._pool.spawn(self._parse_resp, rlt, opts)


    def _parse_resp(self, rlt, opts):
        # print '------------------------_download_finished----------------------------'

        url = rlt.value.url if rlt.value and hasattr(rlt.value, 'url') else None
        oul = opts['__url__'] if opts and '__url__' in opts else None


        rule = None
        if opts and 'rule' in opts:
            rule = self._parser.get_rule(opts['rule'])

        if not rule:
            rule = self._parser.guess_rule(url)

        if not rule and opts and 'default_rule' in opts:
            rule = self._parser.get_rule(opts['default_rule'])


        # self.evtmgr.fire('')


        if rlt.value:
            resp = rlt.value
            url = resp.url
            

            if oul and url != oul:
                self._urlmgr.alias(oul, url)

            changed = self._urlmgr.update(url, response=resp, status=resp.status_code)

            if changed:
                if resp.status_code == 200:
                    try:
                        result, links = self._parser.parse(resp, rule)
                        # print result, links
                        print 'unfinished_tasks ==== ', self._urlmgr.qsize
                        # self.evtmgr.fire()
                    except:
                        self._log.exception(u'页面内容处理异常 %s' % url)
                    else:

                        hds = {'referer': url}
                        data = None
                        if opts and 'data' in opts:
                            data = opts['data']

                        if links and not self._stopping:    #<---------------------------------------------------STOPPING----------------
                            # print '-------------------------------'
                            # print links
                            # print '**************************************'
                            urls = set()

                            for link in links:
                                opt = None
                                if type(link) is tuple:
                                    opt = link[1]
                                    nul = link[0]
                                elif not isinstance(link, basestring):
                                    opt = link.options
                                    nul = link.url
                                else:
                                    nul = link
               
                                if opt:
                                    if 'headers' in opt:
                                        opt['headers'].update(hds)

                                    if data:
                                        if 'data' in opt:
                                            d = copy.deepcopy(data)
                                            d.update(opt['data'])
                                            opt['data'] = d
                                        else:
                                            opt['data'] = data

                                prior = opt and opt.get('prior') or True
                                
                                self._urlmgr.put(nul, opt, prior)

                                urls.add(nul)


                            evts = ['new_links']
                            if rule:
                                evts.insert(0, '%s_new_links' % rule.name)
                            self._evtmgr.fire(evts, urls, {'referer': url}, self)


                        if type(result) is tuple:
                            result, l = result
                            for link in l:
                                opt = None
                                if type(link) is tuple:
                                    opt = link[1]
                                    nul = link[0]
                                elif not isinstance(link, basestring):
                                    opt = link.options
                                    nul = link.url
                                else:
                                    nul = link
               
                                if opt:
                                    if 'headers' in opt:
                                        opt['headers'].update(hds)

                                    if data:
                                        if 'data' in opt:
                                            d = copy.deepcopy(data)
                                            d.update(opt['data'])
                                            opt['data'] = d
                                        else:
                                            opt['data'] = data

                                prior = opt and opt.get('prior') or True
                                
                                self._urlmgr.put(nul, opt, prior)                                


                        evts = ['after_parsed']
                        if rule:
                            evts.insert(0, '%s_after_parsed' % rule.name)

                        od = OptionData()
                        od.options = opts
                        od.response = resp
                        od.url = url
                        od.rule = rule
                        self._evtmgr.fire(evts, result, od)

                else:
                    self._log.warning(u'返回状态码 %s  status_code=%s' % (url, resp.status_code))
            else:  ## status code
                self._log.warning(u'内容没变动，无需重新处理 %s' % url)

        else:

            if not opts:
                opts = {}
            if not 'retry' in opts:
                opts['retry'] = 0

            exce = rlt.exception
            self._log.error(u'下载失败 <%s>: %s' % (opts['retry']+1, url or oul), exc_info=exce)

            if not url and not oul:
                print url, oul, rlt.value, rlt
                self._log.warning('URL IS NONE')
                return



            if opts['retry'] < self.max_retry:
                opts['retry'] += 1

                self._urlmgr.put(url or oul, opts)

        

        if self._urlmgr.qsize == 0 and len(self._pool) == 0:
            pass


        #         evt_name = []
        #         if rule:
        #             evt_name.append('%s_after_parsed' % rule.name)
        #         evt_name.append('after_parsed')
        #         od = OptionData()
        #         od.options = opts
        #         od.response = resp
        #         od.url = url
        #         od.rule = rule
        #         # o = {'options': opts, 'response': resp, 'url': url, 'rule': rule}
        #         self.fire(evt_name, result, od)


        # # if self._qcount == 0:
        # print 'qsize %s -- %s -- th %s' % (self._downloader.qsize, thread_get_ident(), len(self._pool))
        # if self._downloader.qsize == 0 and len(self._pool) == 1:
        #     self._ccount += 1
        #     self.fire('finished_crawl', self)
        #     self._log.info(u'完成抓取 %s' % self._ccount)

    def fetch(self, url, opts):
        return self._downloader.download(url, opts)



    def start_crawl(self, urls=None, opts=None):
        self._log.info("==========START==========")
        print thread_get_ident()
        if urls is None:
            urls = self.start_urls


        if self._urlmgr.qsize == 0:  
            if isinstance(urls, basestring):
                urls = [urls]

            # self.fire('start_crawl', urls, self)

            for url in urls:
                self._urlmgr.put(url, force=True)

        print 'unfinished_tasks start', self._urlmgr.qsize
        self._downloader.start()
        self._stopping = False


    def stop(self):
        """"""
        self._log.info("==========STOP==========")
        # self._downloader.stop()
        # self._pool.kill()
        self._stopping = True
    
    def get_info(self):
        """"""
        return (self._downloader.qsize, self._ccount)   #队列长度，下载中，已完成次数
