#coding=utf8

#from base import register_filter, register_parser
import os.path
import urlparse
# from collections import defaultdict
from .base import EventObject
from .loader import Loader
from .downloader import Downloader
from .parser import Parser
from .log import Logger






class Rule(object):

    def __init__(self, rules):
        self.name = rules.get('name')
        self.container = rules.get('container')
        self.allow_domain = rules.get('allow_domain')
        self.events = rules.get('events')
        rps = rules.get('parsers', {})
        #print rps
        self._parsers = rps
        self._is_default = rules.get('default', False)

        # if 'events' in rules:
        #     self.events = rules['events']
        # else:
        #     self.events = None

        # if 'events' in rules:
        #     evts = rules['events']
        #     for evt in evts:
        #         func = spider.load(evts[evt])
        #         if func and callable(func):
        #             self.add_listener(evt, func)


    def is_allow_domain(self, url):
        if self.allow_domain:
            if url.startswith('http'):
                pr = urlparse.urlparse(url)
                netloc = pr.netloc
            else:
                netloc = url
            return netloc in self.allow_domain

        return True

    def parse_rules(self):
        return self._parsers


    def is_default(self):
        return self._is_default

    # def __getitem__(self, name):
    #     return self._parsers.get(name, None)

    # def __getattr__(self, name):
    #     return self._parsers.get(name, None)

    # def __contains__(self, item):
    #     return item in self._parsers


    def _validate(self, rules):
        for rule in rules:
            if 'name' not in rule:
                continue
            if not ('handler' in rule or 'parsers' in rule or 'selector' in rule):
                continue
            # if 'handler' in rule and rule['handler'] in self._filter and not callable(self._filter[rule['handler']]):
            #     continue





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

        self._qcount = 0            #未完成的队列长度
        self._ccount = 0            #抓取次数

        self._cfg_file = None

        self._dld_urls = set()    #已下载或正在下载的url



    def __call__(self):
        if self._qcount > 0:
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


        if 'plugin_path' in cfg:
            self.plugin_path = os.path.join(cfg['__basepath__'], cfg['plugin_path'])

            #self.load_plugins(plugin_path)

        if 'start_urls' in cfg:
            self.start_urls = cfg['start_urls']
            
        if 'headers' in cfg:
            self._headers = cfg['headers']
        

        if 'sched' in cfg:
            self._sched = cfg['sched']

        c = {}
        c['timeout'] = 30
        if self._headers:
            c['headers'] = self._headers
        self._downloader = Downloader(c)

        self._parser = Parser(self)

        # 插件加载
        if self.plugin_path:
            self._loader = Loader(self.plugin_path)
        # 
        if self.cfg.get('variable'):
            self._env.update(self.cfg.get('variable'))
        #
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


        # 规则及事件
        if 'rules' in cfg:
            for rcfg in cfg['rules']:
                name = rcfg['name']
                #parse_rules = rule['parsers']
                robj = Rule(rcfg)
                self._rules[name] = robj
                if robj.is_default() or self.default_rule is None:
                    self.default_rule = name
                # print 'rules -->>', name, robj.events
                if robj.events:
                    for en in robj.events:
                        evt = robj.events[en]
                        func = self.load(evt)
                        if func and callable(func):
                            ename = '%s_%s' % (name, en)
                            self.add_listener(ename, func)
                            self._log.info(u"监听规则 %s 事件 %s" % (name, evt))

        else:
            raise Exception("No Rule")


        # 事件
        if 'events' in self.cfg:
            evts = self.cfg['events']
            for evt in evts:
                func = self.load(evts[evt])
                if func and callable(func):
                    self.add_listener(evt, func)
                    self._log.info(u"监听事件 %s" % evt)



    # def parse_cfg(self):
    #     # print self.cfg
    #     cfg = self.cfg




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
        obj = self._loader.load(mod, env)

        if func:
            return getattr(obj, func, None)
        return obj

    @property
    def is_running(self):
        return self._qcount > 0

    def init_env(self):
        self._env['CURRENT_SPIDER'] = self
        self._env['get_config'] = self.get_config
        #self._env['register_filter'] = register_filter
        #self._env['register_parser'] = register_parser



    # def load_plugins(self, plugin_path):
    #     for fn in os.listdir(plugin_path):
    #         if fn in ('.', '..'): continue
    #         filename = os.path.join(plugin_path, fn)



    # def init_events(self):
    #     pass

    # def init_parser(self):
    #     pass

    def get_rule(self, name):
        # if name not in self._rules:
        #     cfgs = self._rule_cfgs.get(name)
        #     rule = Rule(cfgs)
        #     self._rules[name] = rule

        # return self._rules[name]
        return self._rules.get(name)


    # def add_filter(self, func, name=None):
    #     self._filter[name] = func


    def clear(self, url):
        o = urlparse.urlparse(url)

        return url


    def _download_finished(self, rlt, opts):

        evt_name = []
        #rule = 'rule' in opts and opts['rule'] or None
        if opts:
            rule_name = opts.get('rule', '')
        else:
            rule_name = ''

        if rule_name:
            evt_name.append('%s_after_download' % rule_name)
        evt_name.append('after_download')
        ret = self.fire(evt_name, rlt, opts)

        try:
            if ret is not False:
                if rlt.successful:
                    
                    resp = rlt.value
                    html = resp.text
                    # print len(html)
                    code = resp.status_code
                    #parser = self.get_parser(rule)
                    ro = self.get_rule(rule_name)
                    rst, nxts = self._parser.parse(html, ro)
                    #if 'persist' in ro:
                    #    pass
                        #apply(o['persist'])
                    evt_name = []
                    if rule_name:
                        evt_name.append('%s_after_parsed' % rule_name)
                    evt_name.append('after_parsed')
                    self.fire(evt_name, rst, nxts, opts)
                    # print '++++++++++++++'*3
                    # #print rst
                    # print nxts
                    # print '++++++++++++++'*3
                    if nxts:
                        for nx in nxts:
                            url = nx['url']
                            o = nx['opts']
                            #o = {'p': rst}
                            if ro.is_allow_domain(url):
                                self._download(url, o)
                            # print '-------------add next', url

                    # if rule == 'r2':
                    #     print '11++++++++++', rst.get('shop', '').encode('gbk') #, rst.get('title', '').encode('gbk')

                else:
                    if not 'retry' in opts:
                        opts['retry'] = 0
                    opts['retry'] += 1
                    url = rlt.value.url #opts['url']
                    self._download(url, opts)
        except:
            self._log.exception(u'处理页面内容时出错 %s' % rlt.value.url)                    
        finally:
            self._qcount -= 1

        print '-------------++++++++++++++++', self._qcount, self._ccount

        if self._qcount == 0:
            self._ccount += 1
            self.fire('finished_crawl', self)
            self._log.info(u'完成抓取 %s' % self._ccount)


    # def _download_finished2(self, g, o):
    #     self._count -= 1
    #     if g.successful():
    #         rule = 'rule' in o and o['rule'] or None
    #         resp = g.value
    #         html = resp.text
    #         code = rest.status_code
    #         #parser = self.get_parser(rule)
    #         ro = self.get_rule(rule)
    #         rst, nxts = self._parser.parse(html, ro.parser_rules)
    #         if 'persist' in ro:
    #             pass
    #             #apply(o['persist'])

    #         if nxts:
    #             for url in nxts:
    #                 o = {'p': rst}
    #                 self._download(url, o)

    #     else:
    #         if not 'retry' in o:
    #             o['retry'] = 0
    #         o['retry'] += 1
    #         self._download(o['url'], o)


    def _download(self, url, o):
        #self.fire('before_download', url, o)
        
        try:
            rule_name = o and o.get('rule') or None
            url = self.clear(url)
            if not url:
                return

            if url in self._dld_urls:   #是否已经下载或正在下载
                evt_name = []
                if rule_name:
                    evt_name.append('%s_reduplicated' % rule_name)
                evt_name.append('reduplicated')
                ret = self.fire(evt_name, url)            
                return


            o['url'] = url
            # if url:
            #     self._size += 1
            #     g = dlMgr.download(url)
            #     c = ArgsLink(self._download_finished, o)
            #     g.rawlink(c)
            req = self._downloader.make_request(url)

            evt_name = []
            if rule_name:
                evt_name.append('%s_before_request' % rule_name)
            evt_name.append('before_request')
            ret = self.fire(evt_name, req, o)
            if ret is not False:
                self._downloader.download(url, self._download_finished, o)
        except:
            self._log.exception(u'新增到下载队列时出错 %s' % url)
        else:
            self._dld_urls.add(url)
            self._qcount += 1



    def start_crawl(self, urls=None):
        if urls is None:
            urls = self.start_urls

        self._dld_urls.clear()   #新的开始

        if type(urls) is basestring:
            urls = [urls]

        self.fire('start_crawl', urls, self)

        for url in urls:
            if type(url) is dict:
                rn = url.get('rule', self.default_rule)
                ul = url.get('url')
                if type(ul) is basestring:
                    ul = [ul]
                for u in ul:
                    o = {'url': u, 'rule': rn}
                    self._download(u, o)
            else:
                o = {'url': url}
                self._download(url, o)

    
    def get_info(self):
        """"""

        return (self._qcount, self._downloader.length, self._ccount)   #队列长度，下载中，已完成次数