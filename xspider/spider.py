#coding=utf8

#from base import register_filter, register_parser
import os.path
import urlparse
from collections import defaultdict
from .base import EventObject
from .loader import Loader
from .downloader import Downloader
from .parser import Parser






class Rule(EventObject):

    def __init__(self, rules, spider):
        self.name = rules.get('name')
        self.container = rules.get('container')
        self.allow_domain = rules.get('allow_domain')
        self._events = rules.get('events')
        rps = rules.get('parsers', {})
        #print rps
        self._parsers = rps
        self._is_default = rules.get('default', False)

        if 'events' in rules:
            evts = rules['events']
            for evt in evts:
                func = spider.load(evts[evt])
                if func and callable(func):
                    self.add_listener(evt, func)


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

        self._qcount = 0
        self._ccount = 0


    def __call__(self):
        self.fire('schedule_crawl', self)
        self.start_crawl()


    def init(self, cfg):
        self.cfg = cfg
        
        self.parse_cfg()

        c = {}
        c['timeout'] = 30
        if self._headers:
            c['headers'] = self._headers
        self._downloader = Downloader(c)

        self._parser = Parser(self)


        if self.plugin_path:
            self._loader = Loader(self.plugin_path)

        if self.cfg.get('variable'):
            self._env.update(self.cfg.get('variable'))

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

        if 'events' in self.cfg:
            evts = self.cfg['events']
            for evt in evts:
                func = self.load(evts[evt])
                if func and callable(func):
                    self.add_listener(evt, func)

        


    def parse_cfg(self):
        # print self.cfg
        cfgs = self.cfg
        if 'name' in cfgs:
            self.name = cfgs['name']
        else:
            self.name = self.__class__.__name__ + str(id(self))

        if 'plugin_path' in cfgs:
            self.plugin_path = os.path.join(cfgs['__basepath__'], cfgs['plugin_path'])

            #self.load_plugins(plugin_path)

        if 'start_urls' in cfgs:
            self.start_urls = cfgs['start_urls']
            

        if 'headers' in cfgs:
            self._headers = cfgs['headers']
        

        
        if 'sched' in cfgs:
            self._sched = cfgs['sched']



        if 'rules' in cfgs:
            for rcfg in cfgs['rules']:
                name = rcfg['name']
                #parse_rules = rule['parsers']
                robj = Rule(rcfg, self)
                self._rules[name] = robj
                if robj.is_default() or self.default_rule is None:
                    self.default_rule = name
            
        else:
            raise Exception("No Rule")



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

    def init_env(self):
        self._env['CURRENT_SPIDER'] = self
        self._env['get_config'] = self.get_config
        #self._env['register_filter'] = register_filter
        #self._env['register_parser'] = register_parser



    # def load_plugins(self, plugin_path):
    #     for fn in os.listdir(plugin_path):
    #         if fn in ('.', '..'): continue
    #         filename = os.path.join(plugin_path, fn)



    def init_events(self):
        pass

    def init_parser(self):
        pass

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
        self.fire('after_download', rlt, opts)
        try:
            if rlt.successful:
                rule = 'rule' in opts and opts['rule'] or None
                resp = rlt.value
                html = resp.text
                # print len(html)
                code = resp.status_code
                #parser = self.get_parser(rule)
                ro = self.get_rule(rule)
                rst, nxts = self._parser.parse(html, ro)
                #if 'persist' in ro:
                #    pass
                    #apply(o['persist'])

                self.fire('after_parse', rst, nxts, opts)
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
        finally:
            self._qcount -= 1

        print '-------------++++++++++++++++', self._qcount, self._ccount

        if self._qcount == 0:
            self._ccount += 1
            self.fire('finished_crawl', self)


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
            rule = 'rule' in o and o['rule'] or None
            url = self.clear(url)
            o['url'] = url
            # if url:
            #     self._size += 1
            #     g = dlMgr.download(url)
            #     c = ArgsLink(self._download_finished, o)
            #     g.rawlink(c)
            req = self._downloader.make_request(url)
            self.fire('before_request', req, o)
            self._downloader.download(url, self._download_finished, o)
        except:
            pass
        else:
            self._qcount += 1



    def start_crawl(self, urls=None):
        if urls is None:
            urls = self.start_urls

        if type(urls) is basestring:
            urls = [urls]

        self.fire('start_crawl', urls, self)

        for url in urls:
            if type(url) is dict:
                rn = url.get('rule', self.default_rule)
                ul = url.get('url')
                for u in ul:
                    o = {'url': u, 'rule': rn}
                    self._download(u, o)
            else:
                o = {'url': url}
                self._download(url, o)

        