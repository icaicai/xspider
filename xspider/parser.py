#coding=utf8

import re
import urlparse
import copy
from collections import namedtuple
from lxml import html
from base import OptionData
from log import Logger
from requests.compat import str as rstr


ParseResult = namedtuple('ParseResult', ['values', 'next'])










class ParseRule(object):

    def __init__(self, cfgs, spider):
        self.name = name = cfgs.get('name')
        self.container = cfgs.get('container')
        # self.clean_rule = cfgs.get('clean_url')
        self.allow_domain = cfgs.get('allow_domain')
        self.collect_links = cfgs.get('collect_links', True)
        als = cfgs.get('link_filter')
        self.link_filter = None

        if als:
            ls = []
            for al in als:
                l = re.compile(al)
                ls.append(l)
            self.link_filter = ls

        self._log = spider.log

        self.parse_handler = None
        if 'parse_handler' in cfgs:
            func = spider.load(cfgs['parse_handler'])
            if func:
                self.parse_handler = func

            del cfgs['parse_handler']


        rps = cfgs.get('rules', {})
        #print rps
        self._rules = self._validate(rps, spider)
        # self._is_default = cfgs.get('default', False)
        
        # print rules.get('events')
        events = cfgs.get('events')
        if events:
            self._log.info(u'设置规则事件 %s' % name)
            for en in events:
                evt = events[en]
                func = spider.load(evt)
                # print evt, func
                if func and callable(func):
                    ename = '%s_%s' % (name, en)
                    spider.evtmgr.add_listener(ename, func)
                    # self._log.info(u"监听规则 %s 事件 %s - %s" % (name, en, evt))

        self._match_rules = None

        if 'match' in cfgs:
            self._match(cfgs['match'], spider)
    # def is_allow_domain(self, url):
    #     if self.allow_domain:
    #         if url.startswith('http'):
    #             pr = urlparse.urlparse(url)
    #             netloc = pr.netloc
    #         else:
    #             netloc = url
    #         return netloc in self.allow_domain

    #     return True

    def is_match(self, url):
        if not self._match_rules:
            return False

        if self._match_rules == '*':
            return True

        for mr in self._match_rules:
            if mr(url):
                return True

        return False



    def rules(self):
        return self._rules


    # def is_default(self):
    #     return self._is_default


    def _match(self, matchs, spider):
        if matchs == '*':
            self._match_rules = '*'
            return

        def _match_func(part, act, val):
            if act != 'eq':
                # val = val.replace('*', '.*').replace('?', '.')
                val = re.compile(val)
            def func(url):
                if part == 'url':
                    v = url
                else:
                    p = urlparse.urlparse(url)
                    v = getattr(p, part)
                if act == 'eq':
                    return v == val
                return val.search(v) is not None
            return func


        self._match_rules = []

        avpart = ('url', 'host', 'path', 'query', 'fragment')
        for key in matchs:
            if '_' in key:
                p, a = key.split('_', 1)
            else:
                p, a = key, None

            if p not in avpart:
                continue

            v = matchs[key]
            if p == 'host':
                p = 'netloc'
            f = _match_func(p, a, v)

            self._match_rules.append(f)


    def _validate(self, prules, spider):
        p = []
        for rule in prules:
            # if 'name' not in rule:
            #     continue
            if not ('value' in rule or 'selector' in rule):
                continue

            
            if 'handler' in rule:
                func = spider.load(rule['handler'])
                if func:
                    rule['handler'] = func
                else:
                    del rule['handler']

            if 'filter' in rule:
                filters = rule['filter']
                fs = []
                if type(filters) is not list:
                    filters = [filters]
                for f in filters:
                    func = spider.load(f)
                    if func:
                        fs.append(func)

                if fs:
                    rule['filter'] = fs
                else:
                    del rule['filter']

            if 'rules' in rule:
                rule['rules'] = self._validate(rule['rules'], spider)



            # if 'handler' in rule and rule['handler'] in self._filter and not callable(self._filter[rule['handler']]):
            #     continue

            p.append(rule)

        return p



class Parser(object):
    """docstring for Parser"""

    # filters = {}

    def __init__(self, cfgs, spider):
        super(Parser, self).__init__()
        self._log = spider.log

        self._rules = []
        # self.clean_rule = cfgs.get('clean_rule')
        self.allow_domain = cfgs.get('allow_domain')

        als = cfgs.get('link_filter')
        self.link_filter = None

        if als:
            ls = []
            for al in als:
                l = re.compile(al)
                ls.append(l)
            self.link_filter = ls

        self.default_rule = None

    # def init(self, cfgs, spider):
        if 'rules' in cfgs and cfgs['rules']:
            for rcfg in cfgs['rules']:
                #name = rcfg['name']
                #parse_rules = rule['parsers']
                robj = ParseRule(rcfg, spider)
                #self._rules[name] = robj
                self._rules.append(robj)
                # if robj.is_default() or self.default_rule is None:
                #     self.default_rule = name
                # print 'rules -->>', name, robj.events


        self._linkre = re.compile('^http(s?)://.+')

        # for rn in self._rules:
        #     rule = self._rules[rn]

    def get_rule(self, name):
        for rule in self._rules:
            if rule.name == name:
                return rule

        return None        


    def guess_rule(self, url):
        if not url:
            return None
        for rule in self._rules:
            if rule.is_match(url):
                return rule

        return None



    def parse(self, resp, rule):


        # Try charset from content-type
        content = None
        encoding = resp.encoding

        if not resp.content:
            # content = str('')
            return None, None

        # Fallback to auto-detected encoding.
        if resp.encoding is None or resp.encoding == 'ISO-8859-1':
            encoding = resp.apparent_encoding

        # Decode unicode from given encoding.
        try:
            content = rstr(resp.content, encoding, errors='replace')
        except (LookupError, TypeError):
            content = rstr(resp.content, errors='replace')


        text = content
        url = resp.url
        try:
            doc = html.document_fromstring(text)
            doc.make_links_absolute(doc.base_url or url)
        except:
            doc = None

        if rule and rule.parse_handler:
            # r = rule.parse_handler(resp, rule)
            od = OptionData()
            od.url = url
            od.doc = doc
            od.rule = rule
            r = rule.parse_handler(resp, od)
            if type(r) in (tuple, list) and len(r) == 2:
                result, links = r
            else:
                result, links = r, None
        elif doc is not None:
            if not rule or (rule and rule.collect_links):
                links = self._get_links(doc, rule)
            else:
                links = None

            if rule:
                result = self._parse(doc, rule.rules(), resp)
                # for l in ls:
                #     if type(l) is tuple:
                #         url = l[0]
                #     else:
                #         url = l
                #     if links and url in links:
                #         links.remove(url)
                #         continue
                #     links.add(l)
                # if ls:
                #     if links is None:
                #         links = ls
                #     else:
                #         links.extend(ls)
            else:
                result = None
        else:
            result = None
            links = None

        return result, links



    def _get_links(self, doc, rule=None):
        urls = []

        if rule:
            allow_domain = rule.allow_domain or self.allow_domain
            link_filter = rule.link_filter or self.link_filter
            # clean_rule = rule.clean_rule or self.clean_rule
        else:
            allow_domain = self.allow_domain
            link_filter = self.link_filter
            # clean_rule = self.clean_rule


        # print clean_rule

            
        # print allow_link, '**************************'
        for el, attr, link, pos in doc.iterlinks():
            if el.tag == "a":
                # if '#' in link:
                #     link = link[:link.index('#')]
                # if clean_rule:
                #     link = self.clean_url(link, clean_rule)

                if not self._linkre.match(link):   #http开头
                    continue

                if link in urls:
                    continue

                if allow_domain:
                    pr = urlparse.urlparse(link)
                    if pr.netloc not in allow_domain:
                        continue
                if link_filter:
                    a = False
                    for alre in link_filter:
                        if alre.search(link):
                            a = True
                            break
                    if not a:
                        continue

                urls.append(link)
        return urls


    def _parse(self, container, items, resp):
        values = {}
        links = []

        for item in items:
            name = item.get('name', None)
            value = None
            if 'selector' in item:
                #nodes = container.xpath(item['selector'])
                #print 'selector ===', item['selector']
                nodes = container.cssselect(item['selector'])

                if 'handler' in item and callable(item['handler']):
                    value = self.apply_parse(item['handler'], nodes, name, values, item)       
                    # print 'handler ==>', value, values         
                elif 'rules' in item and item['rules']:
                    #value = [self.parse(node, item.parsers) for node in nodes]
                    value = []
                    for node in nodes:
                        v, l = self._parse(node, item['rules'], resp)
                        value.append(v)
                        links.extend(l)
                else:
                    if 'attrib' in item and item['attrib']:
                        value = [unicode(node.get(item['attrib'])) for node in nodes]
                    else:
                        value = [unicode(node.text_content()) for node in nodes]

                if type(value) is list:
                    if len(value) == 1: # and item.type !== 'list'
                        value = value[0]
                    elif len(value) == 0:
                        value = None


                if 'filter' in item:
                    value = self.apply_filter(item['filter'], value, name, values, item)


                if value and 'fetch' in item:
                    f = item['fetch']
                    if type(f) is dict:
                        o = copy.deepcopy(f)
                        # if 'rule' in f:
                        #     o['rule'] = f['rule']
                        # if 'data' in f:
                        #     if f['data'] == 'this':
                        #         o['data'] = values
                        #     else:
                        #         o['data'] = f['with']

                        if 'data' in o and o['data'] == 'this':
                            o['data'] = values

                        # print '--------------------------%s' % resp.url
                        # print o
                        # print '++++++++++++++++++++++++++%s' % resp.url

                        # d = OptionData()
                        # d.options = o
                        # d.url = value
                        v = (value, o)
                        print '--------------fetch--', value
                        links.append(v)
                    else:
                        links.append(value)


            elif 'value' in item:
                value = item['value']
                if value == '@url':
                    value = resp.url
            else:
                continue

            if name:
                values[name] = value

        # print '===========parse done', len(nexts)              #<-----------------------------
        return values, links


    def apply_parse(self, handler, *args):
        return apply(handler, args)

    def apply_filter(self, fs, *args):
        if type(fs) is not list:
            fs = [fs]
        
        for func in fs:
            value = apply(func, args)

        return value





            


