#coding=utf8

import re
import urlparse
from collections import namedtuple
from lxml import html
from log import Logger


ParseResult = namedtuple('ParseResult', ['values', 'next'])



class ParseRule(object):

    def __init__(self, rules, spider):
        self.name = name = rules.get('name')
        self.container = rules.get('container')
        self.allow_domain = rules.get('allow_domain')

        self._log = Logger.get(spider.name)

        rps = rules.get('rules', {})
        #print rps
        self._rules = self._validate(rps, spider)
        # self._is_default = rules.get('default', False)
        
        # print rules.get('events')
        events = rules.get('events')
        if events:
            for en in events:
                evt = events[en]
                func = spider.load(evt)
                print evt, func
                if func and callable(func):
                    ename = '%s_%s' % (name, en)
                    spider.add_listener(ename, func)
                    self._log.info(u"监听规则 %s 事件 %s" % (name, evt))

        self._match_rules = None

        if 'match' in rules:
            self._match(rules['match'], spider)
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
            def func(url):
                p = urlparse.urlparse(url)
                v = getattr(p, part)
                if act == 'eq':
                    return v == val
                return val.find(v) != -1
            return func
                    

        self._match_rules = []

        avpart = ('host', 'path', 'query', 'fragment')
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
        self._log = Logger.get(spider.name)

        self._rules = []
        self.allow_domain = cfgs.get('allow_domain')

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


        # for rn in self._rules:
        #     rule = self._rules[rn]

    def get_rule(self, name):
        for rule in self._rules:
            if rule.name == name:
                return rule

        return None        


    def guess_rule(self, url):
        for rule in self._rules:
            if rule.is_match(url):
                return rule

        return None


    def parse(self, text, url, rule):
        # rtn = self.fire('before_parse', text)
        # if rtn === False:
        #     return
        # elif rtn:
        #     text = rtn
        # print 'html == >', len(text), type(text)
        # text = resp.text
        doc = html.document_fromstring(text)
        doc.make_links_absolute(doc.base_url or url)


        links = self._get_links(doc, rule and rule.allow_domain or None)
        if rule:
            result = self._parse(doc, rule.rules())
        else:
            result = None

        return result, links

    def _get_links(self, doc, allow_domain=None):
        urls = set()

        if not allow_domain:
            allow_domain = self.allow_domain
        
        for el, attr, link, pos in doc.iterlinks():
            if el.tag == "a":
                # if '#' in link:
                #     link = link[:link.index('#')]
                if allow_domain:
                    pr = urlparse.urlparse(link)
                    if pr.netloc in allow_domain:
                        urls.add(link)
                else:
                    urls.add(link)
        return urls


    def _parse(self, container, items):
        values = {}

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
                        v = self._parse(node, item['rules'])
                        value.append(v[0])
                else:
                    if 'attrib' in item and item['attrib']:
                        value = [node.get(item['attrib']) for node in nodes]
                    else:
                        value = [node.text_content() for node in nodes]

                if type(value) is list:
                    if len(value) == 1: # and item.type !== 'list'
                        value = value[0]
                    elif len(value) == 0:
                        value = None

                if 'filter' in item:
                    value = self.apply_filter(item['filter'], value, name, values, item)

            elif 'value' in item:
                value = item['value']
            else:
                continue

            if name:
                values[name] = value

        # print '===========parse done', len(nexts)              #<-----------------------------
        return values


    def apply_parse(self, handler, *args):
        return apply(handler, args)

    def apply_filter(self, fs, *args):
        if type(fs) is not list:
            fs = [fs]
        
        for func in fs:
            value = apply(func, args)

        return value





            


