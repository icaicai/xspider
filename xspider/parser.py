

from collections import namedtuple
from lxml import html

ParseResult = namedtuple('ParseResult', ['values', 'next'])




class Parser(object):
    """docstring for Parser"""

    # filters = {}

    def __init__(self, spider=None):
        super(Parser, self).__init__()
        # self.container = container
        # f = Parser.filters
        # self.filters = dict(zip(f.keys(), f.values()))
        # if filters:
        #     self.filters.update(filters)
        self.owner = spider




    def parse(self, text, rule):
        # rtn = self.fire('before_parse', text)
        # if rtn === False:
        #     return
        # elif rtn:
        #     text = rtn
        # print 'html == >', len(text), type(text)
        doc = html.document_fromstring(text)
        result = self._parse(doc, rule.parse_rules())
        # self.fire('after_parse', result)
        return result


    def _parse(self, container, items):
        values = {}
        nexts = []
        for item in items:
            name = item['name']
            value = None
            if 'selector' in item:
                #nodes = container.xpath(item['selector'])
                #print 'selector ===', item['selector']
                nodes = container.cssselect(item['selector'])

                if 'handler' in item and callable(item['handler']):
                    value = self.apply_parser(item['handler'], name, nodes, item, values)                
                elif 'parsers' in item and item['parsers']:
                    #value = [self.parse(node, item.parsers) for node in nodes]
                    value = []
                    for node in nodes:
                        v = self._parse(node, item['parsers'])
                        value.append(v[0])
                        #nexts.append(v[1])
                        nexts.extend(v[1])
                else:
                    if 'attrib' in item and item['attrib']:
                        value = [node.get(item['attrib']) for node in nodes]
                    else:
                        value = [node.text_content() for node in nodes]

                if len(value) == 1: # and item.type !== 'list'
                    value = value[0]

                if 'filter' in item:
                    self.apply_filter(item['filter'], value, item)
            elif 'value' in item:
                value = item['value']
            else:
                continue


            if value and 'next' in item:
                p = {}
                opts = {'rule': item['next'], 'item': values}
                if 'with' in item:
                    opts['parent'] = value

                p['url'] = value
                p['opts'] = opts
                nexts.append(p)

            values[name] = value

        # print '===========parse done', len(nexts)              #<-----------------------------
        return values, nexts


    def apply_parser(self, handler, *args):
        if type(handler) is basestring:
            #handler = self._parsers.get(handler)
            handler = self.owner.load(handler)

        if not handler:
            return

        apply(handler, args)

    def apply_filter(self, fs, value, item):
        if type(fs) is not list:
            fs = [fs]
        
        for f in fs:
            #if f in self._filters:
            #    func = self._filters[f]
            func = self.owner.load(f)

            if func:
                apply(func, [value, item])







            


