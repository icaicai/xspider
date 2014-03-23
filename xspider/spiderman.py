#coding=utf8

import os
import yaml
import json
from .spider import Spider
from .log import Logger

class SpiderMan(object):
    """docstring for Manager"""


    cfgfnre = '*.ymal'

    def __init__(self):
        super(SpiderMan, self).__init__()
        self._spiders = []
        
        self._log = Logger.get('SpiderMan')
    # def load(self, path = '.'):
        
    #     for fn in os.listdir(path):
    #         if fn in ('.', '..'):
    #             continue
    #         fullpath = os.path.join(path, fn)
    #         r, ext = os.path.splitext(fn)
    #         if os.path.isfile(fullpath) and ext in ('.yaml', '.yml', '.json'):
    #             txt = open(fullpath).read()
    #             if ext == '.json':
    #                 cfg = json.load(txt)
    #             else:
    #                 cfg = yaml.load(txt)
                
    #             # print '----------' * 3
    #             # print cfg
    #             # print '----------' * 3
    #             cfg['__basepath__'] = path
    #             s = self.init_spider(cfg)
    #             if s:
    #                 self._spiders.append(s)

    #     return self._spiders

    def load(self, path = '.'):
        
        for fn in os.listdir(path):
            if fn in ('.', '..'):
                continue
            fullpath = os.path.abspath(os.path.join(path, fn))
            r, ext = os.path.splitext(fn)
            if os.path.isfile(fullpath) and ext in ('.yaml', '.yml', '.json'):
                #cfg['__basepath__'] = path
                s = self.init_spider(fullpath)
                if s:
                    self._spiders.append(s)

        return self._spiders

    def init_spider(self, cfg):
        try:
            proj = Spider()
            proj.init(cfg)
            # sched = proj.get_sched()
            # if sched:
            #   self._sched.add_sched(proj, sched)
            self._log.info((u'加载 %s ( %s ) 完成' % (proj.name, cfg)))
            return proj
        except Exception, e:
            self._log.exception((u'解析 %s 出错' % cfg))
        else:
            pass
        finally:
            pass

    def get_spiders(self):
        return self._spiders


    def stop(self):
        for spider in self._spiders:
            spider.stop()
