#coding=utf8

"""\
用于加载
"""
import __builtin__
import sys
import os
import os.path



class Loader(object):

    def __init__(self, path, spider):
        if type(path) is list:
            self.path = path
        else:
            self.path = [path]

        self._cache = {}
        self.o__import__ = None
        self._spider = spider



    def load(self, name, env=None):
        try:
            return self._cache[name]
        except KeyError:
            pass

        if '@' in name:
            func, mod = name.split('@', 1)
        else:
            func, mod = None, name

        if mod in self._cache:
            omod = self._cache[mod]
        else:
            omod = self.load_module(mod)
            self._cache[mod] = omod

        ofunc = None
        if func:
            if hasattr(omod, func):
                ofunc = getattr(mod, func)

            self._cache[name] = ofunc
            return ofunc


        return omod


    def load_module(self, mod):

        for p in self.path:
            sys.path.append(p)

        def n__import__(name, globals=None, locals=None, fromlist=None, level=-1):
            if name == 'spider':
                return self._spider
            return o__import__(name, globals, locals, fromlist, level)

        # print type(__builtins__)
        o__import__ = __builtin__.__import__
        __builtin__.__import__ = n__import__


        omod = o__import__(mod)

        __builtin__.__import__ = o__import__
        o__import__ = None

        for p in self.path:
            sys.path.remove(p) 

        return omod