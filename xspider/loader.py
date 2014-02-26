#coding=utf8

"""\
用于加载
"""
import __builtin__
import sys
import os
import os.path
import imp
import marshal


class ReplaceImport(object):

    def __enter__(self):
        pass

    def __exit__(self):
        pass



class Loader(object):

    def __init__(self, plugin_path):
        #self.env = env
        if type(plugin_path) is list:
            self.plugin_path = plugin_path
        else:
            self.plugin_path = [plugin_path]
        self.search_path = self.plugin_path + sys.path

        self.__cache = {}


    def load(self, name, env=None):
        if '@' in name:
            func, mod = name.split('@', 1)
        else:
            func, mod = None, name

        self.o__import__ = __builtin__.__import__
        __builtin__.__import__ = self.n__import__


        omod = self.n__import__(mod, env)
        ofunc = None
        if func and hasattr(omod, func):
            ofunc = getattr(mod, func)


        __builtin__.__import__ = self.o__import__
        self.o__import__ = None

        return ofunc or omod

    # def load_plugin(self):
    #     try:
    #         self.o__import__ = __builtin__.__import__
    #         __builtin__.__import__ = self.n__import__

    #         mods = set()
    #         for fn in os.listdir(self._path):
    #             if fn in ('.', '..'): continue
    #             if os.path.isdir(os.path.join(self.plugin_path, fn)):
    #                 continue
    #             name, ext = os.path.splitext(fn)
    #             mods.add(name)

    #         for mod in mods:
    #             self.n__import__(mod)


    #     finally:
    #         __builtin__.__import__ = self.o__import__



    def determine_parent(self, globals):
        if not globals or  not globals.has_key("__name__"):
            return None
        pname = globals['__name__']
        if globals.has_key("__path__") and pname in sys.modules:
            parent = sys.modules[pname]
            assert globals is parent.__dict__
            return parent
        if '.' in pname:
            i = pname.rfind('.')
            pname = pname[:i]
            parent = sys.modules[pname]
            assert parent.__name__ == pname
            return parent
        return None

    def read_code(self, fp, typ):
        if typ == imp.PY_SOURCE:
            code = fp.read()
        elif typ == imp.PY_COMPILED:
            magic = fp.read(4)
            if magic != imp.get_magic():
                return None
            fp.read(4) # Skip timestamp
            code = marshal.load(fp)
        else:
            code = None

        return code

    # def is_plugin_module(self.path):
    #     for p in self.plugin_path:
    #         if path.startswith(p):
    #             return True
    #     return False

    def import_plugin_module(self, name, fname, parent, env):
        print '**********import  ==>', name, fname, parent and parent.__path__
        fp = None
        try:
            search_path = self.plugin_path + (parent and parent.__path__ or [])
            fp, pathname, stuff = imp.find_module(name, search_path)

            mod = None
            code = None
            typ = stuff[2]

            if typ == imp.PKG_DIRECTORY:
                #if fp: fp.close()
                #m = import_plugin_module('__init__', fname, parent)
                fp, pathname, stuff = imp.find_module('__init__', [pathname])
                code = self.read_code(fp, stuff[2])
                filename = '__init__'+stuff[0]
                pathname = os.path.dirname(pathname)
            else:
                code = self.read_code(fp, typ)
                filename = pathname
                pathname = os.path.dirname(pathname)

            print ' +++++==> ', filename, pathname
            if code is not None:
                mod = imp.new_module(fname) 
                ke = env.copy()
                ke.update(__name__ = fname, __file__ = filename, __path__ = [pathname])
                exec(code, ke)
                #print 'code ==>',env
                mod.__dict__.update(ke)

            if parent and mod:
                setattr(parent, name, mod)
            return mod

        # except ImportError:
        #     print '----------------eeeeeeeeee--------------'
        #     return None

        finally:
            if fp: fp.close()




    def n__import__(self, name, globals=None, locals=None, fromlist=None):

        try:
            return sys.modules[name]
        except KeyError:
            pass

        print 'n__import__', name
        parent = self.determine_parent(globals)

        if '.' in name:
            pname, subname = name.split('.', 1)
        else:
            pname, subname = name, None

        try:
            fp = None
            search_path = self.plugin_path + (parent and parent.__path__ or [])
            fp, pathname, stuff = imp.find_module(pname, search_path)

        except ImportError:
            return self.o__import__(name, globals, locals, fromlist)
        finally:
            if fp: fp.close()


        mod = self.import_plugin_module(pname, pname, parent, globals)

        print pname, '===>', mod
        if name != pname:
            sys.modules[pname] = mod
            prnt = mod
            while subname:
                i = subname.find('.')
                if i < 0: i = len(subname)
                pname, subname = subname[:i], subname[i+1:]

                fname = '%s.%s' % (prnt.__name__, pname)
                prnt = self.import_plugin_module(pname, fname, prnt)


        return mod