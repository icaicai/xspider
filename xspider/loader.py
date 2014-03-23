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

    def __init__(self, path):
        if type(path) is list:
            self.path = path
        else:
            self.path = [path]
        self.search_path = self.path #+ sys.path

        self._cache = {}
        self.o__import__ = None


    def __enter__(self):
        self.o__import__ = __builtin__.__import__
        __builtin__.__import__ = self.n__import__

    def __exit__(self):
        __builtin__.__import__ = self.o__import__
        self.o__import__ = None


    def load(self, name, env=None):

        try:
            return self._cache[name]
        except KeyError:
            pass

        if '@' in name:
            func, mod = name.split('@', 1)
        else:
            func, mod = None, name


        if self.o__import__ is None:
            self.o__import__ = __builtin__.__import__
            __builtin__.__import__ = self.n__import__

        self._env = env and env or {}
        omod = self.n__import__(mod)
        ofunc = None
        if func and hasattr(omod, func):
            ofunc = getattr(mod, func)


        if self.o__import__:
            __builtin__.__import__ = self.o__import__
            self.o__import__ = None

        f = ofunc or omod
        self._cache[name] = f

        return f



    def n__import__(self, name, globals=None, locals=None, fromlist=None, level=-1):

        try:
            return sys.modules[name]
        except KeyError:
            pass

        print 'n__import__ ==> ' , name

        parent = self.determine_parent(globals)
        try:
            q, tail = self.find_head_package(parent, name)
            m = self.load_tail(q, tail)

            if not fromlist:
                return q
            if hasattr(m, "__path__"):
                self.ensure_fromlist(m, fromlist)
            return m
        except ImportError:
            if self.o__import__:
                return self.o__import__( name, globals, locals, fromlist, level)

        return None

    def n_reload(self, module):
        name = module.__name__
        if '.' not in name:
            return self.import_module(name, name, None)
        i = name.rfind('.')
        pname = name[:i]
        parent = sys.modules[pname]
        return self.import_module(name[i+1:], name, parent)        


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


    def determine_parent(self, globals):
        if not globals or  not globals.has_key("__name__"):
            return None
        pname = globals['__name__']
        if globals.has_key("__path__"):
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

    def find_head_package(self, parent, name):
        if '.' in name:
            i = name.find('.')
            head = name[:i]
            tail = name[i+1:]
        else:
            head = name
            tail = ""
        if parent:
            qname = "%s.%s" % (parent.__name__, head)
        else:
            qname = head
        q = self.import_module(head, qname, parent)
        if q: return q, tail
        if parent:
            qname = head
            parent = None
            q = self.import_module(head, qname, parent)
            if q: return q, tail
        raise ImportError("No module named " + qname)

    def load_tail(self, q, tail):
        m = q
        while tail:
            i = tail.find('.')
            if i < 0: i = len(tail)
            head, tail = tail[:i], tail[i+1:]
            mname = "%s.%s" % (m.__name__, head)
            m = self.import_module(head, mname, m)
            if not m:
                raise ImportError("No module named " + mname)
        return m

    def ensure_fromlist(self, m, fromlist, recursive=0):
        for sub in fromlist:
            if sub == "*":
                if not recursive:
                    try:
                        all = m.__all__
                    except AttributeError:
                        pass
                    else:
                        ensure_fromlist(m, all, 1)
                continue
            if sub != "*" and not hasattr(m, sub):
                subname = "%s.%s" % (m.__name__, sub)
                submod = self.import_module(sub, subname, m)
                if not submod:
                    raise ImportError("No module named " + subname)


    def import_module(self, partname, fqname, parent):
        try:
            return sys.modules[fqname]
        except KeyError:
            pass

        try:
            search_path = self.path
            if parent and hasattr(parent, '__path__'):
                 search_path += parent.__path__ or []
            fp, pathname, stuff = imp.find_module(partname, search_path)
        except ImportError:
            return None

        try:
            # m = imp.load_module(fqname, fp, pathname, stuff)
            m = None
            code = None
            typ = stuff[2]


            if typ == imp.PKG_DIRECTORY:
                #if fp: fp.close()
                #m = import_plugin_module('__init__', fname, parent)
                fp, pathname, stuff = imp.find_module('__init__', [pathname])
                code = self.read_code(fp, stuff[2])
                filename = '__init__'+stuff[0]
                pathname = os.path.dirname(pathname)
            elif typ == imp.C_BUILTIN or typ == imp.PY_FROZEN:
                m = imp.load_module(fqname, fp, pathname, stuff)
            else:
                code = self.read_code(fp, typ)
                filename = pathname
                pathname = os.path.dirname(pathname)

            # print ' +++++==> ', filename, pathname
            if code is not None:
                m = imp.new_module(fqname) 
                ke = self._env.copy()
                m.__dict__.update(__name__ = fqname, __file__ = filename, __path__ = [pathname])
                m.__dict__.update(ke)
                sys.modules[fqname] = m
                try:
                    exec(code, m.__dict__)
                except:
                    if fqname in sys.modules:
                        del sys.modules[fqname]
                    raise                    
                #print 'code ==>',env
                # m.__dict__.update(ke)
                #sys.modules[fqname] = m

            #-----------------------------------------------
        finally:
            if fp: fp.close()


        if parent:
            setattr(parent, partname, m)
        return m




