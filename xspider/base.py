#coding=utf8


def merge_dict(d1, d2):
    mld = []
    for key in d1:
        if key in d2:
            if isinstance(d1[key], dict) and isinstance(d2[key], dict):
                merge_dict(d1[key], d2[key])
            else:
                d1[key] = d2[key]
            mld.append(key)

    for key in d2:
        if key in mld:
            continue
        d1[key] = d2[key]

    return d1



class OptionData(object):

    def __init__(self):
        self.data = {}

    def __setattr__(self, key, val):
        self.__dict__[key] = val

    def __getattr__(self, key):
        return self.__dict__.get(key)

    def __setitem__(self, key, val):
        self.data[key] = val

    def __getitem__(self, key):
        return self.data.get(key)


    # def __hash__(self):
    #     pass


class EventMgr(object):
    """docstring for EventMgr"""
    def __init__(self, log=None):
        super(EventMgr, self).__init__()
        self._events = {}
        self._log = log

    def fire(self, event, *args):
        evts = []
        if isinstance(event, basestring):
            event = [event]
        # if len(event)>1:
        #     print '-----fire event', event

        ret = None
        for e in event:
            evts = self._events.get(e)
            if evts:
                self._log.debug(u"触发事件 %s" % e)
                for evt in evts:
                    func, a = evt
                    try:
                        ret = apply(func, a+args)
                        if ret is False:
                            break
                    except:
                        self._log.exception('事件异常 %s' %  e)

        return ret

    def add_listener(self, event, func, *args):
        evts = self._events.get(event)
        if evts is None:
            evts = self._events[event] = []

        evts.append((func, args))
        self._log.info(u"监听事件 %s" % event)

    def remove_listener(self, event, func):
        if not func:
            if event in self._events:
                del self._events[event]
                self._log.info(u"移除事件 %s" % event)
        else:
            evts = self._events.get(event)
            i = evts and len(evts) or 0
            while i > 0:
                i = i - 1
                evt = evts[i]
                if evt[0] == func:
                    del evts[i]
            self._log.info(u"移除事件 %s <%s>" % (event, func))



class EventObject(object):

    def __init__(self):
        self._events = {}

    def fire(self, event, *args):
        evts = []
        if isinstance(event, basestring):
            event = [event]
        # if len(event)>1:
        #     print '-----fire event', event
        for e in event:
            es = self._events.get(e)
            if es:
                evts.extend(es)

        ret = None
        if evts:
            for evt in evts:
                func, a = evt
                ret = apply(func, a+args)
                if ret is False:
                    break

        return ret

    def add_listener(self, event, func, *args):
        evts = self._events.get(event)
        if evts is None:
            evts = self._events[event] = []

        evts.append((func, args))

    def remove_listener(self, event, func=None):

        if not func:
            if event in self._events:
                del self._events[event]
        else:
            evts = self._events.get(event)
            i = evts and len(evts) or 0
            while i > 0:
                i = i - 1
                evt = evts[i]
                if evt[0] == func:
                    del evts[i]



class Singleton(object):   
    objs  = {}
    def __new__(cls, *args, **kv):
        if cls not in cls.objs:
            cls.objs[cls] = object.__new__(cls)
        return cls.objs[cls]





# def current_project():
#   pass


# def register_filter(name=None):
#   def _filter(func):
#       proj = current_project()
#       if name is None:
#           name = func.__name__
#       proj.add_filter(name, func)

#   return _filter


# def register_parser(name=None):
#   def _parse(func):
#       proj = CURRENT_PROJECT
#       if name is None:
#           name = func.__name__
#       proj.add_parse(name, func)

#   return _parse

