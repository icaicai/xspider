


class EventObject(object):

    def __init__(self):
        self._events = {}

    def fire(self, event, *args):
        evts = []
        if type(event) is basestring:
            event = [event]
        for e in event:
            es = self._events.get(e)
            if es:
                evts.extend(es)

        ret = True
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

