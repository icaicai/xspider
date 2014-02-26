

import signal
import gevent
from gevent import monkey
#from gevent.event import Event
monkey.patch_all()

from .scheduler import Scheduler
from .spiderman import SpiderMan


class Console(object):
    """docstring for Console"""
    def __init__(self):
        super(Console, self).__init__()

        #self._start_event = Event()
        self._started = False
        self._sched = Scheduler()
        self._spiderman = SpiderMan()        
        gevent.signal(signal.SIGINT, self.stop)
        
        

    def init(self, path):
        self._path = path
        ss = self._spiderman.load(self._path)
        for s in ss:
            print ' SCHED ', s._sched, self._sched, Scheduler
            if s._sched:
                self._sched.add(s, s._sched)
            else:
              s.start_crawl()

    def stop(self):
        print 'Shutdown...'
        self._sched.stop()
        #self._start_event.set()
        self._started = False


    def run(self):
        
        print 'Running...'
        self._sched.start()
        #self._start_event.clear()
        #self._start_event.wait()
        self._started = True
        while self._started:
            gevent.sleep(1)



