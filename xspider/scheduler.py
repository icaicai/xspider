




import math
import gevent
from gevent.event import Event
from gevent.pool import Group
from datetime import datetime, timedelta
from .base import Singleton



MIN_VALUES = {'year': 1970, 'month': 1, 'day': 1, 'week': 1,
              'day_of_week': 0, 'hour': 0, 'minute': 0, 'second': 0}
MAX_VALUES = {'year': 2 ** 63, 'month': 12, 'day': 31, 'week': 53,
              'day_of_week': 6, 'hour': 23, 'minute': 59, 'second': 59}
FIELDS = ['year', 'month', 'day', 'hour', 'minute', 'second']
FIELDS2 = ['days', 'seconds', 'minutes', 'hours', 'weeks']

def convert2datetime(dateval, format=None):
    if isinstance(dateval, float):
        dateval = int(dateval)
    if isinstance(dateval, int):
        return datetime.fromtimestamp(dateval)
    elif isinstance(dateval, basestring):
        if format is None:
            format = '%Y-%m-%d %H:%M:%S'
        return datetime.strptime(dateval, format)        
    elif isinstance(dateval, datetime):
        return dateval
    elif isinstance(dateval, date):
        return datetime.fromordinal(dateval.toordinal())

    return None

class Job(object):
    """docstring for Job"""
    def __init__(self, sched, func):
        super(Job, self).__init__()

        self._id = 'id' in sched and sched['id'] or 'id_'+str(id(self))

        self._init(sched)

        self._running = False
        self._count = 0

        self._func = func



    @property
    def is_running(self):
        if hasattr(self._func, 'is_running') and type(self._func.is_running) is bool:
            return self._func.is_running
        return self._running

    @property
    def count(self):
        return self._count

    def _check_condition(self):
        pass

    def _init(self, sched):
        self._sched_start_time = None
        self._sched_end_time = None
        self._sched_repeat = sched.get('repeat', None)
        self._sched_config = {}

        self._sched_type = _sched_type = sched.get('type', None)
        if 'start_time' in sched:
            self._sched_start_time = convert2datetime(sched['start_time'])
        if _sched_type == 'cron':
            for field in FIELDS:
                self._sched_config[field] = sched.get(field, '*')
        elif _sched_type == 'interval':
            _deltap = {}
            for p in ['days', 'seconds', 'minutes', 'hours', 'weeks']:
                if p in sched:
                    _deltap[p] = sched[p]
            self._sched_config['interval'] = timedelta(**_deltap)
            self._sched_config['length'] = self._sched_config['interval'].total_seconds()
            if self._sched_start_time is None:
                self._sched_start_time = datetime.now() + self._sched_config['interval']
        else:
            self._sched_type = None

        print self._sched_type, self._sched_config
            

    def _cron_next_tirgger(self, now):
        _next = now
        # if self._count == 0:
        #     return now
        #fields = FIELDS #['year', 'month', 'day', 'hour', 'minute', 'second']
        for i, field in enumerate(FIELDS):
            val = self._sched_config.get(field)
            if val is None or val == '*':
                continue
            cur = getattr(_next, field)
            kw = {}
            if cur > val and i > 0 and now > _next:
                j = i
                while j > 0:
                    _prev = FIELDS[j-1]
                    if self._sched_config.get(_prev) == '*':
                        kw[_prev] = getattr(_next, _prev) + 1
                        if kw[_prev] > MAX_VALUES[_prev]:
                            kw[_prev] = MAX_VALUES[_prev]
                        else:
                            break
                    j -= 1

            if cur != val:
                kw[field] = val
            if kw:
                _next = _next.replace(**kw)
        print 'CRON Next tirgger ==> ', _next
        return _next

    def _interval_next_tirgger(self, now):
        delta = now - self._sched_start_time
        delta_seconds = delta.days * 24 * 60 * 60 + delta.seconds + delta.microseconds / 1000000.0
        num = math.ceil(delta_seconds/self._sched_config['length'])
        sec = self._sched_config['length'] * num
        _next = self._sched_start_time + timedelta(seconds=sec)
        return _next

    def get_next_tirgger(self, now):
        #now = datetime.now()
        if self._sched_start_time and now < self._sched_start_time:
            return self._sched_start_time

        if self._sched_repeat and self._sched_repeat < self._count:
            return None

        if self._sched_type == 'cron':
            return self._cron_next_tirgger(now)
        elif self._sched_type == 'interval':
            return self._interval_next_tirgger(now)
        else:
            if self._count == 0:
                return now
            else:
                return None


    def stop(self):
        pass

    def run(self):
        print ' --+++--> ', self._id, ' Im Run!!!!', datetime.now()
        def _on_finished():
            print ' --+++--> ', self._id, ' Im Finished!!!!'
            self._running = False
            self._count += 1

        self._running = True
        try:
            self._func()
        except Exception,e:
            self._running = False
            raise e




class Scheduler(Singleton):

    def __init__(self):
        self._thread = None
        self._event = Event()
        self._jobs = []
        self._jobs_thread = Group()
        self._start = False

    def add(self, func, sched):
        job = Job(sched, func)
        self.add_job(job)


    def start(self):
        self._started = True
        self._thread = gevent.spawn(self._run)
        #self._run()

    def stop(self):
        self._started = False
        self._event.set()
        # print 'event set'
        self._thread.join()
        # print 'thread join'
        self._jobs_thread.join()   
        # print 'jobs thread join' 

    def add_job(self, job):
        self._jobs.append(job)
        self._event.set()

    def _run_job(self, job):
        self._jobs_thread.spawn(job.run)
        #gevent.spawn(job.run)

    def _run(self):

        while self._started:
            finished = []
            _next = None
            now = datetime.now()
            for job in self._jobs:
                wakeup = job.get_next_tirgger(now)
                print ' --> ', now, wakeup
                if wakeup is None:
                    finished.append(job)
                elif wakeup <= now:
                    self._run_job(job)
                else:
                    pass

            for job in finished:
                self._jobs.remove(job)

            now = datetime.now()
            for job in self._jobs:
                wakeup = job.get_next_tirgger(now)
                if _next is None or wakeup < _next:
                    _next = wakeup
            print 'Next Run -> ', _next, ' <-'
            self._event.clear()
            if _next:
                t = (_next-now).total_seconds()
                #self._thread = gevent.spawn_later(t, self._run)
                print '============>', t
                self._event.wait(t)
            else:
                print '_________WAIT__________'
                self._event.wait()
                print '_________WAIT 2__________'
                #self._thread = gevent.spawn_later(0, self._run)
            
            print '___________________'
        









