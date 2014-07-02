#! coding=utf8

import sys
import os
import os.path
import logging
from logging.handlers import TimedRotatingFileHandler



class StdioOnnaStick:
    """
    Class that pretends to be stout/err.
    """

    closed = 0
    softspace = 0
    mode = 'wb'
    name = '<stdio (log)>'

    def __init__(self, isError=0):
        self.isError = isError
        self.level = isError and logging.ERROR or logging.INFO
        self.buf = ''
        #self.logger = get_service('Logging').get_logger('CONSOLE')
        #print '#self.logger = logging.getLogger()'

    def close(self):
        pass

    def fileno(self):
        return -1

    def flush(self):
        pass

    def read(self):
        raise IOError("can't read from the log!")

    readline = read
    readlines = read
    seek = read
    tell = read

    def write(self, data):
        #print >> sys.ostdout, data
        d = (self.buf + data).split('\n')
        self.buf = d[-1]
        messages = d[0:-1]
        for message in messages:
            logging.log(self.level, message, exc_info=self.isError)

    def writelines(self, lines):
        for line in lines:
            logging.log(self.level, line, exc_info=self.isError)




class _Logger(object):
    def __init__(self, path='logs'):
        self._loggers = {}

        self.dir = path
        self.datefmt = '%Y-%m-%d %H:%M:%S'
        self.format = '%(asctime)s %(name)s|%(levelname)s|%(message)s'


    def get(self, name, **kwargs):

        try:
            return self._loggers[name]
        except KeyError:
            pass


        if '.' in name:
            fn = name.split('.')[0]
            self.get_logger(fn)
        else:
            fn = name

        logger = logging.getLogger(name)

        if fn == name:
            f = kwargs.get('format', self.format)
            d = kwargs.get('datefmt', self.datefmt)
            fmt = logging.Formatter(f, d)
            h1 = logging.StreamHandler()
            h1.setFormatter(fmt)

            logfile = kwargs.get('logfile')
            logpath = kwargs.get('logpath')
            if not logfile:
                logfile = os.path.join(logpath or self.dir, ('%s.log'% name))

            lp = os.path.dirname(logfile)
            if not os.path.exists(lp):
                os.makedirs(lp)

            h2 = TimedRotatingFileHandler(logfile, 'midnight')
            h2.setFormatter(fmt)

            logger.addHandler(h1)
            logger.addHandler(h2)

            logger.setLevel(20)


            # sys.ostdout = sys.stdout
            # sys.ostderr = sys.stderr

            # sys.stdout = logfile
            # sys.stderr = logerr

        self._loggers[name] = logger

        return logger




try:
    logfile
except NameError:
    logfile = StdioOnnaStick(0)
    logerr = StdioOnnaStick(1)


# sys.ostdout = sys.stdout
# sys.ostderr = sys.stderr

# sys.stdout = logfile
# sys.stderr = logerr

Logger = _Logger()