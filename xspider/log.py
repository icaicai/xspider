#! coding=utf8


import os
import os.path
import logging
from logging.handlers import TimedRotatingFileHandler


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

        self._loggers[name] = logger

        return logger


Logger = _Logger()