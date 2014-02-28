
from gevent.wsgi import WSGIServer
from flask import Flask, request, session, g, redirect, url_for, abort, render_template, flash
from .console import Console


class WebConsole(Console):

    def __init__(self):
        super(WebConsole, self).__init__()

        self._app = Flask('WebConsole')
        self._server = WSGIServer(('', 5000), self._app)
        self._app.config.update(
            DEBUG=True
        )

        self._app.add_url_rule('/', None, self.index)





    def index(self):
        sps = self._spiderman.get_spiders()
        spinfo = []
        for s in sps:
            si = s.get_info()
            info = {}
            info['name'] = s.name
            info['dcount'] = si[1]
            info['qcount'] = si[0]
            info['ccount'] = si[2]
            spinfo.append(info)



        return render_template('show_spider.html', spinfo=spinfo)    





    def stop(self):
        super(WebConsole, self).stop()
        self._server.stop()



    def run(self):
        print 'Running...'

        if self._started == False:
            self.start()        

        self._server.serve_forever()