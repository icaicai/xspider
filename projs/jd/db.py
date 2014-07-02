
import os
import os.path
import gevent

import umysql

from gevent import threadpool, queue

from whoosh.index import create_in,open_dir
from whoosh.fields import *
from whoosh.qparser import QueryParser

from jieba.analyse import ChineseAnalyzer 

analyzer = ChineseAnalyzer()

schema = Schema(id=ID(stored=True), 
				title=TEXT(stored=True), 
				content=TEXT(stored=False, analyzer=analyzer),
				keywords=KEYWORD(stored=True),
				tags=KEYWORD(stored=True),
				created=DATETIME())

ix_path = 'ix'
if os.pah.exists(ix_path):
	ix = open_dir(ix_path)
else:
	os.mkdirs(ix_path)
	ix = create_in(ix_path, schema)

writer = ix.writer()

conn = umysql.Connection()
# conn.connect('localhost', 3306, 'root', '123456', 'fx_smzdm')


def crawl_page(url):
	r = _download(url)

	



def get_goods(data):
	url = data['url']
	r = _download(url)
	data['url'] = r.url
	data['keywords']



def save2db(data, opts):

	print data, opts

	return


	url = data['url']
	sql = "select * from goods where url='%s'" % (url, )
	rs = conn.query(sql)
	if len(rs.rows) == 0:
		sql = "insert into goods (title, url, img) values ('%s', '%s', '%s')"
		conn.query(sql % ())
		sql = "SELECT LAST_INSERT_ID();"
		rs = conn.query(sql)
		last_id = rs.rows[0][0]

	sql = 'insert into entries (title, url, img, price, content, keywords, tags, created) values ()'
	kw = jieba.cut(data['title'])
	data['keywords'] = kw
	data['tags'] = kw

	conn.query()





