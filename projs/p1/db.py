
import os
import os.path
import re
import urlparse, urllib
# import gevent
import traceback
#import umysql
import MySQLdb
# from xspider.base import OptionData

# from gevent import threadpool, queue
# import spiderman
import spider

import PyV8
import bs4
import browser

from whoosh.index import create_in,open_dir
from whoosh.fields import *
from whoosh.qparser import QueryParser

import jieba
from jieba.analyse import ChineseAnalyzer 

analyzer = ChineseAnalyzer()

schema = Schema(id=ID(stored=True), 
                title=TEXT(stored=True), 
                content=TEXT(stored=False, analyzer=analyzer),
                keywords=KEYWORD(stored=True),
                tags=KEYWORD(stored=True),
                created=DATETIME())

ix_path = 'ix_fx'
if os.path.exists(ix_path):
    ix = open_dir(ix_path)
else:
    os.makedirs(ix_path)
    ix = create_in(ix_path, schema)

writer = ix.writer()

# conn = umysql.Connection()
# conn.connect('localhost', 3306, 'root', 'password', 'fx_smzdm')
# conn = MySQLdb.connect(host='192.168.9.9', user='root',passwd='password',db='fx_smzdm', charset='utf8')
from gevent import monkey
thread_get_ident = monkey.get_original('thread', 'get_ident')

__conns = {}
def get_conn():
    tid = thread_get_ident()
    try:
        __conns[tid]
    except KeyError:
        pass
    conn = MySQLdb.connect(host='127.0.0.1', user='root',passwd='123456',db='fx_smzdm', charset='utf8')
    cursor = conn.cursor()
    cursor.execute('set autocommit=1;')

    __conns.update({tid: cursor})
    return cursor

class Global(PyV8.JSClass):
    @property
    def window(self):
        return self

# spider = spiderman.get_spider('fx_smzdm')

print spider, ' <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<'



def parse_go_url_(doc, opts):
    
    resp = opts.response
    html = resp.text
    # print type(html), html
    # html = html.encode('gbk')
    dom = bs4.BeautifulSoup(html)
    # print dom.builder
    win = browser.HtmlWindow(resp.url, dom)
    # print dom
    ss = dom.find_all('script')
    # print type(win.doc), type(win.doc.doc), '---+++++'


    for tag in ss:
        s = str(tag)
        # s = s.encode('gbk')
        # print type(s), s
        # s = 'code='+s[4:]
        s = s[8:-9]
        # print type(s), s
        # win.evalScript(s)    
        with win.context as ctxt:
            ctxt.eval(s)

    print '****$$$$$$$******', win.location.href
    url = win.location.href
    r = {'go_url': url}
    l = set()
    l.add(url)
    return r, l


pyv8_jslocker = PyV8.JSLocker()

def parse_go_url(nodes, name, values, item):
    print '+++go url', nodes
    if not nodes or len(nodes) < 1:
        return

    node = nodes[0]

    jscode = node.text_content()
    # print 'js content' ,jscode
    # print type(jscode)
    jscode = u'code='+jscode[4:]
    # print type(jscode),jscode

    url = None
    obj = Global()
    with pyv8_jslocker:
        with PyV8.JSContext() as ctx:
            ctx.eval(jscode)
            code = ctx.locals.code
            # print type(code), code
            #ga('send','pageview');
            #ga('send','event',
            i1 = code.index("ga('send','pageview');")
            i2 = code.index("ga('send','event',")
            uc = code[i1:i2]
            eq = uc.index('=')
            url = uc[eq+2:-2]
            # cs = code.split(';')
            # ctx.eval(code)
            print '----go ----------', url
        # url = ctx.locals.ewkqmp
    return url


def default_parse(resp, opts):
    doc = opts.doc
    data = {}
    title = doc.xpath('//title')[0].text_content()
    data['title'] = unicode(title)

    return data

def crawl_page(url):
    r = _download(url)


def jd_parse_item_info(nodes, name, values, item):
    # print '================================================================================'
    if len(nodes) < 1:
        return

    node = nodes[0]

    jscode = node.text_content()
    # print 'js content' ,jscode
    info = None
    obj = Global()
    with pyv8_jslocker:
        with PyV8.JSContext(obj) as ctx:
            ctx.eval(jscode)
            info =  PyV8.convert(ctx.locals.pageConfig)

    prod = info['product']
    values.update(prod)

    if not values.get('title'):
        pass


def jd_save2db(data, opt):
    skus = data['skuid']
    url = 'http://p.3.cn/prices/mgets?skuIds=J_%s&type=1' % skus #(',J_'.join(skus))
    o = {'referer': opt.url}
    # print opt
    resp = spider.fetch(url, o)
    jscode = resp.text  #[{'p': '769.00', 'm': '859.00', 'id': 'J_954086'}]
    obj = Global()
    info = None
    with PyV8.JSContext(obj) as ctx:
        c = ctx.eval(jscode)
        info =  PyV8.convert(c)
        # print  info
    print '++++++++++++++++++++++++++++++++++++++++++++++++++++++++'
    data['price'] = info[0]['p']
    data['old_price'] = info[0]['m']
    data['title'] = data['name'].decode('utf8')
    save2db(data, opt)


def sn_parse_item_url(resp, opts):
    text = resp.text
    rs = 'window\.location\s*=\s*"([^"]+)"'
    r = re.compile(rs, re.M)
    m = r.search(text)
    if m:
        url = m.groups()[0]
    else:
        url = None
    
    if url:
        r = {'url': url}
        o = {'default_rule': 'default_parse'}
        l = []
        l.append((url, o))

        return r, l
    else:
        return None, None


def dd_parse_item_url(resp, opts):
    text = resp.text
    rs = 'window\.location\.href\s*=\s*"([^"]+)"'
    r = re.compile(rs, re.M)
    m = r.search(text)
    if m:
        url = m.groups()[0]
    else:
        url = None
    
    if url:
        r = {'url': url}
        o = {'default_rule': 'default_parse'}
        l = []
        l.append((url, o))
        return r, l
    else:
        return None, None





def tb_parse_item_url(resp, opts):
    url = resp.url
    uo = urlparse.urlparse(url)
    qd = urlparse.parse_qs(uo.query)
    tu = qd.get('tu')
    print '==========================$$$$$$$$$$$$================================='
    print tu
    if tu:
        tu = urllib.unquote(tu[0])
        print tu
        if tu.find("http%3A%2F%2Fs.click.taobao.com%2F") == 0 \
           or tu.find("http%3A%2F%2Fi.click.taobao.com%2F") == 0 \
           or tu.find("http%3A%2F%2Fs.click.alimama.com%2F") == 0 \
           or tu.find("http%3A%2F%2Fitem8.taobao.com%2F") == 0 \
           or tu.find("http%3A%2F%2Fshop8.taobao.com%2F") == 0:

            r = {'url': tu}
            o = {'default_rule': 'default_parse'}
            l = []
            l.append((tu, o))
            return r, l

    return None, None


def yqf_clean_url(value, name, values, item):
    i = value.index('=')
    url = value[i+1:]
    return url

def amazon_parse_item(nodes, name, values, item):
    if len(nodes) < 1:
        return
    body = nodes[0]
    title = ''
    t = body.cssselect('h1.parseasinTitle')
    if t:
        title = t[0].text_content()
    else:
        t = body.cssselect('h1#title')
        if t:
            title = t[0].text_content()

    if title:
        title = unicode(title)

    result = {}
    result['title'] = title

    return title


def yixun_parse_price(nodes, name, values, item):
    if len(nodes) < 1:
        return
    body = nodes[0]
    ps = body.cssselect('span.xprice_val')
    oo = {}
    for p in ps:
        ip = p.get('itemprop')
        if ip == 'lowPrice' or ip == 'price':
            oo['price'] = unicode(p.text_content())
        elif ip == 'highPrice':
            oo['old_price'] = unicode(p.text_content())

    values.update(oo)





def default_get_item(resp, opts):
    doc = opts.doc
    data = {}
    ts = doc.xpath('//head/title')
    if ts:
        title = ts[0].text_content()
    else:
        title = '============'
    data['title'] = unicode(title)

    return data









# =====================================================
def default_clean(u):
    subdomain, domain, tld = u.split_host()
    # if domain == 'jd':
    dp = ['utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content', 'tracker_u', 'ytag', 'cm_mmc']
    u.deparam(dp)
    # u.deparam('utm_source')   #jd suning womai yougou
    # u.deparam('utm_medium')   #jd suning womai
    # u.deparam('utm_campaign') #jd suning womai
    # u.deparam('utm_term')     #jd

    # u.deparam('tracker_u')    #yhd
    # u.deparam('utm_content')  #suning womai
    # u.deparam('YTAG')         #yixun
    # u.deparam('cm_mmc')       #newegg


    return u
# =====================================================

start_links = None
def on_new_links(links, opts, spider):
    global start_links
    referer = opts.get('referer', '')
    if referer and re.search('fx\.smzdm\.com/(\d+)?', referer):
        if start_links is None and links:
            start_links = links
            return

        for link in links:
            if link in start_links:
                start_links.remove(link)

        if len(start_links) == 0:
            spider.stop()
            



def get_goods(data):
    url = data['url']
    r = _download(url)
    data['url'] = r.url
    data['keywords']



def save2db(data, o):

    
    print '----------------save to db', o.url
    print '--'*20
    print '--'*20


    # url = o['url']
    # opt = o['options']
    url = o.url
    opt = o.options
    if opt:
        d = opt.get('data', {})
    else:
        d = {}


    if not opt or not d or not data:
        print data, o
        print o.__dict__
        # import sys
        # sys.exit()
        print '++'*20
        print '++'*20

        return


    last_id = 0

    cursor = get_conn()

    if not data.get('price'):
        data['price'] = 0
    if not data.get('title'):
        data['title'] = '___TITLE___'

    data['title'] = data['title'].strip()
    
    try:
        sql = "select * from goods where url='%s'" % (url, )
        # rs = conn.query(sql)
        n = cursor.execute(sql)
        #if len(rs.rows) == 0:
        if n == 0:
            sql = "insert into goods (title, url, img) values (%s, %s, %s)"
            #conn.query(sql % (data['title'], url, ''))        
            cursor.execute(sql, (data['title'], url, ''))        
            sql = "SELECT LAST_INSERT_ID();"
            #rs = conn.query(sql)
            n = cursor.execute(sql)
            rs = cursor.fetchone()
            last_id = rs[0]
        else:
            rs = cursor.fetchone()
            last_id = rs[0]

    except:
        # print '##---##'*5
        # traceback.print_exc()
        # print sql, data['title'], url, ''
        # print '##---##'*5 
        raise


    sql = "select * from entries where url='%s'" % (d['fx_url'], )
    #rs = conn.query(sql)
    print '****',sql
    n = cursor.execute(sql)
    #if len(rs.rows) == 0:    
    if n == 0:    
        sql = "insert into entries (goods_id, title, url, img, price, content, keywords, tags, published, created)" \
              " values (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())"
        kw = jieba.cut(data['title'])
        kw = list(kw)
        kw = filter(lambda k: k, kw)
        kw = ', '.join(kw)
        data['keywords'] = kw
        data['tags'] = kw
        try:
            #conn.query(sql % (last_id, d['title'], d['url'], '', data['price'], d['content'], kw, kw))
            cursor.execute(sql, (last_id, d['title'].strip(), d['fx_url'], '', data['price'], d['content'], kw, kw, d['created']))
        except:
            spider.log.error(d)
            spider.log.error(data)
            # print '-#---#-'*5
            # traceback.print_exc()
            # print sql , last_id, d['title'], d['url'], '', data['price'], d['content'], kw, kw
            # print '-#---#-'*5
            raise





