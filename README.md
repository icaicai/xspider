xspider
=======

xSpider 是基于gevent的一个网络抓取程序/库。

它是可配置的，可根据配置规则来抓取网页并抽取其中的内容。

还可通过插件来对抽取后的内容进行自定义的处理。



Example
=======

    from xspider.console import Console

    if __name__ == '__main__':
        args = sys.argv
        if len(args) > 1:
            path = args[1]
        else:
            path = './projs/'
        c = Console()
        c.init(path)
        c.run()

