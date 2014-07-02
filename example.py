

import sys
import gevent

import os.path

d = os.path.dirname(__file__)
sys.path.append(d)

reload(sys) 
sys.setdefaultencoding('gbk')



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

    print '========== Game Over =========='