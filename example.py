

import sys
import gevent
#sys.path.append('./src')


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