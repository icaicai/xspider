

#print locals()
#print key


def func():
	print '--------------- func'


def save(info):
	print '=========='*3
	print info
	print '=========='*3




def evt_hook(*args):
	print '--------------- hook ~~~~~~~~~~~~~~~~~~~~~~~~~~~'
	print args
	print '--------------- hook ~~~~~~~~~~~~~~~~~~~~~~~~~~~'


class DB(object):

	def __init__(self):
		print '---------------DB __init__'
		print get_var('key')


	def save(self, data, nxts, opts):
		print '---------------DB save==========================================' 
		print data
		print '---------------DB save==========================================' 
