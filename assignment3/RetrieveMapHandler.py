import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.httpclient
import tornado.gen
import tornado.options
from tornado.escape import json_decode,json_encode
import os, json, urllib, subprocess, uuid
from itertools import chain

from assignment3.MapperHandler import MapperHandler
from assignment3.inventory import *

class RetrieveMapHandler(tornado.web.RequestHandler):
	# transfer map output to reducer with index r, 
	def initialize (self, port):
		self.port = port
	@tornado.gen.coroutine
	def get(self):
		reducer_ix = int( self.get_argument("reducer_ix") ) #all keys that can be hashed to reducer_ix
		map_task_id =str( self.get_argument("map_task_id"))
		
		# list of tuples (word, 1), list of list. chain LOTs together from a list of LOT to form together a LOT
		pairs = chain.from_iterable([ [(key, value) for value in values]
						for key, values in MapperHandler.mapout[map_task_id][reducer_ix] ])
		self.write(json.dumps(list(pairs)))



		# print("retrieve output of maptask-id %s for reducer-%d"%(map_task_id,reducer_ix))
		# print("-----print map output-------")
		# print(MapperHandler.mapout["123"][10:101])
		'''list_kvpairs = MapperHandler.mapout[map_task_id]
		ret = []
		for kvpair in list_kvpairs:
			modulo = abs(hash(kvpair[0])) % MapperHandler.reducers_num
			
			if modulo  == reducer_ix: #reducers_num in inventory
				ret.append(kvpair) 	


		self.write(json.dumps( ret ))'''