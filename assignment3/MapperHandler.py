#!/usr/bin/env python3

import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.httpclient
import tornado.gen
import tornado.options
from tornado.escape import json_decode,json_encode
import os, json, urllib, subprocess, uuid, hashlib, time
from collections import defaultdict
from itertools import chain

from assignment3.inventory import *
PARTITIONER = lambda key, num_reducers: int(hashlib.md5(key.encode()).hexdigest()[:8], 16) % num_reducers
# PARTITIONER = lambda key, num_reducers: hash( str(uuid.uuid4()).replace('-','')) % num_reducers
class MapperHandler(tornado.web.RequestHandler):
   
	mapout = defaultdict(lambda: defaultdict(list)) 
	# reducers_num = 0
	map_task_id_list = []
	running_mapper = 0
	def initialize (self, port):
		self.port = port
		self.reducers_num = 1
	@tornado.gen.coroutine
	def get(self):
		while True: 
			if (MapperHandler.running_mapper<MAPPER_MAXNUM):
				MapperHandler.running_mapper+=1 
				mapper_path= self.get_argument("mapper_path") #all keys that can be hashed to reducer_ix
				input_file = self.get_argument("input_file")
				self.reducers_num =int( self.get_argument("num_reducers") )# a list of map_tasks
				# inventory.REDUCERS_NUM = MapperHandler.reducers_num #metadata in inventory
				# run mapper program againt the input file
				out_by_partition = defaultdict(lambda: defaultdict(list))
				map_task_id = hashlib.md5((mapper_path + input_file + str(time.time())).encode()).hexdigest()

				for k_out, v_out in self._mapper(mapper_path, input_file):
					# print("****", doc_id, term_freq) k_out is doc)id, v_out is term_tf
					out_by_partition[PARTITIONER(k_out, self.reducers_num)][k_out].append(v_out)
				for red_idx in out_by_partition.keys():    # for every reducer p is 
					MapperHandler.mapout[map_task_id][red_idx] = [(k_out, out_by_partition[red_idx][k_out]) for k_out in sorted(out_by_partition[red_idx].keys()) ]
		
				'''for k_out, v_out in self._mapper(mapper_path, input_file):   #v_out is always 1 from mapper output
				# for each map_task, divide the output according to reducer_idx,(p)
					#list of tuple (“have", [1,1,1,1]), ('fun', [1,1,1])'''
				
				# print(MapperHandler.mapout[map_task_id][0])

				self.write(json.dumps( dict([("status", "success"), ("map_task_id", map_task_id)]) ))
				MapperHandler.running_mapper -= 1
				break #jump out the loop

	def _mapper( self, mapper_path, input_file ):
		p = subprocess.Popen(mapper_path, stdin=open(input_file), stdout=subprocess.PIPE)
		for line in p.stdout:
			line = line.decode().strip()
			# line = line.strip()
			if len(line) == 0:
				continue
			yield line.split('\t', 1) #split for one time yield [word, 1]
			# yield line

if __name__ == "__main__":
	apps = {}
	apps[START_PORT]= tornado.web.Application(handlers=[(r"/map", MapperHandler, dict(port = START_PORT))], debug = True)
	apps[START_PORT].listen(START_PORT)
	tornado.ioloop.IOLoop.instance().start()
		#apply reducer program to the output against the map output through sys.stdin
