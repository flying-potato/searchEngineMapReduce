# reducer.py
'''
The reducer first fetches its key-value pairs directly from each of the mappers. It then runs the reducer program against this input data, and writes the output to a file in the job's working directory (such as 0.out). The reducer's index (a value in the range [0 ... N-1]) is used to name the output file.
'''

'''
GET /reduce?
  reducer_ix=0&
  reducer_path=wordcount/reducer.py&
  map_task_ids=map_task_id1,map_task_id2&
  job_path=fish_jobs

http://linserv2.cims.nyu.edu:34514/reduce?map_task_ids=e6dffea5c01eadeea832b55fb16dbeb8,
d1d4ec69caff59f6bceac1bcfea56f47,7012419b916123cf8f6a36b3212fa3bd,
095ca1c108618ac918b2c67481826222&
reducer_path=wordcount/reducer.py&job_path=fish_jobs&reducer_ix=0

Next, the reducer merges the M sorted lists of key-value pairs into a single list sorted by key.
2 mapper tasks and 2 sorted lists merged to one list
Finally, the reducer program is run with the sorted list piped in through stdin. The reducer program's stdout is piped directly to an output file (such as job_path/0.out, where 0 is the index of the reducer task).


'''
import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.httpclient
import tornado.gen
import tornado.options
from tornado.escape import json_decode,json_encode
import os, json, urllib, subprocess, heapq

from assignment3.inventory import *

class ReducerHandler(tornado.web.RequestHandler):

	running_reducer_num = 0;
	def initialize (self, port):
		self.port = port

	@tornado.gen.coroutine
	def get(self):
		while True:
			if(ReducerHandler.running_reducer_num < REDUCER_MAXNUM):
				ReducerHandler.running_reducer_num+=1
				reducer_ix= self.get_argument("reducer_ix") #all keys that can be hashed to reducer_ix
				reducer_path = self.get_argument("reducer_path")
				map_task_ids= self.get_argument("map_task_ids")# a list of map_tasks
				map_task_id_list = map_task_ids.split(',')
				job_path = self.get_argument("job_path")
				map_task_num = len(map_task_id_list)

				http = tornado.httpclient.AsyncHTTPClient()
				servers = list( map(lambda port: SERVER_PATTERN % port, PORT_LIST) )# server with port

				futures = []
				print("map tasks num: %d" % len(map_task_id_list))
				for i in range(map_task_num):
					server = str(servers[i % len(servers)])
					params = urllib.parse.urlencode({'reducer_ix': reducer_ix, 'map_task_id': map_task_id_list[i]})
					url = "http://%s/retrieve_map_output?%s" % (server, params)
					print("reduce_task_%s retrieve_map_output_url:" % reducer_ix,url)
					futures.append(http.fetch(url))
				raw_responses = yield futures

				# heapq.merge(*iterables)
				#Merge multiple sorted inputs into a single sorted output (for example, merge timestamped entries from multiple log files). Returns an iterator over the sorted values. 
				kv_pairs = heapq.merge(*[json.loads(r.body.decode()) for r in raw_responses])
				self._reducer(kv_pairs, reducer_path, os.path.join(job_path, "%d.out" % int(reducer_ix)) )
				self.write(json.dumps({"status": "success"}))
				ReducerHandler.running_reducer_num-=1
				break

	def _reducer(self,kv_pairs, reducer_path, output_file):
		# try finally simplified by with block
		# when the “with” statement is executed, Python calls the __enter__ method on the resulting value (which is called a “context guard”), and assigns whatever __enter__ returns to the variable given by as. 
		# will then execute the code body(with block), and no matter what happens in that code, call the guard object’s __exit__ method.
		# the file object has been equipped with __enter__ and __exit__ methods; 
		with open(output_file , 'w') as output:
			p = subprocess.Popen([reducer_path] , stdin = subprocess.PIPE, stdout= output )
			for pair in kv_pairs:
				p.stdin.write(('%s\t%s\n' % (pair[0], pair[1])).encode())
			p.stdin.close()
			p.wait()


# class RetrieveReduceOutput(web.RequestHandler):
#     def head(self):
#         self.finish()

#     def get(self):
#         job_path = self.get_argument('job_path')
#         num_reducers = int(self.get_argument('num_reducers'))
#         self.write('<pre>')
#         for filename in [os.path.join(job_path, '%d.out') % i for i in range(num_reducers)]:
#             self.write(filename + ':\n' + str(open(filename, 'r').read()) + '\n')
#         self.finish()

if __name__ == "__main__":
	apps = {}
	apps[START_PORT]= tornado.web.Application(handlers=[(r"/reduce", ReducerHandler, dict(port = START_PORT))], debug = True)
	apps[START_PORT].listen(START_PORT)
	tornado.ioloop.IOLoop.instance().start()
		#apply reducer program to the output against the map output through sys.stdin
	'''class controlled_execution:
		def __enter__(self):
			set things up
			return thing
		def __exit__(self, type, value, traceback):
			tear things down

	with controlled_execution() as thing:
			some code'''