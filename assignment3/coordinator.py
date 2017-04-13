import os
from os import listdir
from os.path import isfile, join
import sys, urllib
import tornado.httpserver
import tornado.ioloop as ioloop
import tornado.web
import tornado.httpclient
import tornado.gen
import tornado.options


from assignment3.ReducerHandler import ReducerHandler
from assignment3.MapperHandler import MapperHandler
from assignment3.RetrieveMapHandler import RetrieveMapHandler
from assignment3.inventory import *
import argparse
'''
First, the coordinator searches the working directory for files that match the pattern "*.in", such as 0.in, 1.in, etc. These files are inputs to the MapReduce application. For each of the M input files, a mapper task is run. Mapper tasks are assigned to workers in a round-robin fashion.

When the mapper tasks finish, the reducer tasks are run. Reducer tasks are assigned to workers in a round-robin fashion as well. Each reducer task writes its output to a file (such as job_path/0.out, where 0 is the index of the reducer task).

When the reducer tasks finish, the coordinator exits. Aside from any diagnostic output you might want to show, it is not necessary for the coordinator itself to output anything to the console or to disk.
'''


def parse_argv():
	arg_dict = {}
	parser = argparse.ArgumentParser(description='Process some integers.')
	arg_name_list = ["--mapper_path", "--reducer_path", "--job_path", "--num_reducers"]
	for arg_name in arg_name_list:
		# parser.add_argument(arg_name,type=int if arg_name=="--num_reducers" else str,dest = arg_name[2:])
		parser.add_argument(arg_name,type=str,dest = arg_name[2:])
	args = parser.parse_args()
	return args
	# {'reducer_path': 'wordcount/reducer.py', 'job_path': 'fish_jobs', 'mapper_path': 'wordcount/mapper.py', 'num_reducers': 1} 

def read_input(job_path): #submit each file and create a mapper for it
	file_list  = listdir(job_path)
	in_list = [f for f in file_list if (isfile(join(job_path, f)) and f[-3:] == ".in") ]
	return in_list 

def clean_old_out(job_path):
	print("---clean old output file---")
	file_list  = listdir(job_path)
	out_list = [f for f in file_list if (isfile(join(job_path, f)) and f[-4:] == ".out") ]
	for of in out_list:
		os.remove(join(job_path, of))
	print("---finish cleaning---")
	
@tornado.gen.coroutine
def start():
	print("coordinator begins working")
	args = parse_argv();
	mapper_path = args.mapper_path
	reducer_path = args.reducer_path
	job_path = args.job_path
	num_reducers = int(args.num_reducers)

	in_list = read_input(job_path)
	tornado.httpclient.AsyncHTTPClient.configure(None, defaults={'connect_timeout': 300, 'request_timeout': 300})
	http = tornado.httpclient.AsyncHTTPClient()
	futures = []
	# map part
	for i in range(len(in_list)):
		port = PORT_LIST[i]
		infile = in_list[i]
		# create a mapper task for each file
		params = urllib.parse.urlencode({"mapper_path":mapper_path, "input_file":join(job_path,infile), "num_reducers":num_reducers})
		server = SERVER_PATTERN % port
		url = "http://%s/map?%s" % (server, params)
		print("map_url:" ,url)
		futures.append( http.fetch(url) )# submit request
	resp = yield futures


	#clean old output files
	clean_old_out(job_path)

	futures2 = []
	# reduce part
	map_task_ids = [ (eval(r.body.decode()))["map_task_id"] for r in resp ]
	tmp_task_ids = ','.join(map_task_ids)
	print("Map tasks:")
	for i in range(len(map_task_ids)):
		print("map_task_%d: %s" % (i+1, map_task_ids[i])) 
	for ix in range(num_reducers):
		params = urllib.parse.urlencode(
			{"map_task_ids":tmp_task_ids, "reducer_path":reducer_path, "job_path": job_path , "reducer_ix":str(ix)} )
		server = SERVER_PATTERN % PORT_LIST[ix % WORKER_NUM]
		url2  = "http://%s/reduce?%s" % (server, params)
		print("Reduce tasks:")
		print("reduce task %d: %s" %(ix+1, url2))
		futures2.append(http.fetch(url2))
		# yield http.fetch(url2)

	r2 = yield futures2
	sys.exit()

	
if __name__=="__main__":
	ioloop.IOLoop.current().run_sync(start)

