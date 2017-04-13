#assignment4.start
# run your three MapReduce jobs and generate all of the index files
# submit request to workers

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
import subprocess
from assignment3.inventory import *
from assignment2.inventory import *
read_input = lambda job_path: [f for f in listdir(job_path) if f[-3:] == ".in" ]

def clean_old_out(job_path):
	print("---clean old output file---")
	file_list  = listdir(job_path)
	out_list = [f for f in file_list if (isfile(join(job_path, f)) and f[-4:] == ".out") ]
	for of in out_list:
		os.remove(join(job_path, of))
	print("---finish cleaning---")


def start():
	# jobs = ['invindex', 'docs', 'idf' ]
	jobs = JOB_NAMES
	reducer_num_arr = [INDEXER_NUM, NUM_DOC_PART, 1 ]
	
	# jobs = ['invindex' ]
	# reducer_num_arr = [INDEXER_NUM]
	# jobs = ['idf' ]
	# reducer_num_arr = [1]
	
	reducer_num_dict =dict( zip(jobs, reducer_num_arr))
	job_paths={}
	mapper_paths = {}
	reducer_paths = {}
	for job in jobs:
		job_paths[job] = join( 'assignment4' , '%s_jobs' % job)  
		mapper_paths[job] = join('assignment4', 'mr_apps', "%s_mapper.py"%job )
		reducer_paths[job] = join('assignment4', 'mr_apps', "%s_reducer.py"%job )
	
	# submit request to 
	for jobname in jobs:
		
		cmd_pattern = 'python -m assignment3.coordinator \
		--mapper_path=%s \
		--reducer_path=%s --job_path=%s  --num_reducers=%d'
		cmd = cmd_pattern % (mapper_paths[jobname], reducer_paths[jobname], job_paths[jobname], reducer_num_dict[jobname])
		job = subprocess.Popen(cmd, shell = True)
		job.wait()
	sys.exit(1)


if __name__ == "__main__":
	start()
	
	
