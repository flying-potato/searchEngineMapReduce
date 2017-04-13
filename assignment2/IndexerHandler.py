# return a posting list for query word:[(id, score)]
import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.httpclient
import tornado.gen
import tornado.options
from tornado.escape import json_decode,json_encode
from nltk.tokenize import RegexpTokenizer
from collections import deque
import socket , json, nltk, pickle, os, sys
import xml.etree.ElementTree as ET
from math import log2

from assignment2.inventory import *
from assignment2.parseXML import PageNode

inner_prod = lambda a,b: sum([aa*bb for aa,bb in (a,b)])

class IndexerHandler(tornado.web.RequestHandler):
        idf_path = JOB_PATHS['idf']  #IDF is from idf_jobs/0.out
        for f in read_output(idf_path):
                with open(f, 'rb') as idf_fd:
                        IDF = pickle.load(idf_fd)
        INDEXER_ID = 0

        def initialize(self, port):

                self.port = port
                partition_idx = INDEX_PORTS.index(self.port)
                with open( os.path.join( JOB_PATHS['invindex'], "%d.out"%partition_idx) , 'rb' ) as inv_fd:
                        self.inv_dict = pickle.load(inv_fd)


        @tornado.gen.coroutine
        def get(self):
                resp = []
                q = self.get_argument("q")  # /index?q=query_here

                if not IndexerHandler.IDF.get(q): 
                        resp = json.dumps({"postings": []})
                else:
                        idf = IndexerHandler.IDF[q]   # when word's q_df close to page_num, its socre is 0                 
                        # check each partition of invindex
                        if not self.inv_dict.get(q):
                                resp = json.dumps({"postings": []})
                        else:
                                for (docID, tf) in self.inv_dict[q]:
                                        score = inner_prod([idf,1], [int(tf)*idf,1]) 
                                        resp.append([docID, score])

                        sorted_resp = sorted(resp, key=lambda tp: tp[1],  reverse = True)
                        resp = json.dumps({"postings": sorted_resp},sort_keys=True)

                self.write(resp) #encode result to json



if __name__ == "__main__":
        #depend nodelist in PageNode
        apps = {}
        for index_port in INDEX_PORTS:
                url = BASE_ADDR  % index_port 
                print("Indexer%d url: %s" %(IndexerHandler.INDEXER_ID, url))
                IndexerHandler.INDEXER_ID+=1
                apps[index_port] = tornado.web.Application(handlers=[(r"/index", IndexerHandler, dict(port = index_port))],debug = True)
                apps[index_port].listen(index_port)

        tornado.ioloop.IOLoop.instance().start()

#IDF is word's  DocumentFrequency [word, IDF] pair
#