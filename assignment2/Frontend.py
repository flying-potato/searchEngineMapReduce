import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.httpclient
import tornado.gen
import tornado.options
from collections import deque
import socket
import json
from tornado.escape import json_decode, json_encode
from  assignment2.inventory import *


# class MainHandler(tornado.web.RequestHandler):
#         def initialize(self, port):
#                 self.port = port

#         def get(self):
#                 self.write(str(socket.gethostname())+':'+str(self.port))
# 

class ForwardHandler(tornado.web.RequestHandler):
        @tornado.gen.coroutine
        def get(self):
                q = self.get_argument("q") 
		#default no form
                form_bit = self.get_argument("form", default='n') 

                index_resps = []
                futures1 = []
                http_async = tornado.httpclient.AsyncHTTPClient()
                for index_port in INDEX_PORTS:
                        url  = INDEX_PATT%(index_port, q)
                        print("Indexer url: "+url)
                        futures1.append( http_async.fetch(url)) 

                response_future1 = yield futures1
                for r in response_future1:
                        resp = json.loads(r.body.decode() )
                        resp = resp["postings"]
                        index_resps.extend(resp)

                sort_posting = sorted(index_resps, key=lambda idscore: idscore[1],reverse=True)
                # resp = str(sort_posting)
                num_results = min(len(sort_posting), 10)
                top10resps = sort_posting[0:num_results]

                # request the document details
                doc_resps = []
                futures = []
                for id, score in top10resps:
                        idx  = PARTITIONER(id, NUM_DOC_PART)
                        docport = INDEX_PORTS[idx]
                        url = DOC_PATT%(docport, id, q)
                        print("DOC url: " , url)
                        # resp = yield http_async.fetch(url)
                        futures.append(http_async.fetch(url))
                response_future = yield futures
                for r in response_future:
                        resp = json.loads(r.body.decode() )
                        doc_resps.append(resp["results"])                


                #tmp_resp = json.loads ('{"num_results": num_results, "results": doc_resps}')
                #resp = json.dumps (tmp_resp, indent = 4, sort_keys=True )
                resp = json.dumps({"num_results": num_results, "results": doc_resps},indent = 4, sort_keys=True)

                self.write(resp) #encode result to json

if __name__ == "__main__":

        apps[org_port] = tornado.web.Application(handlers=[(r"/search", ForwardHandler)],debug = True)
        apps[org_port].listen(org_port)

        tornado.ioloop.IOLoop.instance().start()
#                                            
