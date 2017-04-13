import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.httpclient
import tornado.gen
import tornado.options
import socket
import json
from tornado.escape import json_decode, json_encode
import xml.etree.ElementTree as ET

from assignment2.inventory import *
from assignment2.IndexerHandler import IndexerHandler
from assignment2.DocHandler import DocHandler
from assignment2.Frontend import ForwardHandler
from assignment2.parseXML import PageNode
resps = {}
postings = []
apps = {}

if __name__ == "__main__":
	for index_port in INDEX_PORTS:
		url = BASE_ADDR  % index_port 
		print("Indexer%d url: %s" %(IndexerHandler.INDEXER_ID, url))
		print("Doc Handler%d url: %s" %(DocHandler.DOCSERVER_ID, url))
		IndexerHandler.INDEXER_ID+=1
		DocHandler.DOCSERVER_ID+=1
		apps[index_port] = tornado.web.Application(handlers=
			[(r"/index", IndexerHandler, dict(port = index_port)),
			(r"/doc", DocHandler,dict(port = index_port)),
		],debug = True)
		apps[index_port].listen(index_port)

	# for doc_port in DOC_PORTS:
	# 	url = BASE_ADDR % doc_port
	# 	print("Doc Handler%d url: %s" %(DocHandler.DOCSERVER_ID, url))
	# 	DocHandler.DOCSERVER_ID+=1
	# 	apps[doc_port] = tornado.web.Application(handlers=[(r"/doc", DocHandler, dict(port = doc_port))],debug = True)
	# 	apps[doc_port].listen(doc_port)

	apps[org_port] = tornado.web.Application(handlers=[(r"/search", ForwardHandler)],debug = True)
	url = BASE_ADDR % org_port
	print("search by word: %s" % (url+"/search?q=financial"))
	apps[org_port].listen(org_port)

	tornado.ioloop.IOLoop.instance().start()
