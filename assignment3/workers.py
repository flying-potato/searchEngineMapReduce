import sys
import tornado.httpserver as httpserver
import tornado.ioloop
import tornado.web as web
import tornado.httpclient
import tornado.gen
import tornado.options
from assignment3.ReducerHandler import ReducerHandler
from assignment3.MapperHandler import MapperHandler
from assignment3.RetrieveMapHandler import RetrieveMapHandler
from assignment3.inventory import *
apps={}
http_server={}
for port in PORT_LIST:
	apps[port] = web.Application(handlers = [
		(r"/map", MapperHandler, dict(port = port)),
		(r"/retrieve_map_output", RetrieveMapHandler,dict(port = port)),
		(r"/reduce", ReducerHandler, dict(port = port)),
	], debug = True)
	http_server[port] = httpserver.HTTPServer(apps[port])
	http_server[port].listen(port)
print("listen to ports: %s" % str(PORT_LIST))
tornado.ioloop.IOLoop.instance().start()