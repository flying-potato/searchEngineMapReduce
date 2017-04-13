from tornado.ioloop import IOLoop
from tornado import gen, httpclient
import json, urllib, subprocess

# To use, put this file in the same directory that you unzipped starter.zip, then run
# python reducer_demo.py

# URLs we fetch from:
# http://linserv2.cims.nyu.edu:34514/retrieve_map_output?reducer_ix=0&map_task_id=8a97fd755ea12827485749036e15d651
# http://linserv2.cims.nyu.edu:34514/retrieve_map_output?reducer_ix=0&map_task_id=d3486112191e4717d17d4fba189bdbf6

@gen.coroutine
def main():
    reducer_ix = 0
    map_task_ids = ["8a97fd755ea12827485749036e15d651", "d3486112191e4717d17d4fba189bdbf6"]
    servers = ["linserv2.cims.nyu.edu:34514", "linserv2.cims.nyu.edu:34515"]
    reducer_path = "wordcount/reducer.py"
    num_mappers = len(map_task_ids)

    http = httpclient.AsyncHTTPClient()
    futures = []
    for i in range(num_mappers):
        server = servers[i % len(servers)]
        params = urllib.parse.urlencode({'reducer_ix': reducer_ix,
                                         'map_task_id': map_task_ids[i]})
        url = "http://%s/retrieve_map_output?%s" % (server, params)
        print("Fetching", url)
        futures.append(http.fetch(url))
    responses = yield futures

    kv_pairs = []
    for r in responses:
        print(json.loads(r.body.decode()))
        kv_pairs.extend(json.loads(r.body.decode()))
    kv_pairs.sort(key=lambda x: x[0])

    kv_string = "\n".join([pair[0] + "\t" + pair[1] for pair in kv_pairs])
    p = subprocess.Popen(reducer_path, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    (out, _) = p.communicate(kv_string.encode())
    print(out.decode())

if __name__ == "__main__":
    IOLoop.current().run_sync(main)
