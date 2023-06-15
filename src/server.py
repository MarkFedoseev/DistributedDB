from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse as urlparse
from urllib.parse import parse_qs
from database import Database
import json
import socketserver, http.server

db = Database()
def parseServersCfg(path):
    servers = []
    with open(path) as cfg:
        jsn = json.load(cfg)
        for server in jsn:
            ip, port = list(server.items())[0]
            servers.append(f'http://{str(ip)}:{str(port)}')
        print(servers)
        #servers = [str(server.).join(':', port) for ip, port in jsn.items()]
    return servers

class ServersCluster():
    def __init__(self, servers = []):
        self.isMaster = False
        self.servers = servers
        self.curent_server = iter(self.servers)

    def resetCurrentServer(self):
        self.curent_server = iter(self.servers)

    def getNumberOfServers(self):
        return len(self.servers)
    
    def getNextServer(self):
        return next(self.curent_server)
    
    def mastersRequests(self):
        if not self.isMaster:
            return
        self.resetCurrentServer()

        

class MyHandler(BaseHTTPRequestHandler):
    def response200(self, key, record):
        self.send_response(200) #OK
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({key: record}).encode())

    def do_GET(self):
        parsed_path = urlparse.urlparse(self.path)
        key = parse_qs(parsed_path.query).get('key', None)
        if key:
            record = db.read(key[0])
            if record.startswith('Error'):
                self.raiseError(record)
                return
            if record is not None:
                self.response200(key[0], record)

            else:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Key not found."}).encode())

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data = json.loads(post_data.decode())
        record = db.create(data.get('key'), data.get('value'))
        if record.startswith('Error'):
            self.raiseError(record)
            return
        self.response200("message", "Record created.")

    def do_PUT(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data = json.loads(post_data.decode())
        record = db.update(data.get('key'), data.get('value'))
        if record.startswith('Error'):
            self.raiseError(record)
            return
        self.response200("message", "Record updated.")

    def do_DELETE(self):
        parsed_path = urlparse.urlparse(self.path)
        key = parse_qs(parsed_path.query).get('key', None)
        if key:
            record = db.delete(key[0])
            if record.startswith('Error'):
                self.raiseError(record)
                return
            self.response200("message", "Record deleted.")

    def raiseError(self, record):
        self.send_response(400) #Bad request
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({"Error:": record.split('Error:')[1]}).encode())

class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    ""

if __name__ == '__main__':
    serversCluster = ServersCluster(parseServersCfg('replicas.json'))
    server = ThreadedHTTPServer(('localhost', 8080), MyHandler)
    #server = HTTPServer(('localhost', 8080), MyHandler)
    print('Starting server at http://localhost:8080')
    server.serve_forever()
