from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse as urlparse
from urllib.parse import parse_qs
from database import Database
import json
import socketserver, http.server

db = Database()
class ServersCluster():
    def __init__(self, servers = []):
        self.servers = servers
        #current_server = None
        #if servers:
        #    current_server = servers[0]
        #self.curent_server = current_server
        self.curent_server = iter(self.servers)

    def resetCurrentServer(self):
        #if self.servers:
        #    self.curent_server = self.servers[0]
        #else:
        #    self.curent_server = None
        self.curent_server = iter(self.servers)

    def getNumberOfServers(self):
        return len(self.servers)
    
    def getNextServer(self):
        return next(self.curent_server)

class MyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed_path = urlparse.urlparse(self.path)
        key = parse_qs(parsed_path.query).get('key', None)
        if key:
            record = db.read(key[0])
            if record.startswith('Error'):
                self.raiseError(record)
                return
            if record is not None:
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({key[0]: record}).encode())
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
        self.send_response(201)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({"message": "Record created."}).encode())

    def do_PUT(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data = json.loads(post_data.decode())
        record = db.update(data.get('key'), data.get('value'))
        if record.startswith('Error'):
            self.raiseError(record)
            return
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({"message": "Record updated."}).encode())

    def do_DELETE(self):
        parsed_path = urlparse.urlparse(self.path)
        key = parse_qs(parsed_path.query).get('key', None)
        if key:
            record = db.delete(key[0])
            if record.startswith('Error'):
                self.raiseError(record)
                return
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"message": "Record deleted."}).encode())

    def raiseError(self, record):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({"Error:": record.split('Error:')[1]}).encode())

class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    "This is an HTTPServer that supports thread-based concurrency."

if __name__ == '__main__':
    server = ThreadedHTTPServer(('localhost', 8080), MyHandler)
    #server = HTTPServer(('localhost', 8080), MyHandler)
    print('Starting server at http://localhost:8080')
    server.serve_forever()