from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse as urlparse
from urllib.parse import parse_qs
from database import Database
from client import Client
import json
import socketserver, http.server
import argparse
import threading
import os

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
    def __init__(self, servers = [], isMaster = False):
        self.isMaster = isMaster
        self.servers = servers
        self.curent_server = iter(self.servers)

    def resetCurrentServer(self):
        self.curent_server = iter(self.servers)

    def getNumberOfServers(self):
        return len(self.servers)
    
    def getNextServer(self):
        try:
            serv = next(self.curent_server)
            #print(f'redirecting to: {str(serv)}')
            return serv
        except StopIteration:
            self.resetCurrentServer()
            serv = next(self.curent_server)
            #print(f'redirecting to: {str(serv)}')
            return serv
    
    def mastersRequests(self):
        if not self.isMaster:
            return
        self.resetCurrentServer()

        

class ServerHandler(BaseHTTPRequestHandler):

    def response200(self, key, record):
        self.send_response(200) #OK
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({key: record}).encode())

    def writeLog(self, req):
        with server.log_lock:
            with open('requests.log', 'a') as log_file:
                log_file.write(json.dumps(req) + '\n')

        
    def do_GET(self):

        if self.server.serversCluster.isMaster:
            self.send_response(302)
            self.send_header('Location', self.server.serversCluster.getNextServer())
            self.end_headers()
            return
        
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
        self.writeLog({'operation' : 'create', 'data' : (data.get('key'), data.get('value'))})
        record = db.create(data.get('key'), data.get('value'))
        if record.startswith('Error'):
            self.raiseError(record)
            return
        self.response200("message", "Record created.")

    def do_PUT(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data = json.loads(post_data.decode())
        self.writeLog({'operation' : 'update', 'data' : (data.get('key'), data.get('value'))})
        record = db.update(data.get('key'), data.get('value'))
        if record.startswith('Error'):
            self.raiseError(record)
            return
        self.response200("message", "Record updated.")

    def do_DELETE(self):
        parsed_path = urlparse.urlparse(self.path)
        key = parse_qs(parsed_path.query).get('key', None)
        if key:
            self.writeLog({'operation' : 'delete', 'data' : key})
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
    def __init__(self, server, handler, serversCluster):
        self.serversCluster = serversCluster
        self.log_lock = threading.Lock()
        super().__init__(server, handler)

def replicate(server, serversCluster):
    while True:
        print('gay')
        if os.path.exists("requests.log"):
            with server.log_lock:
                with open("requests.log", "r") as log_file:
                    lines = log_file.readlines()
                os.remove("requests.log")
            client = Client('http://localhost:0')
            for line in lines:
                log_data = json.loads(line)
                #print(f"Replaying {log_data['method']} with data {log_data['data']}")
                serversCluster.resetCurrentServer()
                for i in range(serversCluster.getNumberOfServers()):
                    if log_data['operation'] == 'create':
                        client.create(log_data['data'])
                    elif log_data['operation'] == 'update':
                        client.update(log_data['data'])
                    elif log_data['operation'] == 'delete':
                        client.delete(log_data['data'])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Make a CRUD operation on the server.')
    parser.add_argument('host', type=str, help='Host of the server.')
    parser.add_argument('port', type=str, help='Port to listen by server.')
    parser.add_argument('--master', action='store_true', help='If provided, server would be a master')
    args = parser.parse_args()
    serversCluster = ServersCluster(parseServersCfg('replicas.json'), args.master)
    server = ThreadedHTTPServer((args.host, int(args.port)), ServerHandler, serversCluster)
    threading.Thread(target=replicate, args=(server, ServersCluster(parseServersCfg('replicas.json'), args.master),)).start()
    #server = HTTPServer(('localhost', 8080), ServerHandler)
    print(f'Starting server at http://{args.host}:{args.port}')
    server.serve_forever()
