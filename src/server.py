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
import time
import requests

db = Database()
myURL = ''
def parseServersCfg(path):
    servers = []
    with open(path) as cfg:
        jsn = json.load(cfg)
        for server in jsn:
            ip, port = list(server.items())[0]
            servers.append(f'http://{str(ip)}:{str(port)}')
        #print(servers)
        #servers = [str(server.).join(':', port) for ip, port in jsn.items()]
    return servers

class ServersCluster():
    def __init__(self, mastersAddress, servers = [], isMaster = False):
        self.isMasterFlag = isMaster
        self.master = mastersAddress
        self.servers = servers
        self.curent_server = iter(self.servers)
        self.lock = threading.Lock()

    def getMaster(self):
        with self.lock:
            return self.master
    
    def setMaster(self, mastersAddress):
        with self.lock:
            self.master = mastersAddress

    def isMaster(self):
        with self.lock:
            return self.isMasterFlag
        
    def setMasterFlag(self, flag):
        with self.lock:
            self.isMasterFlag = flag

    def resetCurrentServer(self):
        with self.lock:
            self.curent_server = iter(self.servers)

    def getNumberOfServers(self):
        with self.lock:
            return len(self.servers)
    
    def getCurrentServer(self):
        with self.lock:
            return self.curent_server
    
    def removeServers(self, servers):
        return
        with self.lock:
            print(f'removing servers: {servers}')
            for server in servers:
                self.servers.remove(server)

    def getServers(self):
        with self.lock:
            return self.servers

    def getNextServer(self):
        with self.lock:
            try:
                serv = next(self.curent_server)
                #print(f'redirecting to: {str(serv)}')
                return serv
            except StopIteration:
                self.resetCurrentServer()
                serv = next(self.curent_server)
                #print(f'redirecting to: {str(serv)}')
                return serv

        

class ServerHandler(BaseHTTPRequestHandler):

    def response200(self, key, record):
        self.send_response(200) #OK
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({key: record}).encode())

    def writeLog(self, req):
        #print('writing log...')
        with server.log_lock:
            with open('requests.log', 'a') as log_file:
                log_file.write(json.dumps(req) + '\n')

        
    def do_GET(self):

        if self.path == "/isAlive":
            self.send_response(200)
            self.end_headers()
            return

        if self.server.serversCluster.isMaster():
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

        if data.get('key') == 'yrMaster':
                becomeMaster(self.server, self.server.masterWatcher.timestamp)
                self.send_response(200)
                self.end_headers()
                return
        if data.get('key') == 'newMaster':
            self.server.masterWatcher.timestamp.setTime(-1)
            print(f'newMaster is: {data.get("mastersAddress")}')
            self.server.serversCluster.setMaster(data.get('mastersAddress'))
            self.send_response(200)
            self.end_headers()
            return

        if data.get('key') == 'getWeight':
                if float(data.get('timestamp')) < self.server.masterWatcher.timestamp.getTime() or \
                    self.server.masterWatcher.timestamp.getTime() < 0:

                    print('sending weight')
                    self.response200(self.server.myURL, str(getWeight(self.server.serversCluster)))
                else:
                    print('Already work on')
                    self.send_response(204)
                    self.end_headers()
                return

        if self.server.serversCluster.isMaster():
            self.writeLog({'operation' : 'create', 'data' : [data.get('key'), data.get('value')]})
        record = db.create(data.get('key'), data.get('value'))
        if record.startswith('Error'):
            self.raiseError(record)
            return
        self.response200("message", "Record created.")

    def do_PUT(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data = json.loads(post_data.decode())
        if self.server.serversCluster.isMaster():
            self.writeLog({'operation' : 'update', 'data' : [data.get('key'), data.get('value')]})
        
        record = db.update(data.get('key'), data.get('value'))
        if record.startswith('Error'):
            self.raiseError(record)
            return
        self.response200("message", "Record updated.")

    def do_DELETE(self):
        parsed_path = urlparse.urlparse(self.path)
        key = parse_qs(parsed_path.query).get('key', None)
        if key:
            if self.server.serversCluster.isMaster():
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

def becomeMaster(server, timestamp):
    print('Im master now')
    server.serversCluster.setMasterFlag(True)
    server.serversClusterReplicate.setMasterFlag(True)
    threadReplicate.start()
    threadRepWatcher.start()
    replicas = serversCluster.getServers().copy()
    droped_preplicas = []
    for replica in replicas:
        try:
            requests.post(replica, json={'key' : 'newMaster', 'mastersAddress' : myURL})
        except:
            droped_preplicas.append(replica)
    print(F'BECOMEMASTER droped servers: {str(droped_preplicas)}')

    timestamp.setTime(-1)

def becomeReplica(server):
    print('Im replica now')
    server.serversCluster.setMasterFlag(False)
    server.serversClusterReplicate.setMasterFlag(False)
    threadMasWatcher.start()

def replicate(server, serversCluster):
    while True:
        if serversCluster.isMaster() == False:
            return
        time.sleep(1)
        if os.path.exists("requests.log"):
            with server.log_lock:
                with open("requests.log", "r") as log_file:
                    lines = log_file.readlines()
                os.remove("requests.log")
            client = Client('http://localhost:0')
            for line in lines:
                log_data = json.loads(line)
                print(f"Replaying {log_data['operation']} with data {log_data['data']}")
                serversCluster.resetCurrentServer()
                droped_replicas = []
                try:
                    for i in range(serversCluster.getNumberOfServers()):
                        if log_data['operation'] == 'create':
                            client.create(log_data['data'][0], log_data['data'][1], serversCluster.getNextServer())
                        elif log_data['operation'] == 'update':
                            client.update(log_data['data'][0], log_data['data'][1], serversCluster.getNextServer())
                        elif log_data['operation'] == 'delete':
                            client.delete(log_data['data'], serversCluster.getNextServer())
                except:
                    droped_replicas.append(serversCluster.getCurrentServer())
                serversCluster.removeServers(droped_replicas)

def getWeight(serversCluster):
    return sum([replica[1] for replica in pingReplicas(serversCluster)])

def pingReplicas(serversCluster):
    print('pinging replicas..')
    aliveReplicas = []
    #serversCluster.resetCurrentServer
    replicas = serversCluster.getServers().copy()
    print(replicas)
    droped_replicas = []
    for replica in replicas:
        try:
            stime = time.time()
            if requests.get(replica + '/isAlive').status_code == 200:
                etime = time.time()
                aliveReplicas.append((replica, etime - stime))
        except:
            droped_replicas.append(replica)
    if len(droped_replicas) == 0:
        print('all replicas are alive')
    serversCluster.removeServers(droped_replicas)
    return aliveReplicas
    
class MasterWatcherTimeStamp:
    def __init__(self):
        self.lock = threading.Lock()
        self.timestamp = -1
    def setTime(self, timestamp):
        with self.lock:
            self.timestamp = timestamp
    def getTime(self):
        with self.lock:
            return self.timestamp

#master drop
class MasterWatcher:
    def __init__(self, serversCluster, mwTimestamp):
        self.serversCluster = serversCluster
        self.timestamp = mwTimestamp
    def watch(self):
        while True:
            print('pinging master..')
            print(f'CURRENT MASTER IS: {self.serversCluster.getMaster()}')
            if self.serversCluster.isMaster == True:
                return
            time.sleep(3)
            newMaster = None
            if self.pingMaster(self.serversCluster.getMaster()):
                print('master is dead')
                self.timestamp.setTime(time.time())
                newMaster = self.chooseNewMaster(
                    self.getWeights(
                        [replica[0] for replica in pingReplicas(serversCluster)]
                    )
                )
            if newMaster:
                requests.post(newMaster, json={'key' : 'yrMaster'})
                self.serversCluster.setMaster(newMaster)
            else:
                print('master is alive')

    def pingMaster(self, mastersAddress):
        try:
            return requests.get(mastersAddress + '/isAlive').status_code != 200
        except:
            return True
    def getWeights(self, aliveReplicas):
        weights = {}
        for replica in aliveReplicas:
            response = requests.post(replica, json={'key' : 'getWeight', 'timestamp' : str(self.timestamp.getTime())})
            if response.status_code == 204:
                return
            else:
                weights.update({replica : response.json()[replica]})
        return weights
    def chooseNewMaster(self, weights):
        return min(weights, key=weights.get) if weights else None
    
def ReplicasWatcher(serversCluster):
    while True:
        time.sleep(3)
        pingReplicas(serversCluster)


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    def __init__(self, server, handler, serversCluster, serversClusterReplicate, masterWatcher, myURL):
        self.serversCluster = serversCluster
        self.serversClusterReplicate = serversClusterReplicate
        self.log_lock = threading.Lock()
        self.masterWatcher = masterWatcher
        self.myURL = myURL
        super().__init__(server, handler)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Make a CRUD operation on the server.')
    parser.add_argument('host', type=str, help='Host of the server.')
    parser.add_argument('port', type=str, help='Port to listen by server.')
    parser.add_argument('--master', action='store_true', help='If provided, server would be a master')
    parser.add_argument('--mastersAddress', type=str, help='Address of master. Use only if server is replica')
    args = parser.parse_args()
    myURL = f'http://{args.host}:{args.port}'
    time.sleep(3)
    mastersAddress =  args.mastersAddress if not args.master else None
    serversCfg = parseServersCfg('replicas.json')
    if myURL in serversCfg:
        serversCfg.remove(myURL)

    serversCluster = ServersCluster(mastersAddress, serversCfg.copy(), args.master)
    serversClusterReplicate = ServersCluster(mastersAddress, serversCfg.copy(), args.master)
    masterWatcher = MasterWatcher(serversCluster, MasterWatcherTimeStamp())

    server = ThreadedHTTPServer((args.host, int(args.port)), ServerHandler, serversCluster, serversClusterReplicate, masterWatcher, myURL)

    threadReplicate = threading.Thread(target=replicate, args=(server, serversClusterReplicate,))
    threadRepWatcher = threading.Thread(target=ReplicasWatcher, args=(serversCluster, ))
    threadMasWatcher = threading.Thread(target=masterWatcher.watch)
    
    if args.master:
        print('Starting as master')
        threadReplicate.start()
        threadRepWatcher.start()
    else:
        print('Starting as replica')
        threadMasWatcher.start()
    #server = HTTPServer(('localhost', 8080), ServerHandler)
    print(f'Starting server at {myURL}')
    server.serve_forever()
