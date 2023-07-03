import requests
import json
import argparse

class Client:
    def __init__(self, serverAddress):
        self.server = serverAddress

    def create(self, key, value, serverAddress = None):
        if not serverAddress:
            return requests.post(self.server, json={"key": key, "value": value})
        else:
            return requests.post(serverAddress, json={"key": key, "value": value})
        
    def read(self, key, serverAddress = None):
        response = None
        if not serverAddress:
            response = requests.get(self.server + "?key=" + key, allow_redirects=False)
        else:
            response = requests.get(serverAddress + "?key=" + key, allow_redirects=False)
        if response.status_code == 302:
            print('got 302')
            return self.read(key, response.headers['Location'])
        else:
            return response
        
    def update(self, key, value, serverAddress = None):
        if not serverAddress:
            return requests.put(self.server, json={"key": key, "value": value})
        else:
            return requests.put(serverAddress, json={"key": key, "value": value})
        
    def delete(self, key, serverAddress = None):
        if not serverAddress:
            return requests.delete(self.server + "?key=" + key)
        else:
            return requests.delete(serverAddress + "?key=" + key)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Make a CRUD operation on the server.')

    parser.add_argument('serverAddress', type=str, help='Address of the server.')
    parser.add_argument('operation', type=str, help='The operation to perform: create, read, update, delete.')
    parser.add_argument('key', type=str, help='The key for the operation.')
    parser.add_argument('--value', type=str, help='The value for create or update operation.')

    args = parser.parse_args()

    if args.serverAddress:
        url = args.serverAddress.lower()

    client = Client(url)

    if args.operation.lower() == 'create':
        if args.value is None:
            print("Error: You must specify a value for create operation.")
        else:
            response = client.create(args.key, args.value)
            print(response.json())

    elif args.operation.lower() == 'read':
        response = client.read(args.key)
        print(response.json())

    elif args.operation.lower() == 'update':
        if args.value is None:
            print("Error: You must specify a value for update operation.")
        else:
            response = client.update(args.key, args.value)
            print(response.json())

    elif args.operation.lower() == 'delete':
        response = client.delete(args.key)
        print(response.json())

    else:
        print("Error: Invalid operation. Valid operations are create, read, update, delete.")
