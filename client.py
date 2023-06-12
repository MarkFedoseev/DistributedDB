import requests
import json
import argparse

# Create the parser
parser = argparse.ArgumentParser(description='Make a CRUD operation on the server.')

# Add the arguments
parser.add_argument('serverAddress', type=str, help='Address of the server.')
parser.add_argument('operation', type=str, help='The operation to perform: create, read, update, delete.')
parser.add_argument('key', type=str, help='The key for the operation.')
parser.add_argument('--value', type=str, help='The value for create or update operation.')

# Parse the arguments
args = parser.parse_args()

# Server URL
#url = "http://localhost:8080"
if args.serverAddress:
    url = args.serverAddress.lower()

#class Client:
#    def __init__():
# Perform the specified operation
if args.operation.lower() == 'create':
    if args.value is None:
        print("Error: You must specify a value for create operation.")
    else:
        response = requests.post(url, json={"key": args.key, "value": args.value})
        print(response.json())
elif args.operation.lower() == 'read':
    response = requests.get(url + "?key=" + args.key)
    print(response.json())
elif args.operation.lower() == 'update':
    if args.value is None:
        print("Error: You must specify a value for update operation.")
    else:
        response = requests.put(url, json={"key": args.key, "value": args.value})
        print(response.json())
elif args.operation.lower() == 'delete':
    response = requests.delete(url + "?key=" + args.key)
    print(response.json())
else:
    print("Error: Invalid operation. Valid operations are create, read, update, delete.")
