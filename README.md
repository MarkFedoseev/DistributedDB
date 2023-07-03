# DistributedDB
Simple database with CRUD options with distributed replicas
# About
Database can be used to handle large amount of requests and store data on multiple servers that leads to fault tolerance.

Two types of modes are available for server: **master** and **replica**.

All request from **client** have to be sent on **master**.

**Master** handles ```create```, ```update``` and ```delete``` requests and replicate them to **replica** servers using WAL paradigm. ```read``` requests redirects to replicas evenly.

**Replica** handles all requests

# Fault tolerance
**Replicas** periodically pinging **master** to check is it alive. When **master** is dead, **replicas** pinging other **replicas** in cluster and **replicas** in cluster choose new **master** using formula:

```min(weights[])```

Where ```weights[]``` is an array of weights. Weight is a characteristic number calculated for every **replica** as a sum of time needed to get answer from every other **replica** in cluster.

That leads to choosing an optimal server to be a **master** that can process requests faster than others

# Usage
Start server as a **master** on specific host and port:

```python server.py <host> <port> --master```

Start server as a **replica** on specific host and port:

```python server.py <host> <port> --mastersAddress <host:port>```

Where *mastersAddress* is an address of **master** server

Client request to **master** on *host:port* with *operation* ```create```, ```read```, ```update```, ```delete```:

```python client.py <host:port> <operation> <key> [<value>]```

Example:

```python server.py "localhost" 8080 --master```

```python server.py "localhost" 8081 --mastersAddress http://localhost:8080```

```python client.py http://localhost:8080 create name --value "Jone"```

```python client.py http://localhost:8080 read name```
