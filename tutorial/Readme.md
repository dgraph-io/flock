# Flock 
Twitter on Dgraph. Pulls twitter data from its open API's and stores it into Dgraph.  

# Motivation 
The goal of the project is to build a interesting use case around Dgraph with a focus on developer experience. The docker-compose setup gets you up and running and flock let's you explore Dgraph's capabilities on the real twitter stream. 

# Usage 

- Clone the repository. 
```sh
$ git clone https://github.com/dgraph-io/flock.git
$ cd flock/tutorial
```

- Export the persistant data directory. Since Dgraph is run using Docker containers its essential to mount a directory on the host machine to persist the data across multiple runs. 
```sh
$ mkdir ./data
$ export DATA_DIR=$(pwd)/data
```

- Export UID. This is to give permissions to Dgraph process inside the container to write to host directory.   
```sh
$ export UID
```

- Finally, give permission to docker-compose to write to unix sockets inorder interact with docker-daemon.
```
$ sudo usermod -aG docker $USER
```

- Start the Dgraph servers with Docker-compose. Visit localhost:8000 on your browser to view the UI.
```sh 
$ docker-compose up
```
