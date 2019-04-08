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

- Export the persistent data directory. Since Dgraph is run using Docker containers its essential to mount a directory on the host machine to persist the data across multiple runs.
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
---

In the next step let's fetch twitter stream data and store them in Dgraph. Here's where you'll see that modeling your data with a GraphDB differs from SQl and NO-SQL databases, welcome to relationship first approach with Dgraph! Using Flock, here's how the data from the twitter stream is modeled.

**TODO: Illustration required to showcase the Model of twitter stream in Dgraph.**  

Graph databases store data by retaining its connected representation. This lets you discover connections and relationships which are not possible with SQL or NO-SQL databases. Imagining and modeling the real world data in its natural connected form is more intuitive than trying hard to squeeze it in a tabular format as rows and tables. The denormalized modeling of NO-SQL databases are even less effective than SQL. These inherent capabilities of a GraphDB combined with ease of use, performance and scalability of Dgraph let's you model easily, develop applications faster and discover relationships which could empower one to have feature sets which were not possible before in real time.

---

# Obtaining twitter credentials

One need to have a twitter developer account and an app to be able to fetch stream of tweets using their API's. Let's start with how to create a twitter developer account.

- Apply for a twitter developer account [here](https://developer.twitter.com/en/apply/user) and follow the instructions. The series of steps would end with your email verification.
- Create a twitter app from [this link](https://developer.twitter.com/en/apps/create). All fields are not `required`.  
- You'll be redirected to the App details tab after creating the tab. Go the `Keys and tokens` tab and create new access and secret tokens.
[./assets/twitter-keys.png]
- Create a copy of the credentials template.
  ```sh
  mv credentials-template.json credentials.json
  ```
- Open the `crendentials.json` file and replace the placeholders with the keys from the twitter app's `Keys and token tab`.
