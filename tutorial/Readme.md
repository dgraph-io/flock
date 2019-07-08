## Flock
Twitter on Dgraph. Pulls twitter data from its open APIs and stores it into Dgraph.  

## Motivation
The goal of the project is to build an interesting use case around Dgraph with a focus on developer 
experience. The docker-compose setup gets you up and running and flock let's you explore Dgraph's 
capabilities on the real twitter stream.

## Table of Contents
- [Obtaining the twitter credentials](#Obtaining-twitter-credentials)
- [Running Flock](#Setup)
- [Welcome to Dgraph](#Welcome-to-Dgraph)
- [Introduction to Ratel](#introduction-to-ratel)
- [Twitter Data](#twitter-data)
- [Data Modeling](#data-modeling)
- [Upsert-directive](#upsert-directive)
---

## Obtaining twitter credentials

One need to have a twitter developer account and an app to be able to fetch stream of tweets using 
their APIs. Let's start with how to create a twitter developer account.

- Apply for a twitter developer account [here](https://developer.twitter.com/en/apply/user) and 
  follow the instructions. The series of steps would end with your email verification.
- Create a twitter app from [this link](https://developer.twitter.com/en/apps/create). 
  All fields are not `required`.  
- You'll be redirected to the App details tab after creating the tab. Go the `Keys and tokens` tab
   and create new access and secret tokens.
![Twitter Developer account](./assets/twitter-keys.png)
- Create a copy of the credentials template.
  ```sh
  mv credentials-template.json ./tutorials/credentials.json
  ```
- Open the `tutorials/crendentials.json` file and replace the placeholders with the keys from the 
  twitter app's `Keys and token tab`.

---

## Setup

- Clone the repository.
```sh
$ git clone https://github.com/dgraph-io/flock.git
$ cd flock/tutorial
```

- Export the persistent data directory. Since Dgraph is run using Docker containers, it is essential
  to mount a directory on the host machine to persist the data across multiple runs.
```sh
$ mkdir ./data
$ export DATA_DIR=$(pwd)/data
```

- Export UID. This is to give permission to Dgraph process inside the container to write to host directory.   
```sh
$ export UID
```

- This command adds the current user to docker group so that docker command line tool can write to 
  unix socket where docker daemon is listening.
  You should logout and login the host again, after called the command.
```
$ sudo usermod -aG docker $USER
```

- Start the Dgraph servers, flock and Ratel with Docker-compose. Visit localhost:8000 on your 
  browser to view the UI.
```sh
$ docker-compose up
```

---

## Welcome to Dgraph.
The docker compose setup runs Dgraph servers and fetches the data from twitter stream and stores 
it in Dgraph. 


Graph databases store data by retaining its connected representation. This lets you discover 
connections through relationships which are not possible with SQL or NO-SQL databases. Imagining and 
modeling the real world data in its natural connected form is more intuitive than trying hard to 
squeeze it in a tabular format as rows and tables. The denormalized modeling of NO-SQL databases 
are even less effective than SQL when it comes to managing connected data. 
These inherent capabilities of a GraphDB combined with ease of 
use, performance and scalability of Dgraph let's you model easily, develop applications faster and 
discover relationships which could empower one to have feature sets which were previously not possible 
in real time.

---

## Introduction to Ratel
Ratel is the user interface to run queries and mutations on Dgraph.
![Ratel](./assets/ratel.png)

---

## Twitter data 
Flock pulls the tweets from the twitter stream using their developer API's and 
organizes the data based on the schema. We'll see about creating schema in the next section. 

Let's pull few tweets using the twitter API and see the available fields/properties, later we could
evaluate these fields and create a Dgraph schema. 

Here is the NodeJS code to pull tweets using the twitter API's

```node

TODO: Code should go here

```

TODO: @Prashant - Please complete the code to pull few tweets from the twitter API's and print them. 
---

## Data Modeling 
If you observe the information in the tweets there are two different types of objects. One is the
`User` and the other is a `Tweet`. 

In Dgraph, an object would be represented as a `Node`. Hence every `User` and each `Tweet` would be stored as 
a node in Dgraph. The relationship between the nodes will be represented as an edge between them.  

Here are the relationships between the nodes which we could represent as nodes, 

- A Tweet belongs to a user. This is a relationship between a `Tweet` and an `User`. We could use 
  an `Author` edge from a `Tweet` node to an `User` node to represent the relationships between these two.
- A `User` can be mentioned in a tweet. We could use a `Mention` edge from a `Tweet` node to an `User`
  node to represent this relationship.

These connections/relationships are between the Nodes are modelled as edges. 

Now we understand the fields available in these tweets. We also have an idea about the type of Nodes
and edges we need to create, 

Let's organize the available fields in the tweets obtained from the twitter API's and assign them as 
fields/properties of either the `User` node and the `Tweet` node. 
As discussed in the getting started guide, the first step would be to get the white paper model of
the application graph.  

TODO: Need schema diagram of the Graph model.

From the white paper model here are the properties of the `User` node with thier types.  
```sh
uid string
user_id string
user_name string
screen_name string
description string
friends_count int
verified bool
profile_banner_url string 
profile_image_url string
```

Every property of the node will be identified as a predicate in Dgraph. You could visualize
a node to have a unique identifier (uid) assigned by Dgraph, with all its properties (user_name,
screen_name, description, friends_count,....) connected as the predicates/edge to it. Using the uid
of the node one could query or mutate the predicates associated with it. 

To start with if you have an idea about the different queries you would be running in the application,
 it could be used as a reference to create indexes in the schema. 

For example, if you would like to have feature/query like "Search for user based on their twitter handle", 
then an index has to be created for `user_name` field for improving the performance of that query. 

But if you are not planning to find users based on their friends count, there is no need to create
an index on the `friends_count` field. 

In practice, unless there's a need to have an index on a predicate/property of a node, there is no
need to have that field as part of the schema. 

Note: Refer to the getting started guide to understanding the basics of creating schema in Dgraph.

There is no need to anticipate all the queries and derive the required indices upfront, 
Dgraph's flexible schema allows modification of the schema and indices at any later point in time. 
This change in schema can be done either from Ratel's interface (As shown in the getting started guide)
or using any client libraries.


Here are predicates/properties in the Tweet node with their types. This tweet data is obtained using 
the Twitter developer API's. 

```sh
  # Node ID assigned by Dgraph
  uid string        
  # ID of the tweet returned by twitter. 
  id_str string       
  # Creation time of tweet as returned by twitter API. 
  CreatedAt string       
  # Message of tweet.
  Message   string        
  URLs      []string      
  HashTags  []string  
  Retweet   bool
```

In the white paper model we see two different edges between the nodes. 

- `Author` edge from a `Tweet` to an `User` node. A User can have multiple tweets, but a tweet 
  belongs only to one User. This means there could be only one `Author` edge emerging out a `Tweet`, 
  but a `User` node could possibly have multiple `Tweets` pointing into it.  

- `Mention` edge from a `Tweet` to a `User`. Whenever an `User` is mentioned in a `Tweet`, `Mention`
  edge points from the `Tweet` to its `User`.  

---

Now we know the fields/properties of the nodes and the edges too. Let's look at how to create an 
`User` node and a `Author` node and create the edges between them.

TODO: @Prashant, please add a code which pulls a tweet and creates the graph.

---  

# Upsert directive
TODO: @Karthic content, @prashath - Example schema with `email string @upsert` with nodeJS code. 

 
---


