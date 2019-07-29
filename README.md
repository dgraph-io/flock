# Flock 

Flock is twitter Graph model built on Dgraph. 

Flock has two parts, 
- [*Mutation program*](./main.go) - Makes use of the tweets available from twitter developer API's and 
builds the Graph model on top of Dgraph.

Here is the Graph schema of Flock, 

![Schema](./schema.JPG)

- [*Query client*](./client/main.go) - Runs interesting graph queries on the tweets data stored in Dgraph. 
  
# Running Flock

## Obtaining twitter credentials

We need to create a twitter developer account and an app to be able to fetch stream of tweets using 
their APIs. Let's start with how to create a twitter developer account.

- Apply for a twitter developer account [here](https://developer.twitter.com/en/apply/user) and 
  follow the instructions. The series of steps would end with your email verification.
- Create a twitter app from [this link](https://developer.twitter.com/en/apps/create). 
  All fields are `not` required.  
- You'll be redirected to the App details tab after creating the app. Go to the `Keys and tokens` tab
   and create new access and secret tokens.
![Twitter Developer account](./twitter-keys.png)
- Create a copy of the credentials template.
  ```sh
  cp credentials-template.json credentials.json
  ```
- Open the `crendentials.json` file and replace the placeholders with the keys from the 
  twitter app's `Keys and token` tab.

---
## Setup

- Clone the repository.
```sh
$ git clone https://github.com/dgraph-io/flock.git
$ cd flock
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
  `newgrp` creates a new terminal session. It is necessary after the user addition.

```
$ sudo usermod -aG docker $USER
$ newgrp docker
```

- Ensure that `credentials.json` with the twitter credentials exist in the root directory of flock. 

- Start the Dgraph servers, flock and Ratel with Docker-compose. Visit http://localhost:8000 on your 
  browser to view the UI.
```sh
$ docker-compose up
```

---
