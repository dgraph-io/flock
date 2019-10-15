## Flock in Go

### Running Tweet Loader

- Ensure that `credentials.json` with the Twitter credentials exist in the root directory of Flock.

- On another terminal, start Flock:

```sh
$ cd go
$ docker-compose up
```

Flock tweet loader will begin printing out periodic log messages mentioning its
loading rate. You're good to go if you see the `commit_rate` higher
than 0/sec, which means data has been successfully committed to
Dgraph.

A few minutes of running the flock loader is sufficient to get enough data for
some interesting queries. To stop running Flock, press Ctrl+C on the
terminal running Flock.

```sh
$ docker-compose up
...
<Ctrl+C>
Killing flock ... done
```

---

### Running Query Client

- On another terminal, start query client:

```sh
$ cd client
$ go run main.go -a localhost:9080
```

Flock query client will begin printing out periodic log messages mentioning its
querying rate. You're good to go if you see the `query_rate` higher
than 0/sec, which means queries to Dgraph have been successful.

A few minutes of running the flock client is sufficient to get
enough data on how queries performed. To stop running flock client,
press Ctrl+C on the terminal running flock client.

```sh
$ docker-compose up
...
<Ctrl+C>
```

---
