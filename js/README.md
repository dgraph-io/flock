## Flock in JavaScript

### Running Tweet Loader

- Ensure that `credentials.json` with the Twitter credentials exist in the root directory of Flock.

- On another terminal, start Flock:

```sh
$ cd js
$ docker-compose up
```

Flock tweet loader will begin printing out periodic log messages mentioning its
loading rate. You're good to go if you see the `commit_rate` higher
than 0/sec, which means data has been successfully committed to
Dgraph.

A few minutes of running the flock loader is sufficient to get enough data for
some interesting queries. To stop running Flock, press Ctrl+C on the
terminal running flock loader.

```sh
$ docker-compose up
...
<Ctrl+C>
Killing flock ... done
```

---
