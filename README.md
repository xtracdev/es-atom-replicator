# Event Store Replicator

[![CircleCI](https://circleci.com/gh/xtracdev/es-atom-replicator.svg?style=svg)](https://circleci.com/gh/xtracdev/es-atom-replicator)

This project implements a replication process for the oraeventstore. It 
works by reading a source store's atom feed, and writing the events
replicated from a remote instance into the local instance.

To simplify processing, each replication action, be it the 'catch up' 
phase or the reading of recent events, will be performed after 
acquiring a mutex via a table lock.

The table use for the lock is 

<pre>
create table t_aerl_replicator_lock (
    ts TIMESTAMP
);
</pre>

To Build:

<pre>
docker run --rm -v $PWD:/go/src/github.com/xtracdev/es-atom-replicator -e DB_USER=<db user> -e DB_PASSWORD=<db password> -e DB_HOST=<db host> -e DB_PORT=<db port> -e DB_SVC=<db service> -w /go/src/github.com/xtracdev/es-atom-replicator xtracdev/goora bash -c "make -f Makefile"
</pre>


For integration testing, you need to install the managed API schema
which includes the above replicator lock (tables to be added to this
project at some point).

### Contributing

To contribute, you must certify you agree with the [Developer Certificate of Origin](http://developercertificate.org/)
by signing your commits via `git -s`. To create a signature, configure your user name and email address in git.
Sign with your real name, do not use pseudonyms or submit anonymous commits.


In terms of workflow:

0. For significant changes or improvement, create an issue before commencing work.
1. Fork the respository, and create a branch for your edits.
2. Add tests that cover your changes, unit tests for smaller changes, acceptance test
for more significant functionality.
3. Run gofmt on each file you change before committing your changes.
4. Run golint on each file you change before committing your changes.
5. Make sure all the tests pass before committing your changes.
6. Commit your changes and issue a pull request.

### License

(c) 2016 Fidelity Investments
Licensed under the Apache License, Version 2.0