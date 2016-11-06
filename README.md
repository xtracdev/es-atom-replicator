# Event Store Replicator

This project implements a replication process for the oraeventstore. It 
works by reading a source store's atom feed, and writing the events
replicated from a remote instance into the local instance.

To simplify processing, each replication action, be it the 'catch up' 
phase or the reading of recent events, will be performed after 
acquiring a mutex via a table lock.

The table use for the lock is 

<pre>
create table replicator_lock (
    ts TIMESTAMP
);
</pre>

To Build:

<pre>
docker run --rm -v $PWD:/go/src/github.com/xtracdev/es-atom-replicator -e DB_USER=<db user> -e DB_PASSWORD=<db password> -e DB_HOST=<db host> -e DB_PORT=<db port> -e DB_SVC=<db service> -w /go/src/github.com/xtracdev/es-atom-replicator xtracdev/goora bash -c "make -f Makefile"
</pre>

