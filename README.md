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

