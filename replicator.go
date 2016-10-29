package replicator

import (
	"golang.org/x/tools/blog/atom"
	"database/sql"
)

type Locker interface {
	GetLock()(bool, error)
	ReleaseLock() error
}

type FeedReader interface {
	GetRecent() (*atom.Feed, error)
	GetFeed(feedid string)(*atom.Feed, error)
}

type Replicator interface {
	ProcessFeed() error
}

type ReplicatorFactory interface {
	New(locker Locker, feedReader FeedReader, extras... interface{}) (Replicator, error)
}


type OraEventStoreReplicator struct {
	db *sql.DB
	locker Locker
	feedReader FeedReader
}

func (r *OraEventStoreReplicator) ProcessFeed() error {
	return nil
}

type OraEventStoreReplicatorFactory struct {}

func (oesFact *OraEventStoreReplicatorFactory) New(locker Locker, feedReader FeedReader,db *sql.DB) (Replicator, error){
	return &OraEventStoreReplicator{
		db: db,
		locker: locker,
		feedReader: feedReader,
	},nil
}



