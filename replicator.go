package replicator

import (
	"database/sql"
	"errors"
	log "github.com/Sirupsen/logrus"
	"golang.org/x/tools/blog/atom"
	"strings"
)

type Locker interface {
	GetLock(lockerArgs ...interface{}) (bool, error)
	ReleaseLock() error
}

type FeedReader interface {
	GetRecent() (*atom.Feed, error)
	GetFeed(feedid string) (*atom.Feed, error)
}

type Replicator interface {
	ProcessFeed() error
}

type ReplicatorFactory interface {
	New(locker Locker, feedReader FeedReader, extras ...interface{}) (Replicator, error)
}

type OraEventStoreReplicator struct {
	db         *sql.DB
	locker     Locker
	feedReader FeedReader
}

func (r *OraEventStoreReplicator) ProcessFeed() error {


	//Do the work in a transaction
	log.Info("ProcessFeed start transaction")
	tx, err := r.db.Begin()
	if err != nil {
		log.Warnf("Error starting transaction: %s", err.Error())
		return err
	}

	//Get lock within the context of a transaction
	log.Info("Get table lock")
	locked, err := r.locker.GetLock(tx)
	if err != nil {
		log.Warnf("Error obtaining lock: %s", err.Error())
		tx.Rollback()
		return err
	}

	if !locked {
		log.Info("ProcessFeed did not get lock... returning.")
		tx.Rollback()
		return nil
	}

	//What's the last event seen?
	log.Info("Select last event observed in replicated event feed")
	var aggregateID string
	err = tx.QueryRow("select aggregate_id from events where id = (select max(id) from events)").Scan(&aggregateID)
	if err != nil && err != sql.ErrNoRows {
		log.Warnf("Error querying for last event: %s",err.Error())
		tx.Rollback()
		return err
	}

	//Find the feed with the event
	var feed *atom.Feed
	var findFeedErr error
	if aggregateID != "" {
		feed, findFeedErr = findFeedByEvent(aggregateID)
	} else {
		feed, findFeedErr = getFirstFeed()
	}

	if findFeedErr != nil {
		log.Warnf("Unable to retrieve feed data: %s", findFeedErr.Error())
		tx.Rollback()
		return findFeedErr
	}

	//Add all the events in this feed that have not been added before
	if feed != nil {
		log.Info("Feed with events to process has been found")
		err = addFeedEvents(feed); if err != nil {
			log.Warnf("Unable to add feed events: %s", err.Error())
			tx.Rollback()
			return err
		}
	}

	//Unlock
	err = tx.Commit()
	if err  != nil {
		log.Warnf("Error commiting work: %s", err.Error())
	}

	return nil
}

func addFeedEvents(feed *atom.Feed) error {
	return nil
}

func findFeedByEvent(aggregateID string)(*atom.Feed,error) {
	return nil,nil
}

func getFirstFeed()(*atom.Feed,error) {
	return nil,nil
}

type OraEventStoreReplicatorFactory struct{}

func (oesFact *OraEventStoreReplicatorFactory) New(locker Locker, feedReader FeedReader, db *sql.DB) (Replicator, error) {
	return &OraEventStoreReplicator{
		db:         db,
		locker:     locker,
		feedReader: feedReader,
	}, nil
}

type TableLocker struct{}

var errGetLockArgument = errors.New("Table locker GetLock expects a single argument of type *sql.Tx")

func (tl *TableLocker) GetLock(args ...interface{}) (bool, error) {
	if len(args) != 1 {
		return false, errGetLockArgument
	}

	tx, ok := args[0].(*sql.Tx)
	if !ok {
		return false, errGetLockArgument
	}

	log.Info("locking table replicator_lock")
	_, err := tx.Exec("lock table replicator_lock nowait")
	if err == nil {
		log.Info("Acquired lock")
		return true, nil
	}

	if strings.Contains(err.Error(), "ORA-00054") {
		log.Info("Did not acquire lock")
		return false, nil
	} else {
		log.Warnf("Error locking table: %s", err.Error())
		return false, err
	}
}

func (tl *TableLocker) ReleaseLock() error {
	return nil //Release done in transaction commit/rollback
}
