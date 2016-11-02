package replicator

import (
	"crypto/tls"
	"database/sql"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"golang.org/x/tools/blog/atom"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
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
	var version int
	err = tx.QueryRow("select aggregate_id, version from events where id = (select max(id) from events)").Scan(&aggregateID, &version)
	if err != nil && err != sql.ErrNoRows {
		log.Warnf("Error querying for last event: %s", err.Error())
		tx.Rollback()
		return err
	}

	//Find the feed with the event
	var feed *atom.Feed
	var findFeedErr error
	if aggregateID != "" {
		feed, findFeedErr = r.findFeedByEvent(aggregateID, version)
	} else {
		feed, findFeedErr = r.getFirstFeed()
	}

	if findFeedErr != nil {
		log.Warnf("Unable to retrieve feed data: %s", findFeedErr.Error())
		tx.Rollback()
		return findFeedErr
	}

	//Add all the events in this feed that have not been added before
	if feed != nil {
		log.Info("Feed with events to process has been found")
		err = r.addFeedEvents(aggregateID, version, feed, tx)
		if err != nil {
			log.Warnf("Unable to add feed events: %s", err.Error())
			tx.Rollback()
			return err
		}
	} else {
		log.Info("No events found in feed")
	}

	//Unlock
	err = tx.Commit()
	if err != nil {
		log.Warnf("Error commiting work: %s", err.Error())
	}

	return nil
}

func findAggregateIndex(id string, entries []*atom.Entry) int {
	log.Infof("Looking for %s", id)
	for idx, entry := range entries {
		log.Infof("got %s", entry.ID)
		if entry.ID == id {
			return idx
		}
	}

	return -1
}

type ByTimestamp []*atom.Entry

func (t ByTimestamp) Len() int      { return len(t) }
func (t ByTimestamp) Swap(i, j int) { t[i], t[j] = t[j], t[i] }
func (t ByTimestamp) Less(i, j int) bool {
	tsI, err := time.Parse(time.RFC3339Nano, string(t[i].Published))
	if err != nil {
		return false
	}

	tsJ, err := time.Parse(time.RFC3339Nano, string(t[j].Published))
	if err != nil {
		return false
	}

	return tsJ.After(tsI)
}

func (r *OraEventStoreReplicator) addFeedEvents(aggregateID string, version int, feed *atom.Feed, tx *sql.Tx) error {
	var idx int

	//Sort by timestamp
	for _, e := range feed.Entry {
		log.Info(e.ID)
	}
	sort.Sort(ByTimestamp(feed.Entry))
	for _, e := range feed.Entry {
		log.Info(e.ID)
	}

	//Find offset into the events
	if aggregateID == "" {
		idx = 0
	} else {
		id := fmt.Sprintf("urn:esid:%s:%d", aggregateID, version)
		idx = findAggregateIndex(id, feed.Entry)

		if idx == -1 {
			//If not found, it means the previous feed contained the event
			//as the last entry. Therefore we add all the events in this feed.
			idx = 0
		}
	}

	//Add the events from the feed
	log.Infof("starting idx is %d", idx)
	for i := idx; i < len(feed.Entry); i++ {
		entry := feed.Entry[i]
		idParts := strings.SplitN(entry.ID, ":", 4)
		payload, err := base64.StdEncoding.DecodeString(entry.Content.Body)
		ts, err := time.Parse(time.RFC3339Nano, string(entry.Published))
		if err != nil {
			log.Errorf("Unable to parse timestamp for entry %-v", entry, "Skip processing of event")
			continue
		}
		if err != nil {
			log.Errorf("Unable to decode payload for entry %-v", entry, "Skip processing of event")
			continue
		}

		if idParts[2] == aggregateID {
			//Already have this aggregate
			continue
		}

		log.Infof("insert event for %v", entry.ID)
		_, err = tx.Exec("insert into events (aggregate_id, version, typecode, timestamp, body) values(:1,:2,:3,:4,:5)",
			idParts[2], idParts[3], entry.Content.Type, ts, payload)
		if err != nil {
			log.Warnf("Replication insert failed: %s", err.Error())
			return err
		}

		_, err = tx.Exec("insert into publish (aggregate_id, version) values(:1,:2)",
			idParts[2], idParts[3])
		if err != nil {
			log.Warnf("Replication publish insert failed: %s", err.Error())
			return err
		}
	}

	return nil
}

func (r *OraEventStoreReplicator) findFeedByEvent(aggregateID string, version int) (*atom.Feed, error) {
	log.Infof("findFeedByEvent - %s %d", aggregateID, version)
	var feedReadError error
	var feed *atom.Feed
	var found bool

	//We need to find the feed containing a specific aggregate. If there are events 'later' in the
	//feed, we return that feed, otherwise we return the 'next' feed (unless we are the recent feed
	id := fmt.Sprintf("urn:esid:%s:%d", aggregateID, version)

	//Is the event in the recent feed?
	feed, err := r.feedReader.GetRecent()
	if err != nil {
		return nil, err
	}

	idx := findAggregateIndex(id, feed.Entry)
	found = (idx != -1)

	log.Infof("...event not found in recent feed, look in previous feeds")
	if !found {

		for {
			prev := getLink("prev-archive", feed)
			if prev == nil {
				log.Info("...feed history exhausted")
				break
			}

			//Extract feed id from prev
			feedID := feedIdFromResource(*prev)
			log.Infof("Prev archive feed id is %s", feedID)
			feed, feedReadError = r.feedReader.GetFeed(feedID)
			if feedReadError != nil {
				return nil, feedReadError
			}

			idx = findAggregateIndex(id, feed.Entry)
			if idx != -1 {
				found = true
				break
			}
		}
	}

	if found {
		log.Infof("Event found in feed %s", feed.ID)
		sort.Sort(ByTimestamp(feed.Entry))
		idx := findAggregateIndex(id, feed.Entry)
		log.Infof("Index in sorted feed: %d", idx)
		if idx == len(feed.Entry)-1 {
			log.Info("Return next feed as current aggregate is last entry in feed")
			next := getLink("next-archive", feed)
			if next == nil {
				return nil, nil //We're at the end of recent
			}

			feedID := feedIdFromResource(*next)
			feed, feedReadError = r.feedReader.GetFeed(feedID)
			if feedReadError != nil {
				return nil, feedReadError
			}

		}

	}

	log.Infof("returning feed %s", feed.ID)

	return feed, nil
}

func getLink(linkRelationship string, feed *atom.Feed) *string {
	if feed == nil {
		return nil
	}

	for _, l := range feed.Link {
		if l.Rel == linkRelationship {
			return &l.Href
		}
	}

	return nil
}

func feedIdFromResource(feedURL string) string {
	url, _ := url.Parse(feedURL)
	parts := strings.Split(url.RequestURI(), "/")
	return parts[len(parts)-1]
}

func (r *OraEventStoreReplicator) getFirstFeed() (*atom.Feed, error) {
	log.Info("Looking for first feed")
	//Start with recent
	var feed *atom.Feed
	var feedReadError error

	feed, feedReadError = r.feedReader.GetRecent()
	if feedReadError != nil {
		return nil, feedReadError
	}

	if feed == nil {
		//Nothing in the feed if there's no recent available...
		return nil, nil
	}

	log.Info("Got feed - navigate prev-archive link")
	for {
		prev := getLink("prev-archive", feed)
		if prev == nil {
			break
		}

		//Extract feed id from prev
		feedID := feedIdFromResource(*prev)
		log.Infof("Prev archive feed id is %s", feedID)
		feed, feedReadError = r.feedReader.GetFeed(feedID)
		if feedReadError != nil {
			return nil, feedReadError
		}
	}

	return feed, nil
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

type HttpReplicator struct {
	endpoint string
	client   *http.Client
	proto    string
}

func NewHttpReplicator(endpoint string, tlsConfig *tls.Config) *HttpReplicator {
	var client *http.Client
	var proto string

	if tlsConfig == nil {
		client = http.DefaultClient
		proto = "http"
	} else {
		transport := &http.Transport{
			TLSClientConfig: tlsConfig,
		}
		client = &http.Client{Transport: transport}
		proto = "https"
	}

	return &HttpReplicator{
		endpoint: endpoint,
		client:   client,
		proto:    proto,
	}
}

func (hr *HttpReplicator) GetRecent() (*atom.Feed, error) {
	url := fmt.Sprintf("%s://%s/notifications/recent", hr.proto, hr.endpoint)
	return hr.getResource(url)
}

func (hr *HttpReplicator) GetFeed(feedid string) (*atom.Feed, error) {
	url := fmt.Sprintf("%s://%s/notifications/%s", hr.proto, hr.endpoint, feedid)
	return hr.getResource(url)
}

func (hr *HttpReplicator) getResource(url string) (*atom.Feed, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := hr.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	responseBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var feed atom.Feed
	err = xml.Unmarshal(responseBytes, &feed)
	if err != nil {
		return nil, err
	}

	return &feed, nil
}
