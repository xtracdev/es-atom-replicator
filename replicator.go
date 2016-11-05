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
	"os"
	"github.com/armon/go-metrics"
)

const (
	replicatedCount = "replicated-events"
	errorCount = "replication-errors"
)

const (
	sqlLastObservedEvent = `select aggregate_id, version from events where id = (select max(id) from events)`
	sqlInsertEvent = `insert into events (aggregate_id, version, typecode, event_time, payload) values(:1,:2,:3,:4,:5)`
	sqlInsertPublish = `insert into publish (aggregate_id, version) values(:1,:2)`
	sqlLockTable = `lock table replicator_lock in exclusive mode nowait`
)

//Locker defines the locking interface needed for processing feed events.
//The goal is to be able to deploy multiple instances of the feed processor,
//with only a single processor processing a set of events at a time.
type Locker interface {
	GetLock(lockerArgs ...interface{}) (bool, error)
	ReleaseLock() error
}

//FeedReader defines the interface used to read the feed.
type FeedReader interface {
	GetRecent() (*atom.Feed, error)
	GetFeed(feedid string) (*atom.Feed, error)
}

//Replicator defines the interface replicators implement
type Replicator interface {
	ProcessFeed() (bool, error)
}

//ReplicatorFactory defines the interface for instantiating replicators.
type ReplicatorFactory interface {
	New(locker Locker, feedReader FeedReader, extras ...interface{}) (Replicator, error)
}

//OraEventStoreReplicator defines a replicator instance that replicates a feed
//to an Oracle event store
type OraEventStoreReplicator struct {
	db         *sql.DB
	locker     Locker
	feedReader FeedReader
}

//ProcessFeed processes the atom feed based on the current state of
//the replicated events
func (r *OraEventStoreReplicator) ProcessFeed() (bool, error) {

	//Do the work in a transaction
	log.Info("ProcessFeed start transaction")
	tx, err := r.db.Begin()
	if err != nil {
		incrementCounts(errorCount,1)
		log.Warnf("Error starting transaction: %s", err.Error())
		return false, err
	}

	//Get lock within the context of a transaction
	log.Info("Get table lock")
	locked, err := r.locker.GetLock(tx)
	if err != nil {
		incrementCounts(errorCount,1)
		log.Warnf("Error obtaining lock: %s", err.Error())
		tx.Rollback()
		return false, err
	}

	if !locked {
		log.Info("ProcessFeed did not get lock... returning.")
		tx.Rollback()
		return false, nil
	}

	//What's the last event seen?
	log.Info("Select last event observed in replicated event feed")
	var aggregateID string
	var version int

	start := time.Now()
	err = tx.QueryRow(sqlLastObservedEvent).Scan(&aggregateID, &version)
	logTimingStats("sqlLastObservedEvent", start, err)

	if err != nil && err != sql.ErrNoRows {
		incrementCounts(errorCount,1)
		log.Warnf("Error querying for last event: %s", err.Error())
		tx.Rollback()
		return true, err
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
		incrementCounts(errorCount,1)
		log.Warnf("Unable to retrieve feed data: %s", findFeedErr.Error())
		tx.Rollback()
		return true, findFeedErr
	}

	//Add all the events in this feed that have not been added before
	if feed != nil {
		log.Info("Feed with events to process has been found")
		err = r.addFeedEvents(aggregateID, version, feed, tx)
		if err != nil {
			incrementCounts(errorCount,1)
			log.Warnf("Unable to add feed events: %s", err.Error())
			tx.Rollback()
			return true, err
		}
	} else {
		log.Info("No events found in feed")
	}

	//Unlock
	err = tx.Commit()
	if err != nil {
		incrementCounts(errorCount,1)
		log.Warnf("Error commiting work: %s", err.Error())
	}

	return true, nil
}

func findAggregateIndex(id string, entries []*atom.Entry) int {
	for idx, entry := range entries {
		if entry.ID == id {
			return idx
		}
	}

	return -1
}

//ByTimestamp defines a type for sorting atom entries by their timestamp
type ByTimestamp []*atom.Entry

func (t ByTimestamp) Len() int {
	return len(t)
}

func (t ByTimestamp) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

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

//addFeedEvents adds events to the replicated feed store based on the offset into the event feed of the
//last observed event. The scope of the events added is a single page, multiple calls of process events
//are needed to get a feed fully synced up
func (r *OraEventStoreReplicator) addFeedEvents(aggregateID string, version int, feed *atom.Feed, tx *sql.Tx) error {
	var idx int

	//Sort by timestamp
	sort.Sort(ByTimestamp(feed.Entry))

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

		start := time.Now()
		_, err = tx.Exec(sqlInsertEvent,
			idParts[2], idParts[3], entry.Content.Type, ts, payload)
		logTimingStats("sqlInsertEvent",start,err)

		if err != nil {
			log.Warnf("Replication insert failed: %s", err.Error())
			return err
		}

		start = time.Now()
		_, err = tx.Exec(sqlInsertPublish,
			idParts[2], idParts[3])
		logTimingStats("sqlInsertPublish",start,err)

		if err != nil {
			log.Warnf("Replication publish insert failed: %s", err.Error())
			return err
		}
	}

	incrementCounts(replicatedCount,len(feed.Entry))

	return nil
}

//findFeedByEvent finds the feed containing the given event, or, if the event is the last event in a feed, the
//next feed.
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

//Get link extracts the given link relationship from the given feed's
//link collection
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

//Grab the feed id as the component of a uri
func feedIdFromResource(feedURL string) string {
	url, _ := url.Parse(feedURL)
	parts := strings.Split(url.RequestURI(), "/")
	return parts[len(parts)-1]
}

//Get first feed navigates a feed set from the recent feed all the way back
//to the first acchived feed
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

//OraEventStoreReplicatorFactory defines a type implementing the ReplicatorFactory interface.
type OraEventStoreReplicatorFactory struct{}

//New instantiates an OraEventStoreReplicator
func (oesFact *OraEventStoreReplicatorFactory) New(locker Locker, feedReader FeedReader, db *sql.DB) (Replicator, error) {
	return &OraEventStoreReplicator{
		db:         db,
		locker:     locker,
		feedReader: feedReader,
	}, nil
}

//TableLocker defines a type for implmenting the Locker interface based on table locking
type TableLocker struct{}

var errGetLockArgument = errors.New("Table locker GetLock expects a single argument of type *sql.Tx")

//GetLock obtains a table lock. If the lock cannot be obtained it returns immediately (it
//does not block)
func (tl *TableLocker) GetLock(args ...interface{}) (bool, error) {
	if len(args) != 1 {
		return false, errGetLockArgument
	}

	tx, ok := args[0].(*sql.Tx)
	if !ok {
		return false, errGetLockArgument
	}

	log.Info("locking table replicator_lock")

	start := time.Now()
	_, err := tx.Exec(sqlLockTable)
	logTimingStats("sqlLockTable", start, err)

	if err == nil {
		log.Info("Acquired lock")
		logTimingStats("sqlLockTable", start, nil)
		return true, nil
	}

	if strings.Contains(err.Error(), "ORA-00054") {
		log.Info("Did not acquire lock")
		logTimingStats("sqlLockTable", start, nil)
		return false, nil
	} else {
		log.Warnf("Error locking table: %s", err.Error())
		logTimingStats("sqlLockTable", start, err)
		return false, err
	}
}

//ReleaseLock - the enclosing transaction for the table locker
//releases the table lock on commit or rollback
func (tl *TableLocker) ReleaseLock() error {
	return nil //Release done in transaction commit/rollback
}

//HttpFeedReader defines a type for an Http Feed reader
type HttpFeedReader struct {
	endpoint string
	client   *http.Client
	proto    string
}

//NewHttpFeedReader is a factory for instantiating HttpFeedReaders
func NewHttpFeedReader(endpoint string, tlsConfig *tls.Config) *HttpFeedReader {
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

	return &HttpFeedReader{
		endpoint: endpoint,
		client:   client,
		proto:    proto,
	}
}

//GetRecent returns the recent notifications
func (hr *HttpFeedReader) GetRecent() (*atom.Feed, error) {
	url := fmt.Sprintf("%s://%s/notifications/recent", hr.proto, hr.endpoint)
	return hr.getResource(url)
}

//GetFeed returns the specific feed
func (hr *HttpFeedReader) GetFeed(feedid string) (*atom.Feed, error) {
	url := fmt.Sprintf("%s://%s/notifications/%s", hr.proto, hr.endpoint, feedid)
	return hr.getResource(url)
}

//getResource does a git on the specified feed resource
func (hr *HttpFeedReader) getResource(url string) (*atom.Feed, error) {
	var start time.Time

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	start = time.Now()
	resp, err := hr.client.Do(req)
	logTimingStats("getResource client.Do",start,err)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		responseBytes, readErr := ioutil.ReadAll(resp.Body)
		if readErr == nil {
			log.Warnf("Error reading feed: %d %s", resp.StatusCode, string([]byte(responseBytes)))
		}
		return nil, errors.New(fmt.Sprintf("Error retrieving resource: %d", resp.StatusCode))
	}

	start = time.Now()
	responseBytes, err := ioutil.ReadAll(resp.Body)
	logTimingStats("getResource response ReadAll",start,err)

	if err != nil {
		return nil, err
	}

	var feed atom.Feed

	start = time.Now()
	err = xml.Unmarshal(responseBytes, &feed)
	logTimingStats("getResource xml.Unmarshall",start,err)

	if err != nil {
		return nil, err
	}

	return &feed, nil
}

//Configure where telemery data goes. Currently this can be send via UDP to a listener, or can be buffered
//internally and dumped via a signal.
func ConfigureStatsD() {
	statsdEndpoint := os.Getenv("STATSD_ENDPOINT")
	log.Infof("STATSD_ENDPOINT: %s", statsdEndpoint)
	
	if statsdEndpoint != "" {

		log.Info("Using vanilla statsd client to send telemetry to ", statsdEndpoint)
		sink, err := metrics.NewStatsdSink(statsdEndpoint)
		if err != nil {
			log.Warn("Unable to configure statds sink", err.Error())
			return
		}
		metrics.NewGlobal(metrics.DefaultConfig(statsdEndpoint), sink)
	} else {
		log.Info("Using in memory metrics accumulator - dump via USR1 signal")
		inm := metrics.NewInmemSink(10*time.Second, 5*time.Minute)
		metrics.DefaultInmemSignal(inm)
		metrics.NewGlobal(metrics.DefaultConfig("xavi"), inm)
	}
}

//Update counters and stats for timings, discriminating errors from non-errors
func logTimingStats(svc string, start time.Time, err error) {
	duration := time.Now().Sub(start)
	go func(svc string, duration time.Duration, err error) {
		ms := float32(duration.Nanoseconds()) / 1000.0 / 1000.0
		if err != nil {
			key := []string{"es-atom-replicator", fmt.Sprintf("%s-error", svc)}
			metrics.AddSample(key, float32(ms))
			metrics.IncrCounter(key, 1)
		} else {
			key := []string{"es-atom-replicator", svc}
			metrics.AddSample(key, float32(ms))
			metrics.IncrCounter(key, 1)
		}
	}(svc, duration, err)
}

//Counters
func incrementCounts(counter string, increment int) {
	go func(counter string, increment int) {
		key := []string{"es-atom-replicator", counter}
		metrics.IncrCounter(key, float32(increment))
	}(counter, increment)
}