package replicator

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/tls"
	"database/sql"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/armon/go-metrics"
	"github.com/aws/aws-sdk-go/service/kms"
	"golang.org/x/tools/blog/atom"
)

const (
	replicatedCount = "replicated-events"
	errorCount      = "replication-errors"
)

const (
	sqlInsertEvent   = `insert into t_aeev_events (aggregate_id, version, typecode, event_time, payload) values(:1,:2,:3,:4,:5)`
	sqlInsertPublish = `insert into t_aepb_publish (aggregate_id, version) values(:1,:2)`
	sqlLockTable     = `lock table t_aerl_replicator_lock in exclusive mode nowait`
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
	IsEventPresentInFeed(string, int) (bool, error)
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

func formLastReplicatedEventQuery() string {
	return `select aggregate_id, version from t_aeev_events where id = (select max(id) from t_aeev_events)`
}

// If we encounter a non-existent entity as the latest in our feed, we need to remove it
// from the database or we might not be able to make progress in reading later events if we
// keep searching the feed for the entity.
func deleteNonexistentEntity(tx *sql.Tx, aggregate_id string, version int) error {
	log.Warnf("Deleting non-existent aggregate %s %d from database", aggregate_id, version)
	_, err := tx.Exec(`delete from t_aepb_publish where aggregate_id = :1 and version = :2`, aggregate_id, version)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`delete from t_aeev_events where aggregate_id = :1 and version = :2`, aggregate_id, version)
	return err
}

func lastReplicatedEvent(feedReader FeedReader, tx *sql.Tx) (string, int, error) {

	found := false

	for {
		//What's the last event seen?
		log.Info("Select last event observed in replicated event feed")
		var aggregateID string
		var version int

		start := time.Now()
		sql := formLastReplicatedEventQuery()
		err := tx.QueryRow(sql).Scan(&aggregateID, &version)
		logTimingStats("sqlLastObservedEvent", start, err)

		if err != nil {
			return "", -1, err
		}

		//Does it exist?
		log.Infof("Check existance of latest aggregate/version is %s - %d in source feed", aggregateID, version)
		found, err = feedReader.IsEventPresentInFeed(aggregateID, version)
		if err != nil {
			return "", -1, err
		}

		if found == true {
			log.Info("Found latest agrgregate/version in source")
			return aggregateID, version, nil
		}

		//If the aggregate does not exist, delete it.
		err = deleteNonexistentEntity(tx, aggregateID, version)
		if err != nil {
			return "", -1, err
		}
	}

}

//ProcessFeed processes the atom feed based on the current state of
//the replicated events
func (r *OraEventStoreReplicator) ProcessFeed() (bool, error) {

	//Do the work in a transaction
	log.Info("ProcessFeed start transaction")
	tx, err := r.db.Begin()
	if err != nil {
		incrementCounts(errorCount, 1)
		log.Warnf("Error starting transaction: %s", err.Error())
		return false, err
	}

	//Get lock within the context of a transaction
	log.Info("Get table lock")
	locked, err := r.locker.GetLock(tx)
	if err != nil {
		incrementCounts(errorCount, 1)
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
	aggregateID, version, err := lastReplicatedEvent(r.feedReader, tx)

	if err != nil && err != sql.ErrNoRows {
		incrementCounts(errorCount, 1)
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
		incrementCounts(errorCount, 1)
		log.Warnf("Unable to retrieve feed data: %s", findFeedErr.Error())
		tx.Rollback()
		return true, findFeedErr
	}

	//Add all the events in this feed that have not been added before
	if feed != nil {
		log.Info("Feed with events to process has been found")
		err = r.addFeedEvents(aggregateID, version, feed, tx)
		if err != nil {
			incrementCounts(errorCount, 1)
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
		incrementCounts(errorCount, 1)
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

func isUniqueConstraintViolation(errText string) bool {
	return strings.Contains(errText, "ORA-00001")
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
		if err != nil {
			log.Errorf("Unable to decode payload for entry %-v", entry, "Skip processing of event")
			continue
		}

		ts, err := time.Parse(time.RFC3339Nano, string(entry.Published))
		if err != nil {
			log.Errorf("Unable to parse timestamp for entry %-v", entry, "Skip processing of event")
			continue
		}

		ver, err := strconv.Atoi(idParts[3])
		if err != nil {
			log.Errorf("Unable to convert version integer for entry %-v", entry, "Skip processing of event")
			continue
		}

		if idParts[2] == aggregateID && ver == version {
			//Already have this aggregate
			log.Debugf("Event with aggregrateId %s and version %s not being replicated because it's already replicated.", aggregateID, ver)
			continue
		}

		log.Infof("insert event for %v", entry.ID)

		start := time.Now()
		_, err = tx.Exec(sqlInsertEvent,
			idParts[2], idParts[3], entry.Content.Type, ts, payload)
		logTimingStats("sqlInsertEvent", start, err)

		if err != nil {
			log.Warnf("Replication insert failed: %s", err.Error())
			if isUniqueConstraintViolation(err.Error()) {
				log.Warnf("Unique constraint violation - Skip processing of event")
				continue
			}

			return err
		}

		start = time.Now()
		_, err = tx.Exec(sqlInsertPublish,
			idParts[2], idParts[3])
		logTimingStats("sqlInsertPublish", start, err)

		if err != nil {
			log.Warnf("Replication publish insert failed: %s", err.Error())
			if isUniqueConstraintViolation(err.Error()) {
				log.Warnf("Unique constraint violation - Skip duplicate publishing of event")
				continue
			}

			return err
		}
	}

	incrementCounts(replicatedCount, len(feed.Entry))

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
		log.Info("Nothing in the feed")
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
	keyAlias string
	kmsSvc   *kms.KMS
}

//NewHttpFeedReader is a factory for instantiating HttpFeedReaders
func NewHttpFeedReader(endpoint, feedProto, keyAlias string, kmsSvc *kms.KMS) *HttpFeedReader {

	client := http.DefaultClient
	if feedProto == "https" {
		tr := http.DefaultTransport
		defTransAsTransPort := tr.(*http.Transport)
		defTransAsTransPort.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		client = &http.Client{Transport: tr}
	}

	return &HttpFeedReader{
		endpoint: endpoint,
		client:   client,
		proto:    feedProto,
		keyAlias: keyAlias,
		kmsSvc:   kmsSvc,
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

func (hr *HttpFeedReader) IsEventPresentInFeed(aggregateID string, version int) (bool, error) {
	url := fmt.Sprintf("%s://%s/events", hr.proto, hr.endpoint)
	return hr.isPresentInSource(url, aggregateID, version)
}

//IsFeedEncrypted indicates if we use a key alias for decrypting the feed
func (hr *HttpFeedReader) IsFeedEncrypted() bool {
	return hr.keyAlias != ""
}

//Decrypt from cryptopasta commit bc3a108a5776376aa811eea34b93383837994340
//used via the CC0 license. See https://github.com/gtank/cryptopasta
func (hr *HttpFeedReader) decrypt(ciphertext []byte, key *[32]byte) (plaintext []byte, err error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	if len(ciphertext) < gcm.NonceSize() {
		return nil, errors.New("malformed ciphertext")
	}

	return gcm.Open(nil,
		ciphertext[:gcm.NonceSize()],
		ciphertext[gcm.NonceSize():],
		nil,
	)
}

//DecryptFeed uses the AWS KMS to decrypt the feed text.
func (hr *HttpFeedReader) DecryptFeed(feedBytes []byte) ([]byte, error) {
	//Message is encrypted encryption key + :: + encrypted message
	parts := strings.Split(string(feedBytes), "::")
	if len(parts) != 2 {
		err := errors.New(fmt.Sprintf("Expected two parts, got %d", len(parts)))
		return nil, err
	}

	//Decode the key and the text
	keyBytes, err := base64.StdEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, err
	}

	//Get the encrypted bytes
	msgBytes, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, err
	}

	//Decrypt the encryption key
	di := &kms.DecryptInput{
		CiphertextBlob: keyBytes,
	}

	decryptedKey, err := hr.kmsSvc.Decrypt(di)
	if err != nil {
		return nil, err
	}

	//Use the decrypted key to decrypt the message text
	decryptKey := [32]byte{}

	copy(decryptKey[:], decryptedKey.Plaintext[0:32])

	return hr.decrypt(msgBytes, &decryptKey)
}

func (hr *HttpFeedReader) isPresentInSource(url, aggregateID string, version int) (bool, error) {
	var start time.Time

	resource := fmt.Sprintf("%s/%s/%d", url, aggregateID, version)
	log.Info("check aggregate via ", resource)
	req, err := http.NewRequest("GET", resource, nil)
	if err != nil {
		return false, err
	}

	start = time.Now()
	resp, err := hr.client.Do(req)
	logTimingStats("isPresentInSource client.Do", start, err)

	if err != nil {
		return false, err
	}

	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, errors.New(fmt.Sprintf("Unexpected status code verifying event in source: %d", resp.StatusCode))
	}

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
	logTimingStats("getResource client.Do", start, err)

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
	logTimingStats("getResource response ReadAll", start, err)

	if err != nil {
		return nil, err
	}

	//Are we using a key to decrypt the feed?
	if hr.IsFeedEncrypted() {
		responseBytes, err = hr.DecryptFeed(responseBytes)
		if err != nil {
			return nil, err
		}
	}

	var feed atom.Feed

	start = time.Now()
	err = xml.Unmarshal(responseBytes, &feed)
	logTimingStats("getResource xml.Unmarshall", start, err)

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
