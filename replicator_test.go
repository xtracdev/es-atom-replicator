package replicator

import (
	"encoding/xml"
	"errors"
	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	feedmock "github.com/xtracdev/es-atom-replicator/testing"
	"golang.org/x/tools/blog/atom"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

var testFactory = &OraEventStoreReplicatorFactory{}

func testExpectLock(mock sqlmock.Sqlmock, lockError bool, gotLock bool) {
	if lockError {
		mock.ExpectExec("lock table t_aerl_replicator_lock in exclusive mode nowait").WillReturnError(errors.New("BAM!"))
		return
	}

	if gotLock {
		execOkResult := sqlmock.NewResult(1, 1)
		mock.ExpectExec("lock table t_aerl_replicator_lock in exclusive mode nowait").WillReturnResult(execOkResult)
	} else {
		mock.ExpectExec("lock table t_aerl_replicator_lock in exclusive mode nowait").WillReturnError(errors.New("ORA-00054: resource busy and acquire with NOWAIT specified or timeout expired"))
	}

}

//TODO - add insert argument expectations then make sure we get the events in the correct order
func testExpectInsertIntoEvents(mock sqlmock.Sqlmock, aggID, version string) {
	execOkResult := sqlmock.NewResult(1, 1)
	mock.ExpectExec("insert into t_aeev_events").WithArgs(aggID, version, sqlmock.AnyArg(),
		sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnResult(execOkResult)
	mock.ExpectExec("insert into t_aepb_publish").WillReturnResult(execOkResult)
}

func testExpectQueryReturnNoRows(mock sqlmock.Sqlmock) {
	rows := sqlmock.NewRows([]string{"aggregate_id", "version"})
	mock.ExpectQuery("select aggregate_id, version from t_aeev_events where id").WillReturnRows(rows)
}

func testExpectQueryReturnError(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("select aggregate_id, version from t_aeev_events where id").WillReturnError(errors.New("dang"))
}

func testExpectQueryReturnAggregateAndVersion(mock sqlmock.Sqlmock, aggID, version string) {
	rows := sqlmock.NewRows([]string{"aggregate_id", "version"}).AddRow(aggID, version)
	mock.ExpectQuery("select aggregate_id, version from t_aeev_events where id").WillReturnRows(rows)
}

type testFeedReader struct {
	Feeds map[string]*atom.Feed
}

func (tfr *testFeedReader) GetRecent() (*atom.Feed, error) {

	if tfr.Feeds == nil {
		log.Info("No feeds in test reader")
		return nil, nil
	} else {
		log.Infof("Test feeder returning feed %+v", tfr.Feeds["recent"])
		return tfr.Feeds["recent"], nil
	}
}

func (tfr *testFeedReader) GetFeed(feedid string) (*atom.Feed, error) {
	if tfr.Feeds == nil {
		return nil, nil
	} else {
		return tfr.Feeds[feedid], nil
	}
}

func initTestFeedReader() *testFeedReader {
	reader := testFeedReader{}
	reader.Feeds = make(map[string]*atom.Feed)

	var recentFeed atom.Feed
	xml.Unmarshal([]byte(feedmock.Recent), &recentFeed)
	reader.Feeds["recent"] = &recentFeed

	var secondArchiveFeed atom.Feed
	xml.Unmarshal([]byte(feedmock.SecondArchive), &secondArchiveFeed)
	reader.Feeds["9BC3EA7D-51E2-8C61-0E08-02368CD22054"] = &secondArchiveFeed

	var firstArchiveFeed atom.Feed
	xml.Unmarshal([]byte(feedmock.FirstArchive), &firstArchiveFeed)
	reader.Feeds["9AF82230-6137-4DA3-3580-80EDA74B0DE2"] = &firstArchiveFeed

	return &reader
}

func TestReplicateTxnStartError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectBegin().WillReturnError(errors.New("Bam"))

	replicator, err := testFactory.New(new(TableLocker), new(testFeedReader), db)
	assert.Nil(t, err)

	_, err = replicator.ProcessFeed()
	assert.NotNil(t, err)

	err = mock.ExpectationsWereMet()
	assert.Nil(t, err)
}

func TestLockError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	testExpectLock(mock, true, false)

	replicator, err := testFactory.New(new(TableLocker), new(testFeedReader), db)
	assert.Nil(t, err)

	_, err = replicator.ProcessFeed()
	assert.NotNil(t, err)

	err = mock.ExpectationsWereMet()
	assert.Nil(t, err)
}

func TestLockNotAcquired(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	testExpectLock(mock, false, false)
	mock.ExpectRollback()

	replicator, err := testFactory.New(new(TableLocker), new(testFeedReader), db)
	assert.Nil(t, err)

	_, err = replicator.ProcessFeed()
	assert.Nil(t, err)

	err = mock.ExpectationsWereMet()
	assert.Nil(t, err)
}

func TestReplicateEmpty(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	testExpectLock(mock, false, true)
	testExpectQueryReturnNoRows(mock)
	mock.ExpectCommit()

	replicator, err := testFactory.New(new(TableLocker), new(testFeedReader), db)
	assert.Nil(t, err)

	replicator.ProcessFeed()

	err = mock.ExpectationsWereMet()
	assert.Nil(t, err)
}

func TestReplicateLastEventQueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	testExpectLock(mock, false, true)
	testExpectQueryReturnError(mock)
	mock.ExpectRollback()

	locker := new(TableLocker)
	replicator, err := testFactory.New(locker, new(testFeedReader), db)
	assert.Nil(t, err)

	_, err = replicator.ProcessFeed()
	assert.NotNil(t, err)

	err = mock.ExpectationsWereMet()
	assert.Nil(t, err)

	err = locker.ReleaseLock() //For code coverage
	assert.Nil(t, err)

}

func TestReplicateFromScratch(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	testExpectLock(mock, false, true)
	testExpectQueryReturnNoRows(mock)
	testExpectInsertIntoEvents(mock, "cee18efc-0568-48f9-764c-149085ea0324", "1")
	testExpectInsertIntoEvents(mock, "3a56b98b-0a03-4822-44c7-93216255d857", "1")
	testExpectInsertIntoEvents(mock, "e44afbe7-e24f-4bdf-4fa8-9cfc46e4c496", "1")
	mock.ExpectCommit()

	feedReader := initTestFeedReader()

	replicator, err := testFactory.New(new(TableLocker), feedReader, db)

	replicator.ProcessFeed()

	err = mock.ExpectationsWereMet()
	assert.Nil(t, err)
}

//For this test case, the last aggregate seen in the replicated store is the last event added to
//the first feed. We expect all the events in the second feed to be added.
func TestReplWhenLastInPageOneCurrent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	testExpectLock(mock, false, true)
	testExpectQueryReturnAggregateAndVersion(mock, "e44afbe7-e24f-4bdf-4fa8-9cfc46e4c496", "1")
	testExpectInsertIntoEvents(mock, "3418b971-0ea8-483d-4520-9bfbc6a1d356", "1")
	testExpectInsertIntoEvents(mock, "f3234d82-0cff-4221-64de-315c8ab6dbd6", "1")
	testExpectInsertIntoEvents(mock, "9f02eae0-bf8c-46c1-7afb-9af83616b0ae", "1")
	mock.ExpectCommit()

	feedReader := initTestFeedReader()

	replicator, err := testFactory.New(new(TableLocker), feedReader, db)

	replicator.ProcessFeed()

	err = mock.ExpectationsWereMet()
	assert.Nil(t, err)
}

func TestReplWhenLastInMidFeed(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	testExpectLock(mock, false, true)
	testExpectQueryReturnAggregateAndVersion(mock, "f3234d82-0cff-4221-64de-315c8ab6dbd6", "1")
	testExpectInsertIntoEvents(mock, "9f02eae0-bf8c-46c1-7afb-9af83616b0ae", "1")
	mock.ExpectCommit()

	feedReader := initTestFeedReader()

	replicator, err := testFactory.New(new(TableLocker), feedReader, db)

	replicator.ProcessFeed()

	err = mock.ExpectationsWereMet()
	assert.Nil(t, err)
}

func TestReplWhenLastIsLatest(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	testExpectLock(mock, false, true)
	testExpectQueryReturnAggregateAndVersion(mock, "9c5f255c-c5f2-42cb-7f06-5be564e91fd9", "1")

	mock.ExpectCommit()

	feedReader := initTestFeedReader()

	replicator, err := testFactory.New(new(TableLocker), feedReader, db)
	assert.Nil(t, err)

	replicator.ProcessFeed()

	err = mock.ExpectationsWereMet()
	assert.Nil(t, err)
}

func TestLockerGetLockErrors(t *testing.T) {
	locker := new(TableLocker)
	_, err := locker.GetLock()
	assert.NotNil(t, err)

	_, err = locker.GetLock(1)
	assert.NotNil(t, err)
}

func TestHttpFeedReaderGetRecent(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(feedmock.RecentHandler))
	defer ts.Close()

	url, _ := url.Parse(ts.URL)

	log.Infof("test server endpoint is %s", url.Host)
	httpReplicator := NewHttpFeedReader(url.Host, "http", "", nil)
	assert.Equal(t, "http", httpReplicator.proto)

	feed, err := httpReplicator.GetRecent()
	assert.Nil(t, err)
	if assert.NotNil(t, feed) {
		assert.Equal(t, "recent", feed.ID)
	}
}

func TestHttpFeedReaderGetError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Fam... no way", http.StatusInternalServerError)
	}))
	defer ts.Close()

	url, _ := url.Parse(ts.URL)

	log.Infof("test server endpoint is %s", url.Host)
	httpReplicator := NewHttpFeedReader(url.Host, "http","", nil)
	assert.Equal(t, "http", httpReplicator.proto)

	_, err := httpReplicator.GetRecent()
	assert.NotNil(t, err)
}

//TestReplEmptyFeed verifies that when there is nothing to replicate from scratch
//that the table lock is released (via transaction commit)
func TestReplEmptyFeed(t *testing.T) {
	//Set up DB mock
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	testExpectLock(mock, false, true)
	testExpectQueryReturnNoRows(mock)
	mock.ExpectCommit()

	//Set up http feed reader
	ts := httptest.NewServer(http.HandlerFunc(feedmock.EmptryFeedHandler))
	defer ts.Close()

	url, _ := url.Parse(ts.URL)

	log.Infof("test server endpoint is %s", url.Host)
	httpReplicator := NewHttpFeedReader(url.Host, "http", "", nil)

	replicator, err := testFactory.New(new(TableLocker), httpReplicator, db)

	replicator.ProcessFeed()

	err = mock.ExpectationsWereMet()
	assert.Nil(t, err)
}
