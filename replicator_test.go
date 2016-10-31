package replicator

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"golang.org/x/tools/blog/atom"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"testing"
)

var testFactory = &OraEventStoreReplicatorFactory{}

func testExpectLock(mock sqlmock.Sqlmock, lockError bool, gotLock bool) {
	if lockError {
		mock.ExpectExec("lock table replicator_lock nowait").WillReturnError(errors.New("BAM!"))
		return
	}

	if gotLock {
		execOkResult := sqlmock.NewResult(1, 1)
		mock.ExpectExec("lock table replicator_lock nowait").WillReturnResult(execOkResult)
	} else {
		mock.ExpectExec("lock table replicator_lock nowait").WillReturnError(errors.New("ORA-00054: resource busy and acquire with NOWAIT specified or timeout expired"))
	}

}

func testExpectQueryReturnNoRows(mock sqlmock.Sqlmock) {
	rows := sqlmock.NewRows([]string{"aggregate_id"})
	mock.ExpectQuery("select aggregate_id from events where id").WillReturnRows(rows)
}

type testFeedReader struct{
	Feeds map[string]*atom.Feed
}

func (tfr *testFeedReader) GetRecent() (*atom.Feed, error) {
	if tfr.Feeds == nil {
		return nil, nil
	} else {
		return tfr.Feeds["recent"],nil
	}
}

func (tfr *testFeedReader) GetFeed(feedid string) (*atom.Feed, error) {
	if tfr.Feeds == nil {
		return nil, nil
	} else {
		return tfr.Feeds["feedid"],nil
	}
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

	err = replicator.ProcessFeed()
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

	err = replicator.ProcessFeed()
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

	err = replicator.ProcessFeed()
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

func TestReplicateFromScratch(t *testing.T) {

}

func TestReplicateFromMidFeed(t *testing.T) {

}

func TestReplicateWithNewFeedAdded(t *testing.T) {

}

func TestReplicateCantLock(t *testing.T) {

}
