package replicator

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"golang.org/x/tools/blog/atom"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"testing"
)

var testFactory = &OraEventStoreReplicatorFactory{}

func testExpectLock(mock sqlmock.Sqlmock, lockError bool) {
	if lockError {
		mock.ExpectExec("lock table replicator_lock nowait").WillReturnError(errors.New("BAM!"))
	} else {
		execOkResult := sqlmock.NewResult(1, 1)
		mock.ExpectExec("lock table replicator_lock nowait").WillReturnResult(execOkResult)
	}

}

type testFeedReader struct{}

func (tfr *testFeedReader) GetRecent() (*atom.Feed, error) {
	return nil, nil
}

func (tfr *testFeedReader) GetFeed(feedid string) (*atom.Feed, error) {
	return nil, nil
}

func TestReplicateEmpty(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	testExpectLock(mock, false)

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
