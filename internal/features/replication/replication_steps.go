package replication

import (
	"database/sql"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	log "github.com/Sirupsen/logrus"
	. "github.com/gucumber/gucumber"
	"github.com/stretchr/testify/assert"
	"github.com/xtracdev/es-atom-replicator"
	feedmock "github.com/xtracdev/es-atom-replicator/testing"
	"golang.org/x/tools/blog/atom"
	"time"
)

func init() {

	var replicator replicator.Replicator
	var db *sql.DB
	var envInitErr error

	replicator, db, envInitErr = initializeEnvironment()

	Given(`^a replicator$`, func() {
		log.Info("check init")
		if envInitErr != nil {
			assert.Nil(T, envInitErr, "Test env init failure", envInitErr.Error())
			return
		}

	})

	And(`^an empty replication db$`, func() {
		_, err := db.Exec("delete from t_aepb_publish")
		assert.Nil(T, err)

		_, err = db.Exec("delete from t_aeev_events")
		assert.Nil(T, err)
	})

	When(`^I process events$`, func() {
		_, err := replicator.ProcessFeed()
		assert.Nil(T, err)
	})

	Then(`^the first feed page is replicated$`, func() {
		log.Info("==> the first feed page is replicated")
		dbEntries, err := getEntries(db)
		if assert.Nil(T, err) {
			var feed atom.Feed
			xml.Unmarshal([]byte(feedmock.FirstArchive), &feed)

			feedEntries := feed.Entry
			if assert.Equal(T, len(feed.Entry), len(dbEntries), "Different no of entries in feeds") {
				for idx, entry := range dbEntries {
					assert.Equal(T, entry.ID, feedEntries[idx].ID)
				}
			}
		}
	})

	And(`^new events to replicate$`, func() {
		replicator = getMoreReplicator(db)
	})

	When(`^the latest aggregate reference is not in the source$`, func() {
		_,err := db.Exec(`insert into t_aeev_events(aggregate_id, version, typecode) values('foo',1, 'footc')`)
		assert.Nil(T,err)
	})

	And(`^I replicate$`, func() {
		//Next replication run picks up second page
		_, err := replicator.ProcessFeed()
		assert.Nil(T, err)

		//Running the replicator again picks up the most recent events
		_, err = replicator.ProcessFeed()
		assert.Nil(T, err)
	})

	Then(`^I pick up the new events anyway$`, func() {
		dbEntries, err := getEntries(db)
		var foundLatestFromMoreFeed bool
		if assert.Nil(T,err) {
			for _, entry := range dbEntries {
				log.Print(entry.ID)
				if entry.ID == "urn:esid:ad5f255c-c5f2-42cb-7f06-5be564e91fd9:1" {
					foundLatestFromMoreFeed = true
					break
				}
			}
		}
		assert.True(T, foundLatestFromMoreFeed)

	})

	And(`^the non-existant aggregate is removed from the database$`, func() {
		count := 1
		err := db.QueryRow(`select count(*) from t_aeev_events where aggregate_id = 'foo' and version = 1`).Scan(&count)
		if assert.Nil(T,err) {
			assert.Equal(T, 0, count)
		}
	})


}

func getEntries(db *sql.DB) ([]*atom.Entry, error) {
	var entries []*atom.Entry

	rows, err := db.Query("select event_time,aggregate_id,version,typecode,payload from t_aeev_events order by event_time desc")
	if err != nil {
		return entries, err
	}

	defer rows.Close()

	for rows.Next() {
		var ts time.Time
		var aggID string
		var version int
		var typecode string
		var payload []byte

		err := rows.Scan(&ts, &aggID, &version, &typecode, &payload)
		if err != nil {
			return entries, err
		}

		content := &atom.Text{
			Type: typecode,
			Body: base64.StdEncoding.EncodeToString(payload),
		}

		entry := &atom.Entry{
			Title:     "event",
			ID:        fmt.Sprintf("urn:esid:%s:%d", aggID, version),
			Published: atom.TimeStr(ts.Format(time.RFC3339Nano)),
			Content:   content,
		}

		entries = append(entries, entry)
	}

	return entries, nil

}
