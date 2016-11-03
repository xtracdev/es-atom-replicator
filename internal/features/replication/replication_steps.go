package replication

import (
	. "github.com/gucumber/gucumber"
	"database/sql"
	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/xtracdev/es-atom-replicator"
	feedmock "github.com/xtracdev/es-atom-replicator/testing"
	"golang.org/x/tools/blog/atom"
	"time"
	"encoding/base64"
	"fmt"
	"encoding/xml"
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
		_, err := db.Exec("delete from events")
		assert.Nil(T, err)

		_, err = db.Exec("delete from publish")
		assert.Nil(T, err)
	})

	When(`^I process events$`, func() {
		err := replicator.ProcessFeed()
		assert.Nil(T, err)
	})

	Then(`^the first feed page is replicated$`, func() {
		dbEntries,err := getEntries(db)
		if assert.Nil(T,err) {
			var feed atom.Feed
			xml.Unmarshal([]byte(feedmock.FirstArchive),&feed)

			feedEntries := feed.Entry
			if assert.Equal(T, len(feed.Entry),len(dbEntries), "Different no of entries in feeds") {
				for idx, entry := range dbEntries {
					assert.Equal(T, entry.ID, feedEntries[idx].ID)
				}
			}
		}
	})

}

func getEntries(db *sql.DB) ([]*atom.Entry,error) {
	var entries []*atom.Entry

	rows,err := db.Query("select event_time,aggregate_id,version,typecode,payload from events order by event_time desc")
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

		err := rows.Scan(&ts, &aggID,&version,&typecode,&payload)
		if err != nil {
			return entries,err
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

	return entries,nil

}
