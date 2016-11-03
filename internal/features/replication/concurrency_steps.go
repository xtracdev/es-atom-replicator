package replication

import (
	"database/sql"
	"encoding/xml"
	log "github.com/Sirupsen/logrus"
	. "github.com/gucumber/gucumber"
	"github.com/stretchr/testify/assert"
	"github.com/xtracdev/es-atom-replicator"
	feedmock "github.com/xtracdev/es-atom-replicator/testing"
	"golang.org/x/tools/blog/atom"
	"sync"
)

func init() {
	var rep1Locked bool
	var rep2Locked bool

	var replicator1 replicator.Replicator
	var db1 *sql.DB
	var envInitErr1 error

	replicator1, db1, envInitErr1 = initializeEnvironment()

	var replicator2 replicator.Replicator
	//var db2 *sql.DB
	var envInitErr2 error

	replicator2, _, envInitErr2 = initializeEnvironment()

	Given(`^two replicators$`, func() {
		if envInitErr1 != nil || envInitErr2 != nil {
			log.Error("Unable to initialize two replicators")
		}
	})

	And(`^no events have been replicated$`, func() {
		_, err := db1.Exec("delete from events")
		assert.Nil(T, err)

		_, err = db1.Exec("delete from publish")
		assert.Nil(T, err)
	})

	When(`^both are started$`, func() {

	})

	Then(`^only one may execute the catch up logic$`, func() {
		var wg sync.WaitGroup
		wg.Add(2)

		//It is possible that one process feed can complete in the
		//nowait interval during the table lock - I saw this happen
		//one time.

		go func() {
			rep1Locked, _ = replicator1.ProcessFeed()
			wg.Done()
		}()

		go func() {
			rep2Locked, _ = replicator2.ProcessFeed()
			wg.Done()
		}()

		wg.Wait()

	})

	And(`^at least the first feed page is replicated$`, func() {
		if rep1Locked && rep2Locked {
			log.Info("===> Two process feeds completed <====")
			//In this case both got locks which meant on go routine's lock
			//attempt succeeded after the other go routine finished processing. I
			//observed this one time...
			dbEntries, err := getEntries(db1)
			if assert.Nil(T, err) {
				var feed atom.Feed
				xml.Unmarshal([]byte(feedmock.FirstArchive), &feed)

				feedEntries := feed.Entry

				for idx, entry := range feedEntries {
					assert.Equal(T, dbEntries[idx].ID, entry.ID)
				}

			}
		} else {
			dbEntries, err := getEntries(db1)
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
		}
	})

}
