package replication

import (
	"database/sql"
	log "github.com/Sirupsen/logrus"
	. "github.com/gucumber/gucumber"
	"github.com/stretchr/testify/assert"
	"github.com/xtracdev/es-atom-replicator"
	"sync"
)

func init() {
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

		go func() {
			replicator1.ProcessFeed()
			wg.Done()
		}()

		go func() {
			replicator2.ProcessFeed()
			wg.Done()
		}()

		wg.Wait()

	})

}
