package replication

import (
	. "github.com/gucumber/gucumber"
	//"github.com/xtracdev/es-atom-replicator"
	"database/sql"
	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/xtracdev/es-atom-replicator"
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
		T.Skip() // pending
	})

}
