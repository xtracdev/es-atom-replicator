package replication

import (
	. "github.com/gucumber/gucumber"
	//"github.com/xtracdev/es-atom-replicator"
	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"database/sql"
)

func init() {

	//var replicator replicator.Replicator
	var db *sql.DB
	var envInitErr error

	_, db, envInitErr = initializeEnvironment()

	Given(`^a replicator$`, func() {
		log.Info("check init")
		if envInitErr != nil {
			assert.Nil(T, envInitErr, "Test env init failure", envInitErr.Error())
			return
		}

	})

	And(`^an empty replication db$`, func() {
		_,err := db.Exec("delete from events")
		assert.Nil(T, err)

		_,err = db.Exec("delete from publish")
		assert.Nil(T,err)
	})

	When(`^I process events$`, func() {
		T.Skip() // pending
	})

	Then(`^the first feed page is replicated$`, func() {
		T.Skip() // pending
	})

}
