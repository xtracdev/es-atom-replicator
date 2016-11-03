package replication

import (
	. "github.com/gucumber/gucumber"
	//"github.com/xtracdev/es-atom-replicator"
	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func init() {

	//var replicator replicator.Replicator
	var envInitErr error

	_, envInitErr = initializeEnvironment()

	Given(`^a replicator$`, func() {
		log.Info("check init")
		if envInitErr != nil {
			assert.Nil(T, envInitErr, "Test env init failure", envInitErr.Error())
			return
		}

	})

	And(`^an empty replication db$`, func() {
		T.Skip() // pending
	})

	When(`^I process events$`, func() {
		T.Skip() // pending
	})

	Then(`^the first feed page is replicated$`, func() {
		T.Skip() // pending
	})

}
