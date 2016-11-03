package replication

import (
	"github.com/xtracdev/es-atom-replicator"
	"github.com/xtracdev/oraconn"
)

func initializeEnvironment() (replicator.Replicator, error) {
	dbEnvConfig, err := oraconn.NewEnvConfig()
	if err != nil {
		return nil, err
	}

	oraDB, err := oraconn.OpenAndConnect(dbEnvConfig.ConnectString(), 10)
	if err != nil {
		return nil, err
	}

	locker := new(replicator.TableLocker)
	feedReader := new(replicator.HttpReplicator)

	factory := replicator.OraEventStoreReplicatorFactory{}

	rep, err := factory.New(locker, feedReader, oraDB.DB)
	return rep, err
}
