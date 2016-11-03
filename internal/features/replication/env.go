package replication

import (
	"github.com/xtracdev/es-atom-replicator"
	"github.com/xtracdev/oraconn"
	"database/sql"
)

func initializeEnvironment() (replicator.Replicator, *sql.DB, error) {
	dbEnvConfig, err := oraconn.NewEnvConfig()
	if err != nil {
		return nil, nil, err
	}

	oraDB, err := oraconn.OpenAndConnect(dbEnvConfig.ConnectString(), 10)
	if err != nil {
		return nil, nil, err
	}

	locker := new(replicator.TableLocker)
	feedReader := new(replicator.HttpReplicator)

	factory := replicator.OraEventStoreReplicatorFactory{}

	rep, err := factory.New(locker, feedReader, oraDB.DB)
	return rep, oraDB.DB, err
}
