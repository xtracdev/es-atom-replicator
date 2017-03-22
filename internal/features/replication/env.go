package replication

import (
	"database/sql"
	log "github.com/Sirupsen/logrus"
	"github.com/xtracdev/es-atom-replicator"
	feedmock "github.com/xtracdev/es-atom-replicator/testing"
	"github.com/xtracdev/oraconn"
	"net/http"
	"net/http/httptest"
	"net/url"
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

	ts := httptest.NewServer(http.HandlerFunc(feedmock.GetFeedHandler))
	url, _ := url.Parse(ts.URL)

	log.Infof("test server endpoint is %s", url.Host)
	httpReplicator := replicator.NewHttpFeedReader(url.Host, "", nil)

	locker := new(replicator.TableLocker)

	factory := replicator.OraEventStoreReplicatorFactory{}

	rep, err := factory.New(locker, httpReplicator, oraDB.DB)
	return rep, oraDB.DB, err
}
