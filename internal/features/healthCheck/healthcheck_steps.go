package healthCheck

import (
	"database/sql"
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	. "github.com/gucumber/gucumber"
	"github.com/stretchr/testify/assert"
	"github.com/xtracdev/es-atom-replicator/health"
	"github.com/xtracdev/oraconn"
	"io/ioutil"
	"net/http"
)

func init() {

	log.SetLevel(log.DebugLevel)

	var db *sql.DB
	var port string = "9999"
	var resp *http.Response

	Given(`^a database$`, func() {
		dbEnvConfig, err := oraconn.NewEnvConfig()
		assert.Nil(T, err)

		oraDB, err := oraconn.OpenAndConnect(dbEnvConfig.ConnectString(), 10)
		assert.Nil(T, err)

		db = oraDB.DB
	})

	And(`^healch check endpont is enabled$`, func() {
		go health.EnableHealthEndpoint(port, db)
	})

	And(`^I call health check endpont$`, func() {
		response, err := http.Get(fmt.Sprintf("http://localhost:%s/health", port))
		assert.Nil(T, err)
		resp = response
	})

	And(`^the response indicates the replicator is healthy$`, func() {
		assert.Equal(T, http.StatusOK, resp.StatusCode)
		assert.Equal(T, "application/json", resp.Header.Get("Content-Type"))
		rawPayload, err := ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
		assert.Nil(T, err)
		hr := new(health.HealthResponse)
		err = json.Unmarshal(rawPayload, hr)
		assert.Nil(T, err)
		assert.Equal(T, 1, len(hr.AppStatus))
		assert.Equal(T, "atom-feed-replicator", hr.AppStatus[0].ApplicationName)
		assert.Equal(T, 1, len(hr.AppStatus[0].Dependencies))
		assert.Equal(T, "Yes", hr.AppStatus[0].Dependencies[0].Available)
	})

}
