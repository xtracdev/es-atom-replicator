package health

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnableHealthEndpoint(t *testing.T) {

	go EnableHealthEndpoint("8003", nil)

	Version = "1.0.0"

	resp, err := http.DefaultClient.Get("http://localhost:8003/health")

	assert.NoError(t, err)
	assert.NotNil(t, resp)

	respBody, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	defer resp.Body.Close()

	health := new(HealthResponse)
	assert.NoError(t, json.Unmarshal(respBody, health))

	assert.Equal(t, "1.0.0", health.ApplicationVersion)

	assert.Len(t, health.AppStatus, 1)
	assert.Equal(t, "atom-feed-replicator", health.AppStatus[0].ApplicationName)
	assert.Equal(t, noStr, health.AppStatus[0].Available)
	assert.Len(t, health.AppStatus[0].Dependencies, 1)

	assert.Equal(t, noStr, health.AppStatus[0].Dependencies[0].Available)
	assert.Equal(t, "database", health.AppStatus[0].Dependencies[0].DependencyName)
}
