package health

import (
	"database/sql"
	"encoding/json"
	"net/http"

	log "github.com/Sirupsen/logrus"
)

const (
	appName = "atom-feed-replicator"
	yesStr  = "Yes"
	noStr   = "No"
)

type HealthResponse struct {
	AppStatus          []AppStatus `json:"appStatus"`
	ApplicationVersion string      `json:"applicationVersion,omitempty"`
}

type AppStatus struct {
	ApplicationName string       `json:"applicationName"`
	Available       string       `json:"available"`
	Dependencies    []Dependency `json:"dependencies"`
}

type Dependency struct {
	DependencyName string `json:"dependencyName"`
	Available      string `json:"available"`
}

func EnableHealthEndpoint(healthPort string, db *sql.DB) {
	if healthPort == "" {
		log.Error("Health endpoint port is not set!")
		return
	}
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		log.Debug("Requesting atom feed replicator health...")

		hr := &HealthResponse{
			AppStatus:          []AppStatus{getReplicatorHealth(db)},
			ApplicationVersion: Version,
		}

		b, err := json.Marshal(hr)
		if err != nil {
			log.Error("Error marshaling health check response: ", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(b)
	})
	log.Info("Health endpoint is on port: ", healthPort)
	log.Error(http.ListenAndServe(":"+healthPort, nil))
}

func getReplicatorHealth(db *sql.DB) AppStatus {
	availableStr := yesStr

	if db == nil {
		availableStr = noStr
	} else {

		var result string
		if dbError := db.QueryRow("select DUMMY from DUAL").Scan(&result); dbError != nil {
			log.Warn("Atom Feed Replicator is not healthy: ", dbError)
			availableStr = noStr
		}
	}

	return AppStatus{
		ApplicationName: appName,
		Available:       availableStr,
		Dependencies: []Dependency{Dependency{
			DependencyName: "database",
			Available:      availableStr,
		}},
	}
}
