package main

import (
	"github.com/xtracdev/oraconn"
	"github.com/xtracdev/es-atom-replicator"
	"os"
	"errors"
	log "github.com/Sirupsen/logrus"
	"time"
	"github.com/xtracdev/tlsconfig"
)

const (
	maxDBReconnectAttempts = 100
)

func connectToDB()(*oraconn.OracleDB,error) {
	dbEnvConfig, err := oraconn.NewEnvConfig()
	if err != nil {
		return nil,err
	}

	oraDB, err := oraconn.OpenAndConnect(dbEnvConfig.ConnectString(), maxDBReconnectAttempts)
	if err != nil {
		return nil, err
	}

	return oraDB,nil
}

func createFeedReader()(*replicator.HttpFeedReader, error) {
	feedAddr := os.Getenv("ATOMFEED_ENDPOINT")
	if feedAddr == "" {
		return nil,errors.New("Missing ATOMFEED_ENDPOINT environment variable value")
	}

	privateKey := os.Getenv("PRIVATE_KEY")
	certificate := os.Getenv("CERTIFICATE")
	caCert := os.Getenv("CACERT")

	if privateKey == "" || certificate == "" || caCert == "" {
		log.Info("Using non-TLS configuration to read atom feed.")
		log.Info("SPecify PRIVATE_KEY, CERTIFICATE, and CACERT envionment variables for TLS config")
		return replicator.NewHttpFeedReader(feedAddr, nil),nil
	} else {
		config, err := tlsconfig.GetTLSConfiguration(privateKey, certificate, caCert)
		if err != nil {
			return nil,err
		}

		return replicator.NewHttpFeedReader(feedAddr, config),nil
	}




}

func handleFatal(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

func main() {
	var feedReplicator replicator.Replicator
	var createReplictorErr error
	oraDB, err := connectToDB()
	handleFatal(err)

	feedReader, err := createFeedReader()
	handleFatal(err)

	factory := replicator.OraEventStoreReplicatorFactory{}

	feedReplicator,createReplictorErr = factory.New(new(replicator.TableLocker), feedReader, oraDB.DB)
	handleFatal(createReplictorErr)

	replicator.ConfigureStatsD()

	for {
		_,err := feedReplicator.ProcessFeed()
		if err != nil {
			if oraconn.IsConnectionError(err) {
				oraDB.Reconnect(maxDBReconnectAttempts)
				feedReplicator, createReplictorErr = factory.New(new(replicator.TableLocker), feedReader, oraDB.DB)
				handleFatal(createReplictorErr)
			} else {
				log.Error(err.Error())
			}
		}

		//Sleep to avoid hammering the db when there's nothing to do
		time.Sleep(10 * time.Second)
	}
}
