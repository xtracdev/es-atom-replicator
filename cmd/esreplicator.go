package main

import (
	"errors"
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/xtracdev/es-atom-replicator"
	"github.com/xtracdev/es-atom-replicator/health"
	"github.com/xtracdev/oraconn"
	"os"
	"time"
)

const (
	maxDBReconnectAttempts = 100
)

func connectToDB() (*oraconn.OracleDB, error) {
	dbEnvConfig, err := oraconn.NewEnvConfig()
	if err != nil {
		return nil, err
	}

	oraDB, err := oraconn.OpenAndConnect(dbEnvConfig.ConnectString(), maxDBReconnectAttempts)
	if err != nil {
		return nil, err
	}

	return oraDB, nil
}

func createFeedReader() (*replicator.HttpFeedReader, error) {
	feedAddr := os.Getenv("ATOMFEED_ENDPOINT")
	if feedAddr == "" {
		return nil, errors.New("Missing ATOMFEED_ENDPOINT environment variable value")
	}

	var kmsService *kms.KMS
	keyAlias := os.Getenv("KEY_ALIAS")
	if keyAlias != "" {
		log.Info("Configuration indicates use of KMS with alias ", keyAlias)

		sess, err := session.NewSession()
		if err != nil {
			log.Errorf("Unable to establish AWS session: %s. Exiting.", err.Error())
			os.Exit(1)
		}
		kmsService = kms.New(sess)
	}

	return replicator.NewHttpFeedReader(feedAddr, keyAlias, kmsService), nil
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

	feedReplicator, createReplictorErr = factory.New(new(replicator.TableLocker), feedReader, oraDB.DB)
	handleFatal(createReplictorErr)

	replicator.ConfigureStatsD()

	go health.EnableHealthEndpoint(os.Getenv("ATOMREPLICATOR_HEALTH_PORT"), oraDB.DB)

	for {
		_, err := feedReplicator.ProcessFeed()
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
