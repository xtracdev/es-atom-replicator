package oraconn

import (
	"database/sql"
	//Canonical anonymous import of driver specifics
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	_ "github.com/mattn/go-oci8"
	"strings"
	"time"
)

//OracleDB embeds sql.DB and extends it with the ability to retry connecting to
//the database when errors occur, detecting certain disconnections, and reconnecting
//to the database.
type OracleDB struct {
	*sql.DB
	connectStr string
}

//ErrRetryCount is used to indicate invalid retry count values passed to
//OpenAndConnect.
var ErrRetryCount = errors.New("Retry count must be greater than 1")

// OpenAndConnect attempts to open a connection to Oracle as specified in the
// provided connection string. The connection is considered open when Ping returns
// with a nil error. The connection Ping is is tried the number of times
// indicated by the retry count, with a sleep delay of the current attempt in
// seconds, i.e. 1s, 2s, 3s ... retryCount
func OpenAndConnect(connectString string, retryCount int) (*OracleDB, error) {
	if retryCount < 1 {
		return nil, ErrRetryCount
	}

	log.Infof("Open the database, %d retries", retryCount)
	db, err := sql.Open("oci8", connectString)
	if err != nil {
		return nil, err
	}

	log.Info("Ping the db as open might not actually connect")

	//Use a backoff/retry strategy - we can start this client before
	//the database is started, and see it eventually connect and process
	//queries
	var dbError error
	maxAttempts := retryCount
	for attempts := 1; attempts <= maxAttempts; attempts++ {
		log.Info("ping database...")
		dbError = db.Ping()
		if dbError == nil {
			break
		}

		log.Infof("Ping failed: %s", strings.TrimSpace(dbError.Error()))
		log.Infof("Retry in %d seconds", attempts)
		time.Sleep(time.Duration(attempts) * time.Second)
	}
	if dbError != nil {
		return nil, dbError
	}

	return &OracleDB{DB: db, connectStr: connectString}, nil
}

//Reconnect to the database. Useful when a loss of connection has been detected
func (odb *OracleDB) Reconnect(retryCount int) error {
	odb.Close()
	db, err := OpenAndConnect(odb.connectStr, retryCount)
	if err != nil {
		return err
	}

	odb.DB = db.DB
	return nil
}

//BuildConnectString builds an Oracle connect string from its constituent parts.
func BuildConnectString(user, password, host, port, service string) string {
	return fmt.Sprintf("%s/%s@//%s:%s/%s",
		user, password, host, port, service)
}

//IsConnectionError returns error if the argument is a connection error
func IsConnectionError(err error) bool {
	errStr := err.Error()
	return strings.HasPrefix(errStr, "ORA-03114") || strings.HasPrefix(errStr, "ORA-03113")
}
