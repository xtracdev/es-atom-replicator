package oraconn

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

type EnvConfig struct {
	DBUser     string
	DBPassword string
	DBHost     string
	DBPort     string
	DBSvc      string
}

const (
	DBUser = "DB_USER"
	DBPassword = "DB_PASSWORD"
	DBHost = "DB_HOST"
	DBPort = "DB_PORT"
	DBSvc = "DB_SVC"
)

func (ec *EnvConfig) MaskedConnectString() string {
	return fmt.Sprintf("%s/%s@//%s:%s/%s",
		ec.DBUser, "XXX", ec.DBHost, ec.DBPort, ec.DBSvc)
}

func (ec *EnvConfig) ConnectString() string {
	return fmt.Sprintf("%s/%s@//%s:%s/%s",
		ec.DBUser, ec.DBPassword, ec.DBHost, ec.DBPort, ec.DBSvc)
}

func NewEnvConfig() (*EnvConfig, error) {
	var configErrors []string

	user := os.Getenv(DBUser)
	if user == "" {
		configErrors = append(configErrors, "Configuration missing DB_USER env variable")
	}

	password := os.Getenv(DBPassword)
	if password == "" {
		configErrors = append(configErrors, "Configuration missing DB_PASSWORD env variable")
	}

	dbhost := os.Getenv(DBHost)
	if dbhost == "" {
		configErrors = append(configErrors, "Configuration missing DB_HOST env variable")
	}

	dbPort := os.Getenv(DBPort)
	if dbPort == "" {
		configErrors = append(configErrors, "Configuration missing DB_PORT env variable")
	}

	dbSvc := os.Getenv(DBSvc)
	if dbSvc == "" {
		configErrors = append(configErrors, "Configuration missing DB_SVC env variable")
	}

	if len(configErrors) != 0 {
		return nil, errors.New(strings.Join(configErrors, "\n"))
	}

	return &EnvConfig{
		DBUser:     user,
		DBPassword: password,
		DBHost:     dbhost,
		DBPort:     dbPort,
		DBSvc:      dbSvc,
	}, nil

}
