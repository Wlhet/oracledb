package oracledb

import (
	"database/sql"
	"errors"
)

type OracleClient struct {
	db   *sql.DB
	conf *OracleConfig
}

// 构造函数
func NewOracleClient(conf *OracleConfig) (*OracleClient, error) {
	if conf == nil {
		return nil, errors.New("config is nil")
	}
	if conf.driverName == "" {
		conf.driverName = "oracle"
	}
	if conf.userName == "" {
		return nil, errors.New("username is empty")
	}
	if conf.passWord == "" {
		return nil, errors.New("password is empty")
	}
	if conf.host == "" {
		return nil, errors.New("host is empty")
	}
	if conf.port == 0 {
		return nil, errors.New("port is empty")
	}
	if conf.dataBase == "" {
		return nil, errors.New("database is empty")
	}
	conf.EnableRecoverPanic()
	db, err := sql.Open(conf.driverName, conf.GetDsn())
	if err != nil {
		return nil, err
	}
	return &OracleClient{db: db, conf: conf}, nil
}

func (oc *OracleClient) GetConfig() *OracleConfig {
	return oc.conf
}

func (oc *OracleClient) GetDB() *sql.DB {
	return oc.db
}
