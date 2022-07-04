package database

import (
	"context"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"gorm.io/driver/clickhouse"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
	"time"
)

const (
	TypeMySQL      = "mysql"
	TypePostgreSQL = "postgres"
	TypeSQLite     = "SQLite"
	TypeSQLServer  = "SQLServer"
	TypeClickhouse = "clickhouse"
)

type Config struct {
	Type            string `json:"type" yaml:"type"` // database type
	DSN             string `json:"dsn" yaml:"dsn"`   //
	MaxOpenConn     int    `json:"max_open_conn" yaml:"max_open_conn"`
	MaxIdleConn     int    `json:"max_idle_conn" yaml:"max_idle_conn"`
	ConnMaxLifetime int    `json:"conn_max_lifetime" yaml:"conn_max_lifetime"`   //sec
	ConnMaxIdleTime int    `json:"conn_max_idle_time" yaml:"conn_max_idle_time"` //sec
}

type Database struct {
	Type string
	Gorm *gorm.DB
	Sqlx *sqlx.DB
}

func (d *Database) NewSession(ctx context.Context) *gorm.DB {
	return d.Gorm.Session(&gorm.Session{NewDB: true, Context: ctx})
}

func FromGormDB(gormDB *gorm.DB, driverName string) (*Database, error) {
	_db, err := gormDB.DB()
	if err != nil {
		return nil, err
	}
	// 添加gormTrace
	err = gormDB.Use(&TraceGorm{})
	if err != nil {
		return nil, err
	}
	return &Database{
		Type: driverName,
		Gorm: gormDB,
		Sqlx: sqlx.NewDb(_db, driverName),
	}, nil
}

func NewDB(config Config) (*Database, error) {
	var (
		gormDB *gorm.DB
		err    error
	)
	switch config.Type {
	case TypeMySQL:
		gormDB, err = gorm.Open(mysql.Open(config.DSN), &gorm.Config{})
	case TypePostgreSQL:
		gormDB, err = gorm.Open(postgres.Open(config.DSN), &gorm.Config{})
	case TypeSQLite:
		gormDB, err = gorm.Open(sqlite.Open(config.DSN), &gorm.Config{})
	case TypeSQLServer:
		gormDB, err = gorm.Open(sqlserver.Open(config.DSN), &gorm.Config{})
	case TypeClickhouse:
		gormDB, err = gorm.Open(clickhouse.Open(config.DSN), &gorm.Config{})
	default:
		return nil, errors.New("error database type")
	}
	if err != nil {
		return nil, err
	}
	_db, err := gormDB.DB()
	if err != nil {
		return nil, err
	}
	if config.MaxOpenConn > 0 {
		_db.SetMaxOpenConns(config.MaxOpenConn)
	}
	if config.MaxIdleConn > 0 {
		_db.SetMaxIdleConns(config.MaxIdleConn)
	}
	if config.ConnMaxLifetime > 0 {
		_db.SetConnMaxLifetime(time.Duration(config.ConnMaxLifetime) * time.Second)
	}
	if config.ConnMaxIdleTime > 0 {
		_db.SetConnMaxIdleTime(time.Duration(config.ConnMaxIdleTime) * time.Second)
	}
	// 添加gormTrace
	err = gormDB.Use(&TraceGorm{})
	if err != nil {
		return nil, err
	}
	return &Database{
		Type: config.Type,
		Gorm: gormDB,
		Sqlx: sqlx.NewDb(_db, config.Type),
	}, nil
}
