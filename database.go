package mysql

import (
	"database/sql"
	"net"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-sql-driver/mysql"
)

// Database is a cool MySQL connection
type Database struct {
	Writes *sql.DB
	Reads  *sql.DB

	Log      LogFunc
	Finished FinishedFunc

	die bool

	maxInsertSize int

	redis *redis.Client

	// DisableForeignKeyChecks only affects foreign keys for transactions
	DisableForeignKeyChecks bool
}

// Clone returns a copy of the db with the same connections
// but with an empty query log
func (db *Database) Clone() *Database {
	clone := *db
	return &clone
}

// EnableRedis enables redis cache for select queries with cache times
// with the given connection information
func (db *Database) EnableRedis(address string, password string, redisDB int) {
	db.redis = redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password, // no password set
		DB:       redisDB,  // use default DB
	})
}

// LogFunc is called after the query executes
type LogFunc func(query string, params Params, duration time.Duration)

// FinishedFunc executes after all rows have been processed,
// including being read from the channel if used
type FinishedFunc func(cached bool, replacedQuery string, mergedParams Params, execDuration time.Duration, fetchDuration time.Duration)

func (db *Database) callLog(query string, params Params, duration time.Duration) {
	if db.Log != nil {
		db.Log(query, params, duration)
	}
}

// New creates a new Database
func New(wUser, wPass, wSchema, wHost string, wPort int,
	rUser, rPass, rSchema, rHost string, rPort int,
	timeZone *time.Location) (db *Database, err error) {
	writes := mysql.NewConfig()
	writes.User = wUser
	writes.Passwd = wPass
	writes.DBName = wSchema
	writes.Net = "tcp"
	writes.Addr = net.JoinHostPort(wHost, strconv.Itoa(wPort))
	writes.Loc = timeZone
	writes.ParseTime = true
	writes.InterpolateParams = true

	reads := mysql.NewConfig()
	reads.User = rUser
	reads.Passwd = rPass
	reads.DBName = rSchema
	reads.Net = "tcp"
	reads.Addr = net.JoinHostPort(rHost, strconv.Itoa(rPort))
	reads.Loc = timeZone
	reads.ParseTime = true
	reads.InterpolateParams = true

	return NewFromDSN(writes.FormatDSN(), reads.FormatDSN())
}

// NewFromDSN creates a new Database from config
// DSN strings for both connections
func NewFromDSN(writes, reads string) (db *Database, err error) {
	db = new(Database)

	db.Writes, err = sql.Open("mysql", writes)
	if err != nil {
		return nil, err
	}

	err = db.Writes.Ping()
	if err != nil {
		return nil, err
	}

	writesDSN, _ := mysql.ParseDSN(writes)
	db.maxInsertSize = writesDSN.MaxAllowedPacket

	if reads != writes {
		db.Reads, err = sql.Open("mysql", reads)
		if err != nil {
			return nil, err
		}

		err = db.Reads.Ping()
		if err != nil {
			return nil, err
		}
	} else {
		db.Reads = db.Writes
	}

	db.Writes.SetConnMaxLifetime(MaxConnectionTime)
	db.Reads.SetConnMaxLifetime(MaxConnectionTime)

	return
}
