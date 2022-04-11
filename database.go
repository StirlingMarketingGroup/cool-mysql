package mysql

import (
	"context"
	"database/sql"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

// Database is a cool MySQL connection
type Database struct {
	Writes *sql.DB
	Reads  *sql.DB

	WritesDSN string
	ReadsDSN  string

	Log      LogFunc
	Finished FinishedFunc

	die bool

	maxInsertSize int

	redis *redis.Client

	// DisableForeignKeyChecks only affects foreign keys for transactions
	DisableForeignKeyChecks bool

	testMx *sync.Mutex
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
	db.testMx = new(sync.Mutex)

	db.WritesDSN = writes
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

	db.Writes.SetConnMaxLifetime(MaxConnectionTime)

	if reads != writes {
		db.ReadsDSN = reads
		db.Reads, err = sql.Open("mysql", reads)
		if err != nil {
			return nil, err
		}

		err = db.Reads.Ping()
		if err != nil {
			return nil, err
		}

		db.Reads.SetConnMaxLifetime(MaxConnectionTime)
	} else {
		db.ReadsDSN = writes
		db.Reads = db.Writes
	}

	return
}

// Reconnect creates new connection(s) for writes and reads
// and replaces the existing connections with the new ones
func (db *Database) Reconnect() error {
	new, err := NewFromDSN(db.WritesDSN, db.ReadsDSN)
	if err != nil {
		return errors.Wrapf(err, "failed to reconnect")
	}

	*db.Writes = *new.Writes
	*db.Reads = *new.Reads

	return nil
}

// Test pings both writes and reads connection and if either fail
// reconnects both connections
func (db *Database) Test() error {
	db.testMx.Lock()
	defer db.testMx.Unlock()

	if db.Writes.Ping() != nil || db.Reads.Ping() != nil {
		return db.Reconnect()
	}

	return nil
}

func (db *Database) DefaultInsertOptions() *Inserter {
	return &Inserter{
		db: db,
	}
}

func (db *Database) I() *Inserter {
	return db.DefaultInsertOptions()
}

func (db *Database) Insert(insert string, source any) error {
	return db.I().insert(db.Writes, context.Background(), insert, source)
}

func (db *Database) InsertContext(ctx context.Context, insert string, source any) error {
	return db.I().insert(db.Writes, ctx, insert, source)
}

func (db *Database) InsertReads(insert string, source any) error {
	return db.I().insert(db.Reads, context.Background(), insert, source)
}

func (db *Database) InsertReadsContext(ctx context.Context, insert string, source any) error {
	return db.I().insert(db.Reads, ctx, insert, source)
}

// ExecContext executes a query and nothing more
func (db *Database) ExecContextResult(ctx context.Context, query string, params ...Params) (sql.Result, error) {
	return db.exec(db.Writes, ctx, query, params...)
}

// ExecContext executes a query and nothing more
func (db *Database) ExecContext(ctx context.Context, query string, params ...Params) error {
	_, err := db.ExecContextResult(ctx, query, params...)
	return err
}

// ExecResult executes a query and nothing more
func (db *Database) ExecResult(query string, params ...Params) (sql.Result, error) {
	return db.ExecContextResult(context.Background(), query, params...)
}

// Exec executes a query and nothing more
func (db *Database) Exec(query string, params ...Params) error {
	_, err := db.ExecContextResult(context.Background(), query, params...)
	return err
}
