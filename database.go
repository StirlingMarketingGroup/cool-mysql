package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Database is a cool MySQL connection
type Database struct {
	Writes *sql.DB
	Reads  *sql.DB

	WritesDSN string
	ReadsDSN  string

	Log              LogFunc
	Finished         FinishedFunc
	HandleRedisError HandleRedisError

	die bool

	MaxInsertSize *synct[int]

	redis redis.UniversalClient
	rs    *redsync.Redsync

	// DisableForeignKeyChecks only affects foreign keys for transactions
	DisableForeignKeyChecks bool

	testMx *sync.Mutex

	Logger                      *zap.Logger
	DisableUnusedColumnWarnings bool
}

// Clone returns a copy of the db with the same connections
// but with an empty query log
func (db *Database) Clone() *Database {
	clone := *db
	return &clone
}

// EnableRedis enables redis cache for select queries with cache times
// with the given connection information
func (db *Database) EnableRedis(redisClient redis.UniversalClient) *Database {
	db.redis = redisClient
	db.rs = redsync.New(goredis.NewPool(db.redis))

	return db
}

// LogFunc is called after the query executes
type LogFunc func(query string, params Params, duration time.Duration, cacheHit bool)

// FinishedFunc executes after all rows have been processed,
// including being read from the channel if used
type FinishedFunc func(cached bool, replacedQuery string, params Params, execDuration time.Duration, fetchDuration time.Duration)

// HandleRedisError is executed on a redis error, so it can be handled by the user
// return false to let the function return the error, or return to let the function continue executing despite the redis error
type HandleRedisError func(err error) bool

func (db *Database) callLog(query string, params Params, duration time.Duration, cacheHit bool) {
	if db.Log != nil {
		db.Log(query, params, duration, cacheHit)
	}
}

// New creates a new Database
func New(wUser, wPass, wSchema, wHost string, wPort int,
	rUser, rPass, rSchema, rHost string, rPort int,
	collation string, timeZone *time.Location) (db *Database, err error) {
	writes := mysql.NewConfig()
	writes.User = wUser
	writes.Passwd = wPass
	writes.DBName = wSchema
	writes.Net = "tcp"
	writes.Addr = net.JoinHostPort(wHost, strconv.Itoa(wPort))
	if timeZone != nil {
		writes.Loc = timeZone
	}
	writes.ParseTime = true
	writes.InterpolateParams = true
	if len(collation) != 0 {
		writes.Collation = collation
	}

	reads := mysql.NewConfig()
	reads.User = rUser
	reads.Passwd = rPass
	reads.DBName = rSchema
	reads.Net = "tcp"
	reads.Addr = net.JoinHostPort(rHost, strconv.Itoa(rPort))
	if timeZone != nil {
		reads.Loc = timeZone
	}
	reads.ParseTime = true
	reads.InterpolateParams = true
	if len(collation) != 0 {
		reads.Collation = collation
	}

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
	db.MaxInsertSize = new(synct[int])
	db.MaxInsertSize.Set(writesDSN.MaxAllowedPacket)

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

	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	l, err := config.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}
	db.Logger = l.Named("cool-mysql")

	return
}

// Reconnect creates new connection(s) for writes and reads
// and replaces the existing connections with the new ones
func (db *Database) Reconnect() error {
	new, err := NewFromDSN(db.WritesDSN, db.ReadsDSN)
	if err != nil {
		return fmt.Errorf("failed to reconnect: %w", err)
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
		db:   db,
		conn: db.Writes,
	}
}

func (db *Database) I() *Inserter {
	return db.DefaultInsertOptions()
}

func (db *Database) Insert(insert string, source any) error {
	return db.I().Insert(insert, source)
}

func (db *Database) InsertContext(ctx context.Context, insert string, source any) error {
	return db.I().InsertContext(ctx, insert, source)
}

func (db *Database) InsertReads(insert string, source any) error {
	return db.I().SetExecutor(db.Reads).Insert(insert, source)
}

func (db *Database) InsertReadsContext(ctx context.Context, insert string, source any) error {
	return db.I().SetExecutor(db.Reads).InsertContext(ctx, insert, source)
}

func (db *Database) InsertUniquely(insertQuery string, uniqueColumns []string, active string, args any) error {
	return db.I().InsertUniquely(insertQuery, uniqueColumns, active, args)
}

// ExecContext executes a query and nothing more
func (db *Database) ExecContextResult(ctx context.Context, query string, params ...any) (sql.Result, error) {
	return db.exec(db.Writes, ctx, query, params...)
}

// ExecContext executes a query and nothing more
func (db *Database) ExecContext(ctx context.Context, query string, params ...any) error {
	_, err := db.ExecContextResult(ctx, query, params...)
	return err
}

// ExecResult executes a query and nothing more
func (db *Database) ExecResult(query string, params ...any) (sql.Result, error) {
	return db.ExecContextResult(context.Background(), query, params...)
}

// Exec executes a query and nothing more
func (db *Database) Exec(query string, params ...any) error {
	_, err := db.ExecContextResult(context.Background(), query, params...)
	return err
}

func (db *Database) Select(dest any, q string, cache time.Duration, params ...any) error {
	return query(db, db.Reads, context.Background(), dest, q, cache, params...)
}

func (db *Database) SelectRows(q string, cache time.Duration, params ...any) (Rows, error) {
	var rows Rows
	err := query(db, db.Reads, context.Background(), &rows, q, cache, params...)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (db *Database) SelectContext(ctx context.Context, dest any, q string, cache time.Duration, params ...any) error {
	return query(db, db.Reads, ctx, dest, q, cache, params...)
}

func (db *Database) SelectWrites(dest any, q string, cache time.Duration, params ...any) error {
	return query(db, db.Writes, context.Background(), dest, q, cache, params...)
}

func (db *Database) SelectWritesContext(ctx context.Context, dest any, q string, cache time.Duration, params ...any) error {
	return query(db, db.Writes, ctx, dest, q, cache, params...)
}

func (db *Database) SelectJSON(dest any, query string, cache time.Duration, params ...any) error {
	return db.SelectJSONContext(context.Background(), dest, query, cache, params...)
}

func (db *Database) SelectJSONContext(ctx context.Context, dest any, query string, cache time.Duration, params ...any) error {
	var store struct {
		JSON []byte
	}

	err := db.SelectContext(ctx, &store, query, cache, params...)
	if err != nil {
		return err
	}

	err = json.Unmarshal(store.JSON, dest)
	if err != nil {
		return err
	}

	return nil
}

// Exists efficiently checks if there are any rows in the given query using the `Reads` connection
func (db *Database) Exists(query string, cache time.Duration, params ...any) (bool, error) {
	return exists(db, db.Reads, context.Background(), query, cache, params...)
}

// ExistsContext efficiently checks if there are any rows in the given query using the `Reads` connection
func (db *Database) ExistsContext(ctx context.Context, query string, cache time.Duration, params ...any) (bool, error) {
	return exists(db, db.Reads, ctx, query, cache, params...)
}

// ExistsWrites efficiently checks if there are any rows in the given query using the `Writes` connection
func (db *Database) ExistsWrites(query string, cache time.Duration, params ...any) (bool, error) {
	return exists(db, db.Writes, context.Background(), query, cache, params...)
}

// ExistsWritesContext efficiently checks if there are any rows in the given query using the `Writes` connection
func (db *Database) ExistsWritesContext(ctx context.Context, query string, cache time.Duration, params ...any) (bool, error) {
	return exists(db, db.Writes, ctx, query, cache, params...)
}
