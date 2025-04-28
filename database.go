package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"text/template"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Database is a cool MySQL connection
type Database struct {
	Writes handlerWithContext
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

	tmplFuncs   template.FuncMap
	valuerFuncs map[reflect.Type]reflect.Value
}

// Clone returns a copy of the db with the same connections
// but with an empty query log
func (db *Database) Clone() *Database {
	clone := *db
	return &clone
}

func (db *Database) WriterWithSubdir(subdir string) *Database {
	db = db.Clone()
	db.Writes = &sqlWriter{
		path:  filepath.Join(db.Writes.(*sqlWriter).path, subdir),
		index: new(synct[int]),
	}

	return db
}

// EnableRedis enables redis cache for select queries with cache times
// with the given connection information
func (db *Database) EnableRedis(redisClient redis.UniversalClient) *Database {
	db.redis = redisClient
	db.rs = redsync.New(goredis.NewPool(db.redis))

	return db
}

type LogDetail struct {
	Query        string
	Params       Params
	Duration     time.Duration
	CacheHit     bool
	Tx           *sql.Tx
	RowsAffected int64
	Attempt      int
	Error        error
}

// LogFunc is called after the query executes
type LogFunc func(detail LogDetail)

// FinishedFunc executes after all rows have been processed,
// including being read from the channel if used
type FinishedFunc func(cached bool, replacedQuery string, params Params, execDuration time.Duration, fetchDuration time.Duration)

// HandleRedisError is executed on a redis error, so it can be handled by the user
// return false to let the function return the error, or return to let the function continue executing despite the redis error
type HandleRedisError func(err error) error

func (db *Database) callLog(detail LogDetail) {
	if db.Log != nil {
		db.Log(detail)
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
	writes.ClientFoundRows = true
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
	reads.ClientFoundRows = true
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
	var writesConn *sql.DB
	writesConn, err = sql.Open("mysql", writes)
	if err != nil {
		return nil, err
	}

	err = writesConn.Ping()
	if err != nil {
		return nil, err
	}

	writesDSN, _ := mysql.ParseDSN(writes)
	db.MaxInsertSize = new(synct[int])
	db.MaxInsertSize.Set(writesDSN.MaxAllowedPacket)

	writesConn.SetConnMaxLifetime(MaxConnectionTime)

	db.Writes = writesConn

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
		db.Reads = writesConn
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

// NewFromConn creates a new Database given existing *sql.DB connections.
// It will query the writesConn for @@max_allowed_packet to set MaxInsertSize.
// If readsConn == writesConn, both Reads and Writes share the same pool.
func NewFromConn(writesConn, readsConn *sql.DB) (*Database, error) {
	db := new(Database)
	db.testMx = new(sync.Mutex)

	// 1) Pull the server's max_allowed_packet value
	var maxPacket int64
	if err := writesConn.
		QueryRow("SELECT @@max_allowed_packet").
		Scan(&maxPacket); err != nil {
		return nil, fmt.Errorf("failed to query max_allowed_packet: %w", err)
	}
	db.MaxInsertSize = new(synct[int])
	db.MaxInsertSize.Set(int(maxPacket))

	// 2) Wire up Writes
	db.Writes = writesConn
	db.WritesDSN = "" // not known from *sql.DB
	writesConn.SetConnMaxLifetime(MaxConnectionTime)

	// 3) Wire up Reads (may be same as Writes)
	db.Reads = readsConn
	if readsConn == writesConn {
		db.ReadsDSN = ""
	} else {
		db.ReadsDSN = ""
		readsConn.SetConnMaxLifetime(MaxConnectionTime)
	}

	// 4) Logger setup (identical to NewFromDSN)
	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	l, err := config.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}
	db.Logger = l.Named("cool-mysql")

	return db, nil
}

func NewLocalWriter(path string) (*Database, error) {
	db := new(Database)
	sqlWriter := &sqlWriter{
		path:  path,
		index: new(synct[int]),
	}
	db.Writes = sqlWriter

	db.testMx = new(sync.Mutex)

	db.MaxInsertSize = new(synct[int])
	db.MaxInsertSize.Set(1 << 20)

	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	l, err := config.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}
	db.Logger = l.Named("cool-mysql")

	return db, nil
}

// AddTemplateFuncs adds template functions to the database
func (db *Database) AddTemplateFuncs(funcs template.FuncMap) {
	if db.tmplFuncs == nil {
		db.tmplFuncs = make(template.FuncMap)
	}

	for k, v := range funcs {
		db.tmplFuncs[k] = v
	}
}

func (db *Database) AddValuerFuncs(funcs ...any) {
	for _, f := range funcs {
		r := reflect.ValueOf(f)
		rt := r.Type()
		if !isValuerFunc(rt) {
			panic(fmt.Errorf("invalid valuer func: %T", f))
		}

		if db.valuerFuncs == nil {
			db.valuerFuncs = make(map[reflect.Type]reflect.Value)
		}

		db.valuerFuncs[rt.In(0)] = r
	}
}

// Reconnect creates new connection(s) for writes and reads
// and replaces the existing connections with the new ones
func (db *Database) Reconnect() error {
	new, err := NewFromDSN(db.WritesDSN, db.ReadsDSN)
	if err != nil {
		return fmt.Errorf("failed to reconnect: %w", err)
	}

	db.Writes = new.Writes
	db.Reads = new.Reads

	return nil
}

// Test pings both writes and reads connection and if either fail
// reconnects both connections
func (db *Database) Test() error {
	db.testMx.Lock()
	defer db.testMx.Unlock()

	if writesConn, ok := db.Writes.(*sql.DB); ok {
		if writesConn.Ping() != nil {
			return db.Reconnect()
		}
	}

	if db.Reads != nil {
		if db.Reads.Ping() != nil {
			return db.Reconnect()
		}
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

// ExecContext executes a query and nothing more
func (db *Database) ExecContextResult(ctx context.Context, query string, params ...any) (sql.Result, error) {
	return db.exec(db.Writes, ctx, nil, true, query, params...)
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
	return db.query(db.Reads, context.Background(), dest, q, cache, params...)
}

func (db *Database) SelectRows(q string, cache time.Duration, params ...any) (Rows, error) {
	var rows Rows
	err := db.query(db.Reads, context.Background(), &rows, q, cache, params...)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (db *Database) SelectContext(ctx context.Context, dest any, q string, cache time.Duration, params ...any) error {
	return db.query(db.Reads, ctx, dest, q, cache, params...)
}

func (db *Database) SelectWrites(dest any, q string, cache time.Duration, params ...any) error {
	return db.query(db.Writes, context.Background(), dest, q, cache, params...)
}

func (db *Database) SelectWritesContext(ctx context.Context, dest any, q string, cache time.Duration, params ...any) error {
	return db.query(db.Writes, ctx, dest, q, cache, params...)
}

func (db *Database) SelectJSON(dest any, query string, cache time.Duration, params ...any) error {
	return db.SelectJSONContext(context.Background(), dest, query, cache, params...)
}

func (db *Database) SelectJSONContext(ctx context.Context, dest any, query string, cache time.Duration, params ...any) error {
	var j []byte
	err := db.SelectContext(ctx, &j, query, cache, params...)
	if err != nil {
		return err
	}

	err = json.Unmarshal(j, dest)
	if err != nil {
		return err
	}

	return nil
}

// Exists efficiently checks if there are any rows in the given query using the `Reads` connection
func (db *Database) Exists(query string, cache time.Duration, params ...any) (bool, error) {
	return db.exists(db.Reads, context.Background(), query, cache, params...)
}

// ExistsContext efficiently checks if there are any rows in the given query using the `Reads` connection
func (db *Database) ExistsContext(ctx context.Context, query string, cache time.Duration, params ...any) (bool, error) {
	return db.exists(db.Reads, ctx, query, cache, params...)
}

// ExistsWrites efficiently checks if there are any rows in the given query using the `Writes` connection
func (db *Database) ExistsWrites(query string, cache time.Duration, params ...any) (bool, error) {
	return db.exists(db.Writes, context.Background(), query, cache, params...)
}

// ExistsWritesContext efficiently checks if there are any rows in the given query using the `Writes` connection
func (db *Database) ExistsWritesContext(ctx context.Context, query string, cache time.Duration, params ...any) (bool, error) {
	return db.exists(db.Writes, ctx, query, cache, params...)
}

func (db *Database) Upsert(insert string, uniqueColumns, updateColumns []string, where string, source any) error {
	return db.I().Upsert(insert, uniqueColumns, updateColumns, where, source)
}

func (db *Database) UpsertContext(ctx context.Context, insert string, uniqueColumns, updateColumns []string, where string, source any) error {
	return db.I().UpsertContext(ctx, insert, uniqueColumns, updateColumns, where, source)
}

func (db *Database) InterpolateParams(query string, params ...any) (replacedQuery string, normalizedParams Params, err error) {
	return InterpolateParams(query, db.tmplFuncs, db.valuerFuncs, params...)
}

func (db *Database) interpolateParams(query string, params ...any) (replacedQuery string, normalizedParams Params, err error) {
	return interpolateParams(query, db.tmplFuncs, db.valuerFuncs, params...)
}
