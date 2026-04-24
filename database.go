package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"text/template"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
)

// Database is a cool MySQL connection
type Database struct {
	Writes handlerWithContext
	Reads  *sql.DB

	WritesDSN string
	ReadsDSN  string

	Log              LogFunc
	Finished         FinishedFunc
	HandleCacheError HandleCacheError

	die bool

	MaxInsertSize *synct[int]

	// MaxExecutionTime caps the total time (including retries) a single
	// query is allowed to run before giving up. Zero means no cap.
	// Initialized from the package-level MaxExecutionTime var (Lambda-
	// oriented 27s default); long-running processes should set this to 0
	// or a larger value that matches their workload.
	MaxExecutionTime time.Duration

	// MaxConnectionTime is the SetConnMaxLifetime value applied to both
	// pools at construction. Zero means connections are reused forever.
	// Initialized from the package-level MaxConnectionTime var. Change at
	// runtime with SetMaxConnectionTime so the underlying pools pick up
	// the new value.
	MaxConnectionTime time.Duration

	cache  Cache
	locker Locker

	// DisableForeignKeyChecks only affects foreign keys for transactions
	DisableForeignKeyChecks bool

	testMx *sync.Mutex

	Logger                      Logger
	DisableUnusedColumnWarnings bool

	tmplFuncs   template.FuncMap
	valuerFuncs map[reflect.Type]reflect.Value

	// forceDualPool, when set, tells Reconnect to rebuild two independent
	// pools from WritesDSN even if WritesDSN == ReadsDSN. Set by
	// NewFromDSNDualPool. Not exported; external callers should pick the
	// constructor that matches their intent.
	forceDualPool bool
}

// Clone returns a copy of the db with the same connections
// but with an empty query log
func (db *Database) Clone() *Database {
	clone := *db
	return &clone
}

// applyTimeZoneToConfig writes the current Loc offset to
// Params["time_zone"] as a SQL-quoted offset string (e.g. "'+00:00'").
// go-sql-driver passes Params verbatim into the `SET <k> = <v>` it runs
// at connection init, so the single quotes must be part of the value.
//
// This is wired via BeforeConnect (see openPool) so it runs on every
// new conn. Per-conn recomputation matters for Locs with DST — a pool
// opened in EST (-05:00) would otherwise keep handing out `-05:00` to
// every new conn after the DST transition to EDT (-04:00). See #152.
//
// No-op if Loc is nil or Params["time_zone"] is already set: caller
// intent wins over the Loc-derived default.
func applyTimeZoneToConfig(cfg *mysql.Config) {
	if cfg.Loc == nil || cfg.Params["time_zone"] != "" {
		return
	}
	_, offset := time.Now().In(cfg.Loc).Zone()
	tzStr := time.Unix(0, 0).In(time.FixedZone("", offset)).Format("-07:00")

	if cfg.Params == nil {
		cfg.Params = make(map[string]string)
	}
	cfg.Params["time_zone"] = "'" + tzStr + "'"
}

func (db *Database) WriterWithSubdir(subdir string) *Database {
	db = db.Clone()
	db.Writes = &sqlWriter{
		path:   filepath.Join(db.Writes.(*sqlWriter).path, subdir),
		index:  new(synct[int]),
		logger: db.Logger,
	}

	return db
}

// EnableRedis enables redis cache for select queries with cache times
// with the given connection information
func (db *Database) EnableRedis(redisClient redis.UniversalClient) *Database {
	db.UseCache(NewRedisCache(redisClient))
	return db
}

// EnableMemcache configures memcached as the cache backend.
func (db *Database) EnableMemcache(mc *memcache.Client) *Database {
	db.UseCache(NewMemcacheCache(mc))
	return db
}

// UseCache sets a custom cache implementation.
func (db *Database) UseCache(c Cache) *Database {
	db.cache = c
	if l, ok := c.(Locker); ok {
		db.locker = l
	}
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

// HandleCacheError is executed on a cache error so it can be handled by the user.
// Returning a non-nil error will abort execution.
type HandleCacheError func(err error) error

// HandleRedisError is kept for backwards compatibility.
type HandleRedisError = HandleCacheError

func (db *Database) callLog(detail LogDetail) {
	if db.Log != nil {
		db.Log(detail)
	}
}

// New creates a new Database
func New(wUser, wPass, wSchema, wHost string, wPort int,
	rUser, rPass, rSchema, rHost string, rPort int,
	collation string, timeZone *time.Location,
) (db *Database, err error) {
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

// sqlOpenFunc is the function openPool uses to build a *sql.DB from a
// parsed mysql.Config. It's a package-level variable so tests can
// substitute a fake that returns a sqlmock-backed pool instead of
// hitting a real MySQL server.
var sqlOpenFunc = func(cfg *mysql.Config) (*sql.DB, error) {
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(connector), nil
}

// openPool opens a MySQL pool against the given DSN, pings it, and
// configures the connection lifetime. A BeforeConnect hook is wired so
// go-sql-driver runs `SET time_zone = <Loc offset>` on every new conn
// the pool opens — see applyTimeZoneToConfig. On any error after Open
// the pool is closed so the caller doesn't have to. connType is used
// only in error messages (e.g. "writes", "reads"). connMaxLifetime is
// passed straight to SetConnMaxLifetime — zero means "reuse forever".
func openPool(dsn, connType string, connMaxLifetime time.Duration) (*sql.DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s DSN: %w", connType, err)
	}

	// The driver hands BeforeConnect a fresh Clone() of cfg for every
	// new conn, so mutating c here scopes to that one conn.
	_ = cfg.Apply(mysql.BeforeConnect(func(_ context.Context, c *mysql.Config) error {
		applyTimeZoneToConfig(c)
		return nil
	}))

	conn, err := sqlOpenFunc(cfg)
	if err != nil {
		return nil, err
	}

	if err := conn.Ping(); err != nil {
		_ = conn.Close()
		return nil, err
	}

	conn.SetConnMaxLifetime(connMaxLifetime)
	return conn, nil
}

// NewFromDSN creates a new Database from DSN strings for the writes and
// reads connections.
//
// If writes == reads (same string), Reads and Writes share a single
// *sql.DB pool — useful for callers without a read replica who are fine
// with one pool. If you want two independent pools against the same DSN
// (to avoid reads and writes contending for connections under concurrent
// load), use NewFromDSNDualPool instead.
func NewFromDSN(writes, reads string) (db *Database, err error) {
	db = new(Database)
	db.testMx = new(sync.Mutex)
	db.Logger = DefaultLogger()
	db.MaxExecutionTime = MaxExecutionTime
	db.MaxConnectionTime = MaxConnectionTime

	writesConn, err := openPool(writes, "writes", db.MaxConnectionTime)
	if err != nil {
		return nil, err
	}
	db.WritesDSN = writes
	db.Writes = writesConn

	writesDSN, err := mysql.ParseDSN(writes)
	if err != nil {
		_ = writesConn.Close()
		return nil, fmt.Errorf("failed to parse writes DSN: %w", err)
	}
	db.MaxInsertSize = new(synct[int])
	db.MaxInsertSize.Set(writesDSN.MaxAllowedPacket)

	if reads != writes {
		readsConn, err := openPool(reads, "reads", db.MaxConnectionTime)
		if err != nil {
			_ = writesConn.Close()
			return nil, err
		}
		db.ReadsDSN = reads
		db.Reads = readsConn
	} else {
		db.ReadsDSN = writes
		db.Reads = writesConn
	}

	return db, nil
}

// NewFromDSNDualPool creates a new Database with two independent
// connection pools backed by the same DSN.
//
// Use this when you don't have a read replica but still want Reads and
// Writes to use separate pools. The dual-pool design exists to keep reads
// and writes from starving each other under concurrent load, which is
// defeated by NewFromDSN(dsn, dsn) because equal DSN strings collapse to
// a single shared pool.
func NewFromDSNDualPool(dsn string) (db *Database, err error) {
	db = new(Database)
	db.testMx = new(sync.Mutex)
	db.Logger = DefaultLogger()
	db.forceDualPool = true
	db.MaxExecutionTime = MaxExecutionTime
	db.MaxConnectionTime = MaxConnectionTime

	writesConn, err := openPool(dsn, "writes", db.MaxConnectionTime)
	if err != nil {
		return nil, err
	}
	db.WritesDSN = dsn
	db.Writes = writesConn

	parsed, err := mysql.ParseDSN(dsn)
	if err != nil {
		_ = writesConn.Close()
		return nil, fmt.Errorf("failed to parse DSN: %w", err)
	}
	db.MaxInsertSize = new(synct[int])
	db.MaxInsertSize.Set(parsed.MaxAllowedPacket)

	readsConn, err := openPool(dsn, "reads", db.MaxConnectionTime)
	if err != nil {
		_ = writesConn.Close()
		return nil, err
	}
	db.ReadsDSN = dsn
	db.Reads = readsConn

	return db, nil
}

// NewFromConn creates a new Database given existing *sql.DB connections.
// It will query the writesConn for @@max_allowed_packet to set MaxInsertSize.
// If readsConn == writesConn, both Reads and Writes share the same pool.
func NewFromConn(writesConn, readsConn *sql.DB) (*Database, error) {
	db := new(Database)
	db.testMx = new(sync.Mutex)
	db.MaxExecutionTime = MaxExecutionTime
	db.MaxConnectionTime = MaxConnectionTime

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
	writesConn.SetConnMaxLifetime(db.MaxConnectionTime)

	// 3) Wire up Reads (may be same as Writes)
	db.Reads = readsConn
	if readsConn == writesConn {
		db.ReadsDSN = ""
	} else {
		db.ReadsDSN = ""
		readsConn.SetConnMaxLifetime(db.MaxConnectionTime)
	}

	// 4) Logger setup (identical to NewFromDSN)
	db.Logger = DefaultLogger()

	return db, nil
}

func NewLocalWriter(path string) (*Database, error) {
	db := new(Database)
	sqlWriter := &sqlWriter{
		path:   path,
		index:  new(synct[int]),
		logger: DefaultLogger(),
	}
	db.Writes = sqlWriter

	db.testMx = new(sync.Mutex)
	db.MaxExecutionTime = MaxExecutionTime
	db.MaxConnectionTime = MaxConnectionTime

	db.MaxInsertSize = new(synct[int])
	db.MaxInsertSize.Set(1 << 20)

	db.Logger = DefaultLogger()

	return db, nil
}

func NewWriter(w io.Writer) (*Database, error) {
	db := new(Database)
	writer := &writer{
		Writer: w,
	}
	db.Writes = writer

	db.testMx = new(sync.Mutex)
	db.MaxExecutionTime = MaxExecutionTime
	db.MaxConnectionTime = MaxConnectionTime

	db.MaxInsertSize = new(synct[int])
	db.MaxInsertSize.Set(1 << 20)

	db.Logger = DefaultLogger()

	return db, nil
}

// SetMaxConnectionTime updates db.MaxConnectionTime and applies the new
// value to the underlying write and read pools via SetConnMaxLifetime.
// Pass 0 for "reuse connections forever". Assigning the field directly
// won't affect already-opened pools — go through this setter to take
// effect at runtime.
func (db *Database) SetMaxConnectionTime(d time.Duration) {
	db.MaxConnectionTime = d
	if w, ok := db.Writes.(*sql.DB); ok {
		w.SetConnMaxLifetime(d)
	}
	if db.Reads != nil {
		db.Reads.SetConnMaxLifetime(d)
	}
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

// Close closes the underlying connection pool(s). Safe to call on a
// *Database whose Writes handler is not a *sql.DB (e.g. sqlWriter /
// writer) — those handlers are treated as no-ops. When Reads and Writes
// share the same *sql.DB pointer, it is closed only once. After Close
// the Database is unusable; call Reconnect to rebuild it.
func (db *Database) Close() error {
	var errs []error

	writesDB, _ := db.Writes.(*sql.DB)

	if db.Reads != nil {
		if err := db.Reads.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close reads: %w", err))
		}
	}

	if writesDB != nil && writesDB != db.Reads {
		if err := writesDB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close writes: %w", err))
		}
	}

	return errors.Join(errs...)
}

// Reconnect creates new connection(s) for writes and reads
// and replaces the existing connections with the new ones.
// The old pools are closed before being replaced; any close error
// is logged as a warning rather than returned, because Reconnect is
// typically called when the old pools are already broken and the new
// ones are what the caller needs to move forward with.
//
// Any per-instance overrides to MaxConnectionTime are re-applied to
// the new pools. The fresh Database built by the constructors
// otherwise carries the package-level defaults, which would silently
// revert an override set via SetMaxConnectionTime.
func (db *Database) Reconnect() error {
	var fresh *Database
	var err error
	if db.forceDualPool {
		fresh, err = NewFromDSNDualPool(db.WritesDSN)
	} else {
		fresh, err = NewFromDSN(db.WritesDSN, db.ReadsDSN)
	}
	if err != nil {
		return fmt.Errorf("failed to reconnect: %w", err)
	}

	if closeErr := db.Close(); closeErr != nil {
		db.Logger.Warn("failed to close old pools during reconnect", "err", closeErr)
	}

	db.Writes = fresh.Writes
	db.Reads = fresh.Reads

	if freshW, ok := fresh.Writes.(*sql.DB); ok {
		freshW.SetConnMaxLifetime(db.MaxConnectionTime)
	}
	if fresh.Reads != nil && fresh.Reads != fresh.Writes {
		fresh.Reads.SetConnMaxLifetime(db.MaxConnectionTime)
	}

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
