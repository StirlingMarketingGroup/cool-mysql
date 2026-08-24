package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
)

// poolRole is where a statement's effects land, stated by the API that
// issued it rather than inferred from pool identity — Reads and Writes can be
// the same *sql.DB, so pointer equality cannot tell InsertReads from Insert.
// Only poolWriter statements count as read-your-writes.
type poolRole int

const (
	poolUnknown poolRole = iota // SetExecutor escape hatch: never marks
	poolWriter
	poolReader
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

	// ReadYourWritesWindow is how long after a durable writer-pool write
	// Select/Exists/Count should hit Writes instead of Reads so a lagging
	// replica cannot hide those rows. Zero disables. Seeded from the
	// package-level ReadYourWritesWindow at construction.
	ReadYourWritesWindow time.Duration

	// lastWrite is the last durable writer-pool write, as nanoseconds of
	// monotonic time since rywEpoch — not a wall-clock stamp, so a host
	// clock jump can neither expire the pin early nor extend it. A pointer
	// so Clone() (clone := *db) shares it: a clone is a copy of the same
	// database, not a new session, and a clone made after a write must
	// still see that write. Zero means "never wrote" (any real write lands
	// strictly after the epoch). Nil is tolerated (zero-value Database →
	// no pinning).
	lastWrite *atomic.Int64

	cache  Cache
	locker Locker

	// Loc is the time.Location used to format time.Time values when
	// building SQL literals. It is sourced from the DSN's `loc`
	// parameter at construction time (defaulting to time.UTC) and is
	// matched by go-sql-driver's own parsing on the read side, so a
	// write/read round-trip preserves the original instant —
	// including across DST. Constructors that take no DSN default Loc
	// to time.UTC. See #157.
	//
	// The BeforeConnect hook installed by openPool also pins
	// @@session.time_zone to Loc's current offset on every new conn,
	// so TIMESTAMP columns and NOW() / CURRENT_TIMESTAMP stay
	// consistent with the naive Loc-formatted DATETIME literals the
	// marshaller emits. The unavoidable edge case is a single
	// connection that lives across a DST transition: TIMESTAMP writes
	// through that one conn drift by an hour until the pool cycles it
	// (controlled by MaxConnectionTime). DATETIME columns are
	// unaffected — they use Loc on both sides and ignore session.tz.
	Loc *time.Location

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

// rywEpoch anchors lastWrite to the monotonic clock: time.Since(rywEpoch)
// never observes wall-clock jumps.
var rywEpoch = time.Now()

func (db *Database) markWrite() {
	if db.lastWrite == nil {
		return
	}
	// CAS-max rather than a plain Store: a goroutine preempted between
	// sampling the clock and storing must not drag the marker backward past
	// a newer write's stamp, shortening that write's window.
	now := int64(time.Since(rywEpoch))
	for {
		prev := db.lastWrite.Load()
		if now <= prev || db.lastWrite.CompareAndSwap(prev, now) {
			return
		}
	}
}

// readYourWrites picks the pool for a plain read. While a durable write on
// this Database (or a Clone of it) is inside ReadYourWritesWindow it returns
// the Writes pool and a zero cache duration, so neither a lagging replica nor
// a cached pre-write result can hide those rows. Writes must be a *sql.DB —
// file/io.Writer sinks cannot serve reads.
func (db *Database) readYourWrites(cache time.Duration) (*sql.DB, time.Duration) {
	if db.ReadYourWritesWindow <= 0 || db.lastWrite == nil {
		return db.Reads, cache
	}
	ts := db.lastWrite.Load()
	if ts == 0 || time.Since(rywEpoch)-time.Duration(ts) >= db.ReadYourWritesWindow {
		return db.Reads, cache
	}
	w, ok := db.Writes.(*sql.DB)
	if !ok {
		return db.Reads, cache
	}
	return w, 0
}

// applyTimeZoneToConfig writes cfg.Loc's current offset to
// Params["time_zone"] as a SQL-quoted offset string (e.g. "'-05:00'").
// go-sql-driver passes Params verbatim into the `SET <k> = <v>` it
// runs at conn init, so the single quotes must be part of the value.
//
// The marshaller emits time.Time as a naive datetime literal formatted
// in cfg.Loc (see marshalAppend's time.Time case), which makes DATETIME
// columns round-trip correctly across DST regardless of what
// session.time_zone is — that was the #157 fix. session.time_zone is
// still load-bearing for TIMESTAMP columns and NOW() /
// CURRENT_TIMESTAMP, though: MySQL uses it to convert the naive literal
// into the UTC instant it stores for TIMESTAMP, and to format the wall
// clock NOW() returns. Pinning it to match cfg.Loc's current offset
// keeps all three (DATETIME, TIMESTAMP, NOW()) consistent under a
// non-UTC Loc, which is what most callers actually want and what was
// broken when a previous revision of this hook hard-coded '+00:00'.
//
// The remaining edge case is a connection that lives across a DST
// boundary: its session.time_zone is the offset at conn-open time, so
// new TIMESTAMP writes through that conn drift by an hour until the
// pool cycles it. BeforeConnect re-fires per conn (see openPool), so
// short-lived conns and pools with a finite MaxConnectionTime
// converge. Fully eliminating that drift requires a named-zone
// session.time_zone, which MySQL only supports when the server's
// `mysql.time_zone_name` table is populated — out of our control.
//
// No-op if cfg.Loc is nil or Params["time_zone"] is already set:
// caller intent wins over the Loc-derived default.
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

// applyNetTimeoutsToConfig copies the package-level ReadTimeout /
// WriteTimeout / DialTimeout defaults onto cfg, but only for fields the DSN
// left at zero — an explicit *non-zero* DSN value (readTimeout= /
// writeTimeout= / timeout=) always wins, matching applyTimeZoneToConfig's
// caller-intent precedence. (A parsed value of zero is indistinguishable
// from an omitted one, so an explicit DSN zero can't override a non-zero
// default — see ReadTimeout's doc.) All three default to 0 (off), so this
// is a no-op unless a caller opts in. See #172 for why these matter.
func applyNetTimeoutsToConfig(cfg *mysql.Config) {
	if cfg.ReadTimeout == 0 && ReadTimeout > 0 {
		cfg.ReadTimeout = ReadTimeout
	}
	if cfg.WriteTimeout == 0 && WriteTimeout > 0 {
		cfg.WriteTimeout = WriteTimeout
	}
	if cfg.Timeout == 0 && DialTimeout > 0 {
		cfg.Timeout = DialTimeout
	}
}

// keepAliveDialer builds a *net.Dialer carrying the package-level keepalive
// settings (read at dial time so a value set before pool open applies to every
// conn the pool dials). go-sql-driver applies cfg.Timeout as a ctx deadline
// around the dial, so the dialer itself sets no Timeout. Zero-value when
// TCPKeepAlive is unset — same semantics as the pre-DialFunc keepalive path.
func keepAliveDialer() *net.Dialer {
	d := &net.Dialer{}
	if TCPKeepAlive > 0 {
		d.KeepAliveConfig = net.KeepAliveConfig{
			Enable:   true,
			Idle:     TCPKeepAlive,
			Interval: TCPKeepAlive,
			Count:    TCPKeepAliveCount,
		}
	}
	return d
}

// dialContextFunc is the go-sql-driver Config.DialFunc signature. The
// retrying wrapper takes one as an injected inner dial so tests can drive
// the loop without a real network.
type dialContextFunc func(ctx context.Context, network, addr string) (net.Conn, error)

const (
	// dialRetryFastFailure is the threshold under which a failed dial is
	// treated as a fast failure (connection-refused during RDS failover is
	// ~1ms) and we pause before redialing so the loop can't spin.
	dialRetryFastFailure = time.Second
	dialRetryPause       = 250 * time.Millisecond
)

// applyDialerToConfig installs a per-config DialFunc that composes keepalive
// settings with optional dial retry. cfg.Net is left as-is (tcp/tcp4/tcp6)
// so DSN FormatDSN/ParseDSN round-trips stay clean and other go-sql-driver
// users in the process are unaffected (unlike the previous registered-net
// approach).
//
// DialFunc is set when TCPKeepAlive > 0 or when dial retry is active
// (DialAttemptTimeout > 0 AND cfg.Timeout > 0 — the effective total budget
// after applyNetTimeoutsToConfig, so a DSN timeout= counts). When neither
// knob is on, DialFunc is left nil (stock driver path, zero behavior change).
// TCP only — unix-socket pools are left untouched. See TCPKeepAlive,
// DialAttemptTimeout, and #174.
func applyDialerToConfig(cfg *mysql.Config) {
	switch cfg.Net {
	case "tcp", "tcp4", "tcp6":
	default:
		return
	}
	retry := dialRetryActive(cfg)
	if TCPKeepAlive <= 0 && !retry {
		return
	}
	cfg.DialFunc = wrapDial(func(ctx context.Context, network, addr string) (net.Conn, error) {
		return keepAliveDialer().DialContext(ctx, network, addr)
	}, retry)
}

// dialRetryActive reports whether this pool should retry failed dials.
// Both a per-attempt cap and a total budget are required: without
// cfg.Timeout the pool's background-opener ctx can be unbounded.
func dialRetryActive(cfg *mysql.Config) bool {
	return DialAttemptTimeout > 0 && cfg.Timeout > 0
}

// wrapDial returns inner unchanged when retry is off, so a keepalive-only
// DialFunc does exactly one attempt with no extra deadline. When retry is
// on, each call runs retryingDial around inner.
func wrapDial(inner dialContextFunc, retry bool) dialContextFunc {
	if !retry {
		return inner
	}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return retryingDial(ctx, network, addr, inner)
	}
}

// retryingDial redials on every failure until the outer ctx (the driver's
// cfg.Timeout budget) is done. Each attempt is additionally bounded by
// DialAttemptTimeout. Fast failures sleep dialRetryPause (bounded by the
// outer ctx) so a connection-refused loop during failover can't spin.
func retryingDial(ctx context.Context, network, addr string, inner dialContextFunc) (net.Conn, error) {
	for {
		attemptCtx := ctx
		var cancel context.CancelFunc
		if DialAttemptTimeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, DialAttemptTimeout)
		}
		start := time.Now()
		conn, err := inner(attemptCtx, network, addr)
		if cancel != nil {
			cancel()
		}
		if err == nil {
			return conn, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("%w (last dial error: %w)", ctxErr, err)
		}
		if time.Since(start) < dialRetryFastFailure {
			timer := time.NewTimer(dialRetryPause)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, fmt.Errorf("%w (last dial error: %w)", ctx.Err(), err)
			case <-timer.C:
			}
		}
	}
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

	// Apply the package-level socket timeouts (off by default) before
	// building the connector so a half-open conn surfaces as
	// mysql.ErrInvalidConn for the retry path instead of hanging the read
	// to the caller's deadline. See #172.
	applyNetTimeoutsToConfig(cfg)

	// Install the per-config DialFunc (off by default) composing keepalive
	// with optional dial retry. cfg.Net stays "tcp". See applyDialerToConfig.
	applyDialerToConfig(cfg)

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

// newDatabase seeds the per-instance state every constructor needs so a new
// constructor cannot silently miss a package-default field.
func newDatabase() *Database {
	return &Database{
		testMx:               new(sync.Mutex),
		MaxExecutionTime:     MaxExecutionTime,
		MaxConnectionTime:    MaxConnectionTime,
		ReadYourWritesWindow: ReadYourWritesWindow,
		lastWrite:            new(atomic.Int64),
	}
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
	db = newDatabase()
	db.Logger = DefaultLogger()

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
	db.Loc = locOrUTC(writesDSN.Loc)

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
	db = newDatabase()
	db.Logger = DefaultLogger()
	db.forceDualPool = true

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
	db.Loc = locOrUTC(parsed.Loc)

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
//
// Loc is set to time.UTC because we don't have a DSN to read it from. If
// the caller built the pools with a non-UTC loc, set db.Loc explicitly so
// time.Time literals format in a location matching the read-side parser.
//
// The ReadTimeout / WriteTimeout / DialTimeout / DialAttemptTimeout /
// TCPKeepAlive defaults are NOT applied here — the pools are already
// built, so their socket timeouts and DialFunc are fixed by whatever
// DSN/connector the caller used. Bake readTimeout= / timeout= into that
// DSN if you want the half-open recovery described on ReadTimeout (#172)
// or the dial-retry described on DialAttemptTimeout.
func NewFromConn(writesConn, readsConn *sql.DB) (*Database, error) {
	db := newDatabase()
	db.Loc = time.UTC

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
	db := newDatabase()
	sqlWriter := &sqlWriter{
		path:   path,
		index:  new(synct[int]),
		logger: DefaultLogger(),
	}
	db.Writes = sqlWriter
	db.Loc = time.UTC

	db.MaxInsertSize = new(synct[int])
	db.MaxInsertSize.Set(1 << 20)

	db.Logger = DefaultLogger()

	return db, nil
}

func NewWriter(w io.Writer) (*Database, error) {
	db := newDatabase()
	writer := &writer{
		Writer: w,
	}
	db.Writes = writer
	db.Loc = time.UTC

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

	maps.Copy(db.tmplFuncs, funcs)
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
		role: poolWriter,
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
	return db.exec(db.Writes, ctx, nil, poolWriter, query, params...)
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
	conn, cache := db.readYourWrites(cache)
	return db.query(conn, context.Background(), dest, q, cache, params...)
}

func (db *Database) SelectRows(q string, cache time.Duration, params ...any) (Rows, error) {
	var rows Rows
	conn, cache := db.readYourWrites(cache)
	err := db.query(conn, context.Background(), &rows, q, cache, params...)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (db *Database) SelectContext(ctx context.Context, dest any, q string, cache time.Duration, params ...any) error {
	conn, cache := db.readYourWrites(cache)
	return db.query(conn, ctx, dest, q, cache, params...)
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
	conn, cache := db.readYourWrites(cache)
	return db.exists(conn, context.Background(), query, cache, params...)
}

// ExistsContext efficiently checks if there are any rows in the given query using the `Reads` connection
func (db *Database) ExistsContext(ctx context.Context, query string, cache time.Duration, params ...any) (bool, error) {
	conn, cache := db.readYourWrites(cache)
	return db.exists(conn, ctx, query, cache, params...)
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
	return interpolateParams(query, db.tmplFuncs, db.valuerFuncs, db.location(), params...)
}

func (db *Database) interpolateParams(query string, params ...any) (replacedQuery string, normalizedParams Params, err error) {
	return interpolateParams(query, db.tmplFuncs, db.valuerFuncs, db.location(), params...)
}

// location returns db.Loc, defaulting to time.UTC. Centralized so callers
// that build SQL literals never have to nil-check.
func (db *Database) location() *time.Location {
	return locOrUTC(db.Loc)
}

// locOrUTC returns loc if non-nil, otherwise time.UTC. go-sql-driver
// treats a nil Loc the same way, so matching that contract here keeps
// our literal formatting in sync with what the driver will parse back.
func locOrUTC(loc *time.Location) *time.Location {
	if loc == nil {
		return time.UTC
	}
	return loc
}
