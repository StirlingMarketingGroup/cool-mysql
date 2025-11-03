# cool-mysql API Reference

Complete API documentation for all cool-mysql methods, organized by category.

## Database Creation

### New
```go
func New(wUser, wPass, wSchema, wHost string, wPort int,
         rUser, rPass, rSchema, rHost string, rPort int,
         collation, timeZone string) (*Database, error)
```

Create a new database connection from connection parameters.

**Parameters:**
- `wUser`, `wPass`, `wSchema`, `wHost`, `wPort` - Write connection credentials
- `rUser`, `rPass`, `rSchema`, `rHost`, `rPort` - Read connection credentials
- `collation` - Database collation (e.g., `"utf8mb4_unicode_ci"`)
- `timeZone` - Time zone for connections (e.g., `"America/New_York"`, `"UTC"`)

**Returns:**
- `*Database` - Database instance with dual connection pools
- `error` - Connection error if unable to establish connections

**Example:**
```go
db, err := mysql.New(
    "root", "password", "mydb", "localhost", 3306,
    "root", "password", "mydb", "localhost", 3306,
    "utf8mb4_unicode_ci",
    "UTC",
)
```

### NewFromDSN
```go
func NewFromDSN(writesDSN, readsDSN string) (*Database, error)
```

Create database connection from DSN strings.

**Parameters:**
- `writesDSN` - Write connection DSN
- `readsDSN` - Read connection DSN

**DSN Format:**
```
username:password@protocol(address)/dbname?param=value
```

**Example:**
```go
writesDSN := "user:pass@tcp(write-host:3306)/dbname?parseTime=true&loc=UTC"
readsDSN := "user:pass@tcp(read-host:3306)/dbname?parseTime=true&loc=UTC"
db, err := mysql.NewFromDSN(writesDSN, readsDSN)
```

### NewFromConn
```go
func NewFromConn(writesConn, readsConn *sql.DB) (*Database, error)
```

Create database from existing `*sql.DB` connections.

**Parameters:**
- `writesConn` - Existing write connection
- `readsConn` - Existing read connection

**Example:**
```go
writesConn, _ := sql.Open("mysql", writesDSN)
readsConn, _ := sql.Open("mysql", readsDSN)
db, err := mysql.NewFromConn(writesConn, readsConn)
```

## Query Methods (SELECT)

### Select
```go
func (db *Database) Select(dest any, query string, cacheTTL time.Duration, params ...mysql.Params) error
```

Execute SELECT query and scan results into destination. Uses read connection pool.

**Parameters:**
- `dest` - Destination for results (struct, slice, map, channel, function, or primitive)
- `query` - SQL query with `@@paramName` placeholders
- `cacheTTL` - Cache duration (`0` = no cache, `> 0` = cache for duration)
- `params` - Query parameters (`mysql.Params{}` or structs)

**Destination Types:**
- `*[]StructType` - Slice of structs
- `*StructType` - Single struct (returns `sql.ErrNoRows` if not found)
- `*string`, `*int`, `*time.Time`, etc. - Single value
- `chan StructType` - Channel for streaming results
- `func(StructType)` - Function called for each row
- `*[]map[string]any` - Slice of maps
- `*json.RawMessage` - JSON result

**Returns:**
- `error` - Query error or `sql.ErrNoRows` for single-value queries with no results

**Examples:**
```go
// Select into struct slice
var users []User
err := db.Select(&users, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE age > @@minAge", 5*time.Minute,
    18)

// Select single value
var name string
err := db.Select(&name, "SELECT `name` FROM `users` WHERE `id` = @@id", 0,
    1)

// Select into channel
userCh := make(chan User)
go func() {
    defer close(userCh)
    db.Select(userCh, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users`", 0)
}()

// Select with function
db.Select(func(u User) {
    log.Printf("User: %s", u.Name)
}, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users`", 0)
```

### SelectContext
```go
func (db *Database) SelectContext(ctx context.Context, dest any, query string,
                                  cacheTTL time.Duration, params ...mysql.Params) error
```

Context-aware version of `Select()`. Supports cancellation and deadlines.

**Parameters:**
- `ctx` - Context for cancellation/timeout
- Additional parameters same as `Select()`

**Example:**
```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

var users []User
err := db.SelectContext(ctx, &users, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users`", 0)
```

### SelectWrites
```go
func (db *Database) SelectWrites(dest any, query string, cacheTTL time.Duration,
                                 params ...mysql.Params) error
```

Select using write connection pool. Use for read-after-write consistency.

**When to Use:**
- Immediately after INSERT/UPDATE/DELETE when you need to read the modified data
- When you need strong consistency and can't risk reading stale replica data

**Example:**
```go
// Insert then immediately read
db.Insert("users", user)
db.SelectWrites(&user, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `id` = @@id", 0,
    user.ID)
```

### SelectWritesContext
```go
func (db *Database) SelectWritesContext(ctx context.Context, dest any, query string,
                                        cacheTTL time.Duration, params ...mysql.Params) error
```

Context-aware version of `SelectWrites()`.

### SelectJSON
```go
func (db *Database) SelectJSON(dest *json.RawMessage, query string,
                               cacheTTL time.Duration, params ...mysql.Params) error
```

Select query results as JSON.

**Example:**
```go
var result json.RawMessage
err := db.SelectJSON(&result,
    "SELECT JSON_OBJECT('id', id, 'name', name) FROM `users` WHERE `id` = @@id",
    0, 1)
```

### SelectJSONContext
```go
func (db *Database) SelectJSONContext(ctx context.Context, dest *json.RawMessage,
                                      query string, cacheTTL time.Duration,
                                      params ...mysql.Params) error
```

Context-aware version of `SelectJSON()`.

## Utility Query Methods

### Count
```go
func (db *Database) Count(query string, cacheTTL time.Duration, params ...mysql.Params) (int64, error)
```

Execute COUNT query and return result as `int64`. Uses read pool.

**Parameters:**
- `query` - Query that returns a single integer (typically `SELECT COUNT(*)`)
- `cacheTTL` - Cache duration
- `params` - Query parameters

**Returns:**
- `int64` - Count result
- `error` - Query error

**Example:**
```go
count, err := db.Count("SELECT COUNT(*) FROM `users` WHERE `active` = @@active",
    5*time.Minute, 1)
```

### CountContext
```go
func (db *Database) CountContext(ctx context.Context, query string, cacheTTL time.Duration,
                                 params ...mysql.Params) (int64, error)
```

Context-aware version of `Count()`.

### Exists
```go
func (db *Database) Exists(query string, cacheTTL time.Duration, params ...mysql.Params) (bool, error)
```

Check if query returns any rows. Uses read pool.

**Parameters:**
- `query` - Query to check (typically `SELECT 1 FROM ... WHERE ...`)
- `cacheTTL` - Cache duration
- `params` - Query parameters

**Returns:**
- `bool` - `true` if rows exist, `false` otherwise
- `error` - Query error

**Example:**
```go
exists, err := db.Exists("SELECT 1 FROM `users` WHERE `email` = @@email", 0,
    "user@example.com")
```

### ExistsContext
```go
func (db *Database) ExistsContext(ctx context.Context, query string, cacheTTL time.Duration,
                                  params ...mysql.Params) (bool, error)
```

Context-aware version of `Exists()`.

### ExistsWrites
```go
func (db *Database) ExistsWrites(query string, params ...mysql.Params) (bool, error)
```

Check existence using write pool for read-after-write consistency.

### ExistsWritesContext
```go
func (db *Database) ExistsWritesContext(ctx context.Context, query string,
                                        params ...mysql.Params) (bool, error)
```

Context-aware version of `ExistsWrites()`.

## Insert Operations

### Insert
```go
func (db *Database) Insert(table string, data any) error
```

Insert data into table. Automatically chunks large batches based on `max_allowed_packet`.

**Parameters:**
- `table` - Table name
- `data` - Single struct, slice of structs, or channel of structs

**Returns:**
- `error` - Insert error

**Examples:**
```go
// Single insert
user := User{Name: "Alice", Email: "alice@example.com"}
err := db.Insert("users", user)

// Batch insert
users := []User{
    {Name: "Bob", Email: "bob@example.com"},
    {Name: "Charlie", Email: "charlie@example.com"},
}
err := db.Insert("users", users)

// Streaming insert
userCh := make(chan User)
go func() {
    for _, u := range users {
        userCh <- u
    }
    close(userCh)
}()
err := db.Insert("users", userCh)
```

### InsertContext
```go
func (db *Database) InsertContext(ctx context.Context, table string, data any) error
```

Context-aware version of `Insert()`.

**Example:**
```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

err := db.InsertContext(ctx, "users", users)
```

## Upsert Operations

### Upsert
```go
func (db *Database) Upsert(table string, uniqueCols, updateCols []string,
                           where string, data any) error
```

Perform INSERT ... ON DUPLICATE KEY UPDATE operation.

**Parameters:**
- `table` - Table name
- `uniqueCols` - Columns that define uniqueness (used in conflict detection)
- `updateCols` - Columns to update on duplicate key
- `where` - Optional WHERE clause for conditional update (can be empty)
- `data` - Single struct, slice of structs, or channel of structs

**Returns:**
- `error` - Upsert error

**Examples:**
```go
// Basic upsert on unique email
err := db.Upsert(
    "users",
    []string{"email"},              // unique column
    []string{"name", "updated_at"}, // columns to update
    "",                             // no WHERE clause
    user,
)

// Upsert with conditional update
err := db.Upsert(
    "users",
    []string{"id"},
    []string{"name", "email"},
    "updated_at < VALUES(updated_at)", // only update if newer
    users,
)

// Batch upsert
err := db.Upsert(
    "users",
    []string{"email"},
    []string{"name", "last_login"},
    "",
    []User{{Email: "a@example.com", Name: "Alice"}, ...},
)
```

### UpsertContext
```go
func (db *Database) UpsertContext(ctx context.Context, table string, uniqueCols,
                                  updateCols []string, where string, data any) error
```

Context-aware version of `Upsert()`.

## Execute Operations

### Exec
```go
func (db *Database) Exec(query string, params ...mysql.Params) error
```

Execute query without returning results (UPDATE, DELETE, etc.). Uses write pool.

**Parameters:**
- `query` - SQL query with `@@paramName` placeholders
- `params` - Query parameters

**Returns:**
- `error` - Execution error

**Example:**
```go
err := db.Exec("UPDATE `users` SET `active` = @@active WHERE `id` = @@id",
    mysql.Params{"active": 1, "id": 123})

err := db.Exec("DELETE FROM `users` WHERE last_login < @@cutoff",
    time.Now().Add(-365*24*time.Hour))
```

### ExecContext
```go
func (db *Database) ExecContext(ctx context.Context, query string, params ...mysql.Params) error
```

Context-aware version of `Exec()`.

### ExecResult
```go
func (db *Database) ExecResult(query string, params ...mysql.Params) (sql.Result, error)
```

Execute query and return `sql.Result` for accessing `LastInsertId()` and `RowsAffected()`.

**Returns:**
- `sql.Result` - Execution result
- `error` - Execution error

**Example:**
```go
result, err := db.ExecResult("UPDATE `users` SET `name` = @@name WHERE `id` = @@id",
    mysql.Params{"name": "Alice", "id": 1})
if err != nil {
    return err
}

rowsAffected, _ := result.RowsAffected()
log.Printf("Updated %d rows", rowsAffected)
```

### ExecResultContext
```go
func (db *Database) ExecResultContext(ctx context.Context, query string,
                                      params ...mysql.Params) (sql.Result, error)
```

Context-aware version of `ExecResult()`.

## Transaction Management

### GetOrCreateTxFromContext
```go
func GetOrCreateTxFromContext(ctx context.Context) (*sql.Tx, func() error, func(), error)
```

Get existing transaction from context or create new one.

**Returns:**
- `*sql.Tx` - Transaction instance
- `func() error` - Commit function
- `func()` - Cancel function (rolls back if not committed)
- `error` - Transaction creation error

**Usage Pattern:**
```go
tx, commit, cancel, err := mysql.GetOrCreateTxFromContext(ctx)
defer cancel() // Always safe to call - rolls back if commit() not called
if err != nil {
    return err
}

// Store transaction in context
ctx = mysql.NewContextWithTx(ctx, tx)

// Do database operations...

if err := commit(); err != nil {
    return err
}
```

### NewContextWithTx
```go
func NewContextWithTx(ctx context.Context, tx *sql.Tx) context.Context
```

Store transaction in context for use by database operations.

### TxFromContext
```go
func TxFromContext(ctx context.Context) (*sql.Tx, bool)
```

Retrieve transaction from context.

**Returns:**
- `*sql.Tx` - Transaction if present
- `bool` - `true` if transaction exists in context

## Context Management

### NewContext
```go
func NewContext(ctx context.Context, db *Database) context.Context
```

Store database instance in context.

**Example:**
```go
ctx := mysql.NewContext(context.Background(), db)
```

### NewContextWithFunc
```go
func NewContextWithFunc(ctx context.Context, f func() *Database) context.Context
```

Store database factory function in context for lazy initialization.

**Example:**
```go
ctx := mysql.NewContextWithFunc(ctx, sync.OnceValue(func() *Database {
    db, err := mysql.New(...)
    if err != nil {
        panic(err)
    }
    return db
}))
```

### FromContext
```go
func FromContext(ctx context.Context) *Database
```

Retrieve database from context.

**Returns:**
- `*Database` - Database instance or `nil` if not found

**Example:**
```go
db := mysql.FromContext(ctx)
if db == nil {
    return errors.New("database not in context")
}
```

## Caching Configuration

### EnableRedis
```go
func (db *Database) EnableRedis(client *redis.Client)
```

Enable Redis caching with distributed locking.

**Example:**
```go
redisClient := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
})
db.EnableRedis(redisClient)
```

### EnableMemcache
```go
func (db *Database) EnableMemcache(client *memcache.Client)
```

Enable Memcached caching.

**Example:**
```go
memcacheClient := memcache.New("localhost:11211")
db.EnableMemcache(memcacheClient)
```

### UseCache
```go
func (db *Database) UseCache(cache Cache)
```

Use custom cache implementation.

**Examples:**
```go
// In-memory cache
db.UseCache(mysql.NewWeakCache())

// Multi-level cache
db.UseCache(mysql.NewMultiCache(
    mysql.NewWeakCache(),           // L1: Local fast cache
    mysql.NewRedisCache(redisClient), // L2: Distributed cache
))
```

### NewWeakCache
```go
func NewWeakCache() *WeakCache
```

Create in-memory cache with weak pointers (GC-managed).

### NewRedisCache
```go
func NewRedisCache(client *redis.Client) *RedisCache
```

Create Redis cache with distributed locking support.

### NewMultiCache
```go
func NewMultiCache(caches ...Cache) *MultiCache
```

Create layered cache that checks caches in order.

## Parameter Interpolation

### InterpolateParams
```go
func (db *Database) InterpolateParams(query string, params ...mysql.Params) (string, []any, error)
```

Manually interpolate parameters in query. Useful for debugging or logging.

**Parameters:**
- `query` - Query with `@@paramName` placeholders
- `params` - Parameters to interpolate

**Returns:**
- `string` - Query with `?` placeholders
- `[]any` - Normalized parameter values
- `error` - Interpolation error

**Example:**
```go
replacedQuery, normalizedParams, err := db.InterpolateParams(
    "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `id` = @@id",
    mysql.Params{"id": 1},
)
// replacedQuery: "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `id` = ?"
// normalizedParams: []any{1}
```

## Template Functions

### AddTemplateFuncs
```go
func (db *Database) AddTemplateFuncs(funcs template.FuncMap)
```

Add custom functions available in query templates.

**Example:**
```go
db.AddTemplateFuncs(template.FuncMap{
    "upper": strings.ToUpper,
    "lower": strings.ToLower,
})

db.Select(&users,
    "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `name` = @@name{{ if .UpperCase }} COLLATE utf8mb4_bin{{ end }}",
    0,
    mysql.Params{"name": "alice", "upperCase": true})
```

## Special Types

### Params
```go
type Params map[string]any
```

Parameter map for query placeholders.

### Raw
```go
type Raw string
```

Literal SQL that won't be escaped. **Use with caution - SQL injection risk.**

**Example:**
```go
db.Select(&users, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE @@condition", 0,
    mysql.Params{
        "condition": mysql.Raw("created_at > NOW() - INTERVAL 1 DAY"),
    })
```

### MapRow / SliceRow / MapRows / SliceRows
```go
type MapRow map[string]any
type SliceRow []any
type MapRows []map[string]any
type SliceRows [][]any
```

Flexible result types when struct mapping isn't needed.

**Example:**
```go
var rows mysql.MapRows
db.Select(&rows, "SELECT `id`, name FROM `users`", 0)
for _, row := range rows {
    fmt.Printf("ID: %v, Name: %v\n", row["id"], row["name"])
}
```

## Custom Interfaces

### Zeroer
```go
type Zeroer interface {
    IsZero() bool
}
```

Implement for custom zero-value detection with `defaultzero` tag.

**Example:**
```go
type CustomTime struct {
    time.Time
}

func (ct CustomTime) IsZero() bool {
    return ct.Time.IsZero() || ct.Time.Unix() == 0
}
```

### Valueser
```go
type Valueser interface {
    Values() []any
}
```

Implement for custom value conversion during inserts.

**Example:**
```go
type Point struct {
    X, Y float64
}

func (p Point) Values() []any {
    return []any{p.X, p.Y}
}
```

## Error Handling

### Automatic Retries

cool-mysql automatically retries these MySQL error codes:
- `1213` - Deadlock detected
- `1205` - Lock wait timeout exceeded
- `2006` - MySQL server has gone away
- `2013` - Lost connection to MySQL server during query

Retry behavior uses exponential backoff and can be configured with `COOL_MAX_ATTEMPTS` environment variable.

### sql.ErrNoRows

- **Single value/struct queries**: Returns `sql.ErrNoRows` when no results
- **Slice queries**: Returns empty slice (not `sql.ErrNoRows`)

**Example:**
```go
var name string
err := db.Select(&name, "SELECT `name` FROM `users` WHERE `id` = @@id", 0,
    999)
if errors.Is(err, sql.ErrNoRows) {
    // Handle not found
}

var users []User
err := db.Select(&users, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `id` = @@id", 0,
    999)
// err is nil, users is empty slice []
```
