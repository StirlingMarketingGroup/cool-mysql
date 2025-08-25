# cool-mysql

[![Go Reference](https://pkg.go.dev/badge/github.com/StirlingMarketingGroup/cool-mysql.svg)](https://pkg.go.dev/github.com/StirlingMarketingGroup/cool-mysql)
[![license](https://img.shields.io/badge/license-MIT-red.svg)](LICENSE)

`cool-mysql` is a small library that wraps Go's `database/sql` with MySQL oriented helpers. It keeps the underlying interfaces intact while providing conveniences that save you time when writing data access code.

## Features

- **Dual pools** for reads and writes
- **Named template parameters** using `@@name` tokens
- **Automatic retries** with exponential backoff
- **Pluggable caching** (Redis, Memcached, or in-memory weak pointers) with optional distributed locks
- **Insert/Upsert helpers** that chunk large sets to respect `max_allowed_packet`
- **Go template syntax** in queries for conditional logic
- **Flexible selection** into structs, slices, maps, channels or functions
- **Select single values** (e.g. `string`, `time.Time`)
- **JSON columns** can unmarshal directly into struct fields
- **Channels** supported for selecting and inserting
- Optional **query logging** and transaction helpers
- **Pluggable logging** using `log/slog` by default with a Zap adapter

## Installation

```bash
go get github.com/StirlingMarketingGroup/cool-mysql
```

## Quick Start

```go
package main

import (
    "log"
    "time"

    mysql "github.com/StirlingMarketingGroup/cool-mysql"
)

type User struct {
    ID   int    `mysql:"id"`
    Name string `mysql:"name"`
}

func main() {
    db, err := mysql.New(
        "writeUser", "writePass", "mydb", "127.0.0.1", 3306,
        "readUser", "readPass", "mydb", "127.0.0.1", 3306,
        "utf8mb4_unicode_ci", time.Local,
    )
    if err != nil {
        log.Fatal(err)
    }

    var users []User
    err = db.Select(&users,
        "SELECT id, name FROM users WHERE created_at > @@since",
        time.Minute, // cache TTL when caching is configured
        mysql.Params{"since": time.Now().Add(-24 * time.Hour)},
    )
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("loaded %d users", len(users))
}
```

## Configuration

cool-mysql can be configured using environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `COOL_MAX_EXECUTION_TIME_TIME` | `27` (seconds) | Maximum query execution time (90% of 30 seconds) |
| `COOL_REDIS_LOCK_RETRY_DELAY` | `0.020` (seconds) | Delay between Redis lock retry attempts |
| `COOL_MYSQL_MAX_QUERY_LOG_LENGTH` | `4096` (bytes) | Maximum length of queries in error logs |

**Example:**
```bash
export COOL_MAX_EXECUTION_TIME_TIME=60  # 60 second timeout
export COOL_REDIS_LOCK_RETRY_DELAY=0.050  # 50ms retry delay
export COOL_MYSQL_MAX_QUERY_LOG_LENGTH=8192  # 8KB log limit
```

### Enabling caching

```go
// use Redis
r := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
db.EnableRedis(r)

// or Memcached
db.EnableMemcache(memcache.New("localhost:11211"))

// or a simple in-memory cache using weak pointers
db.UseCache(mysql.NewWeakCache())

// caches can be stacked
db.UseCache(mysql.NewMultiCache(mysql.NewWeakCache(), mysql.NewRedisCache(r)))
```

## Usage

### Selecting into structs

```go
type Profile struct {
    Likes []string `json:"likes"`
}

type User struct {
    ID      int
    Name    string
    Profile Profile `db:"profile_json"`
}

var u User
err := db.Select(&u,
    "SELECT id, name, profile_json FROM users WHERE id=@@id",
    0,
    mysql.Params{"id": 1},
)
if err != nil {
    // if no row is returned, err == sql.ErrNoRows
    log.Fatal(err)
}
```

Selecting into a slice never returns `sql.ErrNoRows` if empty:

```go
var all []User
err := db.Select(&all, "SELECT * FROM users WHERE active=1", 0)
if err != nil {
    log.Fatal(err)
}
log.Println(len(all))
```

### Selecting into single values

```go
var name string
err := db.Select(&name, "SELECT name FROM users WHERE id=@@id", 0, 5) // single param value
```

### Selecting into channels

```go
userCh := make(chan User)
go func() {
    defer close(userCh)
    if err := db.Select(userCh, "SELECT id, name FROM users", 0); err != nil {
        log.Fatal(err)
    }
}()
for u := range userCh {
    log.Printf("%d: %s", u.ID, u.Name)
}
```

### Selecting with a function receiver

```go
err = db.Select(func(u User) {
    log.Printf("found %s", u.Name)
}, "SELECT id, name FROM users WHERE active=1", 0)
```

### Additional query methods

**Count records efficiently:**
```go
count, err := db.Count("SELECT COUNT(*) FROM users WHERE active = @@active", 0, mysql.Params{"active": 1})
```

**Check existence:**
```go
exists, err := db.Exists("SELECT 1 FROM users WHERE email = @@email", 0, mysql.Params{"email": "user@example.com"})
// Use ExistsWrites() to query the write connection
existsOnWrite, err := db.ExistsWrites("SELECT 1 FROM users WHERE email = @@email", mysql.Params{"email": "user@example.com"})
```

**Query against write connection:**
```go
var users []User
err := db.SelectWrites(&users, "SELECT id, name FROM users WHERE id = @@id", mysql.Params{"id": 123})
```

**Direct JSON results:**
```go
var result json.RawMessage
err := db.SelectJSON(&result, "SELECT JSON_OBJECT('id', id, 'name', name) FROM users WHERE id = @@id", 0, mysql.Params{"id": 123})
```

**Execute with detailed results:**
```go
result, err := db.ExecResult("UPDATE users SET name = @@name WHERE id = @@id", mysql.Params{"name": "Alice", "id": 123})
if err != nil {
    log.Fatal(err)
}
rowsAffected, _ := result.RowsAffected()
lastInsertID, _ := result.LastInsertId()
```

**Context-aware operations:**
All major functions have Context variants (`SelectContext`, `InsertContext`, `UpsertContext`, etc.) for cancellation and timeout support:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

var users []User
err := db.SelectContext(ctx, &users, "SELECT id, name FROM users", 0)
```

**Raw SQL strings:**
Use `mysql.Raw` for literal SQL that shouldn't be escaped:

```go
err := db.Select(&users, 
    "SELECT id, name FROM users WHERE created_at > @@date AND @@condition", 
    0, 
    mysql.Params{
        "date": time.Now().Add(-24*time.Hour),
        "condition": mysql.Raw("status = 'active'"), // not escaped
    },
)
```

### Conditional queries with templates

```go
var since *time.Time
query := `SELECT id, name FROM users WHERE 1=1 {{ if .since }}AND created_at > @@since{{ end }}`
err = db.Select(&users, query, 0, mysql.Params{"since": since})
```

### Insert helper

```go
newUser := User{ID: 123, Name: "Alice"}
err = db.Insert("users", newUser) // query is built automatically
```

The source can also be a channel of structs for batch inserts.

```go
ch := make(chan User)
go func() {
    for _, u := range users {
        ch <- u
    }
    close(ch)
}()
if err := db.Insert("users", ch); err != nil { // batch insert
    log.Fatal(err)
}
```

### Upsert helper

```go
up := User{ID: 123, Name: "Alice"}
err = db.Upsert(
    "users",            // table name only
    []string{"id"},    // unique columns
    []string{"name"},  // columns to update on conflict
    "",                // additional WHERE clause
    up,
)
```

### Struct tags

Fields in a struct can include a `mysql` tag to control how they map to the database. The tag name overrides the column name used by the insert and upsert helpers and when scanning query results.

**Available options:**

- `defaultzero` – write `default(column_name)` instead of the zero value during inserts and parameter interpolation
- `insertDefault` – alias for `defaultzero` (same behavior)  
- `omitempty` – alias for `defaultzero` (same behavior)
- `"-"` – skip this field entirely (not included in inserts, selects, or parameter interpolation)

**Hex encoding support:**
Column names can include hex-encoded characters using `0x` notation (e.g., `0x2c` for comma, `0x20` for space).

```go
type Person struct {
    ID       int       `mysql:"id"`
    Name     string    `mysql:"name,defaultzero"`
    Email    string    `mysql:"email,omitempty"`        // same as defaultzero
    Internal string    `mysql:"-"`                      // completely ignored
    Created  time.Time `mysql:"created_at,insertDefault"` // same as defaultzero
    Special  string    `mysql:"column0x2cname"`         // becomes "column,name"
}

db.Insert("people", Person{}) 
// name, email, created_at become default(`name`), default(`email`), default(`created_at`)
// Internal field is completely ignored

_, _, _ = mysql.InterpolateParams(
    "SELECT * FROM people WHERE name = @@Name",
    Person{},
) // produces: SELECT * FROM people WHERE name = default(`name`)

tmpl := `SELECT * FROM people {{ if .Name }}WHERE name=@@Name{{ end }}`
```

**Important notes:**
- When using template syntax, the struct field name (`.Name` above) is used for lookups, not the column name from the `mysql` tag
- All three options (`defaultzero`, `insertDefault`, `omitempty`) have identical behavior
- The `"-"` option completely excludes the field from all database operations

### Transactions

```go
tx, commit, cancel, err := mysql.GetOrCreateTxFromContext(ctx)
defer cancel()
if err != nil {
    return fmt.Errorf("failed to create transaction: %w", err)
}
ctx = mysql.NewContextWithTx(ctx, tx)

// do DB work with tx in context

if err := commit(); err != nil {
    return fmt.Errorf("failed to commit tx: %w", err)
}
```

## Advanced Features

### Context Management

cool-mysql provides utilities for managing database connections and transactions through context:

```go
// Create a new context with a database connection
ctx := mysql.NewContext(context.Background(), db)

// Retrieve the database from context
dbFromCtx := mysql.FromContext(ctx)

// Transaction management with context
tx, commit, cancel, err := mysql.GetOrCreateTxFromContext(ctx)
if err != nil {
    return err
}
defer cancel()

// Use the transaction
ctx = mysql.NewContextWithTx(ctx, tx)
err = db.SelectContext(ctx, &users, "SELECT * FROM users WHERE active = 1", 0)
if err != nil {
    return err
}

// Commit the transaction
if err := commit(); err != nil {
    return err
}
```

### Interfaces and Custom Types

**Zeroer Interface:** Custom zero-value detection
```go
type CustomTime struct {
    time.Time
}

func (ct CustomTime) IsZero() bool {
    return ct.Time.IsZero() || ct.Year() < 1900
}

// Use in struct with defaultzero tag
type Event struct {
    ID   int        `mysql:"id"`
    Date CustomTime `mysql:"created_at,defaultzero"`
}
```

**Valueser Interface:** Custom value conversion
```go
type Status int

const (
    StatusInactive Status = 0
    StatusActive   Status = 1
)

func (s Status) Values() []any {
    return []any{int(s)}
}

// Use in parameters or struct fields
type User struct {
    ID     int    `mysql:"id"`
    Status Status `mysql:"status"`
}
```

### Advanced Caching

**MultiCache:** Stack multiple cache layers
```go
// Combine in-memory and Redis caching
weak := mysql.NewWeakCache()
redis := mysql.NewRedisCache(redisClient)
multi := mysql.NewMultiCache(weak, redis)

db.UseCache(multi)
```

**Cache with distributed locking:**
```go
db.EnableRedis(redisClient)
// Queries will use distributed locks to prevent cache stampedes
err := db.Select(&users, "SELECT * FROM users WHERE popular = 1", 
    5*time.Minute, // cache TTL
)
```

### Row Types and Converters

```go
// MapRow - convert query results to maps
var rows []mysql.MapRow
err := db.Select(&rows, "SELECT id, name, email FROM users", 0)

// SliceRow - convert to slices
var rows []mysql.SliceRow  
err := db.Select(&rows, "SELECT id, name, email FROM users", 0)

// Custom row processing
err = db.SelectRows("SELECT * FROM large_table", 0, func(rows *sql.Rows) error {
    for rows.Next() {
        // Process each row individually
        var id int
        var name string
        if err := rows.Scan(&id, &name); err != nil {
            return err
        }
        // Handle row...
    }
    return rows.Err()
})
```

## Performance & Best Practices

### Connection Pooling

cool-mysql uses separate connection pools for read and write operations:

```go
// Reads use the read pool (optimized for read-heavy workloads)
var users []User
err := db.Select(&users, "SELECT * FROM users", cacheTTL)

// Writes use the write pool (ensures consistency)
err := db.Insert("users", newUser)

// Force use of write pool for reads (when read consistency is critical)
err := db.SelectWrites(&users, "SELECT * FROM users WHERE just_created = 1", nil)
```

### Large Dataset Handling

**Chunked inserts** automatically respect MySQL's `max_allowed_packet`:

```go
// Automatically chunks large slices
largeUserSlice := make([]User, 10000)
err := db.Insert("users", largeUserSlice) // Inserts in optimal chunks

// Channel-based streaming inserts
userCh := make(chan User, 100)
go func() {
    defer close(userCh)
    for _, user := range largeUserSlice {
        userCh <- user
    }
}()
err := db.Insert("users", userCh) // Processes in batches
```

**Streaming selects** for large result sets:

```go
// Use channels for memory-efficient processing
userCh := make(chan User, 100)
go func() {
    defer close(userCh)
    err := db.Select(userCh, "SELECT * FROM users", 0)
    if err != nil {
        log.Fatal(err)
    }
}()

for user := range userCh {
    // Process each user without loading all into memory
    processUser(user)
}
```

### Query Optimization

**Effective caching strategies:**

```go
// Short TTL for frequently changing data
err := db.Select(&activeUsers, "SELECT * FROM users WHERE active = 1", 
    30*time.Second)

// Long TTL for relatively static data
err := db.Select(&countries, "SELECT * FROM countries", 
    24*time.Hour)

// No caching for real-time data
err := db.Select(&currentBalance, "SELECT balance FROM accounts WHERE id = ?", 
    0, userID) // TTL = 0 means no caching
```

**Template optimization:**

```go
// Use templates for dynamic queries to reduce query plan cache pollution
query := `
SELECT * FROM users 
WHERE 1=1
{{ if .ActiveOnly }}AND active = 1{{ end }}
{{ if .Department }}AND department = @@Department{{ end }}
`

params := struct {
    ActiveOnly bool
    Department string
}{
    ActiveOnly: true,
    Department: "engineering",
}

err := db.Select(&users, query, cacheTTL, params)
```

### Best Practices

1. **Use appropriate TTL values:**
   - Static data: hours to days
   - Semi-static data: minutes to hours  
   - Dynamic data: seconds to minutes
   - Real-time data: no caching (TTL = 0)

2. **Leverage read/write separation:**
   - Use regular `Select()` for most reads
   - Use `SelectWrites()` only when read-after-write consistency is critical

3. **Handle large datasets efficiently:**
   - Use channels for streaming large result sets
   - Let the library handle insert chunking automatically
   - Consider using `Count()` instead of `SELECT COUNT(*)`

4. **Optimize for your caching setup:**
   - Use `MultiCache` to combine fast local cache with shared Redis cache
   - Configure appropriate Redis lock retry delays for your workload
   - Monitor cache hit rates and adjust TTLs accordingly

## Error Handling & Reliability

cool-mysql includes comprehensive error handling and automatic retry mechanisms:

### Automatic Retries

The library automatically retries operations that fail due to transient MySQL errors:

**Retry-eligible MySQL error codes:**
- `1213` - Deadlock found when trying to get lock
- `1205` - Lock wait timeout exceeded
- `2006` - MySQL server has gone away
- `2013` - Lost connection to MySQL server during query

**Retry behavior:**
- Uses exponential backoff with jitter
- Maximum retry attempts determined by context timeout
- Delays start at ~20ms and increase exponentially

```go
// Operations will automatically retry on transient errors
err := db.Select(&users, "SELECT * FROM users", 0)
// If this fails with a deadlock (1213), it will retry automatically
```

### Custom Error Handling

```go
// Check for specific error types
var users []User
err := db.Select(&users, "SELECT * FROM users WHERE id = ?", 0, 999)
if err == sql.ErrNoRows {
    log.Println("No users found")
} else if err != nil {
    log.Printf("Database error: %v", err)
}
```

### Transaction Retry Pattern

```go
func performComplexOperation(ctx context.Context, db *mysql.DB) error {
    return mysql.RetryableTransaction(ctx, db, func(tx *sql.Tx) error {
        // Your transactional operations here
        // If this returns a retryable error, the entire transaction will be retried
        _, err := tx.ExecContext(ctx, "UPDATE accounts SET balance = balance - 100 WHERE id = ?", 1)
        if err != nil {
            return err
        }
        _, err = tx.ExecContext(ctx, "UPDATE accounts SET balance = balance + 100 WHERE id = ?", 2)
        return err
    })
}
```

## License

This project is licensed under the [MIT License](LICENSE).

