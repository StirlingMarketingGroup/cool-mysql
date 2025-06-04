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
    ID   int    `db:"id"`
    Name string `db:"name"`
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

Fields in a struct can include a `mysql` tag to control how they map to the database. The tag name overrides the column name used by the insert and upsert helpers and when scanning query results. One option modifies how zero values are marshaled:

- `defaultzero` – write `default(column)` instead of the zero value.

This option is also honored when a struct is passed to `InterpolateParams`.

```go
type Person struct {
    ID   int    `mysql:"id"`
    Name string `mysql:"name,defaultzero"`
}

db.Insert("people", Person{}) // name becomes default(`name`)

_, _, _ = mysql.InterpolateParams(
    "SELECT * FROM people WHERE name = @@Name",
    Person{},
) // produces: SELECT * FROM people WHERE name = default(`name`)

tmpl := `SELECT * FROM people {{ if .Name }}WHERE name=@@Name{{ end }}`
```

When using template syntax the struct field name (`.Name` above) is used for
lookups, not the column name from the `mysql` tag.

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

## License

This project is licensed under the [MIT License](LICENSE).

