# cool-mysql

[![Go Reference](https://pkg.go.dev/badge/github.com/StirlingMarketingGroup/cool-mysql.svg)](https://pkg.go.dev/github.com/StirlingMarketingGroup/cool-mysql)
[![license](https://img.shields.io/badge/license-MIT-red.svg)](LICENSE)

`cool-mysql` is a small library that wraps Go's `database/sql` with MySQL oriented helpers. It keeps the underlying interfaces intact while providing conveniences that save you time when writing data access code.

## Features

- **Dual pools** for reads and writes
- **Named template parameters** using `@@name` tokens
- **Automatic retries** with exponential backoff
- **Redis backed caching** with optional distributed locks
- **Insert/Upsert helpers** that chunk large sets to respect `max_allowed_packet`
- **Flexible selection** into structs, slices, maps, channels or functions
- **Select single values** (e.g. `string`, `time.Time`)
- **JSON columns** can unmarshal directly into struct fields
- **Channels** supported for selecting and inserting
- Optional **query logging** and transaction helpers

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
        time.Minute,
        mysql.Params{"since": time.Now().Add(-24 * time.Hour)},
    )
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("loaded %d users", len(users))
}
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
err := db.Select(&name, "SELECT name FROM users WHERE id=@@id", 0, mysql.Params{"id": 5})
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

### Insert helper

```go
newUser := User{ID: 123, Name: "Alice"}
err = db.Insert("INSERT INTO users (id, name) VALUES (@@id, @@name)", newUser)
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
if err := db.Insert("INSERT INTO users (id, name) VALUES (@@id, @@name)", ch); err != nil {
    log.Fatal(err)
}
```

### Upsert helper

```go
up := User{ID: 123, Name: "Alice"}
err = db.Upsert(
    "INSERT INTO users (id, name) VALUES (@@id, @@name)",
    []string{"id"},    // unique columns
    []string{"name"},  // columns to update on conflict
    "",                // additional WHERE clause
    up,
)
```

## License

This project is licensed under the MIT License.

