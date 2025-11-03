# Testing Patterns Guide

Complete guide to testing applications that use cool-mysql, including mocking strategies and test patterns.

## Table of Contents

1. [Testing Strategies](#testing-strategies)
2. [Using sqlmock](#using-sqlmock)
3. [Test Database Setup](#test-database-setup)
4. [Testing Patterns](#testing-patterns)
5. [Context-Based Testing](#context-based-testing)
6. [Testing Caching](#testing-caching)
7. [Integration Testing](#integration-testing)
8. [Best Practices](#best-practices)

## Testing Strategies

### Three Approaches

| Approach | Pros | Cons | Best For |
|----------|------|------|----------|
| **sqlmock** | Fast, no DB needed, precise control | Manual setup, brittle | Unit tests |
| **Test Database** | Real MySQL behavior | Slower, requires DB | Integration tests |
| **In-Memory DB** | Fast, real SQL | Limited MySQL features | Quick tests |

### When to Use Each

**sqlmock:**
- Unit testing business logic
- Testing error handling
- CI/CD pipelines without database
- Rapid iteration

**Test Database:**
- Integration testing
- Testing complex queries
- Verifying MySQL-specific behavior
- End-to-end tests

**In-Memory (SQLite):**
- Quick local tests
- Testing SQL logic (not MySQL-specific)
- Prototyping

## Using sqlmock

### Setup

```go
import (
    "testing"
    "github.com/DATA-DOG/go-sqlmock"
    "github.com/StirlingMarketingGroup/cool-mysql"
)

func setupMockDB(t *testing.T) (*mysql.Database, sqlmock.Sqlmock) {
    // Create mock SQL connection
    mockDB, mock, err := sqlmock.New()
    if err != nil {
        t.Fatalf("Failed to create mock: %v", err)
    }

    // Create cool-mysql Database from mock connection
    db, err := mysql.NewFromConn(mockDB, mockDB)
    if err != nil {
        t.Fatalf("Failed to create db: %v", err)
    }

    return db, mock
}
```

### Basic Select Test

```go
func TestSelectUsers(t *testing.T) {
    db, mock := setupMockDB(t)
    defer mock.ExpectClose()

    // Define expected query and result
    rows := sqlmock.NewRows([]string{"id", "name", "email"}).
        AddRow(1, "Alice", "alice@example.com").
        AddRow(2, "Bob", "bob@example.com")

    mock.ExpectQuery("SELECT (.+) FROM users WHERE age > ?").
        WithArgs(18).
        WillReturnRows(rows)

    // Execute query
    var users []User
    err := db.Select(&users,
        "SELECT * FROM users WHERE age > @@minAge",
        0,
        mysql.Params{"minAge": 18})

    // Verify
    if err != nil {
        t.Errorf("Select failed: %v", err)
    }

    if len(users) != 2 {
        t.Errorf("Expected 2 users, got %d", len(users))
    }

    if users[0].Name != "Alice" {
        t.Errorf("Expected Alice, got %s", users[0].Name)
    }

    // Verify all expectations met
    if err := mock.ExpectationsWereMet(); err != nil {
        t.Errorf("Unfulfilled expectations: %v", err)
    }
}
```

### Insert Test

```go
func TestInsertUser(t *testing.T) {
    db, mock := setupMockDB(t)
    defer mock.ExpectClose()

    user := User{
        ID:    1,
        Name:  "Alice",
        Email: "alice@example.com",
    }

    // Expect INSERT statement
    mock.ExpectExec("INSERT INTO users").
        WithArgs(1, "Alice", "alice@example.com").
        WillReturnResult(sqlmock.NewResult(1, 1))

    // Execute insert
    err := db.Insert("users", user)

    // Verify
    if err != nil {
        t.Errorf("Insert failed: %v", err)
    }

    if err := mock.ExpectationsWereMet(); err != nil {
        t.Errorf("Unfulfilled expectations: %v", err)
    }
}
```

### Update Test

```go
func TestUpdateUser(t *testing.T) {
    db, mock := setupMockDB(t)
    defer mock.ExpectClose()

    // Expect UPDATE statement
    mock.ExpectExec("UPDATE users SET name = \\? WHERE id = \\?").
        WithArgs("Alice Updated", 1).
        WillReturnResult(sqlmock.NewResult(0, 1))

    // Execute update
    err := db.Exec("UPDATE users SET name = @@name WHERE id = @@id",
        mysql.Params{"name": "Alice Updated", "id": 1})

    // Verify
    if err != nil {
        t.Errorf("Update failed: %v", err)
    }

    if err := mock.ExpectationsWereMet(); err != nil {
        t.Errorf("Unfulfilled expectations: %v", err)
    }
}
```

### Error Handling Test

```go
func TestSelectError(t *testing.T) {
    db, mock := setupMockDB(t)
    defer mock.ExpectClose()

    // Expect query to return error
    mock.ExpectQuery("SELECT (.+) FROM users").
        WillReturnError(sql.ErrNoRows)

    // Execute query
    var user User
    err := db.Select(&user, "SELECT * FROM users WHERE id = @@id", 0,
        mysql.Params{"id": 999})

    // Verify error returned
    if !errors.Is(err, sql.ErrNoRows) {
        t.Errorf("Expected sql.ErrNoRows, got %v", err)
    }

    if err := mock.ExpectationsWereMet(); err != nil {
        t.Errorf("Unfulfilled expectations: %v", err)
    }
}
```

### Transaction Test

```go
func TestTransaction(t *testing.T) {
    db, mock := setupMockDB(t)
    defer mock.ExpectClose()

    // Expect transaction
    mock.ExpectBegin()
    mock.ExpectExec("INSERT INTO users").
        WithArgs(1, "Alice", "alice@example.com").
        WillReturnResult(sqlmock.NewResult(1, 1))
    mock.ExpectCommit()

    // Execute transaction
    ctx := context.Background()
    tx, commit, cancel, err := mysql.GetOrCreateTxFromContext(ctx)
    if err != nil {
        t.Fatalf("Failed to create tx: %v", err)
    }
    defer cancel()

    user := User{ID: 1, Name: "Alice", Email: "alice@example.com"}
    err = db.Insert("users", user)
    if err != nil {
        t.Errorf("Insert failed: %v", err)
    }

    if err := commit(); err != nil {
        t.Errorf("Commit failed: %v", err)
    }

    if err := mock.ExpectationsWereMet(); err != nil {
        t.Errorf("Unfulfilled expectations: %v", err)
    }
}
```

## Test Database Setup

### Docker Compose Setup

```yaml
# docker-compose.test.yml
version: '3.8'
services:
  mysql-test:
    image: mysql:8.0
    environment:
      MYSQL_ROOT_PASSWORD: testpass
      MYSQL_DATABASE: testdb
    ports:
      - "3307:3306"
    tmpfs:
      - /var/lib/mysql  # In-memory for speed
```

### Test Helper

```go
// testutil/db.go
package testutil

import (
    "database/sql"
    "testing"
    "github.com/StirlingMarketingGroup/cool-mysql"
)

func SetupTestDB(t *testing.T) *mysql.Database {
    db, err := mysql.New(
        "root", "testpass", "testdb", "localhost", 3307,
        "root", "testpass", "testdb", "localhost", 3307,
        "utf8mb4_unicode_ci",
        "UTC",
    )
    if err != nil {
        t.Fatalf("Failed to connect to test DB: %v", err)
    }

    // Clean database before test
    cleanDB(t, db)

    // Setup schema
    setupSchema(t, db)

    return db
}

func cleanDB(t *testing.T, db *mysql.Database) {
    tables := []string{"users", "orders", "products"}
    for _, table := range tables {
        db.Exec("DROP TABLE IF EXISTS " + table)
    }
}

func setupSchema(t *testing.T, db *mysql.Database) {
    schema := `
        CREATE TABLE users (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `
    if err := db.Exec(schema); err != nil {
        t.Fatalf("Failed to create schema: %v", err)
    }
}
```

### Using Test Database

```go
func TestInsertUserIntegration(t *testing.T) {
    db := testutil.SetupTestDB(t)

    user := User{
        Name:  "Alice",
        Email: "alice@example.com",
    }

    // Insert user
    err := db.Insert("users", user)
    if err != nil {
        t.Fatalf("Insert failed: %v", err)
    }

    // Verify insertion
    var retrieved User
    err = db.Select(&retrieved,
        "SELECT * FROM users WHERE email = @@email",
        0,
        mysql.Params{"email": "alice@example.com"})

    if err != nil {
        t.Fatalf("Select failed: %v", err)
    }

    if retrieved.Name != "Alice" {
        t.Errorf("Expected Alice, got %s", retrieved.Name)
    }
}
```

## Testing Patterns

### Table-Driven Tests

```go
func TestSelectUsers(t *testing.T) {
    tests := []struct {
        name        string
        minAge      int
        expected    []User
        expectError bool
    }{
        {
            name:   "adults only",
            minAge: 18,
            expected: []User{
                {ID: 1, Name: "Alice", Age: 25},
                {ID: 2, Name: "Bob", Age: 30},
            },
            expectError: false,
        },
        {
            name:        "no results",
            minAge:      100,
            expected:    []User{},
            expectError: false,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            db, mock := setupMockDB(t)
            defer mock.ExpectClose()

            rows := sqlmock.NewRows([]string{"id", "name", "age"})
            for _, u := range tt.expected {
                rows.AddRow(u.ID, u.Name, u.Age)
            }

            mock.ExpectQuery("SELECT (.+) FROM users").
                WithArgs(tt.minAge).
                WillReturnRows(rows)

            var users []User
            err := db.Select(&users,
                "SELECT * FROM users WHERE age > @@minAge",
                0,
                mysql.Params{"minAge": tt.minAge})

            if tt.expectError && err == nil {
                t.Error("Expected error, got nil")
            }

            if !tt.expectError && err != nil {
                t.Errorf("Unexpected error: %v", err)
            }

            if len(users) != len(tt.expected) {
                t.Errorf("Expected %d users, got %d",
                    len(tt.expected), len(users))
            }

            if err := mock.ExpectationsWereMet(); err != nil {
                t.Errorf("Unfulfilled expectations: %v", err)
            }
        })
    }
}
```

### Testing Named Parameters

```go
func TestNamedParameters(t *testing.T) {
    db, mock := setupMockDB(t)
    defer mock.ExpectClose()

    // cool-mysql converts @@param to ? internally
    mock.ExpectQuery("SELECT (.+) FROM users WHERE age > \\? AND status = \\?").
        WithArgs(18, "active").
        WillReturnRows(sqlmock.NewRows([]string{"id", "name"}))

    var users []User
    err := db.Select(&users,
        "SELECT * FROM users WHERE age > @@minAge AND status = @@status",
        0,
        mysql.Params{"minAge": 18, "status": "active"})

    if err != nil {
        t.Errorf("Query failed: %v", err)
    }

    if err := mock.ExpectationsWereMet(); err != nil {
        t.Errorf("Unfulfilled expectations: %v", err)
    }
}
```

### Testing Struct Tags

```go
func TestStructTagMapping(t *testing.T) {
    db, mock := setupMockDB(t)
    defer mock.ExpectClose()

    type CustomUser struct {
        UserID int    `mysql:"id"`
        Name   string `mysql:"user_name"`
    }

    // Expect query with actual column names
    rows := sqlmock.NewRows([]string{"id", "user_name"}).
        AddRow(1, "Alice")

    mock.ExpectQuery("SELECT id, user_name FROM users").
        WillReturnRows(rows)

    var users []CustomUser
    err := db.Select(&users, "SELECT id, user_name FROM users", 0)

    if err != nil {
        t.Fatalf("Query failed: %v", err)
    }

    if users[0].UserID != 1 {
        t.Errorf("Expected UserID=1, got %d", users[0].UserID)
    }

    if users[0].Name != "Alice" {
        t.Errorf("Expected Name=Alice, got %s", users[0].Name)
    }

    if err := mock.ExpectationsWereMet(); err != nil {
        t.Errorf("Unfulfilled expectations: %v", err)
    }
}
```

## Context-Based Testing

### Testing with Context

```go
func TestSelectWithContext(t *testing.T) {
    db, mock := setupMockDB(t)
    defer mock.ExpectClose()

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    rows := sqlmock.NewRows([]string{"id", "name"}).
        AddRow(1, "Alice")

    mock.ExpectQuery("SELECT (.+) FROM users").
        WillReturnRows(rows)

    var users []User
    err := db.SelectContext(ctx, &users, "SELECT * FROM users", 0)

    if err != nil {
        t.Errorf("Query failed: %v", err)
    }

    if err := mock.ExpectationsWereMet(); err != nil {
        t.Errorf("Unfulfilled expectations: %v", err)
    }
}
```

### Testing Context Cancellation

```go
func TestContextCancellation(t *testing.T) {
    db, mock := setupMockDB(t)
    defer mock.ExpectClose()

    ctx, cancel := context.WithCancel(context.Background())
    cancel() // Cancel immediately

    mock.ExpectQuery("SELECT (.+) FROM users").
        WillDelayFor(100 * time.Millisecond)

    var users []User
    err := db.SelectContext(ctx, &users, "SELECT * FROM users", 0)

    if err == nil {
        t.Error("Expected context cancellation error")
    }
}
```

### Testing Database in Context

```go
func TestDatabaseInContext(t *testing.T) {
    db := testutil.SetupTestDB(t)

    // Store DB in context
    ctx := mysql.NewContext(context.Background(), db)

    // Retrieve DB from context
    retrievedDB := mysql.FromContext(ctx)

    if retrievedDB == nil {
        t.Error("Expected database in context")
    }

    // Use DB from context
    var users []User
    err := retrievedDB.Select(&users, "SELECT * FROM users", 0)

    if err != nil {
        t.Errorf("Query failed: %v", err)
    }
}
```

## Testing Caching

### Testing Cache Hits

```go
func TestCacheHit(t *testing.T) {
    db, mock := setupMockDB(t)
    defer mock.ExpectClose()

    // Enable weak cache for testing
    db.UseCache(mysql.NewWeakCache())

    rows := sqlmock.NewRows([]string{"id", "name"}).
        AddRow(1, "Alice")

    // First query - cache miss
    mock.ExpectQuery("SELECT (.+) FROM users").
        WillReturnRows(rows)

    var users1 []User
    err := db.Select(&users1, "SELECT * FROM users", 5*time.Minute)
    if err != nil {
        t.Fatalf("First query failed: %v", err)
    }

    // Second query - cache hit (no DB query expected)
    var users2 []User
    err = db.Select(&users2, "SELECT * FROM users", 5*time.Minute)
    if err != nil {
        t.Fatalf("Second query failed: %v", err)
    }

    // Verify same results
    if len(users1) != len(users2) {
        t.Error("Cache returned different results")
    }

    if err := mock.ExpectationsWereMet(); err != nil {
        t.Errorf("Unfulfilled expectations: %v", err)
    }
}
```

### Testing Cache Bypass

```go
func TestCacheBypass(t *testing.T) {
    db, mock := setupMockDB(t)
    defer mock.ExpectClose()

    db.UseCache(mysql.NewWeakCache())

    rows := sqlmock.NewRows([]string{"id", "name"}).
        AddRow(1, "Alice")

    // Each query with TTL=0 should hit database
    mock.ExpectQuery("SELECT (.+) FROM users").
        WillReturnRows(rows)
    mock.ExpectQuery("SELECT (.+) FROM users").
        WillReturnRows(rows)

    var users []User
    db.Select(&users, "SELECT * FROM users", 0) // TTL=0, no cache
    db.Select(&users, "SELECT * FROM users", 0) // TTL=0, no cache

    if err := mock.ExpectationsWereMet(); err != nil {
        t.Errorf("Expected 2 queries, got different: %v", err)
    }
}
```

## Integration Testing

### End-to-End Test

```go
func TestUserWorkflow(t *testing.T) {
    db := testutil.SetupTestDB(t)

    // Create user
    user := User{
        Name:  "Alice",
        Email: "alice@example.com",
    }

    err := db.Insert("users", user)
    if err != nil {
        t.Fatalf("Insert failed: %v", err)
    }

    // Query user
    var retrieved User
    err = db.Select(&retrieved,
        "SELECT * FROM users WHERE email = @@email",
        0,
        mysql.Params{"email": "alice@example.com"})
    if err != nil {
        t.Fatalf("Select failed: %v", err)
    }

    // Update user
    err = db.Exec("UPDATE users SET name = @@name WHERE email = @@email",
        mysql.Params{"name": "Alice Updated", "email": "alice@example.com"})
    if err != nil {
        t.Fatalf("Update failed: %v", err)
    }

    // Verify update
    err = db.Select(&retrieved,
        "SELECT * FROM users WHERE email = @@email",
        0,
        mysql.Params{"email": "alice@example.com"})
    if err != nil {
        t.Fatalf("Select after update failed: %v", err)
    }

    if retrieved.Name != "Alice Updated" {
        t.Errorf("Expected 'Alice Updated', got '%s'", retrieved.Name)
    }

    // Delete user
    err = db.Exec("DELETE FROM users WHERE email = @@email",
        mysql.Params{"email": "alice@example.com"})
    if err != nil {
        t.Fatalf("Delete failed: %v", err)
    }

    // Verify deletion
    err = db.Select(&retrieved,
        "SELECT * FROM users WHERE email = @@email",
        0,
        mysql.Params{"email": "alice@example.com"})
    if !errors.Is(err, sql.ErrNoRows) {
        t.Error("Expected user to be deleted")
    }
}
```

## Best Practices

### 1. Use Helper Functions

```go
func expectUserQuery(mock sqlmock.Sqlmock, users []User) {
    rows := sqlmock.NewRows([]string{"id", "name", "email"})
    for _, u := range users {
        rows.AddRow(u.ID, u.Name, u.Email)
    }
    mock.ExpectQuery("SELECT (.+) FROM users").WillReturnRows(rows)
}

func TestWithHelper(t *testing.T) {
    db, mock := setupMockDB(t)
    defer mock.ExpectClose()

    expectedUsers := []User{{ID: 1, Name: "Alice", Email: "alice@example.com"}}
    expectUserQuery(mock, expectedUsers)

    var users []User
    db.Select(&users, "SELECT * FROM users", 0)

    // Assertions...
}
```

### 2. Test Error Paths

```go
func TestInsertDuplicateEmail(t *testing.T) {
    db, mock := setupMockDB(t)
    defer mock.ExpectClose()

    mock.ExpectExec("INSERT INTO users").
        WillReturnError(&mysql.MySQLError{Number: 1062}) // Duplicate entry

    user := User{Name: "Alice", Email: "alice@example.com"}
    err := db.Insert("users", user)

    if err == nil {
        t.Error("Expected duplicate key error")
    }
}
```

### 3. Clean Up Resources

```go
func TestWithCleanup(t *testing.T) {
    db, mock := setupMockDB(t)
    t.Cleanup(func() {
        mock.ExpectClose()
        // Any other cleanup
    })

    // Test code...
}
```

### 4. Test Concurrent Access

```go
func TestConcurrentAccess(t *testing.T) {
    db := testutil.SetupTestDB(t)

    var wg sync.WaitGroup
    errors := make(chan error, 10)

    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()

            user := User{
                Name:  fmt.Sprintf("User%d", id),
                Email: fmt.Sprintf("user%d@example.com", id),
            }

            if err := db.Insert("users", user); err != nil {
                errors <- err
            }
        }(i)
    }

    wg.Wait()
    close(errors)

    for err := range errors {
        t.Errorf("Concurrent insert failed: %v", err)
    }

    // Verify all users inserted
    var count int64
    count, err := db.Count("SELECT COUNT(*) FROM users", 0)
    if err != nil {
        t.Fatalf("Count failed: %v", err)
    }

    if count != 10 {
        t.Errorf("Expected 10 users, got %d", count)
    }
}
```

### 5. Use Subtests

```go
func TestUserOperations(t *testing.T) {
    db := testutil.SetupTestDB(t)

    t.Run("Insert", func(t *testing.T) {
        user := User{Name: "Alice", Email: "alice@example.com"}
        err := db.Insert("users", user)
        if err != nil {
            t.Fatalf("Insert failed: %v", err)
        }
    })

    t.Run("Select", func(t *testing.T) {
        var users []User
        err := db.Select(&users, "SELECT * FROM users", 0)
        if err != nil {
            t.Fatalf("Select failed: %v", err)
        }
        if len(users) == 0 {
            t.Error("Expected at least one user")
        }
    })

    t.Run("Update", func(t *testing.T) {
        err := db.Exec("UPDATE users SET name = @@name WHERE email = @@email",
            mysql.Params{"name": "Alice Updated", "email": "alice@example.com"})
        if err != nil {
            t.Fatalf("Update failed: %v", err)
        }
    })
}
```

### 6. Verify Expectations

```go
func TestAlwaysVerify(t *testing.T) {
    db, mock := setupMockDB(t)
    defer func() {
        if err := mock.ExpectationsWereMet(); err != nil {
            t.Errorf("Unfulfilled expectations: %v", err)
        }
    }()

    // Test code...
}
```

### 7. Test Parameter Interpolation

```go
func TestParameterInterpolation(t *testing.T) {
    db, _ := setupMockDB(t)

    query := "SELECT * FROM users WHERE age > @@minAge AND status = @@status"
    params := mysql.Params{"minAge": 18, "status": "active"}

    replacedQuery, normalizedParams, err := db.InterpolateParams(query, params)
    if err != nil {
        t.Fatalf("InterpolateParams failed: %v", err)
    }

    expectedQuery := "SELECT * FROM users WHERE age > ? AND status = ?"
    if replacedQuery != expectedQuery {
        t.Errorf("Expected query '%s', got '%s'", expectedQuery, replacedQuery)
    }

    if len(normalizedParams) != 2 {
        t.Errorf("Expected 2 params, got %d", len(normalizedParams))
    }
}
```
