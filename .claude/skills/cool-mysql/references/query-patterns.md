# Query Patterns Guide

Practical examples and patterns for common cool-mysql query scenarios.

## Table of Contents

1. [Basic SELECT Patterns](#basic-select-patterns)
2. [Named Parameters](#named-parameters)
3. [Template Syntax](#template-syntax)
4. [Result Mapping](#result-mapping)
5. [Streaming with Channels](#streaming-with-channels)
6. [Function Receivers](#function-receivers)
7. [JSON Handling](#json-handling)
8. [Complex Queries](#complex-queries)
9. [Raw SQL](#raw-sql)

## Basic SELECT Patterns

### Select into Struct Slice

```go
type User struct {
    ID        int       `mysql:"id"`
    Name      string    `mysql:"name"`
    Email     string    `mysql:"email"`
    CreatedAt time.Time `mysql:"created_at"`
}

var users []User
err := db.Select(&users,
    "SELECT id, name, email, created_at FROM users WHERE age > @@minAge",
    5*time.Minute, // Cache for 5 minutes
    mysql.Params{"minAge": 18})
```

### Select Single Struct

```go
var user User
err := db.Select(&user,
    "SELECT * FROM users WHERE id = @@id",
    0, // No caching
    mysql.Params{"id": 123})

if errors.Is(err, sql.ErrNoRows) {
    // User not found
    return fmt.Errorf("user not found")
}
```

### Select Single Value

```go
// String value
var name string
err := db.Select(&name,
    "SELECT name FROM users WHERE id = @@id",
    0,
    mysql.Params{"id": 123})

// Integer value
var count int
err := db.Select(&count,
    "SELECT COUNT(*) FROM users WHERE active = @@active",
    0,
    mysql.Params{"active": 1})

// Time value
var lastLogin time.Time
err := db.Select(&lastLogin,
    "SELECT last_login FROM users WHERE id = @@id",
    0,
    mysql.Params{"id": 123})
```

### Select Multiple Values (First Row Only)

```go
type UserInfo struct {
    Name  string
    Email string
    Age   int
}

var info UserInfo
err := db.Select(&info,
    "SELECT name, email, age FROM users WHERE id = @@id",
    0,
    mysql.Params{"id": 123})
```

## Named Parameters

### Basic Parameter Usage

```go
// Simple parameters
err := db.Select(&users,
    "SELECT * FROM users WHERE age > @@minAge AND status = @@status",
    0,
    mysql.Params{"minAge": 18, "status": "active"})
```

### Struct as Parameters

```go
// Use struct fields as parameters
filter := struct {
    MinAge int
    Status string
    City   string
}{
    MinAge: 18,
    Status: "active",
    City:   "New York",
}

err := db.Select(&users,
    "SELECT * FROM users WHERE age > @@MinAge AND status = @@Status AND city = @@City",
    0,
    filter)
```

### Multiple Parameter Sources

```go
// Parameters are merged (last wins for duplicates)
err := db.Select(&users,
    "SELECT * FROM users WHERE age > @@minAge AND status = @@status AND city = @@city",
    0,
    mysql.Params{"minAge": 18, "status": "active"},
    mysql.Params{"city": "New York"},
)
```

### Parameter Reuse

```go
// Same parameter used multiple times
err := db.Select(&users,
    `SELECT * FROM users
     WHERE (age BETWEEN @@minAge AND @@maxAge)
     AND (created_at > @@date OR updated_at > @@date)`,
    0,
    mysql.Params{
        "minAge": 18,
        "maxAge": 65,
        "date":   time.Now().Add(-7*24*time.Hour),
    })
```

### Case-Insensitive Parameter Merging

```go
// These parameters are treated as the same (normalized to lowercase)
err := db.Select(&users,
    "SELECT * FROM users WHERE name = @@userName",
    0,
    mysql.Params{"username": "Alice"}, // lowercase 'u'
    mysql.Params{"UserName": "Bob"},   // uppercase 'U' - this wins
)
// Effective parameter: "Bob"
```

## Template Syntax

### Conditional Query Parts

```go
// Add WHERE conditions dynamically
params := mysql.Params{
    "minAge": 18,
    "status": "active",
}

query := `
    SELECT * FROM users
    WHERE 1=1
    {{ if .MinAge }}AND age > @@minAge{{ end }}
    {{ if .Status }}AND status = @@status{{ end }}
`

var users []User
err := db.Select(&users, query, 0, params)
```

### Dynamic ORDER BY

```go
type QueryParams struct {
    SortBy    string
    SortOrder string
}

params := QueryParams{
    SortBy:    "created_at",
    SortOrder: "DESC",
}

query := `
    SELECT * FROM users
    WHERE active = 1
    {{ if .SortBy }}
    ORDER BY {{ .SortBy }} {{ .SortOrder }}
    {{ end }}
`

var users []User
err := db.Select(&users, query, 0, params)
```

### Conditional JOINs

```go
type SearchParams struct {
    IncludeOrders bool
    IncludeAddress bool
}

params := SearchParams{
    IncludeOrders: true,
    IncludeAddress: false,
}

query := `
    SELECT u.*
    {{ if .IncludeOrders }}, o.order_count{{ end }}
    {{ if .IncludeAddress }}, a.city{{ end }}
    FROM users u
    {{ if .IncludeOrders }}
    LEFT JOIN (
        SELECT user_id, COUNT(*) as order_count
        FROM orders
        GROUP BY user_id
    ) o ON u.id = o.user_id
    {{ end }}
    {{ if .IncludeAddress }}
    LEFT JOIN addresses a ON u.id = a.user_id
    {{ end }}
`

var users []User
err := db.Select(&users, query, 0, params)
```

### Template with Custom Functions

```go
// Add custom template functions
db.AddTemplateFuncs(template.FuncMap{
    "upper": strings.ToUpper,
    "quote": func(s string) string { return fmt.Sprintf("'%s'", s) },
})

// Use in query
query := `
    SELECT * FROM users
    WHERE status = {{ quote (upper .Status) }}
`

err := db.Select(&users, query, 0, mysql.Params{"Status": "active"})
// Generates: WHERE status = 'ACTIVE'
```

### Template Best Practices

```go
// DON'T: Use column names from tags in templates
type User struct {
    Username string `mysql:"user_name"` // Column is "user_name"
}

// WRONG - uses column name
query := `SELECT * FROM users {{ if .user_name }}WHERE name = @@name{{ end }}`

// CORRECT - uses field name
query := `SELECT * FROM users {{ if .Username }}WHERE name = @@name{{ end }}`
```

## Result Mapping

### Map Results

```go
// Single row as map
var row mysql.MapRow
err := db.Select(&row,
    "SELECT id, name, email FROM users WHERE id = @@id",
    0,
    mysql.Params{"id": 123})
fmt.Printf("Name: %v\n", row["name"])

// Multiple rows as maps
var rows mysql.MapRows
err := db.Select(&rows,
    "SELECT id, name, email FROM users",
    0)
for _, row := range rows {
    fmt.Printf("ID: %v, Name: %v\n", row["id"], row["name"])
}
```

### Slice Results

```go
// Single row as slice
var row mysql.SliceRow
err := db.Select(&row,
    "SELECT id, name, email FROM users WHERE id = @@id",
    0,
    mysql.Params{"id": 123})
fmt.Printf("First column: %v\n", row[0])

// Multiple rows as slices
var rows mysql.SliceRows
err := db.Select(&rows,
    "SELECT id, name, email FROM users",
    0)
for _, row := range rows {
    fmt.Printf("Row: %v\n", row)
}
```

### Partial Struct Mapping

```go
// Struct with subset of columns
type UserSummary struct {
    ID   int    `mysql:"id"`
    Name string `mysql:"name"`
}

var summaries []UserSummary
err := db.Select(&summaries,
    "SELECT id, name FROM users",
    0)
```

### Embedded Structs

```go
type Timestamps struct {
    CreatedAt time.Time `mysql:"created_at"`
    UpdatedAt time.Time `mysql:"updated_at"`
}

type User struct {
    ID    int    `mysql:"id"`
    Name  string `mysql:"name"`
    Email string `mysql:"email"`
    Timestamps
}

var users []User
err := db.Select(&users,
    "SELECT id, name, email, created_at, updated_at FROM users",
    0)
```

### Pointer Fields

```go
type User struct {
    ID        int        `mysql:"id"`
    Name      string     `mysql:"name"`
    Email     *string    `mysql:"email"` // NULL-able
    LastLogin *time.Time `mysql:"last_login"` // NULL-able
}

var users []User
err := db.Select(&users, "SELECT * FROM users", 0)

for _, user := range users {
    if user.Email != nil {
        fmt.Printf("Email: %s\n", *user.Email)
    }
    if user.LastLogin != nil {
        fmt.Printf("Last login: %v\n", *user.LastLogin)
    }
}
```

## Streaming with Channels

### Select into Channel

```go
// Stream results to avoid loading all into memory
userCh := make(chan User, 100) // Buffered channel

go func() {
    defer close(userCh)
    if err := db.Select(userCh, "SELECT * FROM users", 0); err != nil {
        log.Printf("Select error: %v", err)
    }
}()

// Process as they arrive
for user := range userCh {
    if err := processUser(user); err != nil {
        log.Printf("Process error: %v", err)
    }
}
```

### Insert from Channel

```go
// Stream inserts to avoid building large slice
userCh := make(chan User, 100)

// Producer
go func() {
    defer close(userCh)
    for i := 0; i < 10000; i++ {
        userCh <- User{
            Name:  fmt.Sprintf("User %d", i),
            Email: fmt.Sprintf("user%d@example.com", i),
        }
    }
}()

// Consumer - automatically chunks and inserts
if err := db.Insert("users", userCh); err != nil {
    log.Printf("Insert error: %v", err)
}
```

### Channel with Context

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

userCh := make(chan User, 100)

go func() {
    defer close(userCh)
    db.SelectContext(ctx, userCh, "SELECT * FROM users", 0)
}()

for user := range userCh {
    select {
    case <-ctx.Done():
        log.Println("Timeout reached")
        return ctx.Err()
    default:
        processUser(user)
    }
}
```

## Function Receivers

### Basic Function Receiver

```go
// Process each row with function
err := db.Select(func(u User) {
    log.Printf("Processing user: %s (%s)", u.Name, u.Email)
}, "SELECT * FROM users", 0)
```

### Function Receiver with Error Handling

```go
// Return error to stop iteration
var processErr error
err := db.Select(func(u User) {
    if err := validateUser(u); err != nil {
        processErr = err
        return
    }
    processUser(u)
}, "SELECT * FROM users", 0)

if err != nil {
    return err
}
if processErr != nil {
    return processErr
}
```

### Aggregation with Function Receiver

```go
// Collect aggregate data
var totalAge int
var count int

err := db.Select(func(u User) {
    totalAge += u.Age
    count++
}, "SELECT age FROM users WHERE active = 1", 0)

if count > 0 {
    avgAge := float64(totalAge) / float64(count)
    fmt.Printf("Average age: %.2f\n", avgAge)
}
```

## JSON Handling

### JSON Column to Struct Field

```go
type UserMeta struct {
    Preferences map[string]any `json:"preferences"`
    Settings    map[string]any `json:"settings"`
}

type User struct {
    ID   int      `mysql:"id"`
    Name string   `mysql:"name"`
    Meta UserMeta `mysql:"meta"` // JSON column
}

var users []User
err := db.Select(&users,
    "SELECT id, name, meta FROM users",
    0)

for _, user := range users {
    fmt.Printf("Preferences: %+v\n", user.Meta.Preferences)
}
```

### Select as JSON

```go
var result json.RawMessage
err := db.SelectJSON(&result,
    `SELECT JSON_OBJECT(
        'id', id,
        'name', name,
        'email', email
    ) FROM users WHERE id = @@id`,
    0,
    mysql.Params{"id": 123})

fmt.Printf("JSON: %s\n", string(result))
```

### JSON Array Results

```go
var results json.RawMessage
err := db.SelectJSON(&results,
    `SELECT JSON_ARRAYAGG(
        JSON_OBJECT(
            'id', id,
            'name', name
        )
    ) FROM users`,
    0)
```

## Complex Queries

### Subqueries with Named Parameters

```go
query := `
    SELECT u.*,
           (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) as order_count
    FROM users u
    WHERE u.created_at > @@since
    AND u.status = @@status
`

var users []struct {
    User
    OrderCount int `mysql:"order_count"`
}

err := db.Select(&users, query, 5*time.Minute,
    mysql.Params{
        "since":  time.Now().Add(-30*24*time.Hour),
        "status": "active",
    })
```

### JOINs with Named Parameters

```go
query := `
    SELECT
        u.id,
        u.name,
        u.email,
        o.order_id,
        o.total
    FROM users u
    INNER JOIN orders o ON u.id = o.user_id
    WHERE u.status = @@status
    AND o.created_at > @@since
    AND o.total > @@minTotal
    ORDER BY o.created_at DESC
`

type UserOrder struct {
    UserID  int     `mysql:"id"`
    Name    string  `mysql:"name"`
    Email   string  `mysql:"email"`
    OrderID int     `mysql:"order_id"`
    Total   float64 `mysql:"total"`
}

var results []UserOrder
err := db.Select(&results, query, 0,
    mysql.Params{
        "status":   "active",
        "since":    time.Now().Add(-7*24*time.Hour),
        "minTotal": 100.0,
    })
```

### IN Clause with Multiple Values

```go
// For small lists, use template
ids := []int{1, 2, 3, 4, 5}
idsStr := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(ids)), ","), "[]")

query := `SELECT * FROM users WHERE id IN (@@ids)`
var users []User
err := db.Select(&users, query, 0,
    mysql.Params{"ids": mysql.Raw(idsStr)})

// Better approach: JSON_TABLE (MySQL 8.0+)
idsJSON, _ := json.Marshal(ids)
query := `
    SELECT u.*
    FROM users u
    JOIN JSON_TABLE(
        @@ids,
        '$[*]' COLUMNS(id INT PATH '$')
    ) AS j ON u.id = j.id
`
err := db.Select(&users, query, 0,
    mysql.Params{"ids": string(idsJSON)})
```

### Window Functions

```go
query := `
    SELECT
        id,
        name,
        salary,
        RANK() OVER (ORDER BY salary DESC) as salary_rank,
        AVG(salary) OVER () as avg_salary
    FROM employees
    WHERE department = @@dept
`

type EmployeeStats struct {
    ID          int     `mysql:"id"`
    Name        string  `mysql:"name"`
    Salary      float64 `mysql:"salary"`
    SalaryRank  int     `mysql:"salary_rank"`
    AvgSalary   float64 `mysql:"avg_salary"`
}

var stats []EmployeeStats
err := db.Select(&stats, query, 5*time.Minute,
    mysql.Params{"dept": "Engineering"})
```

## Raw SQL

### Literal SQL Injection

```go
// Use Raw() for SQL that shouldn't be escaped
// WARNING: Never use with user input - SQL injection risk!

query := `
    SELECT * FROM users
    WHERE @@dynamicCondition
    AND status = @@status
`

err := db.Select(&users, query, 0,
    mysql.Params{
        "dynamicCondition": mysql.Raw("created_at > NOW() - INTERVAL 7 DAY"),
        "status":           "active", // This IS escaped
    })
```

### Dynamic Table Names

```go
// Table names can't be parameterized - use Raw carefully
tableName := "users" // Validate this!

query := fmt.Sprintf("SELECT * FROM %s WHERE status = @@status",
    tableName) // Ensure tableName is validated/sanitized!

var users []User
err := db.Select(&users, query, 0,
    mysql.Params{"status": "active"})
```

### CASE Statements with Raw

```go
query := `
    SELECT
        id,
        name,
        @@statusCase as status_label
    FROM users
`

statusCase := mysql.Raw(`
    CASE status
        WHEN 1 THEN 'Active'
        WHEN 2 THEN 'Inactive'
        WHEN 3 THEN 'Suspended'
        ELSE 'Unknown'
    END
`)

type UserWithLabel struct {
    ID          int    `mysql:"id"`
    Name        string `mysql:"name"`
    StatusLabel string `mysql:"status_label"`
}

var users []UserWithLabel
err := db.Select(&users, query, 0,
    mysql.Params{"statusCase": statusCase})
```

## Debugging Queries

### Inspect Interpolated Query

```go
query := "SELECT * FROM users WHERE age > @@minAge AND status = @@status"
params := mysql.Params{"minAge": 18, "status": "active"}

replacedQuery, normalizedParams, err := db.InterpolateParams(query, params)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Query: %s\n", replacedQuery)
fmt.Printf("Params: %+v\n", normalizedParams)
// Query: SELECT * FROM users WHERE age > ? AND status = ?
// Params: [18 active]
```

### Log Query Execution

```go
// Set up query logging
db.SetQueryLogger(func(query string, args []any, duration time.Duration, err error) {
    log.Printf("[%v] %s %+v (err: %v)", duration, query, args, err)
})

// Now all queries will be logged
db.Select(&users, "SELECT * FROM users WHERE age > @@age", 0,
    mysql.Params{"age": 18})
```

## Performance Tips

1. **Use caching for expensive queries**: Set appropriate TTL based on data volatility
2. **Stream large result sets**: Use channels instead of loading all into memory
3. **Batch inserts**: Use slices or channels instead of individual inserts
4. **Use SelectWrites sparingly**: Only when you need read-after-write consistency
5. **Index your parameters**: Ensure WHERE clause columns are indexed
6. **Avoid SELECT ***: Specify only columns you need for better performance
7. **Use templates wisely**: Don't overcomplicate queries - keep them readable
8. **Monitor cache hit rates**: Tune TTLs based on actual hit rates
