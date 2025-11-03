# Struct Tags Reference

Complete guide to struct tag usage in cool-mysql for controlling column mapping and behavior.

## Table of Contents

1. [Basic Tag Syntax](#basic-tag-syntax)
2. [Tag Options](#tag-options)
3. [Default Value Handling](#default-value-handling)
4. [Special Characters](#special-characters)
5. [Custom Interfaces](#custom-interfaces)
6. [Advanced Patterns](#advanced-patterns)
7. [Common Gotchas](#common-gotchas)

## Basic Tag Syntax

### Column Mapping

```go
type User struct {
    ID    int    `mysql:"id"`      // Maps to 'id' column
    Name  string `mysql:"name"`    // Maps to 'name' column
    Email string `mysql:"email"`   // Maps to 'email' column
}
```

**Default Behavior (No Tag):**
```go
type User struct {
    ID   int    // Maps to 'ID' column (exact field name)
    Name string // Maps to 'Name' column
}
```

### Multiple Tags

```go
type User struct {
    ID        int       `mysql:"id" json:"id"`
    Name      string    `mysql:"name" json:"name"`
    CreatedAt time.Time `mysql:"created_at" json:"created_at"`
}
```

## Tag Options

### Available Options

| Option | Syntax | Behavior |
|--------|--------|----------|
| Column name | `mysql:"column_name"` | Maps to specific column |
| Default zero | `mysql:"column_name,defaultzero"` | Use DEFAULT() for zero values |
| Omit empty | `mysql:"column_name,omitempty"` | Same as `defaultzero` |
| Insert default | `mysql:"column_name,insertDefault"` | Same as `defaultzero` |
| Ignore field | `mysql:"-"` | Completely ignore field |

### Column Name Only

```go
type User struct {
    UserID int `mysql:"id"` // Field name differs from column name
}

// INSERT INTO `users` (id) VALUES (?)
```

### defaultzero Option

```go
type User struct {
    ID        int       `mysql:"id"`
    Name      string    `mysql:"name"`
    CreatedAt time.Time `mysql:"created_at,defaultzero"`
}

// If CreatedAt.IsZero():
//   INSERT INTO `users` (id, `name`, created_at) VALUES (?, ?, DEFAULT(created_at))
// Else:
//   INSERT INTO `users` (id, `name`, created_at) VALUES (?, ?, ?)
```

### omitempty Option

```go
type User struct {
    ID        int       `mysql:"id"`
    UpdatedAt time.Time `mysql:"updated_at,omitempty"`
}

// Equivalent to defaultzero
```

### insertDefault Option

```go
type User struct {
    ID        int       `mysql:"id"`
    CreatedAt time.Time `mysql:"created_at,insertDefault"`
}

// Equivalent to defaultzero
```

### Ignore Field

```go
type User struct {
    ID       int    `mysql:"id"`
    Password string `mysql:"-"` // Never included in queries
    internal string // Unexported fields also ignored
}

// INSERT INTO `users` (id) VALUES (?)
// Password is never inserted or selected
```

## Default Value Handling

### When to Use defaultzero

Use `defaultzero` when:
- Database column has a DEFAULT value
- You want to use database default for zero values
- Common for timestamps with `DEFAULT CURRENT_TIMESTAMP`

### Database Setup

```sql
CREATE TABLE users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

### Struct Definition

```go
type User struct {
    ID        int       `mysql:"id"`
    Name      string    `mysql:"name"`
    CreatedAt time.Time `mysql:"created_at,defaultzero"`
    UpdatedAt time.Time `mysql:"updated_at,defaultzero"`
}
```

### Usage

```go
// CreatedAt and UpdatedAt are zero values
user := User{
    Name: "Alice",
}

db.Insert("users", user)
// INSERT INTO `users` (name, created_at, updated_at)
// VALUES (?, DEFAULT(created_at), DEFAULT(updated_at))
// Database sets timestamps automatically
```

### Zero Value Detection

**Built-in zero values:**
- `int`, `int64`, etc.: `0`
- `string`: `""`
- `bool`: `false`
- `time.Time`: `time.Time{}.IsZero()` returns `true`
- `*T` (pointers): `nil`
- `[]T` (slices): `nil` or `len == 0`

**Custom zero detection:**
Implement `Zeroer` interface (see [Custom Interfaces](#custom-interfaces))

## Special Characters

### Hex Encoding

For column names with special characters, use hex encoding:

```go
type Data struct {
    // Column name: "column,name"
    Value string `mysql:"column0x2cname"`
}

// 0x2c is hex for ','
```

### Common Hex Codes

| Character | Hex Code | Example |
|-----------|----------|---------|
| `,` | `0x2c` | `column0x2cname` |
| `:` | `0x3a` | `column0x3aname` |
| `"` | `0x22` | `column0x22name` |
| Space | `0x20` | `column0x20name` |

### Generating Hex Codes

```go
// Get hex code for character
char := ','
hexCode := fmt.Sprintf("0x%x", char)
fmt.Println(hexCode) // 0x2c
```

## Custom Interfaces

### Zeroer Interface

Implement custom zero-value detection:

```go
type Zeroer interface {
    IsZero() bool
}
```

**Example:**

```go
type CustomTime struct {
    time.Time
}

func (ct CustomTime) IsZero() bool {
    // Consider Unix epoch (0) as zero
    return ct.Time.IsZero() || ct.Time.Unix() == 0
}

type Event struct {
    ID        int        `mysql:"id"`
    Timestamp CustomTime `mysql:"timestamp,defaultzero"`
}

// If Timestamp.Unix() == 0:
//   INSERT ... VALUES (..., DEFAULT(timestamp))
```

**Use Cases:**
- Custom "empty" definitions
- Sentinel values treated as zero
- Domain-specific zero logic

### Valueser Interface

Implement custom value conversion:

```go
type Valueser interface {
    Values() []any
}
```

**Example:**

```go
type Point struct {
    X, Y float64
}

func (p Point) Values() []any {
    return []any{p.X, p.Y}
}

type Location struct {
    ID       int   `mysql:"id"`
    Position Point `mysql:"x,y"` // Note: two columns
}

// INSERT INTO locations (id, x, y) VALUES (?, ?, ?)
// Point.Values() returns [X, Y]
```

**Use Cases:**
- Mapping Go type to multiple columns
- Custom serialization
- Complex type conversion

## Advanced Patterns

### Embedded Structs

```go
type Timestamps struct {
    CreatedAt time.Time `mysql:"created_at,defaultzero"`
    UpdatedAt time.Time `mysql:"updated_at,defaultzero"`
}

type User struct {
    ID    int    `mysql:"id"`
    Name  string `mysql:"name"`
    Timestamps // Embedded fields included
}

// SELECT `id`, `name`, created_at, updated_at FROM `users`
```

### Pointer Fields for NULL Handling

```go
type User struct {
    ID          int        `mysql:"id"`
    Name        string     `mysql:"name"`
    Email       *string    `mysql:"email"`       // NULL-able
    PhoneNumber *string    `mysql:"phone_number"` // NULL-able
    LastLogin   *time.Time `mysql:"last_login"`  // NULL-able
}

// Nil pointer = NULL in database
user := User{
    ID:   1,
    Name: "Alice",
    Email: nil, // Will be NULL in database
}

db.Insert("users", user)
// INSERT INTO `users` (id, `name`, `email`, phone_number, last_login)
// VALUES (?, ?, NULL, NULL, NULL)
```

### Partial Struct Selects

```go
// Full struct
type User struct {
    ID        int       `mysql:"id"`
    Name      string    `mysql:"name"`
    Email     string    `mysql:"email"`
    CreatedAt time.Time `mysql:"created_at"`
    UpdatedAt time.Time `mysql:"updated_at"`
}

// Partial struct for specific query
type UserSummary struct {
    ID   int    `mysql:"id"`
    Name string `mysql:"name"`
}

var summaries []UserSummary
db.Select(&summaries, "SELECT `id`, name FROM `users`", 0)
// Only maps id and name columns
```

### JSON Column Mapping

```go
type UserMeta struct {
    Theme       string                 `json:"theme"`
    Preferences map[string]interface{} `json:"preferences"`
}

type User struct {
    ID   int      `mysql:"id"`
    Name string   `mysql:"name"`
    Meta UserMeta `mysql:"meta"` // JSON column in MySQL
}

// cool-mysql automatically marshals/unmarshals JSON
db.Insert("users", User{
    ID:   1,
    Name: "Alice",
    Meta: UserMeta{
        Theme: "dark",
        Preferences: map[string]interface{}{"notifications": true},
    },
})
```

### Ignored Fields with Computed Values

```go
type User struct {
    ID        int       `mysql:"id"`
    FirstName string    `mysql:"first_name"`
    LastName  string    `mysql:"last_name"`
    FullName  string    `mysql:"-"` // Computed, not in DB
}

var users []User
db.Select(&users, "SELECT `id`, first_name, last_name FROM `users`", 0)

// Compute FullName after query
for i := range users {
    users[i].FullName = users[i].FirstName + " " + users[i].LastName
}
```

## Common Gotchas

### 1. Tag Takes Precedence Over Field Name

```go
type User struct {
    UserID int `mysql:"id"` // Column is 'id', not 'UserID'
}

// Query must use actual column name
db.Select(&user, "SELECT id FROM `users` WHERE `id` = @@id", 0,
    1)
```

### 2. Templates Use Field Names, Not Column Names

```go
type User struct {
    Username string `mysql:"user_name"` // Column: user_name, Field: Username
}

// CORRECT - uses field name
query := "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` {{ if .Username }}WHERE `user_name` = @@name{{ end }}"

// WRONG - user_name is column, not field
query := "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` {{ if .user_name }}WHERE `user_name` = @@name{{ end }}"
```

### 3. Unexported Fields Are Ignored

```go
type User struct {
    ID       int    `mysql:"id"`
    name     string `mysql:"name"` // Ignored - unexported
}

// Only ID is inserted
db.Insert("users", user)
```

### 4. Multiple Option Order Doesn't Matter

```go
// These are equivalent
`mysql:"column_name,defaultzero"`
`mysql:"column_name,omitempty"`
`mysql:"column_name,insertDefault"`
```

### 5. Embedded Struct Tag Conflicts

```go
type Base struct {
    ID int `mysql:"id"`
}

type User struct {
    Base
    ID int `mysql:"user_id"` // Shadows Base.ID
}

// User.ID takes precedence
```

### 6. Zero Values vs NULL

```go
// Zero value != NULL
type User struct {
    Age int `mysql:"age"` // 0 is inserted, not NULL
}

// Use pointer for NULL
type User struct {
    Age *int `mysql:"age"` // nil = NULL, 0 = 0
}
```

### 7. defaultzero Doesn't Affect SELECT

```go
type User struct {
    CreatedAt time.Time `mysql:"created_at,defaultzero"`
}

// defaultzero only affects INSERT/UPSERT, not SELECT
db.Select(&users, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users`", 0)
// CreatedAt is populated from database regardless of tag
```

## Tag Comparison

### json vs mysql Tags

```go
type User struct {
    ID   int    `mysql:"id" json:"userId"`   // DB: id, JSON: userId
    Name string `mysql:"name" json:"name"`   // Same for both
}

// MySQL: id, name
// JSON:  userId, name
```

### When Tags Differ

```go
type User struct {
    DatabaseID   int    `mysql:"id"`      // Database column
    UserName     string `mysql:"username"` // Database column
    ComputedRank int    `mysql:"-"`       // Not in database
}

// Database columns: id, username
// Struct fields: DatabaseID, UserName, ComputedRank
```

## Best Practices

### 1. Explicit Tags for Clarity

```go
// GOOD - explicit tags
type User struct {
    ID   int    `mysql:"id"`
    Name string `mysql:"name"`
}

// OKAY - relies on field names matching columns
type User struct {
    ID   int
    Name string
}
```

### 2. Use defaultzero for Timestamps

```go
type User struct {
    CreatedAt time.Time `mysql:"created_at,defaultzero"`
    UpdatedAt time.Time `mysql:"updated_at,defaultzero"`
}
```

### 3. Use Pointers for NULL-able Columns

```go
type User struct {
    Email     *string    `mysql:"email"`      // Can be NULL
    LastLogin *time.Time `mysql:"last_login"` // Can be NULL
}
```

### 4. Ignore Computed Fields

```go
type User struct {
    FirstName string `mysql:"first_name"`
    LastName  string `mysql:"last_name"`
    FullName  string `mysql:"-"` // Computed field
}
```

### 5. Document Custom Interfaces

```go
// CustomTime implements Zeroer for custom zero detection
type CustomTime struct {
    time.Time
}

func (ct CustomTime) IsZero() bool {
    return ct.Time.Unix() <= 0
}
```

### 6. Consistent Naming Convention

```go
// GOOD - consistent snake_case in tags
type User struct {
    ID        int    `mysql:"id"`
    FirstName string `mysql:"first_name"`
    LastName  string `mysql:"last_name"`
}

// AVOID - mixing conventions
type User struct {
    ID        int    `mysql:"id"`
    FirstName string `mysql:"firstName"` // camelCase
    LastName  string `mysql:"last_name"` // snake_case
}
```

### 7. Test Zero Value Behavior

```go
func TestUserInsertDefaults(t *testing.T) {
    user := User{Name: "Alice"} // CreatedAt is zero
    err := db.Insert("users", user)
    // Verify database used DEFAULT value
}
```

## Examples

### Complete User Struct

```go
type User struct {
    // Primary key
    ID int `mysql:"id"`

    // Basic fields
    Email    string `mysql:"email"`
    Username string `mysql:"username"`

    // NULL-able fields
    FirstName   *string    `mysql:"first_name"`
    LastName    *string    `mysql:"last_name"`
    PhoneNumber *string    `mysql:"phone_number"`
    LastLogin   *time.Time `mysql:"last_login"`

    // Timestamps with defaults
    CreatedAt time.Time `mysql:"created_at,defaultzero"`
    UpdatedAt time.Time `mysql:"updated_at,defaultzero"`

    // JSON column
    Metadata json.RawMessage `mysql:"metadata"`

    // Ignored fields
    Password     string `mysql:"-"` // Never persisted
    PasswordHash string `mysql:"password_hash"`
}
```

### Product with Custom Types

```go
type Decimal struct {
    Value *big.Float
}

func (d Decimal) Values() []any {
    if d.Value == nil {
        return []any{nil}
    }
    f, _ := d.Value.Float64()
    return []any{f}
}

func (d Decimal) IsZero() bool {
    return d.Value == nil || d.Value.Cmp(big.NewFloat(0)) == 0
}

type Product struct {
    ID          int     `mysql:"id"`
    Name        string  `mysql:"name"`
    Price       Decimal `mysql:"price,defaultzero"`
    Description *string `mysql:"description"`
    CreatedAt   time.Time `mysql:"created_at,defaultzero"`
}
```
