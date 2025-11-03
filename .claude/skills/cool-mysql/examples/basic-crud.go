// Package examples demonstrates basic CRUD operations with cool-mysql
package examples

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	mysql "github.com/StirlingMarketingGroup/cool-mysql"
)

// User represents a user in the database
type User struct {
	ID        int       `mysql:"id"`
	Name      string    `mysql:"name"`
	Email     string    `mysql:"email"`
	Age       int       `mysql:"age"`
	Active    bool      `mysql:"active"`
	CreatedAt time.Time `mysql:"created_at,defaultzero"`
	UpdatedAt time.Time `mysql:"updated_at,defaultzero"`
}

// BasicCRUDExamples demonstrates basic Create, Read, Update, Delete operations
func BasicCRUDExamples() {
	// Setup database connection
	db, err := setupDatabase()
	if err != nil {
		log.Fatalf("Failed to setup database: %v", err)
	}

	// Create
	fmt.Println("=== CREATE EXAMPLES ===")
	createExamples(db)

	// Read
	fmt.Println("\n=== READ EXAMPLES ===")
	readExamples(db)

	// Update
	fmt.Println("\n=== UPDATE EXAMPLES ===")
	updateExamples(db)

	// Delete
	fmt.Println("\n=== DELETE EXAMPLES ===")
	deleteExamples(db)

	// Utility queries
	fmt.Println("\n=== UTILITY EXAMPLES ===")
	utilityExamples(db)
}

// setupDatabase creates a connection to MySQL
func setupDatabase() (*mysql.Database, error) {
	// Create database connection with read/write pools
	db, err := mysql.New(
		"root",      // write user
		"password",  // write password
		"mydb",      // write schema
		"localhost", // write host
		3306,        // write port
		"root",      // read user
		"password",  // read password
		"mydb",      // read schema
		"localhost", // read host
		3306,        // read port
		"utf8mb4_unicode_ci", // collation
		time.UTC,    // timezone
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create database: %w", err)
	}

	return db, nil
}

// createExamples demonstrates INSERT operations
func createExamples(db *mysql.Database) {
	// Example 1: Insert single user
	fmt.Println("1. Insert single user")
	user := User{
		Name:   "Alice",
		Email:  "alice@example.com",
		Age:    25,
		Active: true,
	}

	err := db.Insert("users", user)
	if err != nil {
		log.Printf("Insert failed: %v", err)
	} else {
		fmt.Println("✓ User inserted successfully")
	}

	// Example 2: Insert with explicit zero values
	fmt.Println("\n2. Insert with timestamp defaults")
	userWithDefaults := User{
		Name:   "Bob",
		Email:  "bob@example.com",
		Age:    30,
		Active: true,
		// CreatedAt and UpdatedAt are zero values
		// With ,defaultzero tag, database will use DEFAULT values
	}

	err = db.Insert("users", userWithDefaults)
	if err != nil {
		log.Printf("Insert failed: %v", err)
	} else {
		fmt.Println("✓ User inserted with database defaults")
	}

	// Example 3: Batch insert
	fmt.Println("\n3. Batch insert multiple users")
	users := []User{
		{Name: "Charlie", Email: "charlie@example.com", Age: 28, Active: true},
		{Name: "Diana", Email: "diana@example.com", Age: 32, Active: true},
		{Name: "Eve", Email: "eve@example.com", Age: 27, Active: false},
	}

	err = db.Insert("users", users)
	if err != nil {
		log.Printf("Batch insert failed: %v", err)
	} else {
		fmt.Printf("✓ Batch inserted %d users\n", len(users))
	}

	// Example 4: Insert with context
	fmt.Println("\n4. Insert with context timeout")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	contextUser := User{
		Name:   "Frank",
		Email:  "frank@example.com",
		Age:    35,
		Active: true,
	}

	err = db.InsertContext(ctx, "users", contextUser)
	if err != nil {
		log.Printf("Insert with context failed: %v", err)
	} else {
		fmt.Println("✓ User inserted with context")
	}
}

// readExamples demonstrates SELECT operations
func readExamples(db *mysql.Database) {
	// Example 1: Select all users into slice
	fmt.Println("1. Select all users")
	var allUsers []User
	err := db.Select(&allUsers, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users`", 0)
	if err != nil {
		log.Printf("Select all failed: %v", err)
	} else {
		fmt.Printf("✓ Found %d users\n", len(allUsers))
		for _, u := range allUsers {
			fmt.Printf("  - %s (%s), Age: %d\n", u.Name, u.Email, u.Age)
		}
	}

	// Example 2: Select with named parameters
	fmt.Println("\n2. Select users with age filter")
	var adults []User
	err = db.Select(&adults,
		"SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE age >= @@minAge",
		0, // No caching
		25)
	if err != nil {
		log.Printf("Select with filter failed: %v", err)
	} else {
		fmt.Printf("✓ Found %d users aged 25+\n", len(adults))
	}

	// Example 3: Select single user
	fmt.Println("\n3. Select single user by email")
	var user User
	err = db.Select(&user,
		"SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `email` = @@email",
		0,
		"alice@example.com")
	if errors.Is(err, sql.ErrNoRows) {
		fmt.Println("✗ User not found")
	} else if err != nil {
		log.Printf("Select failed: %v", err)
	} else {
		fmt.Printf("✓ Found user: %s (ID: %d)\n", user.Name, user.ID)
	}

	// Example 4: Select single value
	fmt.Println("\n4. Select single value (name)")
	var name string
	err = db.Select(&name,
		"SELECT `name` FROM `users` WHERE `email` = @@email",
		0,
		"bob@example.com")
	if err != nil {
		log.Printf("Select name failed: %v", err)
	} else {
		fmt.Printf("✓ User name: %s\n", name)
	}

	// Example 5: Select with multiple conditions
	fmt.Println("\n5. Select with multiple conditions")
	var activeAdults []User
	err = db.Select(&activeAdults,
		"SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users`"+
		" WHERE `age` >= @@minAge"+
		" AND `active` = @@active"+
		" ORDER BY `name`",
		0,
		mysql.Params{
			"minAge": 25,
			"active": true,
		})
	if err != nil {
		log.Printf("Select failed: %v", err)
	} else {
		fmt.Printf("✓ Found %d active adult users\n", len(activeAdults))
	}

	// Example 6: Select with caching
	fmt.Println("\n6. Select with caching (5 minute TTL)")
	var cachedUsers []User
	err = db.Select(&cachedUsers,
		"SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `active` = @@active",
		5*time.Minute, // Cache for 5 minutes
		true)
	if err != nil {
		log.Printf("Cached select failed: %v", err)
	} else {
		fmt.Printf("✓ Found %d active users (cached)\n", len(cachedUsers))
	}

	// Example 7: Select with context
	fmt.Println("\n7. Select with context timeout")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var contextUsers []User
	err = db.SelectContext(ctx, &contextUsers,
		"SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` LIMIT @@limit",
		0,
		10)
	if err != nil {
		log.Printf("Select with context failed: %v", err)
	} else {
		fmt.Printf("✓ Found %d users with context\n", len(contextUsers))
	}
}

// updateExamples demonstrates UPDATE operations
func updateExamples(db *mysql.Database) {
	// Example 1: Simple update
	fmt.Println("1. Update user name")
	err := db.Exec(
		"UPDATE `users` SET `name` = @@name WHERE `email` = @@email",
		mysql.Params{
			"name":  "Alice Smith",
			"email": "alice@example.com",
		})
	if err != nil {
		log.Printf("Update failed: %v", err)
	} else {
		fmt.Println("✓ User name updated")
	}

	// Example 2: Update with result
	fmt.Println("\n2. Update with result check")
	result, err := db.ExecResult(
		"UPDATE `users` SET `age` = @@age WHERE `email` = @@email",
		mysql.Params{
			"age":   26,
			"email": "alice@example.com",
		})
	if err != nil {
		log.Printf("Update failed: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		fmt.Printf("✓ Updated %d row(s)\n", rowsAffected)
	}

	// Example 3: Update multiple rows
	fmt.Println("\n3. Update multiple rows")
	result, err = db.ExecResult(
		"UPDATE `users` SET `active` = @@active WHERE age < @@maxAge",
		mysql.Params{
			"active": false,
			"maxAge": 25,
		})
	if err != nil {
		log.Printf("Bulk update failed: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		fmt.Printf("✓ Deactivated %d user(s)\n", rowsAffected)
	}

	// Example 4: Update with current timestamp
	fmt.Println("\n4. Update timestamp")
	err = db.Exec(
		"UPDATE `users` SET `updated_at` = NOW() WHERE `email` = @@email",
		"bob@example.com")
	if err != nil {
		log.Printf("Update timestamp failed: %v", err)
	} else {
		fmt.Println("✓ Timestamp updated")
	}

	// Example 5: Conditional update
	fmt.Println("\n5. Conditional update (only if age is current value)")
	err = db.Exec(
		"UPDATE `users`"+
		" SET `age` = @@newAge"+
		" WHERE `email` = @@email"+
		" AND `age` = @@currentAge",
		mysql.Params{
			"newAge":     27,
			"email":      "charlie@example.com",
			"currentAge": 28,
		})
	if err != nil {
		log.Printf("Conditional update failed: %v", err)
	} else {
		fmt.Println("✓ Conditional update executed")
	}

	// Example 6: Update with context
	fmt.Println("\n6. Update with context")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = db.ExecContext(ctx,
		"UPDATE `users` SET `active` = @@active WHERE age > @@age",
		mysql.Params{
			"active": true,
			"age":    30,
		})
	if err != nil {
		log.Printf("Update with context failed: %v", err)
	} else {
		fmt.Println("✓ Update executed with context")
	}
}

// deleteExamples demonstrates DELETE operations
func deleteExamples(db *mysql.Database) {
	// Example 1: Delete single record
	fmt.Println("1. Delete single user")
	err := db.Exec(
		"DELETE FROM `users` WHERE `email` = @@email",
		"eve@example.com")
	if err != nil {
		log.Printf("Delete failed: %v", err)
	} else {
		fmt.Println("✓ User deleted")
	}

	// Example 2: Delete with result check
	fmt.Println("\n2. Delete with result check")
	result, err := db.ExecResult(
		"DELETE FROM `users` WHERE `email` = @@email",
		"frank@example.com")
	if err != nil {
		log.Printf("Delete failed: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			fmt.Println("✗ No user found to delete")
		} else {
			fmt.Printf("✓ Deleted %d user(s)\n", rowsAffected)
		}
	}

	// Example 3: Delete multiple records
	fmt.Println("\n3. Delete inactive users")
	result, err = db.ExecResult(
		"DELETE FROM `users` WHERE `active` = @@active",
		false)
	if err != nil {
		log.Printf("Bulk delete failed: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		fmt.Printf("✓ Deleted %d inactive user(s)\n", rowsAffected)
	}

	// Example 4: Delete with age condition
	fmt.Println("\n4. Delete users under age threshold")
	result, err = db.ExecResult(
		"DELETE FROM `users` WHERE age < @@minAge",
		18)
	if err != nil {
		log.Printf("Delete failed: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		fmt.Printf("✓ Deleted %d user(s) under 18\n", rowsAffected)
	}

	// Example 5: Delete with context
	fmt.Println("\n5. Delete with context")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = db.ExecContext(ctx,
		"DELETE FROM `users` WHERE `created_at` < @@cutoff",
		time.Now().Add(-365*24*time.Hour))
	if err != nil {
		log.Printf("Delete with context failed: %v", err)
	} else {
		fmt.Println("✓ Old users deleted with context")
	}
}

// utilityExamples demonstrates utility query methods
func utilityExamples(db *mysql.Database) {
	// Example 1: Count users
	fmt.Println("1. Count all users")
	count, err := db.Count(
		"SELECT COUNT(*) FROM `users`",
		0) // No caching
	if err != nil {
		log.Printf("Count failed: %v", err)
	} else {
		fmt.Printf("✓ Total users: %d\n", count)
	}

	// Example 2: Count with condition
	fmt.Println("\n2. Count active users")
	activeCount, err := db.Count(
		"SELECT COUNT(*) FROM `users` WHERE `active` = @@active",
		0,
		true)
	if err != nil {
		log.Printf("Count failed: %v", err)
	} else {
		fmt.Printf("✓ Active users: %d\n", activeCount)
	}

	// Example 3: Count with caching
	fmt.Println("\n3. Count with caching")
	cachedCount, err := db.Count(
		"SELECT COUNT(*) FROM `users`",
		5*time.Minute) // Cache for 5 minutes
	if err != nil {
		log.Printf("Cached count failed: %v", err)
	} else {
		fmt.Printf("✓ Cached user count: %d\n", cachedCount)
	}

	// Example 4: Check if user exists
	fmt.Println("\n4. Check if email exists")
	exists, err := db.Exists(
		"SELECT 1 FROM `users` WHERE `email` = @@email",
		0,
		"alice@example.com")
	if err != nil {
		log.Printf("Exists check failed: %v", err)
	} else {
		if exists {
			fmt.Println("✓ Email exists in database")
		} else {
			fmt.Println("✗ Email not found")
		}
	}

	// Example 5: Check existence with multiple conditions
	fmt.Println("\n5. Check if active adult exists")
	exists, err = db.Exists(
		"SELECT 1 FROM `users`"+
		" WHERE `active` = @@active"+
		" AND `age` >= @@minAge",
		0,
		mysql.Params{
			"active": true,
			"minAge": 25,
		})
	if err != nil {
		log.Printf("Exists check failed: %v", err)
	} else {
		if exists {
			fmt.Println("✓ Active adult user exists")
		} else {
			fmt.Println("✗ No active adult users found")
		}
	}

	// Example 6: Read-after-write with SelectWrites
	fmt.Println("\n6. Read-after-write consistency")

	// Insert user
	newUser := User{
		Name:   "Grace",
		Email:  "grace@example.com",
		Age:    29,
		Active: true,
	}
	err = db.Insert("users", newUser)
	if err != nil {
		log.Printf("Insert failed: %v", err)
		return
	}

	// Immediately read using write pool for consistency
	var retrieved User
	err = db.SelectWrites(&retrieved,
		"SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `email` = @@email",
		0, // Don't cache writes
		"grace@example.com")
	if err != nil {
		log.Printf("SelectWrites failed: %v", err)
	} else {
		fmt.Printf("✓ User retrieved immediately after insert: %s (ID: %d)\n",
			retrieved.Name, retrieved.ID)
	}
}
