// Package examples demonstrates transaction patterns with cool-mysql
package examples

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	mysql "github.com/StirlingMarketingGroup/cool-mysql"
)

// TransactionExamples demonstrates transaction handling patterns
func TransactionExamples() {
	fmt.Println("=== TRANSACTION EXAMPLES ===")

	// Basic transaction
	fmt.Println("\n1. Basic Transaction")
	basicTransactionExample()

	// Nested transaction handling
	fmt.Println("\n2. Nested Transaction (Context-Based)")
	nestedTransactionExample()

	// Rollback on error
	fmt.Println("\n3. Automatic Rollback on Error")
	rollbackExample()

	// Complex transaction
	fmt.Println("\n4. Complex Multi-Step Transaction")
	complexTransactionExample()
}

// basicTransactionExample demonstrates basic transaction usage
func basicTransactionExample() {
	db, err := setupDatabase()
	if err != nil {
		log.Printf("Setup failed: %v", err)
		return
	}

	ctx := context.Background()

	// Get or create transaction
	tx, commit, cancel, err := mysql.GetOrCreateTxFromContext(ctx)
	defer cancel() // Always safe to call - rolls back if commit() not called
	if err != nil {
		log.Printf("Failed to create transaction: %v", err)
		return
	}

	// Store transaction in context
	ctx = mysql.NewContextWithTx(ctx, tx)

	// Execute operations in transaction
	user := User{
		Name:   "TxUser1",
		Email:  "tx1@example.com",
		Age:    30,
		Active: true,
	}

	err = db.Insert("users", user)
	if err != nil {
		log.Printf("Insert failed: %v", err)
		return // cancel() will rollback
	}

	fmt.Println("  User inserted in transaction")

	// Update in same transaction
	err = db.Exec("UPDATE `users` SET `age` = @@age WHERE `email` = @@email",
		mysql.Params{
			"age":   31,
			"email": "tx1@example.com",
		})

	if err != nil {
		log.Printf("Update failed: %v", err)
		return // cancel() will rollback
	}

	fmt.Println("  User updated in transaction")

	// Commit transaction
	if err := commit(); err != nil {
		log.Printf("Commit failed: %v", err)
		return
	}

	fmt.Println("✓ Transaction committed successfully")
}

// nestedTransactionExample demonstrates nested transaction handling
func nestedTransactionExample() {
	db, err := setupDatabase()
	if err != nil {
		log.Printf("Setup failed: %v", err)
		return
	}

	ctx := context.Background()

	// Start outer transaction
	err = outerTransaction(ctx, db)
	if err != nil {
		log.Printf("Outer transaction failed: %v", err)
		return
	}

	fmt.Println("✓ Nested transactions completed")
}

func outerTransaction(ctx context.Context, db *mysql.Database) error {
	// Get or create transaction
	tx, commit, cancel, err := mysql.GetOrCreateTxFromContext(ctx)
	defer cancel()
	if err != nil {
		return fmt.Errorf("outer tx failed: %w", err)
	}

	// Store in context
	ctx = mysql.NewContextWithTx(ctx, tx)

	fmt.Println("  Started outer transaction")

	// Insert user
	user := User{
		Name:   "OuterTxUser",
		Email:  "outer@example.com",
		Age:    25,
		Active: true,
	}

	err = db.Insert("users", user)
	if err != nil {
		return fmt.Errorf("outer insert failed: %w", err)
	}

	fmt.Println("  Outer: User inserted")

	// Call inner function with same context
	// GetOrCreateTxFromContext will return existing transaction
	err = innerTransaction(ctx, db)
	if err != nil {
		return fmt.Errorf("inner tx failed: %w", err)
	}

	// Commit outer transaction
	if err := commit(); err != nil {
		return fmt.Errorf("outer commit failed: %w", err)
	}

	fmt.Println("  Outer transaction committed")
	return nil
}

func innerTransaction(ctx context.Context, db *mysql.Database) error {
	// Get or create transaction (will reuse existing from context)
	tx, commit, cancel, err := mysql.GetOrCreateTxFromContext(ctx)
	defer cancel()
	if err != nil {
		return fmt.Errorf("inner tx failed: %w", err)
	}

	// Transaction already in context, so this is a no-op
	ctx = mysql.NewContextWithTx(ctx, tx)

	fmt.Println("    Inner: Reusing outer transaction")

	// Update user
	err = db.Exec("UPDATE `users` SET `age` = @@age WHERE `email` = @@email",
		mysql.Params{
			"age":   26,
			"email": "outer@example.com",
		})

	if err != nil {
		return fmt.Errorf("inner update failed: %w", err)
	}

	fmt.Println("    Inner: User updated")

	// Commit (safe to call, won't actually commit until outer commits)
	if err := commit(); err != nil {
		return fmt.Errorf("inner commit failed: %w", err)
	}

	fmt.Println("    Inner: Operations complete")
	return nil
}

// rollbackExample demonstrates automatic rollback on error
func rollbackExample() {
	db, err := setupDatabase()
	if err != nil {
		log.Printf("Setup failed: %v", err)
		return
	}

	ctx := context.Background()

	err = failingTransaction(ctx, db)
	if err != nil {
		fmt.Printf("  Transaction failed as expected: %v\n", err)
		fmt.Println("✓ Transaction automatically rolled back")
	}

	// Verify rollback - user should not exist
	var user User
	err = db.Select(&user,
		"SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `email` = @@email",
		0,
		"rollback@example.com")

	if err == sql.ErrNoRows {
		fmt.Println("✓ Verified: User was not inserted (rollback worked)")
	} else if err != nil {
		log.Printf("Verification query failed: %v", err)
	} else {
		log.Println("✗ Error: User exists (rollback failed)")
	}
}

func failingTransaction(ctx context.Context, db *mysql.Database) error {
	tx, _, cancel, err := mysql.GetOrCreateTxFromContext(ctx)
	defer cancel() // Will rollback since we don't call commit
	if err != nil {
		return err
	}

	ctx = mysql.NewContextWithTx(ctx, tx)

	// Insert user
	user := User{
		Name:   "RollbackUser",
		Email:  "rollback@example.com",
		Age:    28,
		Active: true,
	}

	err = db.Insert("users", user)
	if err != nil {
		return err
	}

	fmt.Println("  User inserted")

	// Simulate error before commit
	fmt.Println("  Simulating error...")
	return fmt.Errorf("simulated error - transaction will rollback")

	// commit() is never called, so cancel() will rollback
}

// complexTransactionExample demonstrates a complex multi-step transaction
func complexTransactionExample() {
	db, err := setupDatabase()
	if err != nil {
		log.Printf("Setup failed: %v", err)
		return
	}

	ctx := context.Background()

	err = transferFunds(ctx, db, "user1@example.com", "user2@example.com", 100)
	if err != nil {
		log.Printf("Transfer failed: %v", err)
		return
	}

	fmt.Println("✓ Complex transaction completed")
}

// transferFunds demonstrates a bank transfer-like transaction
func transferFunds(ctx context.Context, db *mysql.Database, fromEmail, toEmail string, amount int) error {
	tx, commit, cancel, err := mysql.GetOrCreateTxFromContext(ctx)
	defer cancel()
	if err != nil {
		return fmt.Errorf("transaction start failed: %w", err)
	}

	ctx = mysql.NewContextWithTx(ctx, tx)

	fmt.Printf("  Starting transfer: %d from %s to %s\n", amount, fromEmail, toEmail)

	// Step 1: Check sender balance
	type Account struct {
		Email   string `mysql:"email"`
		Balance int    `mysql:"balance"`
	}

	var sender Account
	err = db.SelectWrites(&sender,
		"SELECT email, balance FROM `accounts` WHERE `email` = @@email FOR UPDATE",
		0, // Use write pool for transaction
		fromEmail)

	if err == sql.ErrNoRows {
		return fmt.Errorf("sender account not found")
	} else if err != nil {
		return fmt.Errorf("failed to fetch sender: %w", err)
	}

	fmt.Printf("  Sender balance: %d\n", sender.Balance)

	// Step 2: Verify sufficient funds
	if sender.Balance < amount {
		return fmt.Errorf("insufficient funds: have %d, need %d", sender.Balance, amount)
	}

	// Step 3: Check receiver exists
	var receiver Account
	err = db.SelectWrites(&receiver,
		"SELECT email, balance FROM `accounts` WHERE `email` = @@email FOR UPDATE",
		0,
		toEmail)

	if err == sql.ErrNoRows {
		return fmt.Errorf("receiver account not found")
	} else if err != nil {
		return fmt.Errorf("failed to fetch receiver: %w", err)
	}

	fmt.Printf("  Receiver balance: %d\n", receiver.Balance)

	// Step 4: Deduct from sender
	result, err := db.ExecResult(
		"UPDATE accounts SET `balance` = balance - @@amount WHERE `email` = @@email",
		mysql.Params{
			"amount": amount,
			"email":  fromEmail,
		})

	if err != nil {
		return fmt.Errorf("failed to deduct from sender: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("sender update affected 0 rows")
	}

	fmt.Printf("  Deducted %d from sender\n", amount)

	// Step 5: Add to receiver
	result, err = db.ExecResult(
		"UPDATE accounts SET `balance` = balance + @@amount WHERE `email` = @@email",
		mysql.Params{
			"amount": amount,
			"email":  toEmail,
		})

	if err != nil {
		return fmt.Errorf("failed to add to receiver: %w", err)
	}

	rows, _ = result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("receiver update affected 0 rows")
	}

	fmt.Printf("  Added %d to receiver\n", amount)

	// Step 6: Record transaction log
	type TransactionLog struct {
		FromEmail string `mysql:"from_email"`
		ToEmail   string `mysql:"to_email"`
		Amount    int    `mysql:"amount"`
	}

	txLog := TransactionLog{
		FromEmail: fromEmail,
		ToEmail:   toEmail,
		Amount:    amount,
	}

	err = db.Insert("transaction_logs", txLog)
	if err != nil {
		return fmt.Errorf("failed to log transaction: %w", err)
	}

	fmt.Println("  Transaction logged")

	// Step 7: Commit all changes atomically
	if err := commit(); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}

	fmt.Println("  All changes committed atomically")
	return nil
}

// transactionWithRetry demonstrates transaction retry pattern
func transactionWithRetry(ctx context.Context, db *mysql.Database) error {
	maxRetries := 3
	var err error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err = attemptTransaction(ctx, db)
		if err == nil {
			return nil // Success
		}

		// Check if error is retryable (e.g., deadlock)
		if !isRetryableError(err) {
			return err
		}

		fmt.Printf("  Attempt %d failed (retryable): %v\n", attempt, err)

		if attempt < maxRetries {
			fmt.Printf("  Retrying... (%d/%d)\n", attempt+1, maxRetries)
		}
	}

	return fmt.Errorf("transaction failed after %d attempts: %w", maxRetries, err)
}

func attemptTransaction(ctx context.Context, db *mysql.Database) error {
	tx, commit, cancel, err := mysql.GetOrCreateTxFromContext(ctx)
	defer cancel()
	if err != nil {
		return err
	}

	ctx = mysql.NewContextWithTx(ctx, tx)

	// Perform transaction operations...
	err = db.Insert("users", User{
		Name:   "RetryUser",
		Email:  "retry@example.com",
		Age:    29,
		Active: true,
	})

	if err != nil {
		return err
	}

	return commit()
}

func isRetryableError(err error) bool {
	// Check for MySQL deadlock or lock timeout errors
	// Note: cool-mysql already handles automatic retries for these
	// This is just an example of manual retry logic
	if err == nil {
		return false
	}

	errStr := err.Error()
	return strings.Contains(errStr, "deadlock") ||
		strings.Contains(errStr, "lock wait timeout") ||
		strings.Contains(errStr, "try restarting transaction")
}

// Batch transaction example
func batchTransactionExample(ctx context.Context, db *mysql.Database) error {
	fmt.Println("\nBatch Transaction Example")

	tx, commit, cancel, err := mysql.GetOrCreateTxFromContext(ctx)
	defer cancel()
	if err != nil {
		return err
	}

	ctx = mysql.NewContextWithTx(ctx, tx)

	// Insert multiple users in single transaction
	users := []User{
		{Name: "BatchUser1", Email: "batch1@example.com", Age: 20, Active: true},
		{Name: "BatchUser2", Email: "batch2@example.com", Age: 21, Active: true},
		{Name: "BatchUser3", Email: "batch3@example.com", Age: 22, Active: true},
	}

	err = db.Insert("users", users)
	if err != nil {
		return fmt.Errorf("batch insert failed: %w", err)
	}

	fmt.Printf("  Inserted %d users in transaction\n", len(users))

	// Commit
	if err := commit(); err != nil {
		return fmt.Errorf("batch commit failed: %w", err)
	}

	fmt.Println("✓ Batch transaction committed")
	return nil
}

// savepoint example (MySQL specific)
func savepointExample(ctx context.Context, db *mysql.Database) error {
	fmt.Println("\nSavepoint Example (Advanced)")

	tx, commit, cancel, err := mysql.GetOrCreateTxFromContext(ctx)
	defer cancel()
	if err != nil {
		return err
	}

	ctx = mysql.NewContextWithTx(ctx, tx)

	// Insert first user
	user1 := User{Name: "SavepointUser1", Email: "sp1@example.com", Age: 25, Active: true}
	err = db.Insert("users", user1)
	if err != nil {
		return err
	}
	fmt.Println("  User 1 inserted")

	// Create savepoint
	err = db.Exec("SAVEPOINT sp1")
	if err != nil {
		return fmt.Errorf("savepoint creation failed: %w", err)
	}
	fmt.Println("  Savepoint 'sp1' created")

	// Insert second user
	user2 := User{Name: "SavepointUser2", Email: "sp2@example.com", Age: 26, Active: true}
	err = db.Insert("users", user2)
	if err != nil {
		return err
	}
	fmt.Println("  User 2 inserted")

	// Simulate error and rollback to savepoint
	fmt.Println("  Simulating error, rolling back to savepoint...")
	err = db.Exec("ROLLBACK TO SAVEPOINT sp1")
	if err != nil {
		return fmt.Errorf("rollback to savepoint failed: %w", err)
	}

	fmt.Println("  Rolled back to savepoint (User 2 not saved)")

	// Insert different user
	user3 := User{Name: "SavepointUser3", Email: "sp3@example.com", Age: 27, Active: true}
	err = db.Insert("users", user3)
	if err != nil {
		return err
	}
	fmt.Println("  User 3 inserted")

	// Commit transaction
	if err := commit(); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}

	fmt.Println("✓ Transaction committed (User 1 and 3 saved, User 2 rolled back)")
	return nil
}

// readCommittedIsolation example
func isolationLevelExample(ctx context.Context, db *mysql.Database) error {
	fmt.Println("\nIsolation Level Example")

	// Set isolation level before transaction
	err := db.Exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
	if err != nil {
		return fmt.Errorf("set isolation level failed: %w", err)
	}

	fmt.Println("  Isolation level set to READ COMMITTED")

	tx, commit, cancel, err := mysql.GetOrCreateTxFromContext(ctx)
	defer cancel()
	if err != nil {
		return err
	}

	ctx = mysql.NewContextWithTx(ctx, tx)

	// Transaction operations...
	var users []User
	err = db.SelectWrites(&users, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `active` = 1", 0)
	if err != nil {
		return err
	}

	fmt.Printf("  Read %d users with READ COMMITTED isolation\n", len(users))

	if err := commit(); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}

	return nil
}
