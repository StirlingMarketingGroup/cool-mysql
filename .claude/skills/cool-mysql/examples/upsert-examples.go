// Package examples demonstrates upsert patterns with cool-mysql
package examples

import (
	"fmt"
	"log"
	"time"

	mysql "github.com/StirlingMarketingGroup/cool-mysql"
)

// UpsertExamples demonstrates INSERT ... ON DUPLICATE KEY UPDATE patterns
func UpsertExamples() {
	fmt.Println("=== UPSERT EXAMPLES ===")

	// Basic upsert
	fmt.Println("\n1. Basic Upsert (by unique key)")
	basicUpsertExample()

	// Upsert with multiple unique columns
	fmt.Println("\n2. Upsert with Composite Unique Key")
	compositeKeyUpsertExample()

	// Conditional upsert
	fmt.Println("\n3. Conditional Upsert with WHERE clause")
	conditionalUpsertExample()

	// Batch upsert
	fmt.Println("\n4. Batch Upsert")
	batchUpsertExample()

	// Selective column updates
	fmt.Println("\n5. Selective Column Updates")
	selectiveUpdateExample()

	// Timestamp tracking
	fmt.Println("\n6. Timestamp Tracking with Upsert")
	timestampUpsertExample()
}

// basicUpsertExample demonstrates simple upsert by email
func basicUpsertExample() {
	db, err := setupDatabase()
	if err != nil {
		log.Printf("Setup failed: %v", err)
		return
	}

	// Define user with unique email
	user := User{
		Name:   "UpsertUser",
		Email:  "upsert@example.com", // Unique key
		Age:    30,
		Active: true,
	}

	// First upsert - INSERT (user doesn't exist)
	err = db.Upsert(
		"users",           // table
		[]string{"email"}, // unique columns (conflict detection)
		[]string{"name", "age", "active"}, // columns to update on conflict
		"",    // no WHERE clause
		user,  // data
	)

	if err != nil {
		log.Printf("First upsert failed: %v", err)
		return
	}

	fmt.Println("  First upsert: User inserted")

	// Second upsert - UPDATE (user exists)
	user.Name = "UpsertUser Updated"
	user.Age = 31

	err = db.Upsert(
		"users",
		[]string{"email"},
		[]string{"name", "age", "active"},
		"",
		user,
	)

	if err != nil {
		log.Printf("Second upsert failed: %v", err)
		return
	}

	fmt.Println("  Second upsert: User updated")

	// Verify result
	var retrieved User
	err = db.Select(&retrieved,
		"SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `email` = @@email",
		0,
		"upsert@example.com")

	if err != nil {
		log.Printf("Verification failed: %v", err)
		return
	}

	fmt.Printf("✓ Final state: Name='%s', Age=%d\n", retrieved.Name, retrieved.Age)
}

// compositeKeyUpsertExample demonstrates upsert with multiple unique columns
func compositeKeyUpsertExample() {
	db, err := setupDatabase()
	if err != nil {
		log.Printf("Setup failed: %v", err)
		return
	}

	type ProductInventory struct {
		StoreID   int    `mysql:"store_id"`
		ProductID int    `mysql:"product_id"`
		Quantity  int    `mysql:"quantity"`
		Price     float64 `mysql:"price"`
	}

	// Initial inventory
	inventory := ProductInventory{
		StoreID:   1,
		ProductID: 100,
		Quantity:  50,
		Price:     19.99,
	}

	// First upsert - INSERT
	err = db.Upsert(
		"inventory",
		[]string{"store_id", "product_id"}, // Composite unique key
		[]string{"quantity", "price"},      // Update these on conflict
		"",
		inventory,
	)

	if err != nil {
		log.Printf("First upsert failed: %v", err)
		return
	}

	fmt.Println("  Inventory created: Store 1, Product 100, Qty=50, Price=$19.99")

	// Second upsert - UPDATE existing inventory
	inventory.Quantity = 75
	inventory.Price = 17.99

	err = db.Upsert(
		"inventory",
		[]string{"store_id", "product_id"},
		[]string{"quantity", "price"},
		"",
		inventory,
	)

	if err != nil {
		log.Printf("Second upsert failed: %v", err)
		return
	}

	fmt.Println("  Inventory updated: Qty=75, Price=$17.99")
	fmt.Println("✓ Composite key upsert successful")
}

// conditionalUpsertExample demonstrates upsert with WHERE clause
func conditionalUpsertExample() {
	db, err := setupDatabase()
	if err != nil {
		log.Printf("Setup failed: %v", err)
		return
	}

	type Document struct {
		ID        int       `mysql:"id"`
		Title     string    `mysql:"title"`
		Content   string    `mysql:"content"`
		Version   int       `mysql:"version"`
		UpdatedAt time.Time `mysql:"updated_at"`
	}

	// Initial document
	doc := Document{
		ID:        1,
		Title:     "My Document",
		Content:   "Version 1 content",
		Version:   1,
		UpdatedAt: time.Now(),
	}

	// Insert initial version
	err = db.Insert("documents", doc)
	if err != nil {
		log.Printf("Insert failed: %v", err)
		return
	}

	fmt.Println("  Document v1 created")

	// Upsert with condition: only update if newer
	doc.Content = "Version 2 content"
	doc.Version = 2
	doc.UpdatedAt = time.Now()

	err = db.Upsert(
		"documents",
		[]string{"id"},
		[]string{"title", "content", "version", "updated_at"},
		"version < VALUES(version)", // Only update if new version is higher
		doc,
	)

	if err != nil {
		log.Printf("Conditional upsert failed: %v", err)
		return
	}

	fmt.Println("  Document updated to v2 (condition met)")

	// Try to upsert with older version (should not update)
	oldDoc := Document{
		ID:        1,
		Title:     "My Document",
		Content:   "Old content",
		Version:   1, // Older version
		UpdatedAt: time.Now().Add(-time.Hour),
	}

	err = db.Upsert(
		"documents",
		[]string{"id"},
		[]string{"title", "content", "version", "updated_at"},
		"version < VALUES(version)",
		oldDoc,
	)

	if err != nil {
		log.Printf("Old version upsert failed: %v", err)
		return
	}

	fmt.Println("  Old version upsert executed (but condition prevented update)")

	// Verify current version
	var current Document
	err = db.Select(&current,
		"SELECT `id`, `title`, `content`, `version`, `updated_at` FROM `documents` WHERE `id` = @@id",
		0,
		1)

	if err != nil {
		log.Printf("Verification failed: %v", err)
		return
	}

	fmt.Printf("✓ Final version: %d (conditional update worked)\n", current.Version)
}

// batchUpsertExample demonstrates upserting multiple records
func batchUpsertExample() {
	db, err := setupDatabase()
	if err != nil {
		log.Printf("Setup failed: %v", err)
		return
	}

	type Setting struct {
		Key   string `mysql:"key"`
		Value string `mysql:"value"`
	}

	// Batch of settings
	settings := []Setting{
		{Key: "theme", Value: "dark"},
		{Key: "language", Value: "en"},
		{Key: "notifications", Value: "enabled"},
		{Key: "timezone", Value: "UTC"},
	}

	// Upsert all settings
	err = db.Upsert(
		"settings",
		[]string{"key"},    // Unique on key
		[]string{"value"},  // Update value on conflict
		"",
		settings,
	)

	if err != nil {
		log.Printf("Batch upsert failed: %v", err)
		return
	}

	fmt.Printf("  Inserted/updated %d settings\n", len(settings))

	// Update some settings
	updatedSettings := []Setting{
		{Key: "theme", Value: "light"},        // Changed
		{Key: "language", Value: "es"},        // Changed
		{Key: "notifications", Value: "enabled"}, // Same
		{Key: "font_size", Value: "14"},       // New
	}

	err = db.Upsert(
		"settings",
		[]string{"key"},
		[]string{"value"},
		"",
		updatedSettings,
	)

	if err != nil {
		log.Printf("Update batch upsert failed: %v", err)
		return
	}

	fmt.Printf("  Updated batch: 2 changed, 1 same, 1 new\n")

	// Verify results
	var allSettings []Setting
	err = db.Select(&allSettings,
		"SELECT `key`, `value` FROM `settings` ORDER BY key",
		0)

	if err != nil {
		log.Printf("Verification failed: %v", err)
		return
	}

	fmt.Printf("✓ Total settings: %d\n", len(allSettings))
	for _, s := range allSettings {
		fmt.Printf("  %s = %s\n", s.Key, s.Value)
	}
}

// selectiveUpdateExample demonstrates updating only specific columns
func selectiveUpdateExample() {
	db, err := setupDatabase()
	if err != nil {
		log.Printf("Setup failed: %v", err)
		return
	}

	type UserProfile struct {
		Email     string    `mysql:"email"`
		Name      string    `mysql:"name"`
		Bio       string    `mysql:"bio"`
		Avatar    string    `mysql:"avatar"`
		UpdatedAt time.Time `mysql:"updated_at,defaultzero"`
	}

	// Initial profile
	profile := UserProfile{
		Email:     "profile@example.com",
		Name:      "John Doe",
		Bio:       "Software Developer",
		Avatar:    "avatar1.jpg",
		UpdatedAt: time.Now(),
	}

	err = db.Insert("user_profiles", profile)
	if err != nil {
		log.Printf("Insert failed: %v", err)
		return
	}

	fmt.Println("  Profile created")

	// Update only name and bio (not avatar)
	profile.Name = "John Smith"
	profile.Bio = "Senior Software Developer"
	// Avatar unchanged

	err = db.Upsert(
		"user_profiles",
		[]string{"email"},
		[]string{"name", "bio", "updated_at"}, // Don't include avatar
		"",
		profile,
	)

	if err != nil {
		log.Printf("Selective update failed: %v", err)
		return
	}

	fmt.Println("  Updated name and bio (avatar unchanged)")

	// Later, update only avatar
	profile.Avatar = "avatar2.jpg"

	err = db.Upsert(
		"user_profiles",
		[]string{"email"},
		[]string{"avatar", "updated_at"}, // Only avatar
		"",
		profile,
	)

	if err != nil {
		log.Printf("Avatar update failed: %v", err)
		return
	}

	fmt.Println("  Updated avatar only")

	// Verify
	var final UserProfile
	err = db.Select(&final,
		"SELECT `email`, `name`, `bio`, `avatar`, `updated_at` FROM `user_profiles` WHERE `email` = @@email",
		0,
		"profile@example.com")

	if err != nil {
		log.Printf("Verification failed: %v", err)
		return
	}

	fmt.Printf("✓ Final: Name='%s', Bio='%s', Avatar='%s'\n",
		final.Name, final.Bio, final.Avatar)
}

// timestampUpsertExample demonstrates tracking created_at and updated_at
func timestampUpsertExample() {
	db, err := setupDatabase()
	if err != nil {
		log.Printf("Setup failed: %v", err)
		return
	}

	type Article struct {
		Slug      string    `mysql:"slug"`
		Title     string    `mysql:"title"`
		Content   string    `mysql:"content"`
		Views     int       `mysql:"views"`
		CreatedAt time.Time `mysql:"created_at,defaultzero"`
		UpdatedAt time.Time `mysql:"updated_at,defaultzero"`
	}

	// Initial article
	article := Article{
		Slug:    "my-article",
		Title:   "My Article",
		Content: "Initial content",
		Views:   0,
	}

	// First upsert - INSERT
	// CreatedAt and UpdatedAt will use database defaults
	err = db.Upsert(
		"articles",
		[]string{"slug"},
		[]string{"title", "content", "views", "updated_at"},
		"",
		article,
	)

	if err != nil {
		log.Printf("Insert failed: %v", err)
		return
	}

	fmt.Println("  Article created (created_at set by DB)")

	// Get the article to see timestamps
	var inserted Article
	err = db.Select(&inserted,
		"SELECT `slug`, `title`, `content`, `views`, `created_at`, `updated_at` FROM `articles` WHERE slug = @@slug",
		0,
		"my-article")

	if err != nil {
		log.Printf("Select failed: %v", err)
		return
	}

	fmt.Printf("  Created at: %s\n", inserted.CreatedAt.Format(time.RFC3339))

	time.Sleep(2 * time.Second) // Wait to see time difference

	// Update article
	article.Title = "My Updated Article"
	article.Content = "Updated content"
	article.Views = inserted.Views + 10

	err = db.Upsert(
		"articles",
		[]string{"slug"},
		[]string{"title", "content", "views", "updated_at"},
		"", // Don't update created_at
		article,
	)

	if err != nil {
		log.Printf("Update failed: %v", err)
		return
	}

	fmt.Println("  Article updated (updated_at changed, created_at preserved)")

	// Verify timestamps
	var updated Article
	err = db.Select(&updated,
		"SELECT `slug`, `title`, `content`, `views`, `created_at`, `updated_at` FROM `articles` WHERE slug = @@slug",
		0,
		"my-article")

	if err != nil {
		log.Printf("Verification failed: %v", err)
		return
	}

	fmt.Printf("  Created at: %s (unchanged)\n", updated.CreatedAt.Format(time.RFC3339))
	fmt.Printf("  Updated at: %s (newer)\n", updated.UpdatedAt.Format(time.RFC3339))
	fmt.Printf("✓ Timestamps tracked correctly\n")
}

// incrementCounterExample demonstrates atomic counter updates
func incrementCounterExample() {
	db, err := setupDatabase()
	if err != nil {
		log.Printf("Setup failed: %v", err)
		return
	}

	fmt.Println("\nIncrement Counter Example")

	type PageView struct {
		Page  string `mysql:"page"`
		Views int    `mysql:"views"`
	}

	page := "homepage"

	// Increment views (insert with 1 if not exists, increment if exists)
	// This uses MySQL's VALUES() function to reference the insert value
	viewRecord := PageView{
		Page:  page,
		Views: 1, // Initial value for insert
	}

	// Custom upsert to increment on duplicate
	err = db.Exec(
		"INSERT INTO `page_views` (`page`, `views`)"+
		" VALUES (@@page, @@views)"+
		" ON DUPLICATE KEY UPDATE `views` = `views` + @@views",
		mysql.Params{
			"page":  viewRecord.Page,
			"views": viewRecord.Views,
		})

	if err != nil {
		log.Printf("Increment failed: %v", err)
		return
	}

	fmt.Printf("  View count incremented for %s\n", page)

	// Increment again
	err = db.Exec(
		"INSERT INTO `page_views` (`page`, `views`)"+
		" VALUES (@@page, @@views)"+
		" ON DUPLICATE KEY UPDATE `views` = `views` + @@views",
		mysql.Params{
			"page":  page,
			"views": 1,
		})

	if err != nil {
		log.Printf("Second increment failed: %v", err)
		return
	}

	// Check total views
	var totalViews int
	err = db.Select(&totalViews,
		"SELECT views FROM `page_views` WHERE page = @@page",
		0,
		page)

	if err != nil {
		log.Printf("Select views failed: %v", err)
		return
	}

	fmt.Printf("✓ Total views for %s: %d\n", page, totalViews)
}

// upsertFromChannelExample demonstrates streaming upserts
func upsertFromChannelExample() {
	db, err := setupDatabase()
	if err != nil {
		log.Printf("Setup failed: %v", err)
		return
	}

	fmt.Println("\nUpsert from Channel Example")

	type Metric struct {
		MetricName string    `mysql:"metric_name"`
		Value      float64   `mysql:"value"`
		Timestamp  time.Time `mysql:"timestamp,defaultzero"`
	}

	// Create channel of metrics
	metricCh := make(chan Metric, 100)

	// Producer: generate metrics
	go func() {
		defer close(metricCh)
		metrics := []string{"cpu", "memory", "disk", "network"}
		for i := 0; i < 100; i++ {
			metricCh <- Metric{
				MetricName: metrics[i%len(metrics)],
				Value:      float64(i),
				Timestamp:  time.Now(),
			}
		}
	}()

	// Upsert from channel
	err = db.Upsert(
		"metrics",
		[]string{"metric_name"},
		[]string{"value", "timestamp"},
		"",
		metricCh,
	)

	if err != nil {
		log.Printf("Channel upsert failed: %v", err)
		return
	}

	fmt.Println("✓ Streamed 100 metric upserts")

	// Verify
	var count int
	count, err = db.Count("SELECT COUNT(*) FROM `metrics`", 0)
	if err != nil {
		log.Printf("Count failed: %v", err)
		return
	}

	fmt.Printf("  Total unique metrics: %d\n", count)
}

// UpsertOrIgnoreExample demonstrates choosing between update and ignore
func UpsertOrIgnoreExample() {
	db, err := setupDatabase()
	if err != nil {
		log.Printf("Setup failed: %v", err)
		return
	}

	fmt.Println("\nUpsert vs Insert Ignore Example")

	type UniqueEmail struct {
		Email string `mysql:"email"`
		Count int    `mysql:"count"`
	}

	// With UPSERT - updates on duplicate
	email1 := UniqueEmail{Email: "test1@example.com", Count: 1}
	err = db.Upsert(
		"email_counts",
		[]string{"email"},
		[]string{"count"},
		"",
		email1,
	)
	fmt.Println("  Upsert: Inserts or updates")

	// With INSERT IGNORE - silently ignores duplicate
	err = db.Exec(
		"INSERT IGNORE INTO `email_counts` (email, count) VALUES (@@email, @@count)",
		mysql.Params{"email": "test1@example.com", "count": 999})

	fmt.Println("  Insert Ignore: Keeps original value on duplicate")

	// Verify - count should still be 1 (ignore worked)
	var result UniqueEmail
	err = db.Select(&result,
		"SELECT `email`, `count` FROM `email_counts` WHERE `email` = @@email",
		0,
		"test1@example.com")

	if err != nil {
		log.Printf("Verification failed: %v", err)
		return
	}

	fmt.Printf("✓ Count=%d (INSERT IGNORE kept original)\n", result.Count)
}
