// Package examples demonstrates advanced query patterns with cool-mysql
package examples

import (
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"strings"
	"time"

	mysql "github.com/StirlingMarketingGroup/cool-mysql"
)

// AdvancedQueryExamples demonstrates advanced query patterns
func AdvancedQueryExamples() {
	db, err := setupDatabase()
	if err != nil {
		log.Fatalf("Failed to setup database: %v", err)
	}

	// Template queries
	fmt.Println("=== TEMPLATE QUERY EXAMPLES ===")
	templateExamples(db)

	// Channel streaming
	fmt.Println("\n=== CHANNEL STREAMING EXAMPLES ===")
	channelExamples(db)

	// Function receivers
	fmt.Println("\n=== FUNCTION RECEIVER EXAMPLES ===")
	functionReceiverExamples(db)

	// JSON handling
	fmt.Println("\n=== JSON HANDLING EXAMPLES ===")
	jsonExamples(db)

	// Raw SQL
	fmt.Println("\n=== RAW SQL EXAMPLES ===")
	rawSQLExamples(db)

	// Complex queries
	fmt.Println("\n=== COMPLEX QUERY EXAMPLES ===")
	complexQueryExamples(db)
}

// templateExamples demonstrates Go template syntax in queries
func templateExamples(db *mysql.Database) {
	// Example 1: Conditional WHERE clause
	fmt.Println("1. Conditional WHERE clause")

	type SearchParams struct {
		MinAge int
		Status string
		Name   string
	}

	// Search with all parameters
	params := SearchParams{
		MinAge: 25,
		Status: "active",
		Name:   "Alice",
	}

	query := "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users`" +
		" WHERE 1=1" +
		" {{ if .MinAge }}AND `age` >= @@MinAge{{ end }}" +
		" {{ if .Status }}AND `status` = @@Status{{ end }}" +
		" {{ if .Name }}AND `name` LIKE CONCAT('%', @@Name, '%'){{ end }}"

	var users []User
	err := db.Select(&users, query, 0, params)
	if err != nil {
		log.Printf("Template query failed: %v", err)
	} else {
		fmt.Printf("✓ Found %d users with filters\n", len(users))
	}

	// Example 2: Dynamic ORDER BY (with validation)
	fmt.Println("\n2. Dynamic ORDER BY with whitelisting")

	type SortParams struct {
		SortBy    string
		SortOrder string
	}

	// Whitelist allowed columns - identifiers can't be marshaled
	allowedColumns := map[string]bool{
		"created_at": true,
		"name":       true,
		"age":        true,
	}

	sortParams := SortParams{
		SortBy:    "created_at",
		SortOrder: "DESC",
	}

	// Validate before using in query
	if !allowedColumns[sortParams.SortBy] {
		log.Printf("Invalid sort column: %s", sortParams.SortBy)
		return
	}

	sortQuery := "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users`" +
		" WHERE `active` = 1" +
		" {{ if .SortBy }}" +
		" ORDER BY {{ .SortBy }} {{ .SortOrder }}" +
		" {{ end }}"

	err = db.Select(&users, sortQuery, 0, sortParams)
	if err != nil {
		log.Printf("Sort query failed: %v", err)
	} else {
		fmt.Printf("✓ Users sorted by %s %s\n", sortParams.SortBy, sortParams.SortOrder)
	}

	// Example 3: Conditional JOINs
	fmt.Println("\n3. Conditional JOINs")

	type JoinParams struct {
		IncludeOrders   bool
		IncludeAddress  bool
		IncludeMetadata bool
	}

	joinParams := JoinParams{
		IncludeOrders:  true,
		IncludeAddress: false,
	}

	joinQuery := "SELECT `users`.`id`, `users`.`name`, `users`.`email`, `users`.`age`, `users`.`active`, `users`.`created_at`, `users`.`updated_at`" +
		" {{ if .IncludeOrders }}, COUNT(`orders`.`id`) as `order_count`{{ end }}" +
		" {{ if .IncludeAddress }}, `addresses`.`city`{{ end }}" +
		" FROM `users`" +
		" {{ if .IncludeOrders }}" +
		" LEFT JOIN `orders` ON `users`.`id` = `orders`.`user_id`" +
		" {{ end }}" +
		" {{ if .IncludeAddress }}" +
		" LEFT JOIN `addresses` ON `users`.`id` = `addresses`.`user_id`" +
		" {{ end }}" +
		" GROUP BY `users`.`id`"

	err = db.Select(&users, joinQuery, 0, joinParams)
	if err != nil {
		log.Printf("Join query failed: %v", err)
	} else {
		fmt.Println("✓ Query with conditional joins executed")
	}

	// Example 4: Custom template functions
	fmt.Println("\n4. Custom template functions")

	// Add custom functions
	db.AddTemplateFuncs(template.FuncMap{
		"upper": strings.ToUpper,
		"lower": strings.ToLower,
		"quote": func(s string) string { return fmt.Sprintf("'%s'", s) },
	})

	type CaseParams struct {
		SearchTerm     string
		CaseSensitive  bool
		UseWildcard    bool
	}

	caseParams := CaseParams{
		SearchTerm:    "alice",
		CaseSensitive: false,
		UseWildcard:   true,
	}

	// IMPORTANT: Template values must be marshaled with | marshal
	caseQuery := "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users`" +
		" WHERE {{ if not .CaseSensitive }}UPPER(`name`){{ else }}`name`{{ end }} LIKE CONCAT('%', {{ .SearchTerm | marshal }}, '%')"

	err = db.Select(&users, caseQuery, 0, caseParams)
	if err != nil {
		log.Printf("Custom function query failed: %v", err)
	} else {
		fmt.Println("✓ Query with custom template functions executed")
	}

	// Example 5: Complex conditional logic
	fmt.Println("\n5. Complex conditional logic")

	type FilterParams struct {
		AgeRange    []int
		Statuses    []string
		DateFrom    time.Time
		DateTo      time.Time
		ActiveOnly  bool
	}

	filterParams := FilterParams{
		AgeRange:   []int{25, 40},
		ActiveOnly: true,
		DateFrom:   time.Now().Add(-30 * 24 * time.Hour),
	}

	filterQuery := "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users`" +
		" WHERE 1=1" +
		" {{ if .AgeRange }}" +
		" AND `age` BETWEEN @@AgeMin AND @@AgeMax" +
		" {{ end }}" +
		" {{ if .ActiveOnly }}" +
		" AND `active` = 1" +
		" {{ end }}" +
		" {{ if not .DateFrom.IsZero }}" +
		" AND `created_at` >= @@DateFrom" +
		" {{ end }}" +
		" {{ if not .DateTo.IsZero }}" +
		" AND `created_at` <= @@DateTo" +
		" {{ end }}"

	queryParams := mysql.Params{
		"AgeMin":   filterParams.AgeRange[0],
		"AgeMax":   filterParams.AgeRange[1],
		"DateFrom": filterParams.DateFrom,
	}

	err = db.Select(&users, filterQuery, 0, queryParams, filterParams)
	if err != nil {
		log.Printf("Complex filter query failed: %v", err)
	} else {
		fmt.Printf("✓ Found %d users with complex filters\n", len(users))
	}
}

// channelExamples demonstrates streaming with channels
func channelExamples(db *mysql.Database) {
	// Example 1: Stream SELECT results
	fmt.Println("1. Stream SELECT results to channel")

	userCh := make(chan User, 10) // Buffered channel

	go func() {
		defer close(userCh)
		err := db.Select(userCh, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `active` = 1", 0)
		if err != nil {
			log.Printf("Channel select failed: %v", err)
		}
	}()

	count := 0
	for user := range userCh {
		fmt.Printf("  Received: %s (%s)\n", user.Name, user.Email)
		count++
		if count >= 5 {
			fmt.Println("  (showing first 5 results)")
			// Drain remaining
			for range userCh {
			}
			break
		}
	}

	// Example 2: Stream INSERT from channel
	fmt.Println("\n2. Stream INSERT from channel")

	insertCh := make(chan User, 10)

	go func() {
		defer close(insertCh)
		for i := 0; i < 100; i++ {
			insertCh <- User{
				Name:   fmt.Sprintf("StreamUser%d", i),
				Email:  fmt.Sprintf("stream%d@example.com", i),
				Age:    20 + (i % 50),
				Active: i%2 == 0,
			}
		}
	}()

	err := db.Insert("users", insertCh)
	if err != nil {
		log.Printf("Channel insert failed: %v", err)
	} else {
		fmt.Println("✓ Streamed 100 users for insertion")
	}

	// Example 3: Transform while streaming
	fmt.Println("\n3. Transform data while streaming")

	type EnrichedUser struct {
		User
		Category string
		Priority int
	}

	rawUserCh := make(chan User, 10)
	enrichedCh := make(chan EnrichedUser, 10)

	// Producer: fetch users
	go func() {
		defer close(rawUserCh)
		db.Select(rawUserCh, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` LIMIT @@limit", 0, 50)
	}()

	// Transformer: enrich data
	go func() {
		defer close(enrichedCh)
		for user := range rawUserCh {
			enriched := EnrichedUser{
				User: user,
			}

			// Add category based on age
			if user.Age < 25 {
				enriched.Category = "Young"
				enriched.Priority = 1
			} else if user.Age < 40 {
				enriched.Category = "Middle"
				enriched.Priority = 2
			} else {
				enriched.Category = "Senior"
				enriched.Priority = 3
			}

			enrichedCh <- enriched
		}
	}()

	// Consumer: process enriched data
	processed := 0
	for enriched := range enrichedCh {
		processed++
		_ = enriched // Process enriched user
	}

	fmt.Printf("✓ Processed %d enriched users\n", processed)
}

// functionReceiverExamples demonstrates function receivers
func functionReceiverExamples(db *mysql.Database) {
	// Example 1: Process each row with function
	fmt.Println("1. Process rows with function")

	count := 0
	err := db.Select(func(u User) {
		count++
		if count <= 3 {
			fmt.Printf("  Processing: %s (Age: %d)\n", u.Name, u.Age)
		}
	}, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `active` = 1", 0)

	if err != nil {
		log.Printf("Function receiver failed: %v", err)
	} else {
		fmt.Printf("✓ Processed %d users\n", count)
	}

	// Example 2: Aggregate data with function
	fmt.Println("\n2. Aggregate data with function")

	var totalAge int
	var userCount int

	err = db.Select(func(u User) {
		totalAge += u.Age
		userCount++
	}, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users`", 0)

	if err != nil {
		log.Printf("Aggregation failed: %v", err)
	} else if userCount > 0 {
		avgAge := float64(totalAge) / float64(userCount)
		fmt.Printf("✓ Average age: %.2f (%d users)\n", avgAge, userCount)
	}

	// Example 3: Conditional processing
	fmt.Println("\n3. Conditional processing with function")

	type Stats struct {
		YoungCount  int
		MiddleCount int
		SeniorCount int
	}

	stats := Stats{}

	err = db.Select(func(u User) {
		switch {
		case u.Age < 25:
			stats.YoungCount++
		case u.Age < 40:
			stats.MiddleCount++
		default:
			stats.SeniorCount++
		}
	}, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users`", 0)

	if err != nil {
		log.Printf("Stats failed: %v", err)
	} else {
		fmt.Printf("✓ Age distribution: Young=%d, Middle=%d, Senior=%d\n",
			stats.YoungCount, stats.MiddleCount, stats.SeniorCount)
	}

	// Example 4: Early termination pattern
	fmt.Println("\n4. Early termination with function")

	found := false
	targetEmail := "alice@example.com"

	err = db.Select(func(u User) {
		if u.Email == targetEmail {
			found = true
			fmt.Printf("✓ Found user: %s\n", u.Name)
			// Note: Can't actually stop iteration early
			// This is a limitation of function receivers
		}
	}, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users`", 0)

	if err != nil {
		log.Printf("Search failed: %v", err)
	} else if !found {
		fmt.Println("✗ User not found")
	}
}

// jsonExamples demonstrates JSON handling
func jsonExamples(db *mysql.Database) {
	// Example 1: Store JSON in struct field
	fmt.Println("1. Store JSON column in struct")

	type UserWithMeta struct {
		ID       int             `mysql:"id"`
		Name     string          `mysql:"name"`
		Email    string          `mysql:"email"`
		Metadata json.RawMessage `mysql:"metadata"` // JSON column
	}

	userMeta := UserWithMeta{
		Name:  "JSONUser",
		Email: "json@example.com",
		Metadata: json.RawMessage(`{
			"theme": "dark",
			"language": "en",
			"notifications": true
		}`),
	}

	err := db.Insert("users", userMeta)
	if err != nil {
		log.Printf("JSON insert failed: %v", err)
	} else {
		fmt.Println("✓ User with JSON metadata inserted")
	}

	// Example 2: Select JSON as RawMessage
	fmt.Println("\n2. Select JSON column")

	var usersWithMeta []UserWithMeta
	err = db.Select(&usersWithMeta,
		"SELECT `id`, `name`, `email`, metadata FROM `users` WHERE metadata IS NOT NULL LIMIT @@limit",
		0,
		5)

	if err != nil {
		log.Printf("JSON select failed: %v", err)
	} else {
		fmt.Printf("✓ Retrieved %d users with metadata\n", len(usersWithMeta))
		for _, u := range usersWithMeta {
			fmt.Printf("  %s: %s\n", u.Name, string(u.Metadata))
		}
	}

	// Example 3: SelectJSON for JSON result
	fmt.Println("\n3. SelectJSON for JSON object")

	var jsonResult json.RawMessage
	err = db.SelectJSON(&jsonResult,
		"SELECT JSON_OBJECT("+
		" 'id', `id`,"+
		" 'name', `name`,"+
		" 'email', `email`,"+
		" 'age', `age`"+
		" ) FROM `users` WHERE `id` = @@id",
		0,
		1)

	if err != nil {
		log.Printf("SelectJSON failed: %v", err)
	} else {
		fmt.Printf("✓ JSON result: %s\n", string(jsonResult))
	}

	// Example 4: SelectJSON for JSON array
	fmt.Println("\n4. SelectJSON for JSON array")

	var jsonArray json.RawMessage
	err = db.SelectJSON(&jsonArray,
		"SELECT JSON_ARRAYAGG("+
		" JSON_OBJECT("+
		" 'name', `name`,"+
		" 'email', `email`"+
		" )"+
		" ) FROM `users` WHERE `active` = 1 LIMIT @@limit",
		0,
		5)

	if err != nil {
		log.Printf("SelectJSON array failed: %v", err)
	} else {
		fmt.Printf("✓ JSON array result: %s\n", string(jsonArray))
	}
}

// rawSQLExamples demonstrates using mysql.Raw for literal SQL
func rawSQLExamples(db *mysql.Database) {
	// Example 1: Raw SQL in WHERE clause
	fmt.Println("1. Raw SQL for complex condition")

	var users []User
	err := db.Select(&users,
		"SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE @@condition",
		0,
		mysql.Raw("created_at > NOW() - INTERVAL 7 DAY"))

	if err != nil {
		log.Printf("Raw SQL query failed: %v", err)
	} else {
		fmt.Printf("✓ Found %d users from last 7 days\n", len(users))
	}

	// Example 2: Raw SQL for CASE statement
	fmt.Println("\n2. Raw SQL for CASE statement")

	type UserWithLabel struct {
		Name  string `mysql:"name"`
		Label string `mysql:"label"`
	}

	caseSQL := mysql.Raw(`
		CASE
			WHEN age < 25 THEN 'Young'
			WHEN age < 40 THEN 'Middle'
			ELSE 'Senior'
		END
	`)

	var labeled []UserWithLabel
	err = db.Select(&labeled,
		"SELECT name, @@ageCase as `label` FROM `users`",
		0,
		caseSQL)

	if err != nil {
		log.Printf("CASE query failed: %v", err)
	} else {
		fmt.Printf("✓ Retrieved %d users with age labels\n", len(labeled))
		for i, u := range labeled {
			if i < 3 {
				fmt.Printf("  %s: %s\n", u.Name, u.Label)
			}
		}
	}

	// Example 3: Raw SQL for subquery
	fmt.Println("\n3. Raw SQL for subquery")

	subquery := mysql.Raw("(SELECT AVG(age) FROM `users` WHERE `active` = 1)")

	err = db.Select(&users,
		"SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE age > @@avgAge",
		0,
		subquery)

	if err != nil {
		log.Printf("Subquery failed: %v", err)
	} else {
		fmt.Printf("✓ Found %d users above average age\n", len(users))
	}

	// Example 4: WARNING - Never use Raw with user input!
	fmt.Println("\n4. WARNING: Raw SQL security example")

	// DANGEROUS - SQL injection risk!
	// userInput := "'; DROP TABLE users; --"
	// db.Select(&users, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `name` = @@name", 0,
	//     mysql.Params{"name": mysql.Raw(userInput)})

	// SAFE - use regular parameter
	safeInput := "Alice'; DROP TABLE users; --"
	err = db.Select(&users, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `name` = @@name", 0, safeInput) // Properly escaped

	if err != nil {
		log.Printf("Safe query failed: %v", err)
	} else {
		fmt.Println("✓ User input safely escaped (no SQL injection)")
	}
}

// complexQueryExamples demonstrates complex query patterns
func complexQueryExamples(db *mysql.Database) {
	// Example 1: Subquery with named parameters
	fmt.Println("1. Subquery with parameters")

	type UserWithOrderCount struct {
		User
		OrderCount int `mysql:"order_count"`
	}

	var usersWithOrders []UserWithOrderCount
	err := db.Select(&usersWithOrders,
		"SELECT `users`.`id`, `users`.`name`, `users`.`email`, `users`.`age`, `users`.`active`, `users`.`created_at`, `users`.`updated_at`,"+
		" (SELECT COUNT(*) FROM `orders` WHERE `orders`.`user_id` = `users`.`id`) as `order_count`"+
		" FROM `users`"+
		" WHERE `users`.`created_at` > @@since"+
		" AND `users`.`active` = @@active",
		5*time.Minute,
		mysql.Params{
			"since":  time.Now().Add(-30 * 24 * time.Hour),
			"active": true,
		})

	if err != nil {
		log.Printf("Subquery failed: %v", err)
	} else {
		fmt.Printf("✓ Retrieved %d active users with order counts\n", len(usersWithOrders))
	}

	// Example 2: JOIN with aggregation
	fmt.Println("\n2. JOIN with aggregation")

	query := "SELECT" +
		" `users`.`id`," +
		" `users`.`name`," +
		" `users`.`email`," +
		" COUNT(`orders`.`id`) as `order_count`," +
		" SUM(`orders`.`total`) as `total_spent`" +
		" FROM `users`" +
		" LEFT JOIN `orders` ON `users`.`id` = `orders`.`user_id`" +
		" WHERE `users`.`active` = @@active" +
		" GROUP BY `users`.`id`, `users`.`name`, `users`.`email`" +
		" HAVING COUNT(`orders`.`id`) > @@minOrders" +
		" ORDER BY total_spent DESC" +
		" LIMIT @@limit"

	type UserStats struct {
		ID         int     `mysql:"id"`
		Name       string  `mysql:"name"`
		Email      string  `mysql:"email"`
		OrderCount int     `mysql:"order_count"`
		TotalSpent float64 `mysql:"total_spent"`
	}

	var stats []UserStats
	err = db.Select(&stats, query, 10*time.Minute,
		mysql.Params{
			"active":    true,
			"minOrders": 5,
			"limit":     10,
		})

	if err != nil {
		log.Printf("Aggregation query failed: %v", err)
	} else {
		fmt.Printf("✓ Top %d spenders retrieved\n", len(stats))
	}

	// Example 3: Window function
	fmt.Println("\n3. Window function query")

	windowQuery := "SELECT" +
		" `id`," +
		" `name`," +
		" `age`," +
		" RANK() OVER (ORDER BY `age` DESC) as `age_rank`," +
		" AVG(`age`) OVER () as `avg_age`" +
		" FROM `users`" +
		" WHERE `active` = @@active" +
		" LIMIT @@limit"

	type UserWithRank struct {
		ID      int     `mysql:"id"`
		Name    string  `mysql:"name"`
		Age     int     `mysql:"age"`
		AgeRank int     `mysql:"age_rank"`
		AvgAge  float64 `mysql:"avg_age"`
	}

	var ranked []UserWithRank
	err = db.Select(&ranked, windowQuery, 5*time.Minute,
		mysql.Params{
			"active": true,
			"limit":  20,
		})

	if err != nil {
		log.Printf("Window function query failed: %v", err)
	} else {
		fmt.Printf("✓ Retrieved %d users with age ranking\n", len(ranked))
	}

	// Example 4: CTE (Common Table Expression)
	fmt.Println("\n4. CTE query")

	cteQuery := "WITH recent_users AS (" +
		" SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users`" +
		" WHERE `created_at` > @@since" +
		" )," +
		" active_recent AS (" +
		" SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM recent_users" +
		" WHERE `active` = @@active" +
		" )" +
		" SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM active_recent" +
		" ORDER BY `created_at` DESC" +
		" LIMIT @@limit"

	var cteUsers []User
	err = db.Select(&cteUsers, cteQuery, 5*time.Minute,
		mysql.Params{
			"since":  time.Now().Add(-7 * 24 * time.Hour),
			"active": true,
			"limit":  10,
		})

	if err != nil {
		log.Printf("CTE query failed: %v", err)
	} else {
		fmt.Printf("✓ Retrieved %d recent active users via CTE\n", len(cteUsers))
	}
}
