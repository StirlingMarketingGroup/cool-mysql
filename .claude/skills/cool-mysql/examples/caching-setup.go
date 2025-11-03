// Package examples demonstrates caching configuration with cool-mysql
package examples

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/redis/go-redis/v9"
	mysql "github.com/StirlingMarketingGroup/cool-mysql"
)

// CachingExamples demonstrates various caching setups and strategies
func CachingExamples() {
	fmt.Println("=== CACHING SETUP EXAMPLES ===")

	// In-memory caching
	fmt.Println("\n1. In-Memory Weak Cache")
	weakCacheExample()

	// Redis caching
	fmt.Println("\n2. Redis Cache")
	redisCacheExample()

	// Redis Cluster caching
	fmt.Println("\n3. Redis Cluster Cache")
	redisClusterExample()

	// Memcached caching
	fmt.Println("\n4. Memcached Cache")
	memcachedCacheExample()

	// Multi-level caching
	fmt.Println("\n5. Multi-Level Cache")
	multiCacheExample()

	// Cache strategies
	fmt.Println("\n6. Cache Strategies")
	cacheStrategiesExample()

	// Performance benchmark
	fmt.Println("\n7. Performance Benchmark")
	db, err := setupDatabase()
	if err != nil {
		log.Printf("Setup failed: %v", err)
		return
	}
	performanceBenchmark(db)

	// Cache key debugging
	fmt.Println("\n8. Cache Key Debugging")
	cacheKeyDebug(db)
}

// weakCacheExample demonstrates in-memory weak cache
func weakCacheExample() {
	db, err := setupDatabase()
	if err != nil {
		log.Printf("Setup failed: %v", err)
		return
	}

	// Enable in-memory weak cache
	db.UseCache(mysql.NewWeakCache())

	fmt.Println("✓ Weak cache enabled (GC-managed, local only)")

	// First query - cache miss
	start := time.Now()
	var users []User
	err = db.Select(&users,
		"SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `active` = @@active",
		5*time.Minute, // Cache for 5 minutes
		true)
	duration1 := time.Since(start)

	if err != nil {
		log.Printf("First query failed: %v", err)
		return
	}
	fmt.Printf("  First query (cache miss): %v, %d users\n", duration1, len(users))

	// Second query - cache hit
	start = time.Now()
	err = db.Select(&users,
		"SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `active` = @@active",
		5*time.Minute,
		true)
	duration2 := time.Since(start)

	if err != nil {
		log.Printf("Second query failed: %v", err)
		return
	}
	fmt.Printf("  Second query (cache hit): %v, %d users\n", duration2, len(users))
	fmt.Printf("  Speedup: %.2fx faster\n", float64(duration1)/float64(duration2))
}

// redisCacheExample demonstrates Redis cache setup
func redisCacheExample() {
	db, err := setupDatabase()
	if err != nil {
		log.Printf("Setup failed: %v", err)
		return
	}

	// Setup Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		Password:     "",  // no password
		DB:           0,   // default DB
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
		MinIdleConns: 5,
	})

	// Test Redis connection
	ctx := context.Background()
	_, err = redisClient.Ping(ctx).Result()
	if err != nil {
		log.Printf("Redis connection failed: %v", err)
		log.Println("  Skipping Redis cache example")
		return
	}

	// Enable Redis cache
	db.EnableRedis(redisClient)
	fmt.Println("✓ Redis cache enabled (distributed, with locking)")

	// Query with caching
	var users []User
	err = db.Select(&users,
		"SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE age > @@minAge",
		10*time.Minute, // Cache for 10 minutes
		18)

	if err != nil {
		log.Printf("Redis cached query failed: %v", err)
		return
	}

	fmt.Printf("  Cached %d users in Redis\n", len(users))
	fmt.Println("  ✓ Cache shared across all application instances")
	fmt.Println("  ✓ Distributed locking prevents cache stampedes")
}

// redisClusterExample demonstrates Redis Cluster setup
func redisClusterExample() {
	db, err := setupDatabase()
	if err != nil {
		log.Printf("Setup failed: %v", err)
		return
	}

	// Setup Redis Cluster client
	redisCluster := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{
			"localhost:7000",
			"localhost:7001",
			"localhost:7002",
		},
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
	})

	// Test cluster connection
	ctx := context.Background()
	_, err = redisCluster.Ping(ctx).Result()
	if err != nil {
		log.Printf("Redis cluster connection failed: %v", err)
		log.Println("  Skipping Redis cluster example")
		return
	}

	// Enable Redis cluster cache
	// Note: EnableRedis works with both single-node and cluster
	db.EnableRedis(redisCluster)
	fmt.Println("✓ Redis Cluster cache enabled")

	// Query with caching
	var users []User
	err = db.Select(&users, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` LIMIT @@limit", 5*time.Minute, 100)

	if err != nil {
		log.Printf("Cluster cached query failed: %v", err)
		return
	}

	fmt.Printf("  Cached %d users in Redis Cluster\n", len(users))
}

// memcachedCacheExample demonstrates Memcached cache setup
func memcachedCacheExample() {
	db, err := setupDatabase()
	if err != nil {
		log.Printf("Setup failed: %v", err)
		return
	}

	// Setup Memcached client
	memcacheClient := memcache.New("localhost:11211")
	memcacheClient.Timeout = 3 * time.Second
	memcacheClient.MaxIdleConns = 10

	// Test Memcached connection
	err = memcacheClient.Ping()
	if err != nil {
		log.Printf("Memcached connection failed: %v", err)
		log.Println("  Skipping Memcached example")
		return
	}

	// Enable Memcached
	db.EnableMemcache(memcacheClient)
	fmt.Println("✓ Memcached cache enabled (distributed, simple)")

	// Query with caching
	var users []User
	err = db.Select(&users,
		"SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `active` = @@active",
		15*time.Minute, // Cache for 15 minutes
		true)

	if err != nil {
		log.Printf("Memcached query failed: %v", err)
		return
	}

	fmt.Printf("  Cached %d users in Memcached\n", len(users))
	fmt.Println("  ⚠ No distributed locking (potential cache stampedes)")
}

// multiCacheExample demonstrates multi-level caching
func multiCacheExample() {
	db, err := setupDatabase()
	if err != nil {
		log.Printf("Setup failed: %v", err)
		return
	}

	// Setup Redis
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	ctx := context.Background()
	_, err = redisClient.Ping(ctx).Result()
	if err != nil {
		log.Printf("Redis unavailable, using weak cache only")
		db.UseCache(mysql.NewWeakCache())
		return
	}

	// Create multi-level cache
	// L1: Fast local weak cache
	// L2: Shared Redis cache
	multiCache := mysql.NewMultiCache(
		mysql.NewWeakCache(),           // L1: In-memory
		mysql.NewRedisCache(redisClient), // L2: Redis
	)

	db.UseCache(multiCache)
	fmt.Println("✓ Multi-level cache enabled")
	fmt.Println("  L1: In-memory weak cache (fastest)")
	fmt.Println("  L2: Redis distributed cache (shared)")

	// First query - cold cache (misses both levels)
	start := time.Now()
	var users []User
	err = db.Select(&users,
		"SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE age > @@minAge",
		10*time.Minute,
		21)
	cold := time.Since(start)

	if err != nil {
		log.Printf("Cold cache query failed: %v", err)
		return
	}
	fmt.Printf("\n  Cold cache (DB query): %v, %d users\n", cold, len(users))

	// Second query - warm cache (hits L1)
	start = time.Now()
	err = db.Select(&users,
		"SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE age > @@minAge",
		10*time.Minute,
		21)
	warm := time.Since(start)

	if err != nil {
		log.Printf("Warm cache query failed: %v", err)
		return
	}
	fmt.Printf("  Warm cache (L1 hit): %v\n", warm)
	fmt.Printf("  Speedup: %.2fx faster\n", float64(cold)/float64(warm))
}

// cacheStrategiesExample demonstrates different caching strategies
func cacheStrategiesExample() {
	db, err := setupDatabase()
	if err != nil {
		log.Printf("Setup failed: %v", err)
		return
	}

	db.UseCache(mysql.NewWeakCache())

	// Strategy 1: No caching for real-time data
	fmt.Println("\nStrategy 1: No caching (TTL = 0)")
	var liveUsers []User
	err = db.Select(&liveUsers,
		"SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `last_active` > @@since",
		0, // No caching
		time.Now().Add(-5*time.Minute))

	if err != nil {
		log.Printf("Live query failed: %v", err)
	} else {
		fmt.Printf("  ✓ %d active users (always fresh)\n", len(liveUsers))
	}

	// Strategy 2: Short TTL for frequently changing data
	fmt.Println("\nStrategy 2: Short TTL (30 seconds)")
	err = db.Select(&liveUsers,
		"SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `active` = @@active",
		30*time.Second, // Short TTL
		true)

	if err != nil {
		log.Printf("Short TTL query failed: %v", err)
	} else {
		fmt.Println("  ✓ Balance freshness and performance")
	}

	// Strategy 3: Long TTL for reference data
	fmt.Println("\nStrategy 3: Long TTL (1 hour)")
	type Country struct {
		ID   int    `mysql:"id"`
		Name string `mysql:"name"`
		Code string `mysql:"code"`
	}

	var countries []Country
	err = db.Select(&countries,
		"SELECT `id`, `name`, `code` FROM `countries`",
		time.Hour, // Long TTL for reference data
	)

	if err != nil {
		log.Printf("Long TTL query failed: %v", err)
	} else {
		fmt.Printf("  ✓ %d countries (rarely changes)\n", len(countries))
	}

	// Strategy 4: Conditional caching based on result size
	fmt.Println("\nStrategy 4: Conditional caching")
	conditionalCacheQuery(db)

	// Strategy 5: Read-after-write with SelectWrites
	fmt.Println("\nStrategy 5: Read-after-write consistency")
	readAfterWriteExample(db)
}

// conditionalCacheQuery demonstrates dynamic TTL selection
func conditionalCacheQuery(db *mysql.Database) {
	// First query to check result size
	var users []User
	err := db.Select(&users,
		"SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `status` = @@status",
		0, // No cache for initial check
		"active")

	if err != nil {
		log.Printf("Initial query failed: %v", err)
		return
	}

	// Choose TTL based on result size
	var ttl time.Duration
	if len(users) > 1000 {
		ttl = 30 * time.Minute // Large result - cache longer
		fmt.Println("  Large result set (>1000) - using 30min TTL")
	} else if len(users) > 100 {
		ttl = 10 * time.Minute // Medium result - moderate TTL
		fmt.Println("  Medium result set (100-1000) - using 10min TTL")
	} else {
		ttl = 2 * time.Minute // Small result - short TTL
		fmt.Println("  Small result set (<100) - using 2min TTL")
	}

	// Re-query with appropriate TTL
	err = db.Select(&users,
		"SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `status` = @@status",
		ttl,
		"active")

	if err != nil {
		log.Printf("Cached query failed: %v", err)
	} else {
		fmt.Printf("  ✓ %d users cached with TTL=%v\n", len(users), ttl)
	}
}

// readAfterWriteExample demonstrates read-after-write pattern
func readAfterWriteExample(db *mysql.Database) {
	// Insert new user
	newUser := User{
		Name:   "CacheUser",
		Email:  "cache@example.com",
		Age:    28,
		Active: true,
	}

	err := db.Insert("users", newUser)
	if err != nil {
		log.Printf("Insert failed: %v", err)
		return
	}

	fmt.Println("  User inserted")

	// WRONG: Using Select() might read from stale cache or replica
	// var user User
	// db.Select(&user, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `email` = @@email",
	//     5*time.Minute, mysql.Params{"email": "cache@example.com"})

	// CORRECT: Use SelectWrites for read-after-write consistency
	var user User
	err = db.SelectWrites(&user,
		"SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `email` = @@email",
		0, // Don't cache write-pool reads
		"cache@example.com")

	if err != nil {
		log.Printf("SelectWrites failed: %v", err)
		return
	}

	fmt.Printf("  ✓ User retrieved immediately (ID: %d)\n", user.ID)
	fmt.Println("  ✓ Used write pool for consistency")
}

// performanceBenchmark compares cache vs no-cache performance
func performanceBenchmark(db *mysql.Database) {
	fmt.Println("\nPerformance Benchmark: Cache vs No-Cache")

	query := "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `active` = @@active"
	param := true

	// Benchmark without cache
	start := time.Now()
	iterations := 100
	for i := 0; i < iterations; i++ {
		var users []User
		db.Select(&users, query, 0, param) // No cache
	}
	noCacheDuration := time.Since(start)
	avgNoCache := noCacheDuration / time.Duration(iterations)

	fmt.Printf("  Without cache: %v total, %v avg per query\n",
		noCacheDuration, avgNoCache)

	// Enable cache
	db.UseCache(mysql.NewWeakCache())

	// Warm up cache
	var warmup []User
	db.Select(&warmup, query, 5*time.Minute, param)

	// Benchmark with cache
	start = time.Now()
	for i := 0; i < iterations; i++ {
		var users []User
		db.Select(&users, query, 5*time.Minute, param) // With cache
	}
	cacheDuration := time.Since(start)
	avgCache := cacheDuration / time.Duration(iterations)

	fmt.Printf("  With cache: %v total, %v avg per query\n",
		cacheDuration, avgCache)
	fmt.Printf("  Speedup: %.2fx faster with cache\n",
		float64(noCacheDuration)/float64(cacheDuration))
}

// cacheKeyDebug demonstrates understanding cache keys
func cacheKeyDebug(db *mysql.Database) {
	fmt.Println("\nCache Key Understanding")

	db.UseCache(mysql.NewWeakCache())

	// Same query, same params = same cache key
	fmt.Println("  1. Identical queries share cache:")
	var users1, users2 []User

	db.Select(&users1, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE age > @@minAge", 5*time.Minute,
		18)

	// This hits cache
	db.Select(&users2, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE age > @@minAge", 5*time.Minute,
		18)

	fmt.Println("  ✓ Second query used cached result")

	// Different params = different cache key
	fmt.Println("\n  2. Different params = different cache:")
	var users3 []User
	db.Select(&users3, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE age > @@minAge", 5*time.Minute,
		25) // Different param value

	fmt.Println("  ✓ Different parameters bypass cache")

	// Parameter order doesn't matter
	fmt.Println("\n  3. Parameter order normalized:")
	var users4, users5 []User

	db.Select(&users4,
		"SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE age > @@minAge AND `active` = @@active",
		5*time.Minute,
		mysql.Params{"minAge": 18, "active": true})

	// Same cache even though params in different order
	db.Select(&users5,
		"SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE age > @@minAge AND `active` = @@active",
		5*time.Minute,
		mysql.Params{"active": true, "minAge": 18}) // Reversed

	fmt.Println("  ✓ Parameter order doesn't affect cache key")
}
