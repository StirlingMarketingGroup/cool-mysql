# Caching Guide

Complete guide to caching strategies and configuration in cool-mysql.

## Table of Contents

1. [Caching Overview](#caching-overview)
2. [Cache Types](#cache-types)
3. [Cache Configuration](#cache-configuration)
4. [TTL Selection](#ttl-selection)
5. [Multi-Level Caching](#multi-level-caching)
6. [Cache Invalidation](#cache-invalidation)
7. [Distributed Locking](#distributed-locking)
8. [Performance Optimization](#performance-optimization)
9. [Best Practices](#best-practices)

## Caching Overview

cool-mysql supports pluggable caching for SELECT queries to reduce database load and improve response times.

### How Caching Works

1. **Cache Key Generation**: Automatically generated from query + parameters
2. **Cache Check**: Before executing query, check cache for existing result
3. **Cache Miss**: Execute query and store result with TTL
4. **Cache Hit**: Return cached result without database query

### What Gets Cached

- **Cached**: All SELECT queries with `cacheTTL > 0`
- **Not Cached**: INSERT, UPDATE, DELETE, EXEC operations
- **Not Cached**: SELECT queries with `cacheTTL = 0`

### Cache Behavior

```go
// No caching (TTL = 0)
err := db.Select(&users, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users`", 0)

// Cache for 5 minutes
err := db.Select(&users, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users`", 5*time.Minute)

// Cache for 1 hour
err := db.Select(&users, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `active` = 1", time.Hour)
```

## Cache Types

### 1. In-Memory Weak Cache

**Type**: Local, process-specific, GC-managed
**Use Case**: Single-server applications, development, testing

**Characteristics:**
- Fastest access (no network)
- Memory managed by Go GC
- Weak pointers - automatically freed when under memory pressure
- Not shared across processes
- Lost on restart

**Setup:**
```go
db.UseCache(mysql.NewWeakCache())
```

**Pros:**
- Zero configuration
- No external dependencies
- Automatic memory management
- Extremely fast

**Cons:**
- Not shared across servers
- No distributed locking
- Cache lost on restart
- Memory limited

**Best For:**
- Development
- Testing
- Single-server deployments
- Applications with low cache requirements

### 2. Redis Cache

**Type**: Distributed, persistent
**Use Case**: Multi-server deployments, high-traffic applications

**Characteristics:**
- Shared across all application instances
- Distributed locking to prevent cache stampedes
- Configurable persistence
- Network latency overhead
- Requires Redis server

**Setup:**
```go
import "github.com/redis/go-redis/v9"

redisClient := redis.NewClient(&redis.Options{
    Addr:     "localhost:6379",
    Password: "", // no password set
    DB:       0,  // use default DB
})

db.EnableRedis(redisClient)
```

**Pros:**
- Shared cache across servers
- Distributed locking
- Persistent (optional)
- High capacity
- Cache stampede prevention

**Cons:**
- Network latency
- Requires Redis infrastructure
- More complex setup

**Best For:**
- Production multi-server deployments
- High-traffic applications
- Applications requiring cache consistency
- Preventing thundering herd problems

### 3. Memcached Cache

**Type**: Distributed, volatile
**Use Case**: Multi-server deployments, simple caching needs

**Characteristics:**
- Shared across all application instances
- No persistence
- Simple protocol
- No distributed locking
- Requires Memcached server

**Setup:**
```go
import "github.com/bradfitz/gomemcache/memcache"

memcacheClient := memcache.New("localhost:11211")
db.EnableMemcache(memcacheClient)
```

**Pros:**
- Shared cache across servers
- Simple and fast
- Mature technology
- Good performance

**Cons:**
- No distributed locking
- No persistence
- Cache lost on restart
- No cache stampede prevention

**Best For:**
- Legacy infrastructure with Memcached
- Simple caching needs
- When distributed locking not required

## Cache Configuration

### Basic Setup

```go
// In-memory cache
db := mysql.New(...)
db.UseCache(mysql.NewWeakCache())

// Redis cache
redisClient := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
})
db.EnableRedis(redisClient)

// Memcached
memcacheClient := memcache.New("localhost:11211")
db.EnableMemcache(memcacheClient)
```

### Redis Advanced Configuration

```go
redisClient := redis.NewClient(&redis.Options{
    Addr:         "localhost:6379",
    Password:     "secret",
    DB:           0,
    DialTimeout:  5 * time.Second,
    ReadTimeout:  3 * time.Second,
    WriteTimeout: 3 * time.Second,
    PoolSize:     10,
    MinIdleConns: 5,
})

db.EnableRedis(redisClient)
```

### Redis Cluster

```go
redisClient := redis.NewClusterClient(&redis.ClusterOptions{
    Addrs: []string{
        "localhost:7000",
        "localhost:7001",
        "localhost:7002",
    },
})

db.EnableRedis(redisClient)
```

### Environment Configuration

Configure cache behavior via environment variables:

```bash
# Redis lock retry delay (default: 0.020 seconds)
export COOL_REDIS_LOCK_RETRY_DELAY=0.050

# Max query execution time (default: 27 seconds)
export COOL_MAX_EXECUTION_TIME_TIME=30s

# Max retry attempts (default: unlimited)
export COOL_MAX_ATTEMPTS=5
```

## TTL Selection

### TTL Guidelines

Choose TTL based on data volatility and access patterns:

| Data Type | Recommended TTL | Rationale |
|-----------|----------------|-----------|
| User sessions | 5-15 minutes | Frequently changing |
| Reference data | 1-24 hours | Rarely changing |
| Analytics/Reports | 15-60 minutes | Tolerates staleness |
| Real-time data | 0 (no cache) | Must be fresh |
| Configuration | 5-60 minutes | Infrequent changes |
| Search results | 1-5 minutes | Balance freshness/load |
| Product catalogs | 10-30 minutes | Moderate change rate |

### Dynamic TTL Selection

```go
// Choose TTL based on query type
func getCacheTTL(queryType string) time.Duration {
    switch queryType {
    case "user_profile":
        return 10 * time.Minute
    case "product_catalog":
        return 30 * time.Minute
    case "analytics":
        return time.Hour
    case "real_time":
        return 0 // No caching
    default:
        return 5 * time.Minute
    }
}

var users []User
err := db.Select(&users,
    "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `active` = 1",
    getCacheTTL("user_profile"))
```

### Conditional TTL

```go
// Cache differently based on result size
var users []User
err := db.Select(&users, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `status` = @@status", 0,
    status)

ttl := 5 * time.Minute
if len(users) > 1000 {
    // Large result set - cache longer to reduce load
    ttl = 30 * time.Minute
}

// Re-query with caching
err = db.Select(&users, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `status` = @@status", ttl,
    status)
```

## Multi-Level Caching

### Layered Cache Strategy

Combine fast local cache with shared distributed cache:

```go
db.UseCache(mysql.NewMultiCache(
    mysql.NewWeakCache(),           // L1: Fast local cache
    mysql.NewRedisCache(redisClient), // L2: Shared distributed cache
))
```

**How It Works:**
1. Check L1 (local weak cache) - fastest
2. If miss, check L2 (Redis) - shared
3. If miss, query database
4. Store result in both L1 and L2

**Benefits:**
- Extremely fast for repeated queries in same process
- Shared cache prevents duplicate work across servers
- Best of both worlds: speed + consistency

### Custom Multi-Level Configuration

```go
// Create custom cache layers
type CustomCache struct {
    layers []mysql.Cache
}

func (c *CustomCache) Get(key string) ([]byte, bool) {
    for _, layer := range c.layers {
        if val, ok := layer.Get(key); ok {
            // Backfill previous layers
            for _, prevLayer := range c.layers {
                if prevLayer == layer {
                    break
                }
                prevLayer.Set(key, val, 0)
            }
            return val, true
        }
    }
    return nil, false
}

func (c *CustomCache) Set(key string, val []byte, ttl time.Duration) {
    for _, layer := range c.layers {
        layer.Set(key, val, ttl)
    }
}

// Use custom cache
cache := &CustomCache{
    layers: []mysql.Cache{
        mysql.NewWeakCache(),
        mysql.NewRedisCache(redis1),
        mysql.NewRedisCache(redis2), // Backup Redis
    },
}
db.UseCache(cache)
```

## Cache Invalidation

### Automatic Invalidation

cool-mysql doesn't auto-invalidate on writes. You must handle invalidation explicitly.

### Manual Invalidation Patterns

#### 1. Write-Through Pattern

```go
// Update database
err := db.Exec("UPDATE `users` SET `name` = @@name WHERE `id` = @@id",
    mysql.Params{"name": "Alice", "id": 123})
if err != nil {
    return err
}

// Invalidate cache (if using Redis)
cacheKey := generateCacheKey("SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `id` = ?", 123)
redisClient.Del(ctx, cacheKey)
```

#### 2. Read-After-Write with SelectWrites

```go
// Write to database
err := db.Insert("users", user)
if err != nil {
    return err
}

// Read from write pool for immediate consistency
err = db.SelectWrites(&user,
    "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `id` = @@id",
    0, // Don't cache write-pool reads
    user.ID)
```

#### 3. Tag-Based Invalidation

```go
// Tag queries with invalidation keys
const userCacheTag = "users:all"

// Set cache with tag
err := db.Select(&users, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users`", 10*time.Minute)
redisClient.SAdd(ctx, userCacheTag, cacheKey)

// Invalidate all user queries
keys, _ := redisClient.SMembers(ctx, userCacheTag).Result()
redisClient.Del(ctx, keys...)
redisClient.Del(ctx, userCacheTag)
```

#### 4. TTL-Based Invalidation

```go
// Rely on short TTL for eventual consistency
err := db.Select(&users, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users`",
    30*time.Second) // Short TTL = frequent refresh
```

### Cache Invalidation Strategies

| Strategy | Pros | Cons | Best For |
|----------|------|------|----------|
| Manual invalidation | Precise control | Complex to implement | Critical data |
| SelectWrites | Simple, consistent | Bypasses read pool | Read-after-write |
| Short TTL | Simple, automatic | Higher DB load | Frequently changing data |
| Tag-based | Bulk invalidation | Requires Redis | Related queries |

## Distributed Locking

### Cache Stampede Problem

When cache expires on high-traffic query:
1. Multiple requests see cache miss
2. All execute same expensive query simultaneously
3. Database overload

### Redis Distributed Locking Solution

cool-mysql's Redis cache includes distributed locking:

```go
db.EnableRedis(redisClient)

// Automatic distributed locking
err := db.Select(&users, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `active` = 1", 10*time.Minute)
```

**How It Works:**
1. First request gets lock, executes query
2. Subsequent requests wait for lock
3. First request populates cache
4. Waiting requests get result from cache
5. Lock automatically released

### Lock Configuration

```bash
# Configure lock retry delay
export COOL_REDIS_LOCK_RETRY_DELAY=0.020  # 20ms between retries
```

### Without Distributed Locking (Memcached)

Memcached doesn't support distributed locking. Mitigate stampedes with:

1. **Probabilistic Early Expiration**
```go
// Refresh cache before expiration
func shouldRefresh(ttl time.Duration) bool {
    // Refresh 10% of requests in last 10% of TTL
    return rand.Float64() < 0.1
}
```

2. **Stale-While-Revalidate**
```go
// Serve stale data while refreshing
// (Requires custom cache implementation)
```

## Performance Optimization

### Cache Hit Rate Monitoring

```go
type CacheStats struct {
    Hits   int64
    Misses int64
}

var stats CacheStats

// Wrap cache to track stats
type StatsCache struct {
    underlying mysql.Cache
    stats      *CacheStats
}

func (c *StatsCache) Get(key string) ([]byte, bool) {
    val, ok := c.underlying.Get(key)
    if ok {
        atomic.AddInt64(&c.stats.Hits, 1)
    } else {
        atomic.AddInt64(&c.stats.Misses, 1)
    }
    return val, ok
}

// Use stats cache
statsCache := &StatsCache{
    underlying: mysql.NewRedisCache(redisClient),
    stats:      &stats,
}
db.UseCache(statsCache)

// Check cache performance
hitRate := float64(stats.Hits) / float64(stats.Hits + stats.Misses)
fmt.Printf("Cache hit rate: %.2f%%\n", hitRate*100)
```

### Optimizing Cache Keys

cool-mysql generates cache keys from query + parameters. Optimize by:

1. **Normalizing Queries**
```go
// BAD: Different queries, same intent
db.Select(&users, "SELECT   *   FROM `users` WHERE `id` = @@id", ttl, params)
db.Select(&users, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `id` = @@id", ttl, params)
// ^ Different cache keys due to whitespace

// GOOD: Consistent formatting
const userByIDQuery = "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users` WHERE `id` = @@id"
db.Select(&users, userByIDQuery, ttl, params)
```

2. **Parameter Ordering**
```go
// Parameter order doesn't matter - they're normalized
db.Select(&users, query, ttl,
    mysql.Params{"status": "active", "age": 18})
db.Select(&users, query, ttl,
    mysql.Params{"age": 18, "status": "active"})
// ^ Same cache key
```

### Memory Usage Optimization

```go
// For memory-constrained environments
// Use shorter TTLs to reduce memory usage
db.UseCache(mysql.NewWeakCache())
err := db.Select(&users, "SELECT `id`, `name`, `email`, `age`, `active`, `created_at`, `updated_at` FROM `users`",
    1*time.Minute) // Short TTL = less memory

// Or use Redis with maxmemory policy
// redis.conf:
// maxmemory 100mb
// maxmemory-policy allkeys-lru
```

### Network Latency Optimization

```go
// Minimize Redis roundtrips with pipelining
// (Requires custom cache implementation)

// Or use MultiCache for local-first
db.UseCache(mysql.NewMultiCache(
    mysql.NewWeakCache(),           // Fast local first
    mysql.NewRedisCache(redisClient), // Fallback to Redis
))
```

## Best Practices

### 1. Match TTL to Data Volatility

```go
// Frequently changing - short TTL or no cache
db.Select(&liveData, query, 0)

// Rarely changing - long TTL
db.Select(&refData, query, 24*time.Hour)
```

### 2. Use SelectWrites After Writes

```go
// Write
db.Insert("users", user)

// Read with consistency
db.SelectWrites(&user, query, 0, params)
```

### 3. Cache High-Traffic Queries

```go
// Identify expensive queries
// Use longer TTLs for high-traffic, expensive queries
db.Select(&results, expensiveQuery, 30*time.Minute)
```

### 4. Don't Over-Cache

```go
// Don't cache everything - adds complexity
// Only cache queries that benefit from caching:
// - Expensive to compute
// - Frequently accessed
// - Tolerates staleness
```

### 5. Monitor Cache Performance

```go
// Track hit rates
// Tune TTLs based on metrics
// Remove caching from low-hit-rate queries
```

### 6. Use MultiCache for Best Performance

```go
// Production setup
db.UseCache(mysql.NewMultiCache(
    mysql.NewWeakCache(),           // L1: Fast
    mysql.NewRedisCache(redisClient), // L2: Shared
))
```

### 7. Handle Cache Failures Gracefully

```go
// Cache failures should fallback to database
// cool-mysql handles this automatically
// Even if Redis is down, queries still work
```

### 8. Consider Cache Warming

```go
// Pre-populate cache for known hot queries
func warmCache(db *mysql.Database) {
    db.Select(&refData, "SELECT `id`, `name`, `code` FROM `countries`", 24*time.Hour)
    db.Select(&config, "SELECT `key`, `value` FROM `config`", time.Hour)
}
```

### 9. Use Appropriate Cache for Environment

```go
// Development
db.UseCache(mysql.NewWeakCache())

// Production
db.EnableRedis(redisClient) // Distributed locking + sharing
```

### 10. Document Cache TTLs

```go
const (
    // Cache TTLs
    UserProfileTTL      = 10 * time.Minute // User data changes moderately
    ProductCatalogTTL   = 30 * time.Minute // Products updated infrequently
    AnalyticsTTL        = time.Hour        // Analytics can be stale
    NoCache             = 0                // Real-time data
)

db.Select(&user, query, UserProfileTTL, params)
```

## Troubleshooting

### High Cache Miss Rate

**Symptoms**: Low hit rate, high database load

**Solutions:**
- Increase TTL
- Check if queries are identical (whitespace, parameter names)
- Verify cache is configured correctly
- Check if queries are actually repeated

### Cache Stampede

**Symptoms**: Periodic database spikes, slow response during cache expiration

**Solutions:**
- Use Redis with distributed locking
- Implement probabilistic early refresh
- Increase TTL to reduce expiration frequency

### Memory Issues

**Symptoms**: High memory usage, OOM errors

**Solutions:**
- Reduce TTLs
- Use Redis instead of in-memory
- Configure Redis maxmemory policy
- Cache fewer queries

### Stale Data

**Symptoms**: Users see outdated information

**Solutions:**
- Reduce TTL
- Use SelectWrites after modifications
- Implement cache invalidation
- Consider if data should be cached at all
