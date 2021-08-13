package mysql

import "time"

// default is 5 seconds less than 30 (our max lambda execution time)
// to hopefully allow errors to be logged before the lambda dies
var BackoffDefaultMaxElapsedTime = time.Duration(getEnvInt64("COOL_BACKOFF_DEFAULT_MAX_ELAPSED_SECONDS", 25)) * time.Second

// also defaulting this to 30 seconds because that's our lambda lifetime
// currently unfreezing lambda functions causes queries to try to run on connections
// that have been closed by the server causing random failures
var MaxConnectionTime = time.Duration(getEnvInt64("COOL_MAX_CONNECTION_LIFE_TIME", 30)) * time.Second
