package mysql

import "time"

// default is 5 seconds less than 30 (our max lambda execution time)
// to hopefully allow errors to be logged before the lambda dies
var BackoffDefaultMaxElapsedTime = time.Duration(getEnvInt64("COOL_BACKOFF_DEFAULT_MAX_ELAPSED_SECONDS", 25)) * time.Second
