package mysql

import (
	"time"
)

// MaxExecutionTime is the total time we would like our queries to be able to execute.
// Since we are using 30 second limited AWS Lambda functions, we'll default this time to
// 90% of 30 seconds (27 seconds), with the goal of letting our process clean up and correctly
// log any failed queries
var MaxExecutionTime = time.Duration(getEnvInt64("COOL_MAX_EXECUTION_TIME_TIME", int64(float64(30)*.9))) * time.Second

var MaxConnectionTime = MaxExecutionTime

var RedisLockRetryDelay = time.Duration(getEnvFloat("COOL_REDIS_LOCK_RETRY_DELAY", .020)) * time.Second
