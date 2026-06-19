package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQueryWithBudgetHint(t *testing.T) {
	db := &Database{}

	// No deadline → unchanged.
	require.Equal(t, "SELECT 1", db.queryWithBudgetHint(context.Background(), "SELECT 1"))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Deadline + leading SELECT → hinted.
	require.Contains(t, db.queryWithBudgetHint(ctx, "SELECT 1"), "MAX_EXECUTION_TIME")

	// Deadline but not a leading SELECT → unchanged (inject returns false).
	require.Equal(t, "UPDATE t SET x = 1", db.queryWithBudgetHint(ctx, "UPDATE t SET x = 1"))
}

func TestInjectMaxExecutionTime(t *testing.T) {
	tests := []struct {
		name  string
		query string
		ms    int64
		want  string
		ok    bool
	}{
		{
			name:  "plain select",
			query: "SELECT 1",
			ms:    1000,
			want:  "SELECT /*+ MAX_EXECUTION_TIME(1000) */ 1",
			ok:    true,
		},
		{
			name:  "lowercase select",
			query: "select a from t",
			ms:    250,
			want:  "select /*+ MAX_EXECUTION_TIME(250) */ a from t",
			ok:    true,
		},
		{
			name:  "leading whitespace and newlines",
			query: "\n\t  SELECT a",
			ms:    500,
			want:  "\n\t  SELECT /*+ MAX_EXECUTION_TIME(500) */ a",
			ok:    true,
		},
		{
			name:  "leading block comment",
			query: "/* hi */ SELECT a",
			ms:    500,
			want:  "/* hi */ SELECT /*+ MAX_EXECUTION_TIME(500) */ a",
			ok:    true,
		},
		{
			name:  "leading line comment",
			query: "-- note\nSELECT a",
			ms:    500,
			want:  "-- note\nSELECT /*+ MAX_EXECUTION_TIME(500) */ a",
			ok:    true,
		},
		{
			name:  "merge into existing hint block",
			query: "SELECT /*+ INDEX(t idx) */ a from t",
			ms:    750,
			want:  "SELECT /*+ INDEX(t idx) MAX_EXECUTION_TIME(750) */ a from t",
			ok:    true,
		},
		{
			name:  "existing max_execution_time is left alone",
			query: "SELECT /*+ MAX_EXECUTION_TIME(9) */ a",
			ms:    750,
			want:  "SELECT /*+ MAX_EXECUTION_TIME(9) */ a",
			ok:    false,
		},
		{
			name:  "not a select",
			query: "UPDATE t SET a = 1",
			ms:    1000,
			want:  "UPDATE t SET a = 1",
			ok:    false,
		},
		{
			name:  "with-cte leading is skipped (hint must lead the SELECT)",
			query: "WITH c AS (SELECT 1) SELECT * FROM c",
			ms:    1000,
			want:  "WITH c AS (SELECT 1) SELECT * FROM c",
			ok:    false,
		},
		{
			name:  "word boundary guards against selected",
			query: "selected_columns is not a keyword",
			ms:    1000,
			want:  "selected_columns is not a keyword",
			ok:    false,
		},
		{
			name:  "non-positive ms is a no-op",
			query: "SELECT 1",
			ms:    0,
			want:  "SELECT 1",
			ok:    false,
		},
		{
			name:  "unterminated hint block is left alone",
			query: "SELECT /*+ INDEX(t) a",
			ms:    1000,
			want:  "SELECT /*+ INDEX(t) a",
			ok:    false,
		},
		{
			name:  "leading hash comment",
			query: "# note\nSELECT a",
			ms:    500,
			want:  "# note\nSELECT /*+ MAX_EXECUTION_TIME(500) */ a",
			ok:    true,
		},
		{
			name:  "merge into hint block with no trailing space",
			query: "SELECT /*+ INDEX(t)*/ a",
			ms:    750,
			want:  "SELECT /*+ INDEX(t) MAX_EXECUTION_TIME(750) */ a",
			ok:    true,
		},
		{
			name:  "unterminated block comment is left alone",
			query: "/* unterminated SELECT a",
			ms:    1000,
			want:  "/* unterminated SELECT a",
			ok:    false,
		},
		{
			name:  "unterminated hash comment is left alone",
			query: "# no newline here",
			ms:    1000,
			want:  "# no newline here",
			ok:    false,
		},
		{
			name:  "unterminated line comment is left alone",
			query: "-- no newline here",
			ms:    1000,
			want:  "-- no newline here",
			ok:    false,
		},
		{
			name:  "only whitespace is left alone",
			query: "   \t\n  ",
			ms:    1000,
			want:  "   \t\n  ",
			ok:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := injectMaxExecutionTime(tt.query, tt.ms)
			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMaxExecutionTimeMillis(t *testing.T) {
	// remaining <= 0 → skip
	require.Zero(t, maxExecutionTimeMillis(0))
	require.Zero(t, maxExecutionTimeMillis(-time.Second))

	// large budget: buffer capped at 1s
	require.Equal(t, int64(59000), maxExecutionTimeMillis(60*time.Second))

	// small budget: buffer floored at 50ms
	require.Equal(t, int64(950), maxExecutionTimeMillis(time.Second))

	// budget smaller than the floor buffer → skip
	require.Zero(t, maxExecutionTimeMillis(40*time.Millisecond))

	// mid budget uses the 5% buffer
	require.Equal(t, int64(9500), maxExecutionTimeMillis(10*time.Second))
}
