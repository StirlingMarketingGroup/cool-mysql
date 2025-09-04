package mysql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestSelectTimezoneUnmarshalling demonstrates the timezone issue with SELECT operations
func TestSelectTimezoneUnmarshalling(t *testing.T) {
	t.Run("Demonstrate timezone mismatch issue", func(t *testing.T) {
		// This demonstrates the core problem that our database.go fix addresses:

		t.Log("=== THE TIMEZONE PROBLEM ===")
		t.Log("1. MySQL stores DATETIME columns as naive timestamps (no timezone)")
		t.Log("2. When we SELECT, MySQL returns the raw value: '2024-01-01 12:00:00'")
		t.Log("3. Go's mysql driver interprets this using the DSN's Loc parameter")
		t.Log("4. If MySQL session timezone ≠ DSN Loc timezone → wrong time!")

		// Simulate the problem:
		mysqlStoredValue := "2024-01-01 12:00:00" // What MySQL has in DATETIME column

		// Parse as naive time (what MySQL returns)
		naiveTime, err := time.Parse("2006-01-02 15:04:05", mysqlStoredValue)
		require.NoError(t, err)

		t.Logf("MySQL stored value: %s (naive)", mysqlStoredValue)
		t.Logf("Parsed as naive time: %s", naiveTime.Format(time.RFC3339))

		// Now show what happens with different interpretations:
		eastern, _ := time.LoadLocation("America/New_York")

		// If MySQL session is in Eastern and DSN Loc is Eastern (CORRECT):
		correctTime := time.Date(naiveTime.Year(), naiveTime.Month(), naiveTime.Day(),
			naiveTime.Hour(), naiveTime.Minute(), naiveTime.Second(), naiveTime.Nanosecond(), eastern)
		t.Logf("Correct interpretation (Eastern): %s", correctTime.Format(time.RFC3339))

		// If MySQL session is SYSTEM but DSN Loc is Eastern (WRONG):
		// Driver assumes it's Eastern, but MySQL returned it in system timezone
		wrongTime := time.Date(naiveTime.Year(), naiveTime.Month(), naiveTime.Day(),
			naiveTime.Hour(), naiveTime.Minute(), naiveTime.Second(), naiveTime.Nanosecond(), time.UTC)
		t.Logf("Wrong interpretation (UTC): %s", wrongTime.Format(time.RFC3339))

		// Show the problem
		diff := wrongTime.Sub(correctTime)
		t.Logf("Time difference: %v", diff)

		if diff != 0 {
			t.Logf("✗ This is the bug we fixed!")
			t.Logf("  Solution: SET time_zone in MySQL session to match DSN Loc")
		}
	})

	t.Run("convertAssignRows behavior", func(t *testing.T) {
		// Test that time.Time values are handled correctly
		easternTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.FixedZone("EST", -5*3600))

		var dest time.Time
		err := convertAssignRows(&dest, easternTime)
		require.NoError(t, err)

		t.Logf("convertAssignRows preserves timezone info:")
		t.Logf("  Source: %s", easternTime.Format(time.RFC3339))
		t.Logf("  Dest:   %s", dest.Format(time.RFC3339))

		require.True(t, dest.Equal(easternTime), "Time should be preserved exactly")

		// The real issue is that the MySQL driver calls convertAssignRows with
		// a time.Time that was already parsed with the wrong timezone info
		// Our fix prevents that by ensuring session timezone matches DSN Loc
	})
}
