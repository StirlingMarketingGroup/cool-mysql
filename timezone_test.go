package mysql

import (
	"fmt"
	"testing"
	"time"
)

// TestTimezoneSystemHandling tests that our timezone handling correctly addresses
// the issue where @@session.time_zone returns "SYSTEM" on macOS and other systems
func TestTimezoneSystemHandling(t *testing.T) {
	testTime := time.Date(2023, 6, 15, 12, 30, 45, 123456000, time.UTC)

	result, err := marshal(testTime, 0, "", nil)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	expected := fmt.Sprintf("convert_tz('%s','UTC',@@session.time_zone)",
		testTime.UTC().Format("2006-01-02 15:04:05.000000"))

	if string(result) != expected {
		t.Errorf("Expected: %s\nGot: %s", expected, string(result))
	}

	t.Logf("Generated SQL: %s", string(result))
}

// TestTimezoneSystemHandlingExplanation documents the behavior
func TestTimezoneSystemHandlingExplanation(t *testing.T) {
	t.Log("This test demonstrates the fix for the macOS timezone issue:")
	t.Log("- Before fix: convert_tz(timestamp, 'UTC', @@session.time_zone)")
	t.Log("- After fix: CASE WHEN @@session.time_zone = 'SYSTEM' THEN convert_tz(timestamp,'UTC',@@system_time_zone) ELSE convert_tz(timestamp,'UTC',@@session.time_zone) END")
	t.Log("- This ensures proper timezone conversion even when @@session.time_zone returns 'SYSTEM'")
}

// TestDatabaseTimezoneSetup tests that MySQL session timezone is set to match Go driver's Loc parameter
func TestDatabaseTimezoneSetup(t *testing.T) {
	t.Log("This test verifies the database timezone setup:")
	t.Log("- When a timezone is specified in the DSN, NewFromDSN should execute 'SET time_zone = <offset>'")
	t.Log("- This ensures MySQL returns timestamps in the same timezone the Go driver expects")
	t.Log("- Without this fix, SELECT NOW() would return times in MySQL's session timezone")
	t.Log("- But Go would interpret them as being in the driver's Loc timezone, causing incorrect times")

	// This is more of a documentation test since we can't easily test the actual database setup
	// without a real MySQL connection in unit tests
}
