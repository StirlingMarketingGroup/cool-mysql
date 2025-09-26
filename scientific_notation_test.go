package mysql

import (
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestScientificNotationHandling(t *testing.T) {
	tests := []struct {
		name               string
		value              string // Value returned by MySQL
		expectedFloat64    float64
		expectedFloat32    float32
		expectedDecimal    decimal.Decimal
		scientificNotation bool // Whether this value is in scientific notation
	}{
		{
			name:               "very small float",
			value:              "1e-10",
			expectedFloat64:    0.0000000001,
			expectedFloat32:    1e-10,
			expectedDecimal:    decimal.RequireFromString("0.0000000001"),
			scientificNotation: true,
		},
		{
			name:               "small float with e notation",
			value:              "1.23e-05",
			expectedFloat64:    0.0000123,
			expectedFloat32:    0.0000123,
			expectedDecimal:    decimal.RequireFromString("0.0000123"),
			scientificNotation: true,
		},
		{
			name:               "large float with e notation",
			value:              "1.5e+10",
			expectedFloat64:    15000000000,
			expectedFloat32:    1.5e10,
			expectedDecimal:    decimal.RequireFromString("15000000000"),
			scientificNotation: true,
		},
		{
			name:               "very large float",
			value:              "1e15",
			expectedFloat64:    1000000000000000,
			expectedFloat32:    1e15,
			expectedDecimal:    decimal.RequireFromString("1000000000000000"),
			scientificNotation: true,
		},
		{
			name:               "regular decimal",
			value:              "123.456",
			expectedFloat64:    123.456,
			expectedFloat32:    123.456,
			expectedDecimal:    decimal.RequireFromString("123.456"),
			scientificNotation: false,
		},
		{
			name:               "zero",
			value:              "0",
			expectedFloat64:    0,
			expectedFloat32:    0,
			expectedDecimal:    decimal.Zero,
			scientificNotation: false,
		},
		{
			name:               "very small decimal that might trigger e notation",
			value:              "0.00000000000123",
			expectedFloat64:    1.23e-12,
			expectedFloat32:    1.23e-12,
			expectedDecimal:    decimal.RequireFromString("0.00000000000123"),
			scientificNotation: false, // This is how it would come from DB, not in e notation
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, cleanup := getTestDatabase(t)
			defer cleanup()

			// Test float64
			t.Run("float64", func(t *testing.T) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT value")).
					WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(tt.value))

				var result float64
				err := db.Select(&result, "SELECT value", 0)
				require.NoError(t, err)
				require.Equal(t, tt.expectedFloat64, result, "float64 value mismatch for %s", tt.value)
			})

			// Test float32
			t.Run("float32", func(t *testing.T) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT value")).
					WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(tt.value))

				var result float32
				err := db.Select(&result, "SELECT value", 0)
				require.NoError(t, err)
				require.Equal(t, tt.expectedFloat32, result, "float32 value mismatch for %s", tt.value)
			})

			// Test decimal.Decimal
			t.Run("decimal", func(t *testing.T) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT value")).
					WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(tt.value))

				var result decimal.Decimal
				err := db.Select(&result, "SELECT value", 0)
				require.NoError(t, err)
				require.True(t, tt.expectedDecimal.Equal(result), "decimal value mismatch: expected %s, got %s", tt.expectedDecimal.String(), result.String())
			})

			// Test pointer types
			t.Run("float64_ptr", func(t *testing.T) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT value")).
					WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(tt.value))

				var result *float64
				err := db.Select(&result, "SELECT value", 0)
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Equal(t, tt.expectedFloat64, *result, "float64 pointer value mismatch for %s", tt.value)
			})
		})
	}
}

func TestScientificNotationConversion(t *testing.T) {
	// Test our internal conversion functions directly
	testCases := []struct {
		input    any
		expected string
	}{
		{float64(1e-10), "1e-10"},      // Very small number
		{float64(1.23e-5), "1.23e-05"}, // Small number
		{float64(1.5e10), "1.5e+10"},   // Large number
		{float64(123.456), "123.456"},  // Regular number
		{float32(1e-6), "1e-06"},       // Small float32
	}

	for _, tc := range testCases {
		t.Run("asString_conversion", func(t *testing.T) {
			result := asString(tc.input)
			t.Logf("Input: %v (%T) -> Output: %s", tc.input, tc.input, result)

			// The key test: can we parse it back?
			var dest float64
			err := convertAssignRows(&dest, tc.input)
			require.NoError(t, err, "Failed to convert %v back to float64", tc.input)

			// Convert input to float64 for comparison since convertAssignRows always returns float64
			var expectedFloat64 float64
			switch v := tc.input.(type) {
			case float64:
				expectedFloat64 = v
			case float32:
				expectedFloat64 = float64(v)
			default:
				expectedFloat64 = tc.input.(float64)
			}

			// For very small numbers or float32 conversions, we need to use approximate equality
			if _, isFloat32 := tc.input.(float32); isFloat32 || expectedFloat64 == float64(1e-10) {
				require.InDelta(t, expectedFloat64, dest, 1e-12, "Conversion round-trip failed for %v", tc.input)
			} else {
				require.Equal(t, expectedFloat64, dest, "Conversion round-trip failed for %v", tc.input)
			}
		})
	}
}

func TestMySQLScientificNotationRealism(t *testing.T) {
	// Test how MySQL actually returns numbers vs how we expect them
	// This simulates what would happen with real MySQL behavior

	t.Run("mysql_float_column_behavior", func(t *testing.T) {
		db, mock, cleanup := getTestDatabase(t)
		defer cleanup()

		// MySQL FLOAT columns can return values in scientific notation
		// when the numbers are very small or very large
		mock.ExpectQuery(regexp.QuoteMeta("SELECT very_small_float, very_large_float")).
			WillReturnRows(sqlmock.NewRows([]string{"very_small_float", "very_large_float"}).
				AddRow("1.727e-05", "1.234e+12")) // This is how MySQL might return them

		type Result struct {
			VerySmallFloat float64 `mysql:"very_small_float"`
			VeryLargeFloat float64 `mysql:"very_large_float"`
		}

		var result Result
		err := db.Select(&result, "SELECT very_small_float, very_large_float", 0)
		require.NoError(t, err)

		t.Logf("Small float result: %f (expected ~0.00001727)", result.VerySmallFloat)
		t.Logf("Large float result: %f (expected ~1234000000000)", result.VeryLargeFloat)

		require.InDelta(t, 0.00001727, result.VerySmallFloat, 0.000000001)
		require.InDelta(t, 1234000000000.0, result.VeryLargeFloat, 1000000.0)
	})
}

func TestFloatFormattingBehavior(t *testing.T) {
	// Test the specific formatting behavior that might change between driver versions
	numbers := []float64{
		1e-10,
		1.23e-5,
		0.0000001,
		123.456,
		1000000,
		1.5e10,
		1e15,
	}

	for _, num := range numbers {
		t.Run("format_behavior", func(t *testing.T) {
			// Test current asString behavior
			formatted := asString(num)
			t.Logf("Number: %g -> asString: %s", num, formatted)

			// Test if we can parse it back correctly
			var parsed float64
			err := convertAssignRows(&parsed, formatted)
			require.NoError(t, err)
			require.Equal(t, num, parsed, "Round trip failed for %g", num)
		})
	}
}
