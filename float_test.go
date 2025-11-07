package mysql

import (
	"encoding/json"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

// TestFloatDoubleHandling tests that float32/float64 values are correctly
// handled by the MySQL driver v1.8+ which changed float/double loading behavior.
func TestFloatDoubleHandling(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	t.Run("select directly into float32", func(t *testing.T) {
		var result float32

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"value"}).
				AddRow(float32(3.14159)))

		err := db.Select(&result, "SELECT CAST(3.14159 AS FLOAT)", 0)
		require.NoError(t, err)
		require.InDelta(t, float32(3.14159), result, 0.00001)
	})

	t.Run("select directly into float64", func(t *testing.T) {
		var result float64

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"value"}).
				AddRow(float64(3.141592653589793)))

		err := db.Select(&result, "SELECT CAST(3.141592653589793 AS DOUBLE)", 0)
		require.NoError(t, err)
		require.InDelta(t, 3.141592653589793, result, 0.000000000000001)
	})

	t.Run("select directly into float32 pointer", func(t *testing.T) {
		var result *float32

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"value"}).
				AddRow(float32(2.718)))

		err := db.Select(&result, "SELECT CAST(2.718 AS FLOAT)", 0)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.InDelta(t, float32(2.718), *result, 0.00001)
	})

	t.Run("select directly into float64 pointer", func(t *testing.T) {
		var result *float64

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"value"}).
				AddRow(float64(2.718281828)))

		err := db.Select(&result, "SELECT CAST(2.718281828 AS DOUBLE)", 0)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.InDelta(t, 2.718281828, *result, 0.000000001)
	})

	t.Run("select NULL into float32 pointer", func(t *testing.T) {
		var result *float32

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"value"}).
				AddRow(nil))

		err := db.Select(&result, "SELECT NULL", 0)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("select NULL into float64 pointer", func(t *testing.T) {
		var result *float64

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"value"}).
				AddRow(nil))

		err := db.Select(&result, "SELECT NULL", 0)
		require.NoError(t, err)
		require.Nil(t, result)
	})
}

func TestFloatDoubleStructFields(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	type TestStruct struct {
		Float32Value  float32  `mysql:"float32_value"`
		Float64Value  float64  `mysql:"float64_value"`
		Float32Ptr    *float32 `mysql:"float32_ptr"`
		Float64Ptr    *float64 `mysql:"float64_ptr"`
		Float32PtrNil *float32 `mysql:"float32_ptr_nil"`
		Float64PtrNil *float64 `mysql:"float64_ptr_nil"`
	}

	t.Run("select into struct with float/double fields", func(t *testing.T) {
		var result TestStruct

		float32Val := float32(1.23)
		float64Val := float64(4.56789)

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{
				"float32_value", "float64_value", "float32_ptr", "float64_ptr", "float32_ptr_nil", "float64_ptr_nil",
			}).AddRow(
				float32(1.23),
				float64(4.56789),
				float32(9.87),
				float64(6.54321),
				nil,
				nil,
			))

		err := db.Select(&result, `
			SELECT
				CAST(1.23 AS FLOAT) AS float32_value,
				CAST(4.56789 AS DOUBLE) AS float64_value,
				CAST(9.87 AS FLOAT) AS float32_ptr,
				CAST(6.54321 AS DOUBLE) AS float64_ptr,
				NULL AS float32_ptr_nil,
				NULL AS float64_ptr_nil
		`, 0)
		require.NoError(t, err)

		require.InDelta(t, float32(1.23), result.Float32Value, 0.00001)
		require.InDelta(t, 4.56789, result.Float64Value, 0.000001)
		require.NotNil(t, result.Float32Ptr)
		require.InDelta(t, float32(9.87), *result.Float32Ptr, 0.00001)
		require.NotNil(t, result.Float64Ptr)
		require.InDelta(t, 6.54321, *result.Float64Ptr, 0.000001)
		require.Nil(t, result.Float32PtrNil)
		require.Nil(t, result.Float64PtrNil)

		// Verify pointers are not accidentally pointing to the same values we used
		require.NotSame(t, &float32Val, result.Float32Ptr)
		require.NotSame(t, &float64Val, result.Float64Ptr)
	})

	t.Run("select into slice of structs with float/double fields", func(t *testing.T) {
		var results []TestStruct

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{
				"float32_value", "float64_value", "float32_ptr", "float64_ptr", "float32_ptr_nil", "float64_ptr_nil",
			}).
				AddRow(float32(1.1), float64(2.2), float32(3.3), float64(4.4), nil, nil).
				AddRow(float32(5.5), float64(6.6), float32(7.7), float64(8.8), nil, nil))

		err := db.Select(&results, `
			SELECT
				CAST(1.1 AS FLOAT) AS float32_value,
				CAST(2.2 AS DOUBLE) AS float64_value,
				CAST(3.3 AS FLOAT) AS float32_ptr,
				CAST(4.4 AS DOUBLE) AS float64_ptr,
				NULL AS float32_ptr_nil,
				NULL AS float64_ptr_nil
			UNION ALL
			SELECT
				CAST(5.5 AS FLOAT),
				CAST(6.6 AS DOUBLE),
				CAST(7.7 AS FLOAT),
				CAST(8.8 AS DOUBLE),
				NULL,
				NULL
		`, 0)
		require.NoError(t, err)
		require.Len(t, results, 2)

		// First row
		require.InDelta(t, float32(1.1), results[0].Float32Value, 0.00001)
		require.InDelta(t, 2.2, results[0].Float64Value, 0.000001)
		require.NotNil(t, results[0].Float32Ptr)
		require.InDelta(t, float32(3.3), *results[0].Float32Ptr, 0.00001)
		require.NotNil(t, results[0].Float64Ptr)
		require.InDelta(t, 4.4, *results[0].Float64Ptr, 0.000001)

		// Second row
		require.InDelta(t, float32(5.5), results[1].Float32Value, 0.00001)
		require.InDelta(t, 6.6, results[1].Float64Value, 0.000001)
		require.NotNil(t, results[1].Float32Ptr)
		require.InDelta(t, float32(7.7), *results[1].Float32Ptr, 0.00001)
		require.NotNil(t, results[1].Float64Ptr)
		require.InDelta(t, 8.8, *results[1].Float64Ptr, 0.000001)
	})
}

func TestFloatDoubleSelectJSON(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	type TestStruct struct {
		Float32Value  float32  `json:"float32_value"`
		Float64Value  float64  `json:"float64_value"`
		Float32Ptr    *float32 `json:"float32_ptr"`
		Float64Ptr    *float64 `json:"float64_ptr"`
		Float32PtrNil *float32 `json:"float32_ptr_nil"`
		Float64PtrNil *float64 `json:"float64_ptr_nil"`
	}

	t.Run("SelectJSON with float/double fields", func(t *testing.T) {
		var result TestStruct

		jsonData := `{
			"float32_value": 1.23,
			"float64_value": 4.56789,
			"float32_ptr": 9.87,
			"float64_ptr": 6.54321,
			"float32_ptr_nil": null,
			"float64_ptr_nil": null
		}`

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"json_data"}).
				AddRow([]byte(jsonData)))

		err := db.SelectJSON(&result, "SELECT JSON_OBJECT(...) AS json_data", 0)
		require.NoError(t, err)

		require.InDelta(t, float32(1.23), result.Float32Value, 0.00001)
		require.InDelta(t, 4.56789, result.Float64Value, 0.000001)
		require.NotNil(t, result.Float32Ptr)
		require.InDelta(t, float32(9.87), *result.Float32Ptr, 0.00001)
		require.NotNil(t, result.Float64Ptr)
		require.InDelta(t, 6.54321, *result.Float64Ptr, 0.000001)
		require.Nil(t, result.Float32PtrNil)
		require.Nil(t, result.Float64PtrNil)
	})

	t.Run("SelectJSON with slice of structs containing float/double fields", func(t *testing.T) {
		var results []TestStruct

		jsonData := `[
			{
				"float32_value": 1.1,
				"float64_value": 2.2,
				"float32_ptr": 3.3,
				"float64_ptr": 4.4,
				"float32_ptr_nil": null,
				"float64_ptr_nil": null
			},
			{
				"float32_value": 5.5,
				"float64_value": 6.6,
				"float32_ptr": 7.7,
				"float64_ptr": 8.8,
				"float32_ptr_nil": null,
				"float64_ptr_nil": null
			}
		]`

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"json_data"}).
				AddRow([]byte(jsonData)))

		err := db.SelectJSON(&results, "SELECT JSON_ARRAYAGG(...) AS json_data", 0)
		require.NoError(t, err)
		require.Len(t, results, 2)

		// First element
		require.InDelta(t, float32(1.1), results[0].Float32Value, 0.00001)
		require.InDelta(t, 2.2, results[0].Float64Value, 0.000001)
		require.NotNil(t, results[0].Float32Ptr)
		require.InDelta(t, float32(3.3), *results[0].Float32Ptr, 0.00001)
		require.NotNil(t, results[0].Float64Ptr)
		require.InDelta(t, 4.4, *results[0].Float64Ptr, 0.000001)

		// Second element
		require.InDelta(t, float32(5.5), results[1].Float32Value, 0.00001)
		require.InDelta(t, 6.6, results[1].Float64Value, 0.000001)
		require.NotNil(t, results[1].Float32Ptr)
		require.InDelta(t, float32(7.7), *results[1].Float32Ptr, 0.00001)
		require.NotNil(t, results[1].Float64Ptr)
		require.InDelta(t, 8.8, *results[1].Float64Ptr, 0.000001)
	})

	t.Run("SelectJSON with map containing float/double values", func(t *testing.T) {
		var result map[string]interface{}

		jsonData := `{
			"float_value": 3.14159,
			"double_value": 2.718281828,
			"null_value": null
		}`

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"json_data"}).
				AddRow([]byte(jsonData)))

		err := db.SelectJSON(&result, "SELECT JSON_OBJECT(...) AS json_data", 0)
		require.NoError(t, err)

		require.InDelta(t, 3.14159, result["float_value"].(float64), 0.000001)
		require.InDelta(t, 2.718281828, result["double_value"].(float64), 0.000000001)
		require.Nil(t, result["null_value"])
	})

	t.Run("SelectJSON preserves float precision", func(t *testing.T) {
		type PrecisionTest struct {
			VerySmall float64 `json:"very_small"`
			VeryLarge float64 `json:"very_large"`
			Negative  float64 `json:"negative"`
		}

		var result PrecisionTest

		jsonData := `{
			"very_small": 0.000000123456789,
			"very_large": 123456789.987654321,
			"negative": -999.999999
		}`

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"json_data"}).
				AddRow([]byte(jsonData)))

		err := db.SelectJSON(&result, "SELECT JSON_OBJECT(...) AS json_data", 0)
		require.NoError(t, err)

		require.InDelta(t, 0.000000123456789, result.VerySmall, 0.000000000000001)
		require.InDelta(t, 123456789.987654321, result.VeryLarge, 0.000000001)
		require.InDelta(t, -999.999999, result.Negative, 0.000001)
	})
}

// TestFloatDoubleEdgeCases tests edge cases and special values
func TestFloatDoubleEdgeCases(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	t.Run("zero values", func(t *testing.T) {
		var float32Result float32
		var float64Result float64

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"value"}).
				AddRow(float32(0.0)))
		err := db.Select(&float32Result, "SELECT CAST(0.0 AS FLOAT)", 0)
		require.NoError(t, err)
		require.Equal(t, float32(0.0), float32Result)

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"value"}).
				AddRow(float64(0.0)))
		err = db.Select(&float64Result, "SELECT CAST(0.0 AS DOUBLE)", 0)
		require.NoError(t, err)
		require.Equal(t, float64(0.0), float64Result)
	})

	t.Run("negative values", func(t *testing.T) {
		var float32Result float32
		var float64Result float64

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"value"}).
				AddRow(float32(-123.456)))
		err := db.Select(&float32Result, "SELECT CAST(-123.456 AS FLOAT)", 0)
		require.NoError(t, err)
		require.InDelta(t, float32(-123.456), float32Result, 0.001)

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"value"}).
				AddRow(float64(-987.654321)))
		err = db.Select(&float64Result, "SELECT CAST(-987.654321 AS DOUBLE)", 0)
		require.NoError(t, err)
		require.InDelta(t, -987.654321, float64Result, 0.000001)
	})

	t.Run("very large values", func(t *testing.T) {
		var float32Result float32
		var float64Result float64

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"value"}).
				AddRow(float32(1e20)))
		err := db.Select(&float32Result, "SELECT CAST(1e20 AS FLOAT)", 0)
		require.NoError(t, err)
		require.InDelta(t, float32(1e20), float32Result, 1e15)

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"value"}).
				AddRow(float64(1e100)))
		err = db.Select(&float64Result, "SELECT CAST(1e100 AS DOUBLE)", 0)
		require.NoError(t, err)
		require.InDelta(t, 1e100, float64Result, 1e95)
	})

	t.Run("very small values", func(t *testing.T) {
		var float32Result float32
		var float64Result float64

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"value"}).
				AddRow(float32(1e-20)))
		err := db.Select(&float32Result, "SELECT CAST(1e-20 AS FLOAT)", 0)
		require.NoError(t, err)
		require.InDelta(t, float32(1e-20), float32Result, 1e-25)

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"value"}).
				AddRow(float64(1e-100)))
		err = db.Select(&float64Result, "SELECT CAST(1e-100 AS DOUBLE)", 0)
		require.NoError(t, err)
		require.InDelta(t, 1e-100, float64Result, 1e-105)
	})

	t.Run("JSON unmarshal with scientific notation", func(t *testing.T) {
		type ScientificNotation struct {
			Large float64 `json:"large"`
			Small float64 `json:"small"`
		}

		var result ScientificNotation

		// JSON numbers can use scientific notation
		jsonData := `{"large": 1.23e10, "small": 4.56e-8}`

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"json_data"}).
				AddRow([]byte(jsonData)))

		err := db.SelectJSON(&result, "SELECT JSON_OBJECT(...)", 0)
		require.NoError(t, err)
		require.InDelta(t, 1.23e10, result.Large, 1e5)
		require.InDelta(t, 4.56e-8, result.Small, 1e-13)
	})
}

// TestFloatDoubleJSONRoundTrip ensures that values can round-trip through JSON correctly
func TestFloatDoubleJSONRoundTrip(t *testing.T) {
	type TestData struct {
		Float32 float32 `json:"f32"`
		Float64 float64 `json:"f64"`
	}

	original := TestData{
		Float32: 3.14159,
		Float64: 2.718281828459045,
	}

	// Marshal to JSON
	jsonBytes, err := json.Marshal(original)
	require.NoError(t, err)

	// Unmarshal back
	var decoded TestData
	err = json.Unmarshal(jsonBytes, &decoded)
	require.NoError(t, err)

	// Verify round-trip preserves values (within float32 precision)
	require.InDelta(t, original.Float32, decoded.Float32, 0.00001)
	require.InDelta(t, original.Float64, decoded.Float64, 0.000000000000001)
}
