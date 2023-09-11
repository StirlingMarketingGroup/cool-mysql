package mysql

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"time"
)

var scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
var valuerType = reflect.TypeOf((*driver.Valuer)(nil)).Elem()

var paramsType = reflect.TypeOf((*Params)(nil)).Elem()
var sliceRowType = reflect.TypeOf((*SliceRow)(nil)).Elem()
var mapRowType = reflect.TypeOf((*MapRow)(nil)).Elem()

var timeType = reflect.TypeOf((*time.Time)(nil)).Elem()

// StructFieldIndexes recursively gets all the struct field index,
// including the indexes from embedded structs
func StructFieldIndexes(t reflect.Type) [][]int {
	return structFieldIndexes(t, nil)
}

func structFieldIndexes(t reflect.Type, indexPrefix []int) [][]int {
	indexes := make([][]int, 0)

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)

		newIndex := append(indexPrefix, i)

		indexes = append(indexes, newIndex)
		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			indexes = append(indexes, structFieldIndexes(f.Type, newIndex)...)
		}
	}

	return indexes
}

func reflectUnwrap(v reflect.Value) reflect.Value {
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		// stop "early" if the pointer/interface is nil
		// since a nil pointer/interface of a type is more useful
		// than an untyped nil value
		v2 := v.Elem()
		if !v2.IsValid() {
			return v
		}
		return reflectUnwrap(v2)
	default:
		return v
	}
}

func reflectUnwrapType(t reflect.Type) reflect.Type {
	switch t.Kind() {
	case reflect.Pointer:
		return reflectUnwrapType(t.Elem())
	default:
		return t
	}
}

// isMultiColumn returns true if the value should be interpreted as multiple rows of values.
// Expects an unwrapped reflect type.
func isMultiRow(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Chan:
		return true
	case reflect.Slice, reflect.Array:
		switch t.Elem().Kind() {
		case reflect.Uint8, reflect.Interface:
			return false
		default:
			return true
		}
	default:
		return false
	}
}

// isMultiColumn returns true if the value should be interpreted as multiple values.
// Expects an unwrapped reflect type.
func isMultiColumn(t reflect.Type, valuerFuncs map[reflect.Type]reflect.Value) bool {
	if t == timeType ||
		reflect.PointerTo(t).Implements(valuerType) {
		return false
	}
	if _, _, ok := fromValuerFuncs(reflect.New(t), valuerFuncs); ok {
		return false
	}

	switch t.Kind() {
	case reflect.Map, reflect.Struct:
		return true
	case reflect.Slice, reflect.Array:
		return t.Elem().Kind() != reflect.Uint8
	default:
		return false
	}
}

func typeHasColNames(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Pointer:
		return typeHasColNames(t.Elem())
	case reflect.Map:
		return t.Key().Kind() == reflect.String
	case reflect.Struct:
		return true
	default:
		return false
	}
}
