package mysql

import (
	"reflect"
	"time"

	"github.com/shopspring/decimal"
)

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
		return reflectUnwrap(v.Elem())
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

var timeType = reflect.TypeOf((*time.Time)(nil)).Elem()
var decimalType = reflect.TypeOf((*decimal.Decimal)(nil)).Elem()

func isMultiColumn(t reflect.Type) bool {
	switch t {
	case timeType, decimalType:
		return false
	}

	switch t.Kind() {
	case reflect.Pointer:
		return isMultiColumn(t.Elem())
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
