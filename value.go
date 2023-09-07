package mysql

import (
	"database/sql/driver"
	"reflect"
)

type Valueser interface {
	MySQLValues() ([]driver.Value, error)
}

// isValuerFunc checks if the given is a function that accepts
// a single param and returns a driver.Value and error
func isValuerFunc(rt reflect.Type) bool {
	if rt.Kind() != reflect.Func {
		return false
	}

	if rt.NumIn() != 1 {
		return false
	}

	if rt.NumOut() != 2 {
		return false
	}

	if rt.Out(0) != reflect.TypeOf((*driver.Value)(nil)).Elem() {
		return false
	}

	if rt.Out(1) != reflect.TypeOf((*error)(nil)).Elem() {
		return false
	}

	return true
}
