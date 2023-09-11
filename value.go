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

// fromValuerFuncs checks if the given value has a valuer func
// and returns the value and the valuer func
func fromValuerFuncs(v reflect.Value, valuerFuncs map[reflect.Type]reflect.Value) (reflect.Value, reflect.Value, bool) {
	if valuerFuncs == nil {
		return reflect.Value{}, reflect.Value{}, false
	}

	vt := v.Type()
	fn, ok := valuerFuncs[vt]
	if !ok {
		fn, ok = valuerFuncs[reflectUnwrapType(vt)]
		v = reflectUnwrap(v)
	}
	if !ok {
		return reflect.Value{}, reflect.Value{}, false
	}

	return v, fn, true
}
