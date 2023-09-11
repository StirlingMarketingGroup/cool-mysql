package mysql

import "reflect"

// isScannerFunc checks if the given is a function that accepts
// a destination pointer, a source value, and returns an error
func isScannerFunc(rt reflect.Type) bool {
	if rt.Kind() != reflect.Func {
		return false
	}

	if rt.NumIn() != 2 {
		return false
	}

	if rt.In(0).Kind() != reflect.Ptr {
		return false
	}

	if rt.NumOut() != 1 {
		return false
	}

	if rt.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
		return false
	}

	return true
}

// fromScannerFuncs checks if the given value has a scanner func
// and returns the scanner func
func fromScannerFuncs(v reflect.Type, scannerFuncs map[reflect.Type]reflect.Value) (reflect.Value, bool) {
	if scannerFuncs == nil {
		return reflect.Value{}, false
	}

	vt := reflectUnwrapType(v)
	fn, ok := scannerFuncs[reflect.PointerTo(vt)]
	if !ok {
		return reflect.Value{}, false
	}

	return fn, true
}

func scannerFuncExists(v reflect.Type, scannerFuncs map[reflect.Type]reflect.Value) bool {
	_, ok := fromScannerFuncs(v, scannerFuncs)
	return ok
}
