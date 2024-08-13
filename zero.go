package mysql

import "reflect"

type Zeroer interface {
	IsZero() bool
}

func isZero(v any) bool {
	if v == nil {
		return true
	}

	rv := reflect.ValueOf(v)

	pv := rv
	if rv.Kind() != reflect.Ptr {
		pv = reflect.New(rv.Type())
		pv.Elem().Set(rv)
	}

	if v, ok := pv.Interface().(Zeroer); ok {
		if pv.IsNil() {
			if _, ok := pv.Type().Elem().MethodByName("IsZero"); ok {
				return true
			}
		}

		if v.IsZero() {
			return true
		}

		return false
	}

	if !rv.IsValid() || rv.IsZero() {
		return true
	}

	return false
}
