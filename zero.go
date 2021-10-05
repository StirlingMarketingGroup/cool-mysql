package mysql

import (
	"reflect"
)

type zeroer interface {
	IsZero() bool
}

func isZero(v interface{}) bool {
	if isNil(v) {
		return true
	}

	if z, ok := v.(zeroer); ok {
		return z.IsZero()
	}

	return reflect.ValueOf(v).IsZero()
}
