package mysql

import (
	"reflect"
)

func isNil(a any) bool {
	defer func() { _ = recover() }()
	return a == nil || reflect.ValueOf(a).IsNil()
}
