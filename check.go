package mysql

import (
	"errors"
	"reflect"
)

// ErrDestMustBeChanOfStruct dest must be a channel of structs
var ErrDestMustBeChanOfStruct = errors.New("dest must be a channel of structs")

func checkChanOfStruct(dest interface{}) (ch reflect.Value, strct reflect.Type, err error) {
	ch = reflect.ValueOf(dest)
	if ch.Kind() != reflect.Chan {
		err = ErrDestMustBeChanOfStruct
		return
	}
	strct = ch.Type().Elem()
	if strct.Kind() != reflect.Struct {
		ch.Close()

		err = ErrDestMustBeChanOfStruct
		return
	}

	return
}
