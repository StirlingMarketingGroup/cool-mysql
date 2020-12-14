package mysql

import (
	"bytes"
	"reflect"
	"time"

	"github.com/pkg/errors"
)

// ErrSourceInvalidType is an error about what types are allowed
var ErrSourceInvalidType = errors.New("source must be a channel of structs, a slice of structs, or a single struct")

func checkSource(source interface{}) (reflect.Value, reflect.Kind, reflect.Type, error) {
	switch source.(type) {
	case Params:
		return reflect.Value{}, 0, nil, nil
	}

	ref := reflect.ValueOf(source)
	kind := ref.Kind()

	switch kind {
	case reflect.Struct:
		return ref, kind, ref.Type(), nil
	case reflect.Slice:
		strct := ref.Type().Elem()
		if strct.Kind() == reflect.Struct {
			return ref, kind, strct, nil
		}
	case reflect.Chan:
		strct := ref.Type().Elem()
		if strct.Kind() == reflect.Struct {
			return ref, kind, strct, nil
		}
	}

	return reflect.Value{}, 0, nil, ErrSourceInvalidType
}

// Insert inserts struct rows from the source as a channel, single struct, or slice of structs
func (db *Database) Insert(insert string, src interface{}) error {
	return db.InsertWithRowComplete(insert, src, nil)
}

// InsertWithRowComplete inserts struct rows from the source as a channel, single struct, or slice of structs
// rowComplete func is given the start time of processing the row
// for use of things like progress bars, timing the duration it takes to insert the row(s)
func (db *Database) InsertWithRowComplete(insert string, source interface{}, rowComplete func(start time.Time)) error {
	reflectValue, reflectKind, reflectStruct, err := checkSource(source)
	if err != nil {
		return err
	}

	var columns []string

	switch src := source.(type) {
	case Params:
		columns = make([]string, len(src))
		i := 0
		for c := range src {
			columns[i] = c
			i++
		}
	default:
		switch reflectKind {
		case reflect.Chan:
			columns = make([]string, reflectStruct.NumField())
			for i := 0; i < len(columns); i++ {
				f := reflectStruct.Field(i)
				if t, ok := f.Tag.Lookup("mysql"); ok {
					columns[i] = t
				} else {
					columns[i] = f.Name
				}
			}
		default:
			panic("not ready yet!")
		}
	}

	insertBuf := new(bytes.Buffer)
	insertBuf.WriteString(insert)
	insertBuf.WriteByte('(')
	for i, c := range columns {
		if i != 0 {
			insertBuf.WriteByte(',')
		}
		insertBuf.WriteByte('`')
		insertBuf.WriteString(c)
		insertBuf.WriteByte('`')
	}
	insertBuf.WriteString(")values")
	baseLen := insertBuf.Len()

	curRows := 0
	var r reflect.Value
	for ok := true; ok; {
		switch source.(type) {
		case Params:
			if curRows > 0 {
				ok = false
			}
		default:
			switch reflectKind {
			case reflect.Chan:
				r, ok = reflectValue.Recv()
			default:
				panic("not ready yet!")
			}
		}
		if !ok {
			break
		}

		var start time.Time
		if rowComplete != nil {
			start = time.Now()
		}

		preRowLen := insertBuf.Len()

		if curRows != 0 {
			insertBuf.WriteByte(',')
		}
		insertBuf.WriteByte('(')
		for i := 0; i < len(columns); i++ {
			if i != 0 {
				insertBuf.WriteByte(',')
			}
			switch src := source.(type) {
			case Params:
				WriteEncoded(insertBuf, src[columns[i]], true)
			default:
				switch reflectKind {
				case reflect.Chan:
					WriteEncoded(insertBuf, r.Field(i).Interface(), true)
				default:
					panic("not ready yet!")
				}
			}
		}
		insertBuf.WriteByte(')')

		if insertBuf.Len() > int(float64(db.maxInsertSize)*0.80) && curRows > 1 {
			buf := insertBuf.Bytes()[preRowLen+1:]
			insertBuf.Truncate(preRowLen)
			err := db.Exec(insertBuf.String())
			if err != nil {
				return err
			}

			insertBuf.Truncate(baseLen)
			curRows = 0

			insertBuf.Write(buf)
		}

		curRows++

		if rowComplete != nil {
			rowComplete(start)
		}
	}

	if insertBuf.Len() > baseLen {
		err := db.Exec(insertBuf.String())
		if err != nil {
			return err
		}
	}

	return nil
}
