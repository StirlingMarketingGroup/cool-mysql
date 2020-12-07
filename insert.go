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
func (db *Database) Insert(insert string, columns []string, src interface{}) error {
	return db.InsertWithRowComplete(insert, columns, src, nil)
}

// InsertWithRowComplete inserts struct rows from the source as a channel, single struct, or slice of structs
// rowComplete func is given the start time of processing the row
// for use of things like progress bars, timing the duration it takes to insert the row(s)
func (db *Database) InsertWithRowComplete(insert string, columns []string, src interface{}, rowComplete func(start time.Time)) error {
	_, _, strct, err := checkSource(src)
	if err != nil {
		return err
	}

	strctNumField := strct.NumField()
	structFields := make(map[string]int, strctNumField)
	for i := 0; i < strctNumField; i++ {
		f := strct.Field(i)
		name, ok := f.Tag.Lookup("mysql")
		if !ok {
			name = f.Name
		}
		structFields[name] = i
	}

	structIndexes := make([]int, len(columns))

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

		structIndexes[i] = structFields[c]
	}
	insertBuf.WriteString(")values")
	baseLen := insertBuf.Len()

	curRows := 0
	ch := reflect.ValueOf(src)
	var r reflect.Value
	for ok := true; ok; {
		if r, ok = ch.Recv(); ok {
			var start time.Time
			if rowComplete != nil {
				start = time.Now()
			}

			preRowLen := insertBuf.Len()

			if curRows != 0 {
				insertBuf.WriteByte(',')
			}
			insertBuf.WriteByte('(')
			for i := 0; i < len(structIndexes); i++ {
				if i != 0 {
					insertBuf.WriteByte(',')
				}
				WriteEncoded(insertBuf, r.Field(structIndexes[i]).Interface(), true)
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
	}

	if insertBuf.Len() > baseLen {
		err := db.Exec(insertBuf.String())
		if err != nil {
			return err
		}
	}

	return nil
}
