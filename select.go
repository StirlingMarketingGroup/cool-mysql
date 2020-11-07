package mysql

import (
	"database/sql"
	"errors"
	"reflect"
	"time"

	"github.com/davecgh/go-spew/spew"
)

// ErrDestInvalidType is an error about what types are allowed
var ErrDestInvalidType = errors.New("dest must be a channel of structs, ptr to a slice of structs, or a pointer to a single struct")

func checkDest(dest interface{}) (reflect.Value, reflect.Kind, reflect.Type, error) {
	ref := reflect.ValueOf(dest)
	kind := ref.Kind()

	if kind == reflect.Ptr {
		elem := ref.Elem()
		kind = elem.Kind()

		switch kind {
		case reflect.Struct:
			return ref, kind, elem.Type(), nil
		case reflect.Slice:
			// if dest is a pointer to a slice of structs
			strct := elem.Type().Elem()
			if strct.Kind() == reflect.Struct {
				return ref, kind, strct, nil
			}
		}

		goto Err
	}

	// if dest is a pointer to a slice of structs
	if kind == reflect.Chan {
		strct := ref.Type().Elem()
		if strct.Kind() == reflect.Struct {
			return ref, kind, strct, nil
		}
	}

Err:
	spew.Dump(kind)
	return reflect.Value{}, 0, nil, ErrDestInvalidType
}

// Select selects one or more rows into the
// chan of structs in the destination
func (db *Database) Select(dest interface{}, query string, cache time.Duration, params ...Params) error {
	query = ReplaceParams(query, params...)

	refDest, kind, strct, err := checkDest(dest)
	if err != nil {
		return err
	}

	rows, err := db.Reads.Query(query)
	if err != nil {
		if kind == reflect.Chan {
			refDest.Close()
		}
		return err
	}

	fn := func() error {
		if kind == reflect.Chan {
			defer refDest.Close()
		}
		defer rows.Close()

		cols, _ := rows.Columns()
		pointers := make([]interface{}, len(cols))

		fieldsPositions := make([]int, len(cols))
	colsLoop:
		for i, c := range cols {
			for j := 0; j < strct.NumField(); j++ {
				f := strct.Field(j)
				if f.Name == c || f.Tag.Get("mysql") == c {
					fieldsPositions[i] = j
					continue colsLoop
				}
				fieldsPositions[i] = -1
			}
		}

		var x interface{}

		ran := false
	Rows:
		for rows.Next() {
			ran = true

			s := reflect.New(strct).Elem()

			for i, j := range fieldsPositions {
				if j != -1 {
					switch s.FieldByIndex(i).Kind() {
					case reflect.Array, reflect.Slice, reflect.Map:

					}

					pointers[i] = s.Field(j).Addr().Interface()
				} else {
					pointers[i] = &x
				}
			}
			err = rows.Scan(pointers...)
			if err != nil {
				panic(err)
			}
			switch kind {
			case reflect.Chan:
				refDest.Send(s)
			case reflect.Slice:
				refDest.Set(reflect.Append(refDest, s))
			case reflect.Struct:
				refDest.Set(s)
				break Rows
			}

			x = nil
		}

		if !ran && kind == reflect.Struct {
			return sql.ErrNoRows
		}

		return nil
	}

	switch kind {
	case reflect.Chan:
		go fn()
	case reflect.Slice, reflect.Struct:
		refDest = refDest.Elem()
		return fn()
	}

	return nil
}
