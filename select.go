package mysql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/pkg/errors"
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
	return reflect.Value{}, 0, nil, ErrDestInvalidType
}

type column struct {
	structIndex   uint16
	jsonableIndex uint16
	jsonable      bool
}

type field struct {
	name     string
	jsonable bool
	taken    bool
}

// Select selects one or more rows into the
// chan of structs in the destination
func (db *Database) Select(dest interface{}, query string, cache time.Duration, params ...Params) error {
	originalQuery := query
	query, mergedParams := ReplaceParams(query, params...)
	if db.die {
		fmt.Println(query)
		os.Exit(0)
	}

	refDest, kind, strct, err := checkDest(dest)
	if err != nil {
		return err
	}

	db.logQuery(query)
	rows, err := db.Reads.Query(query)
	if err != nil {
		if kind == reflect.Chan {
			refDest.Close()
		}
		return Error{
			Err:           err,
			OriginalQuery: originalQuery,
			ReplacedQuery: query,
			Params:        mergedParams,
		}
	}

	fn := func() error {
		if kind == reflect.Chan {
			defer refDest.Close()
		}
		defer rows.Close()

		cols, _ := rows.Columns()
		pointers := make([]interface{}, len(cols))

		columns := make([]*column, len(cols))

		fieldsLen := strct.NumField()
		fields := make([]*field, fieldsLen)

		strctEx := reflect.New(strct).Elem()

		var jsonablesCount uint16
		for i, c := range cols {
			for j := 0; j < fieldsLen; j++ {
				if fields[j] == nil {
					f := strct.Field(j)
					name, ok := f.Tag.Lookup("mysql")
					if !ok {
						name = f.Name
					}
					kind := f.Type.Kind()

					var jsonable bool

					switch kind {
					case reflect.Map, reflect.Struct:
						jsonable = true
					case reflect.Array, reflect.Slice:
						// if it's a slice, but not a byte slice
						if f.Type.Elem().Kind() != reflect.Uint8 {
							jsonable = true
						}
					}

					if jsonable {
						prop := strctEx.Field(j)

						// don't let things that already handle themselves get json unmarshalled
						if _, ok := prop.Addr().Interface().(sql.Scanner); ok {
							jsonable = false
						}

						// we also have to ignore times specifically, because sql scanning
						// implements them literally, instead of the time.Time implementing sql.Scanner
						if _, ok := prop.Interface().(time.Time); ok {
							jsonable = false
						}
					}

					fields[j] = &field{
						name:     name,
						jsonable: jsonable,
					}
				}
				if fields[j].taken {
					continue
				}

				if fields[j].name == c {
					columns[i] = &column{
						structIndex:   uint16(j),
						jsonable:      fields[j].jsonable,
						jsonableIndex: jsonablesCount,
					}
					fields[j].taken = true

					if fields[j].jsonable {
						jsonablesCount++
					}
				}
			}
		}

		var x interface{}

		ran := false

		s := reflect.New(strct).Elem()
		var jsonables [][]byte
		if jsonablesCount > 0 {
			jsonables = make([][]byte, jsonablesCount)
		}
	Rows:
		for rows.Next() {
			ran = true

			for i, c := range columns {
				if c != nil {
					if !c.jsonable {
						pointers[i] = s.Field(int(c.structIndex)).Addr().Interface()
					} else {
						pointers[i] = &jsonables[c.jsonableIndex]
					}
				} else {
					pointers[i] = &x
				}
			}
			err = rows.Scan(pointers...)
			if err != nil {
				err = errors.Wrapf(err, "failed to scan rows")
				if kind == reflect.Chan {
					panic(err)
				} else {
					return err
				}
			}

			for _, c := range columns {
				if c == nil || !c.jsonable || jsonables[c.jsonableIndex] == nil {
					continue
				}

				err := json.Unmarshal(jsonables[c.jsonableIndex], s.Field(int(c.structIndex)).Addr().Interface())
				if err != nil {
					err = errors.Wrapf(err, "failed to marshal %q", jsonables[c.jsonableIndex])
					if kind == reflect.Chan {
						panic(err)
					} else {
						return err
					}
				}
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
