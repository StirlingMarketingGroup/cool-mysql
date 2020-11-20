package mysql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/pkg/errors"

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

type column struct {
	structIndex int
	jsonable    bool
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

		for i, c := range cols {
			for j := 0; j < fieldsLen; j++ {
				if fields[j] == nil {
					f := strct.Field(j)
					name, ok := f.Tag.Lookup("mysql")
					if !ok {
						name = f.Name
					}
					kind := f.Type.Kind()
					spew.Dump(name, kind)
					fields[j] = &field{
						name:     name,
						jsonable: kind == reflect.Array || (kind == reflect.Slice && f.Type.Elem().Kind() != reflect.Uint8) || kind == reflect.Map || kind == reflect.Struct,
					}
				}
				if fields[j].taken {
					continue
				}

				if fields[j].name == c {
					columns[i] = &column{
						structIndex: j,
						jsonable:    fields[j].jsonable,
					}
					fields[j].taken = true
				}
			}
		}

		var x interface{}

		ran := false
	Rows:
		for rows.Next() {
			ran = true

			s := reflect.New(strct).Elem()

			jsonables := make([][]byte, len(columns))

			for i, c := range columns {
				if c != nil {
					if !c.jsonable {
						pointers[i] = s.Field(c.structIndex).Addr().Interface()
					} else {
						pointers[i] = &jsonables[i]
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

			for i, c := range columns {
				if jsonables[i] == nil {
					continue
				}

				err := json.Unmarshal(jsonables[i], s.Field(c.structIndex).Addr().Interface())
				if err != nil {
					err = errors.Wrapf(err, "failed to marshal %q", jsonables[i])
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
