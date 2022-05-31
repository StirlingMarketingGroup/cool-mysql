package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/fatih/structtag"
	"github.com/go-sql-driver/mysql"
)

type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

var ErrDestType = fmt.Errorf("cool-mysql: select destination must be a channel or a pointer to something")

func query(db *Database, conn Querier, ctx context.Context, dest any, query string, cacheDuration time.Duration, params ...Params) (err error) {
	replacedQuery, mergedParams := ReplaceParams(query, params...)
	if db.die {
		fmt.Println(replacedQuery)
		os.Exit(0)
	}

	defer func() {
		if err != nil {
			err = Error{
				Err:           err,
				OriginalQuery: query,
				ReplacedQuery: replacedQuery,
				Params:        mergedParams,
			}
		}
	}()

	var rows *sql.Rows
	start := time.Now()
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = BackoffDefaultMaxElapsedTime
	err = backoff.Retry(func() error {
		var err error
		rows, err = conn.QueryContext(ctx, replacedQuery)
		if err != nil {
			if checkRetryError(err) {
				return err
			} else if err == mysql.ErrInvalidConn {
				if err := db.Test(); err != nil {
					return err
				}
				return err
			} else {
				return backoff.Permanent(err)
			}
		}

		return nil
	}, backoff.WithContext(b, ctx))
	db.callLog(replacedQuery, mergedParams, time.Since(start))
	defer rows.Close()
	if err != nil {
		return err
	}

	destRef := reflect.ValueOf(dest)
	destKind := reflect.Indirect(destRef).Kind()

	t, multiRow := getElementTypeFromDest(destRef)
	ptrElements := t.Kind() == reflect.Pointer
	if ptrElements {
		t = t.Elem()
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	for i := range columns {
		columns[i] = strings.ToLower(columns[i])
	}

	ptrs, jsonFields, fieldsMap, isStruct, err := setupElementPtrs(db, t, columns)
	if err != nil {
		return err
	}

	i := 0
	for rows.Next() {
		el := reflect.New(t)
		updateElementPtrs(el.Elem(), &ptrs, jsonFields, columns, fieldsMap)

		err = rows.Scan(ptrs...)
		if err != nil {
			return err
		}

		for _, jsonField := range jsonFields {
			if len(jsonField.j) == 0 {
				continue
			}

			if !isStruct {
				err = json.Unmarshal(jsonField.j, el.Interface())
				if err != nil {
					return fmt.Errorf("failed to unmarshal json into dest: %w", err)
				}
			} else {
				f := el.Elem().FieldByIndex(jsonField.index)
				err = json.Unmarshal(jsonField.j, f.Addr().Interface())
				if err != nil {
					return fmt.Errorf("failed to unmarshal json into struct field %q: %w", f.Type().Name(), err)
				}
			}
		}

		if !ptrElements {
			el = reflect.Indirect(el)
		}

		i++

		if multiRow {
			switch destKind {
			case reflect.Chan:
				destRef.Send(el)
			case reflect.Slice:
				destRef.Elem().Set(reflect.Append(destRef.Elem(), el))
			}
		} else {
			destRef.Elem().Set(el)
			break
		}
	}

	if !multiRow && i == 0 {
		return sql.ErrNoRows
	}

	return nil
}

var scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
var timeType = reflect.TypeOf((*time.Time)(nil)).Elem()

func getElementTypeFromDest(destRef reflect.Value) (t reflect.Type, multiRow bool) {
	indirectDestRef := reflect.Indirect(destRef)
	indirectDestRefType := indirectDestRef.Type()

	if !destRef.Type().Implements(scannerType) && indirectDestRefType != timeType {
		switch k := indirectDestRef.Kind(); k {
		case reflect.Array, reflect.Chan, reflect.Map, reflect.Slice:
			if !((k == reflect.Array || k == reflect.Slice) && indirectDestRefType.Elem().Kind() == reflect.Uint8) {
				return indirectDestRefType.Elem(), true
			}
		}
	}

	return destRef.Type().Elem(), false
}

func isMultiValueElement(t reflect.Type) bool {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	if !t.Implements(scannerType) && t != timeType {
		switch k := t.Kind(); k {
		case reflect.Array, reflect.Chan, reflect.Slice, reflect.Struct:
			if !((k == reflect.Array || k == reflect.Slice) && t.Elem().Kind() == reflect.Uint8) {
				return true
			}
		}
	}

	return false
}

type jsonField struct {
	index []int
	j     []byte
}

func setupElementPtrs(db *Database, t reflect.Type, columns []string) (ptrs []any, jsonFields []jsonField, fieldsMap map[string][]int, isStruct bool, err error) {
	isStruct = !t.Implements(scannerType) && t.Kind() == reflect.Struct
	if !isStruct {
		if isMultiValueElement(t) {
			return make([]any, 1), make([]jsonField, 1), nil, isStruct, nil
		}
		return make([]any, 1), nil, nil, isStruct, nil
	}

	structFieldIndexes := StructFieldIndexes(t)

	fieldsMap = make(map[string][]int, len(structFieldIndexes))
	for _, i := range structFieldIndexes {
		f := t.FieldByIndex(i)

		if !f.IsExported() {
			continue
		}

		tags, err := structtag.Parse(string(f.Tag))
		if err != nil {
			return nil, nil, nil, isStruct, fmt.Errorf("failed to parse struct tag %q: %w", f.Tag, err)
		}

		name := f.Name
		mysqlTag, _ := tags.Get("mysql")
		if mysqlTag != nil && len(mysqlTag.Name) != 0 && mysqlTag.Name != "-" {
			name = mysqlTag.Name
		}

		fieldsMap[strings.ToLower(name)] = i
	}

	for _, c := range columns {
		fieldIndex, ok := fieldsMap[c]
		if !ok {
			db.Logger.Warn(fmt.Sprintf("column %q from query doesn't belong to any struct fields", c))
			continue
		}

		f := t.FieldByIndex(fieldIndex)
		if isMultiValueElement(f.Type) {
			jsonFields = append(jsonFields, jsonField{
				index: fieldIndex,
			})
		}
	}

	return make([]any, len(columns)), jsonFields, fieldsMap, isStruct, nil
}

func updateElementPtrs(ref reflect.Value, ptrs *[]any, jsonFields []jsonField, columns []string, fieldsMap map[string][]int) {
	t := ref.Type()

	isStruct := !t.Implements(scannerType) && t.Kind() == reflect.Struct
	if !isStruct {
		if isMultiValueElement(t) {
			(*ptrs)[0] = &jsonFields[0].j
		}

		(*ptrs)[0] = ref.Addr().Interface()
		return
	}

	x := new(any)
	jsonIndex := 0
	for i, c := range columns {
		fieldIndex, ok := fieldsMap[c]
		if !ok {
			(*ptrs)[i] = x
			continue
		}

		f := t.FieldByIndex(fieldIndex)
		if isMultiValueElement(f.Type) {
			jsonFields[jsonIndex].j = jsonFields[jsonIndex].j[:0]
			(*ptrs)[i] = &jsonFields[jsonIndex].j
			jsonIndex++
		} else {
			(*ptrs)[i] = ref.FieldByIndex(fieldIndex).Addr().Interface()
		}
	}
}
