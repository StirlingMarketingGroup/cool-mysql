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

	"github.com/fatih/structtag"
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

	start := time.Now()
	rows, err := conn.QueryContext(ctx, replacedQuery)
	db.callLog(replacedQuery, mergedParams, time.Since(start))
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

	ptrs, jsonFields, fieldsMap, isStruct, err := setupElementPts(db, t, columns)
	if err != nil {
		return err
	}

	i := 0
	for rows.Next() {
		el := reflect.New(t)
		updateElementPts(el.Elem(), &ptrs, jsonFields, columns, fieldsMap)

		err = rows.Scan(ptrs...)
		if err != nil {
			return err
		}

		for _, jsonField := range jsonFields {
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
var bytesType = reflect.TypeOf((*[]byte)(nil)).Elem()

func getElementTypeFromDest(destRef reflect.Value) (t reflect.Type, multiRow bool) {
	if !destRef.Type().Implements(scannerType) {
		switch k := reflect.Indirect(destRef).Kind(); k {
		case reflect.Array, reflect.Chan, reflect.Map, reflect.Slice:
			if destRef.Type() != bytesType {
				return reflect.Indirect(destRef).Type().Elem(), true
			}
		}
	}

	return destRef.Type().Elem(), false
}

func isMultiValueElement(t reflect.Type) bool {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	if !t.Implements(scannerType) {
		switch t.Kind() {
		case reflect.Array, reflect.Chan, reflect.Slice, reflect.Struct:
			if t != bytesType {
				return true
			}
		}
	}

	return false
}

type jsonField struct {
	index []int
	j     json.RawMessage
}

func setupElementPts(db *Database, t reflect.Type, columns []string) (ptrs []any, jsonFields []jsonField, fieldsMap map[string][]int, isStruct bool, err error) {
	isStruct = !t.Implements(scannerType) && t.Kind() == reflect.Struct
	if !isStruct {
		if isMultiValueElement(t) {
			return make([]any, 1), []jsonField{{j: make(json.RawMessage, 0)}}, nil, isStruct, nil
		}
		return make([]any, 1), nil, nil, isStruct, nil
	}

	numField := t.NumField()

	fieldsMap = make(map[string][]int, numField)
	for i := 0; i < numField; i++ {
		f := t.Field(i)

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

		fieldsMap[strings.ToLower(name)] = f.Index
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
				index: f.Index,
				j:     make(json.RawMessage, 0),
			})
		}
	}

	return make([]any, len(columns)), jsonFields, fieldsMap, isStruct, nil
}

func updateElementPts(ref reflect.Value, ptrs *[]any, jsonFields []jsonField, columns []string, fieldsMap map[string][]int) {
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
