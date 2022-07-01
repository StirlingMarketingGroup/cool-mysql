package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/fatih/structtag"
	"github.com/go-redis/redis/v8"
	"github.com/go-sql-driver/mysql"
	"github.com/vmihailenco/msgpack/v5"
)

type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

var ErrDestType = fmt.Errorf("cool-mysql: select destination must be a channel or a pointer to something")

func query(db *Database, conn Querier, ctx context.Context, dest any, query string, cacheDuration time.Duration, params ...Params) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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

	destRef := reflect.ValueOf(dest)
	destKind := reflect.Indirect(destRef).Kind()

	t, multiRow := getElementTypeFromDest(destRef)
	ptrElements := t.Kind() == reflect.Pointer
	if ptrElements {
		t = t.Elem()
	}

	sendElement := func(el reflect.Value) error {
		if !ptrElements {
			el = reflect.Indirect(el)
		}

		if multiRow {
			switch destKind {
			case reflect.Chan:
				cases := []reflect.SelectCase{
					{
						Dir:  reflect.SelectRecv,
						Chan: reflect.ValueOf(ctx.Done()),
					},
					{
						Dir:  reflect.SelectSend,
						Chan: destRef,
						Send: el,
					},
				}
				switch index, _, _ := reflect.Select(cases); index {
				case 0:
					cancel()
					return context.Canceled
				}
			case reflect.Slice:
				destRef.Elem().Set(reflect.Append(destRef.Elem(), el))
			}
		} else {
			destRef.Elem().Set(el)
		}
		return nil
	}

	var cacheKey string
	var cacheSlice reflect.Value

	if cacheDuration > 0 {
		cacheSlice = reflect.New(reflect.SliceOf(t)).Elem()

		key := new(strings.Builder)
		key.WriteString("cool-mysql:")
		key.WriteString(t.String())
		key.WriteByte(':')
		key.WriteString(replacedQuery)
		key.WriteByte(':')
		key.WriteString(strconv.FormatInt(int64(cacheDuration), 10))

		cacheKey = key.String()

		start := time.Now()
		b, err := db.redis.Get(ctx, cacheKey).Bytes()
		if errors.Is(err, redis.Nil) {
			// cache miss!
		} else if err != nil {
			err = fmt.Errorf("failed to get data from redis: %w", err)
			ok := false
			if db.HandleRedisError != nil {
				ok = db.HandleRedisError(err)
			}
			if !ok {
				return err
			}
		} else {
			db.callLog(replacedQuery, mergedParams, time.Since(start), true)

			err = msgpack.Unmarshal(b, cacheSlice.Addr().Interface())
			if err != nil {
				return fmt.Errorf("failed to unmarshal from cache: %w", err)
			}

			l := cacheSlice.Len()
			if !multiRow && l == 0 {
				return sql.ErrNoRows
			}

			for i := 0; i < l; i++ {
				err = sendElement(cacheSlice.Index(i).Addr())
				if err != nil {
					return err
				}
				if !multiRow {
					break
				}
			}

			return nil
		}
	}

	var rows *sql.Rows
	start := time.Now()

	var b = backoff.NewExponentialBackOff()
	b.MaxElapsedTime = MaxExecutionTime
	err = backoff.Retry(func() error {
		var err error
		rows, err = conn.QueryContext(ctx, replacedQuery)
		if err != nil {
			if checkRetryError(err) {
				return err
			} else if errors.Is(err, mysql.ErrInvalidConn) {
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
	db.callLog(replacedQuery, mergedParams, time.Since(start), false)
	defer func() {
		if rows != nil {
			rows.Close()
		}
	}()
	if err != nil {
		return err
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

		if len(cacheKey) != 0 {
			cacheSlice = reflect.Append(cacheSlice, el.Elem())
		}

		i++

		err = sendElement(el)
		if err != nil {
			return err
		}
		if !multiRow {
			break
		}
	}

	if !multiRow && i == 0 {
		return sql.ErrNoRows
	}

	if len(cacheKey) != 0 {
		b, err := msgpack.Marshal(cacheSlice.Interface())
		if err != nil {
			return fmt.Errorf("failed to marshal results for cache: %w", err)
		}

		err = db.redis.Set(ctx, cacheKey, b, cacheDuration).Err()
		if err != nil {
			err = fmt.Errorf("failed to set redis cache: %w", err)
			ok := false
			if db.HandleRedisError != nil {
				ok = db.HandleRedisError(err)
			}
			if !ok {
				return err
			}
		}
	}

	return nil
}

var scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
var timeType = reflect.TypeOf((*time.Time)(nil)).Elem()

func getElementTypeFromDest(destRef reflect.Value) (t reflect.Type, multiRow bool) {
	indirectDestRef := reflect.Indirect(destRef)
	indirectDestRefType := indirectDestRef.Type()

	if !reflect.New(indirectDestRefType).Type().Implements(scannerType) && indirectDestRefType != timeType {
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

	if !reflect.New(t).Type().Implements(scannerType) && t != timeType {
		switch k := t.Kind(); k {
		case reflect.Array, reflect.Chan, reflect.Map, reflect.Slice, reflect.Struct:
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
			return make([]any, len(columns)), make([]jsonField, 1), nil, isStruct, nil
		}
		return make([]any, len(columns)), nil, nil, isStruct, nil
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
	x := new(any)

	isStruct := !t.Implements(scannerType) && t.Kind() == reflect.Struct
	if !isStruct {
		if isMultiValueElement(t) {
			(*ptrs)[0] = &jsonFields[0].j
		}

		(*ptrs)[0] = ref.Addr().Interface()

		for i := 1; i < len(columns); i++ {
			(*ptrs)[i] = x
		}

		return
	}

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
