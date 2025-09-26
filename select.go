package mysql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/civil"
	"github.com/cenkalti/backoff/v5"
	"github.com/fatih/structtag"
	"github.com/go-sql-driver/mysql"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/crypto/sha3"
)

var (
	ErrDestType        = fmt.Errorf("cool-mysql: select destination must be a channel or a pointer to something")
	ErrUnexportedField = fmt.Errorf("cool-mysql: struct has unexported fields and cannot be used with channels")
)

func (db *Database) query(conn handlerWithContext, ctx context.Context, dest any, query string, cacheDuration time.Duration, params ...any) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	replacedQuery, normalizedParams, err := db.InterpolateParams(query, params...)
	if err != nil {
		return fmt.Errorf("failed to interpolate params: %w", err)
	}

	if db.die {
		fmt.Println(replacedQuery)
		j, _ := json.Marshal(normalizedParams)
		fmt.Println(string(j))
		cancel()
		os.Exit(0)
	}

	defer func() {
		if err != nil {
			err = Error{
				Err:           err,
				OriginalQuery: query,
				ReplacedQuery: replacedQuery,
				Params:        normalizedParams,
			}
		}
	}()

	destRef := reflect.ValueOf(dest)
	destKind := reflect.Indirect(destRef).Kind()

	t, multiRow := getElementTypeFromDest(destRef)
	indirectType := t
	if t.Kind() == reflect.Ptr {
		indirectType = t.Elem()
	}

	if destKind == reflect.Chan && hasUnexportedField(indirectType) {
		return ErrUnexportedField
	}

	sendElement := func(el reflect.Value) error {
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
				if index, _, _ := reflect.Select(cases); index == 0 {
					cancel()
					return context.Canceled
				}
			case reflect.Slice:
				destRef.Elem().Set(reflect.Append(destRef.Elem(), el))
			case reflect.Func:
				destRef.Call([]reflect.Value{el})
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

		h := sha3.Sum224([]byte(key.String()))
		cacheKey = hex.EncodeToString(h[:])

		start := time.Now()

	CHECK_CACHE:
		b, err := db.cache.Get(ctx, cacheKey)
		if errors.Is(err, ErrCacheMiss) {
			// cache miss!

			// grab a lock so we can update the cache
			var unlockFn func() error
			if db.locker != nil {
				unlockFn, err = db.locker.Lock(ctx, cacheKey+":mutex")
				if err != nil {
					time.Sleep(RedisLockRetryDelay)
					goto CHECK_CACHE
				}
			}

			defer func() {
				if unlockFn != nil {
					if err := unlockFn(); err != nil {
						db.Logger.Warn("failed to unlock cache mutex", "err", err)
					}
				}
			}()
		} else if err != nil {
			err = fmt.Errorf("failed to get data from cache: %w", err)
			if db.HandleCacheError != nil {
				err = db.HandleCacheError(err)
			}
			if err != nil {
				return err
			}
		} else {
			tx, _ := conn.(*sql.Tx)
			db.callLog(LogDetail{
				Query:    replacedQuery,
				Params:   normalizedParams,
				Duration: time.Since(start),
				CacheHit: true,
				Tx:       tx,
				Attempt:  1,
			})

			err = msgpack.Unmarshal(b, cacheSlice.Addr().Interface())
			if err != nil {
				return fmt.Errorf("failed to unmarshal from cache: %w", err)
			}

			l := cacheSlice.Len()
			if !multiRow && l == 0 {
				return sql.ErrNoRows
			}

			for i := range l {
				err = sendElement(cacheSlice.Index(i))
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

	if c, ok := conn.(*sql.DB); ok {
		c2, err := c.Conn(ctx)
		if c2 != nil {
			defer func() {
				if err := c2.Close(); err != nil {
					db.Logger.Warn("failed to close connection", "err", err)
				}
			}()
		}
		if err != nil {
			return fmt.Errorf("failed to get connection: %w", err)
		}
		conn = c2
	}

	start := time.Now()

	b := backoff.NewExponentialBackOff()
	var attempt int
	operation := func() (*sql.Rows, error) {
		attempt++
		rows, err := conn.QueryContext(ctx, replacedQuery)

		tx, _ := conn.(*sql.Tx)
		db.callLog(LogDetail{
			Query:    replacedQuery,
			Params:   normalizedParams,
			Duration: time.Since(start),
			Tx:       tx,
			Attempt:  attempt,
			Error:    err,
		})

		if err != nil {
			if rows != nil {
				if closeErr := rows.Close(); closeErr != nil {
					db.Logger.Warn("failed to close rows", "err", closeErr)
				}
			}

			if checkRetryError(err) {
				return nil, err
			}
			if errors.Is(err, mysql.ErrInvalidConn) {
				return nil, db.Test()
			}
			return nil, backoff.Permanent(err)
		}

		return rows, nil
	}

	options := []backoff.RetryOption{
		backoff.WithBackOff(b),
		backoff.WithMaxElapsedTime(MaxExecutionTime),
	}
	if MaxAttempts > 0 {
		options = append(options, backoff.WithMaxTries(uint(MaxAttempts)))
	}

	rows, err := backoff.Retry(ctx, operation, options...)
	if err != nil {
		return err
	}

	defer func() {
		if err := rows.Close(); err != nil {
			db.Logger.Warn("failed to close rows", "err", err)
		}
	}()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	if t != mapRowType {
		// since the map keys are literally the column names, we don't need to compare
		// without case sensitivity. But for structs, we do.
		for i := range columns {
			columns[i] = strings.ToLower(columns[i])
		}
	}

	ptrs, jsonFields, fieldsMap, ptrDests, isStruct, err := setupElementPtrs(db, t, indirectType, columns)
	if err != nil {
		return err
	}

	i := 0
	for rows.Next() {
		el := reflect.New(t).Elem()
		switch indirectType {
		case mapRowType:
			el.Set(reflect.MakeMapWithSize(mapRowType, len(columns)))
		case sliceRowType:
			el.Set(reflect.MakeSlice(reflect.SliceOf(t.Elem()), len(columns), len(columns)))
		}

		updateElementPtrs(el, &ptrs, jsonFields, columns, fieldsMap, ptrDests)

		err = rows.Scan(ptrs...)
		if err != nil {
			return err
		}

		for _, dest := range ptrDests {
			v := dest.tempDest.Elem()

			// special case: if we're scanning into a civil.Date, we need to convert the time.Time
			// we need to convert the time.Time we got from the db to a civil.Date
			if dest.finalDest.Type() == reflect.PointerTo(civilDateType) {
				if !v.IsNil() {
					d := civil.DateOf(v.Elem().Interface().(time.Time))
					dest.finalDest.Elem().Set(reflect.ValueOf(d))
				} else {
					dest.finalDest.Elem().Set(reflect.Zero(civilDateType))
				}
			} else {
				if !v.IsNil() {
					dest.finalDest.Elem().Set(v.Elem())
				} else {
					dest.finalDest.Elem().Set(reflect.Zero(dest.finalDest.Type().Elem()))
				}
			}
		}

		indirectEl := reflect.Indirect(el)

		if indirectType == mapRowType {
			// our map row is actually a map to pointers, not actual values, since
			// you can't take the address of a value by map and key, so we need to fix that here
			// to make usage intuitive

			for _, k := range indirectEl.MapKeys() {
				indirectEl.SetMapIndex(k, indirectEl.MapIndex(k).Elem().Elem())
			}
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
				f := indirectEl.FieldByIndex(jsonField.index)
				err = json.Unmarshal(jsonField.j, f.Addr().Interface())
				if err != nil {
					return fmt.Errorf("failed to unmarshal json into struct field %q: %w", el.Type().FieldByIndex(jsonField.index).Name, err)
				}
			}
		}

		if len(cacheKey) != 0 {
			cacheSlice = reflect.Append(cacheSlice, el)
		}

		i++

		if err = sendElement(el); err != nil {
			return err
		}
		if !multiRow {
			break
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}

	if !multiRow && i == 0 {
		return sql.ErrNoRows
	}

	if len(cacheKey) != 0 {
		b, err := msgpack.Marshal(cacheSlice.Interface())
		if err != nil {
			return fmt.Errorf("failed to marshal results for cache: %w", err)
		}

		err = db.cache.Set(ctx, cacheKey, b, cacheDuration)
		if err != nil {
			err = fmt.Errorf("failed to set cache: %w", err)
			if db.HandleCacheError != nil {
				err = db.HandleCacheError(err)
			}
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func getElementTypeFromDest(destRef reflect.Value) (t reflect.Type, multiRow bool) {
	indirectDestRef := reflect.Indirect(destRef)
	indirectDestRefType := indirectDestRef.Type()

	if !reflect.New(indirectDestRefType).Type().Implements(scannerType) &&
		indirectDestRefType != timeType &&
		indirectDestRefType != civilDateType &&
		indirectDestRefType != sliceRowType &&
		indirectDestRefType != mapRowType {
		switch k := indirectDestRef.Kind(); k {
		case reflect.Array, reflect.Chan, reflect.Map, reflect.Slice:
			if !((k == reflect.Array || k == reflect.Slice) && indirectDestRefType.Elem().Kind() == reflect.Uint8) {
				return indirectDestRefType.Elem(), true
			}
		}
	}

	if destRef.Kind() == reflect.Func && indirectDestRefType.NumIn() == 1 {
		return indirectDestRefType.In(0), true
	}

	return destRef.Type().Elem(), false
}

func isMultiValueElement(t reflect.Type) bool {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	if !reflect.New(t).Type().Implements(scannerType) && t != timeType && t != civilDateType {
		switch k := t.Kind(); k {
		case reflect.Array, reflect.Chan, reflect.Map, reflect.Slice, reflect.Struct:
			if !((k == reflect.Array || k == reflect.Slice) && t.Elem().Kind() == reflect.Uint8) {
				return true
			}
		}
	}

	return false
}

func hasUnexportedField(t reflect.Type) bool {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return false
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			return true
		}
		if f.Anonymous {
			if hasUnexportedField(f.Type) {
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

type ptrDest struct {
	finalDest reflect.Value
	tempDest  reflect.Value
}

func setupElementPtrs(db *Database, t reflect.Type, indirectType reflect.Type, columns []string) (ptrs []any, jsonFields []jsonField, fieldsMap map[string][]int, ptrDests map[int]*ptrDest, isStruct bool, err error) {
	switch {
	case isMultiValueElement(indirectType) && indirectType.Kind() == reflect.Struct:
		structFieldIndexes := StructFieldIndexes(indirectType)

		fieldsMap = make(map[string][]int, len(structFieldIndexes))
		for _, i := range structFieldIndexes {
			f := indirectType.FieldByIndex(i)

			if !f.IsExported() {
				continue
			}

			tags, err := structtag.Parse(string(f.Tag))
			if err != nil {
				return nil, nil, nil, nil, false, fmt.Errorf("failed to parse struct tag %q: %w", f.Tag, err)
			}

			name := f.Name
			mysqlTag, _ := tags.Get("mysql")
			if mysqlTag != nil && len(mysqlTag.Name) != 0 && mysqlTag.Name != "-" {
				name, err = decodeHex(mysqlTag.Name)
				if err != nil {
					return nil, nil, nil, nil, false, fmt.Errorf("failed to decode hex in struct tag name %q: %w", mysqlTag.Name, err)
				}
			}

			fieldsMap[strings.ToLower(name)] = i
		}

		for i, c := range columns {
			fieldIndex, ok := fieldsMap[c]
			if !ok {
				if !db.DisableUnusedColumnWarnings {
					db.Logger.Warn("unused column", "column", c)
				}
				continue
			}

			f := indirectType.FieldByIndex(fieldIndex)
			if isMultiValueElement(f.Type) {
				jsonFields = append(jsonFields, jsonField{
					index: fieldIndex,
				})
			} else {
				if ptrDests == nil {
					ptrDests = make(map[int]*ptrDest)
				}

				var tempDest reflect.Value
				if f.Type == civilDateType {
					tempDest = reflect.New(reflect.PointerTo(timeType))
				} else {
					tempDest = reflect.New(reflect.PointerTo(f.Type))
				}

				ptrDests[i] = &ptrDest{
					tempDest: tempDest,
				}
			}
		}
		return make([]any, len(columns)), jsonFields, fieldsMap, ptrDests, true, nil
	case isMultiValueElement(indirectType):
		return make([]any, len(columns)), make([]jsonField, 1), nil, nil, false, nil
	default:
		var tempDest reflect.Value
		if t == civilDateType {
			tempDest = reflect.New(reflect.PointerTo(timeType))
		} else {
			tempDest = reflect.New(reflect.PointerTo(t))
		}

		return make([]any, len(columns)), nil, nil, map[int]*ptrDest{0: {tempDest: tempDest}}, false, nil
	}
}

func updateElementPtrs(ref reflect.Value, ptrs *[]any, jsonFields []jsonField, columns []string, fieldsMap map[string][]int, ptrDests map[int]*ptrDest) {
	indirectType := ref.Type()
	indirectRef := ref
	if indirectType.Kind() == reflect.Ptr {
		ref.Set(reflect.New(indirectType.Elem()))
		indirectRef = ref.Elem()
		indirectType = indirectType.Elem()
	}
	x := new(any)

	switch {
	case isMultiValueElement(indirectType) && indirectType.Kind() == reflect.Struct:
		jsonIndex := 0
		for i, c := range columns {
			fieldIndex, ok := fieldsMap[c]
			if !ok {
				(*ptrs)[i] = x
				continue
			}

			f := indirectType.FieldByIndex(fieldIndex)
			if isMultiValueElement(f.Type) {
				jsonFields[jsonIndex].j = jsonFields[jsonIndex].j[:0]
				(*ptrs)[i] = &jsonFields[jsonIndex].j
				jsonIndex++
			} else {
				(*ptrs)[i] = ptrDests[i].tempDest.Interface()
				ptrDests[i].finalDest = indirectRef.FieldByIndex(fieldIndex).Addr()
			}
		}
	case indirectType == mapRowType:
		// map row is a special type that allows us to select all the columns from the query into
		// a map of colname => interfaces, letting us do what we want with the data later

		// a normal slice of interfaces will be treated like json normally, and will try to unmarshal the
		// first column as json into that slice.

		for i := range columns {
			v := reflect.ValueOf(new(any))
			indirectRef.SetMapIndex(reflect.ValueOf(columns[i]), v)
			(*ptrs)[i] = v.Interface()
		}
	case indirectType == sliceRowType:
		// slice row is a special type that allows us to select all the columns from the query into
		// a slice of interfaces
		for i := range columns {
			(*ptrs)[i] = indirectRef.Index(i).Addr().Interface()
		}
	case isMultiValueElement(indirectType):
		// this is one element (single column row), but with multiple values, like a map or array.
		// a struct is also technically a multi value element, but that's handled specially above

		// so the first column from the query is the only one that's kept since we only have a single element
		// making the first pointer in our slice of ptrs being scanned into a json byte sink
		(*ptrs)[0] = &jsonFields[0].j

		// and since we don't care about the rest of the column values, each of those will
		// be scanned into the same dummy interface
		for i := 1; i < len(columns); i++ {
			(*ptrs)[i] = x
		}
	default:
		// this is one element (row), like a time, number, or string
		(*ptrs)[0] = ptrDests[0].tempDest.Interface()
		ptrDests[0].finalDest = ref.Addr()
		for i := 1; i < len(columns); i++ {
			(*ptrs)[i] = x
		}
	}
}
