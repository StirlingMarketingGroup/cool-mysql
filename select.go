package mysql

import (
	"context"
	"crypto/sha3"
	"database/sql"
	"database/sql/driver"
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
	if t.Kind() == reflect.Pointer {
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

	var currentConn *sql.Conn
	// getFreshConn reacquires a dedicated conn from the pool originally
	// passed in. It reads db.Reads / db.Writes live so it picks up the
	// replacement pool after Reconnect.
	var getFreshConn func(ctx context.Context) (*sql.Conn, error)

	if c, ok := conn.(*sql.DB); ok {
		if c == db.Reads {
			getFreshConn = func(ctx context.Context) (*sql.Conn, error) {
				return db.Reads.Conn(ctx)
			}
		} else if w, ok := db.Writes.(*sql.DB); ok && w == c {
			getFreshConn = func(ctx context.Context) (*sql.Conn, error) {
				w, _ := db.Writes.(*sql.DB)
				return w.Conn(ctx)
			}
		} else {
			getFreshConn = func(ctx context.Context) (*sql.Conn, error) {
				return c.Conn(ctx)
			}
		}

		c2, err := getFreshConn(ctx)
		if err != nil {
			return fmt.Errorf("failed to get connection: %w", err)
		}
		currentConn = c2
		conn = c2
	}

	defer func() {
		if currentConn != nil {
			if err := currentConn.Close(); err != nil {
				db.Logger.Warn("failed to close connection", "err", err)
			}
		}
	}()

	start := time.Now()

	// swapConn handles a retriable connection-level error (ErrInvalidConn /
	// ErrBadConn) by reconnecting the pool if it is down and swapping the
	// dead dedicated conn for a fresh one, so the retry doesn't hit the same
	// dead conn and burn the whole retry budget. It returns the error to hand
	// back to backoff — either err (retry) or a backoff.Permanent wrapper
	// (give up). It's used for both establishment failures and mid-stream
	// connection drops during scanning.
	swapConn := func(err error) error {
		// Reconnect if the pool itself is down. Test is a no-op when the
		// pool is healthy and only this dedicated conn died.
		if testErr := db.Test(); testErr != nil {
			return testErr
		}
		// If conn wasn't a *sql.DB (e.g. inside a *sql.Tx), we can't
		// reacquire — the tx is bound to the dead conn, so fail fast.
		if getFreshConn == nil {
			return backoff.Permanent(err)
		}
		if currentConn != nil {
			if closeErr := currentConn.Close(); closeErr != nil {
				db.Logger.Warn("failed to close stale connection", "err", closeErr)
			}
			currentConn = nil
		}
		fresh, acqErr := getFreshConn(ctx)
		if acqErr != nil {
			return acqErr
		}
		currentConn = fresh
		conn = fresh
		return err
	}

	// retryable reports whether a connection that drops mid-stream (after rows
	// have started flowing) can be recovered by discarding partial results and
	// re-running the whole query. That's only safe for buffered dests — slice
	// (*[]T) and single (*T) — because results accumulate into destRef.Elem()
	// and aren't observed by the caller until query() returns. Channel and func
	// dests emit each row via sendElement as it is scanned, so their rows are
	// already out the door; they keep establishment-only retry.
	retryable := destKind != reflect.Chan && destKind != reflect.Func

	// initialSliceLen is the caller-supplied length of a slice dest before the
	// query runs. Select appends results onto whatever the caller passed in, so
	// a mid-stream retry must truncate back to this length — not zero — to keep
	// any pre-existing elements the caller is accumulating into.
	var initialSliceLen int
	if multiRow && destKind == reflect.Slice {
		initialSliceLen = destRef.Elem().Len()
	}

	// resetAccumulator discards the partial results collected on a failed
	// attempt before a mid-stream retry so the re-run starts from the caller's
	// pre-query state. Only slice dests accumulate across rows; single (*T)
	// dests Set exactly once after a full row scan, which a mid-stream drop
	// can't reach, so they need no reset.
	resetAccumulator := func() {
		if multiRow && destKind == reflect.Slice {
			destRef.Elem().Set(destRef.Elem().Slice(0, initialSliceLen))
		}
		if cacheDuration > 0 {
			cacheSlice = reflect.New(reflect.SliceOf(t)).Elem()
		}
	}

	// scanRows runs the full row-streaming phase — column metadata, the scan
	// loop, and rows.Err() — returning the number of rows observed. For
	// buffered dests this runs inside the retry so a mid-stream connection
	// drop is recoverable; for channel/func dests it still runs inside the
	// operation but a mid-stream failure is treated as permanent (see below).
	scanRows := func(rows *sql.Rows) (int, error) {
		columns, err := rows.Columns()
		if err != nil {
			return 0, err
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
			return 0, err
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

			if err := rows.Scan(ptrs...); err != nil {
				return i, err
			}

			for _, dest := range ptrDests {
				if err := dest.assign(); err != nil {
					return i, err
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
					if err := json.Unmarshal(jsonField.j, el.Interface()); err != nil {
						return i, fmt.Errorf("failed to unmarshal json into dest: %w", err)
					}
				} else {
					f := indirectEl.FieldByIndex(jsonField.index)
					if err := json.Unmarshal(jsonField.j, f.Addr().Interface()); err != nil {
						return i, fmt.Errorf("failed to unmarshal json into struct field %q: %w", indirectEl.Type().FieldByIndex(jsonField.index).Name, err)
					}
				}
			}

			if len(cacheKey) != 0 {
				cacheSlice = reflect.Append(cacheSlice, el)
			}

			i++

			if err := sendElement(el); err != nil {
				return i, err
			}
			if !multiRow {
				break
			}
		}
		if err := rows.Err(); err != nil {
			return i, err
		}

		return i, nil
	}

	b := backoff.NewExponentialBackOff()
	var attempt int
	var rowCount int
	operation := func() (struct{}, error) {
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

			if tx != nil && checkTxRetryError(err) {
				// A tx-fatal deadlock / lock-wait timeout ends the transaction on
				// the session; statement-level retry would run in autocommit and
				// silently drop any FOR UPDATE locks the tx held. Surface it so the
				// caller (or RunInTx) can restart the whole transaction (#167).
				return struct{}{}, backoff.Permanent(err)
			}
			if checkRetryError(err) {
				return struct{}{}, err
			}
			if errors.Is(err, mysql.ErrInvalidConn) {
				return struct{}{}, swapConn(err)
			}
			return struct{}{}, backoff.Permanent(err)
		}

		n, scanErr := scanRows(rows)
		if closeErr := rows.Close(); closeErr != nil {
			db.Logger.Warn("failed to close rows", "err", closeErr)
		}
		if scanErr != nil {
			// A connection that dies mid-stream surfaces as ErrInvalidConn /
			// ErrBadConn from rows.Scan or rows.Err. For buffered dests the
			// caller hasn't observed any rows yet, so discard the partial
			// result and re-run the whole query on a fresh conn. Everything
			// else — and every error on a channel/func dest — is permanent.
			if retryable && (errors.Is(scanErr, mysql.ErrInvalidConn) || errors.Is(scanErr, driver.ErrBadConn)) {
				resetAccumulator()
				return struct{}{}, swapConn(scanErr)
			}
			return struct{}{}, backoff.Permanent(scanErr)
		}

		rowCount = n
		return struct{}{}, nil
	}

	options := []backoff.RetryOption{
		backoff.WithBackOff(b),
		backoff.WithMaxElapsedTime(db.MaxExecutionTime),
	}
	if MaxAttempts > 0 {
		options = append(options, backoff.WithMaxTries(uint(MaxAttempts)))
	}

	if _, err := backoff.Retry(ctx, operation, options...); err != nil {
		return unwrapBackoffPermanent(err)
	}

	if !multiRow && rowCount == 0 {
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
			if (k != reflect.Array && k != reflect.Slice) || indirectDestRefType.Elem().Kind() != reflect.Uint8 {
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
			if (k != reflect.Array && k != reflect.Slice) || t.Elem().Kind() != reflect.Uint8 {
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
	finalDest  reflect.Value
	tempDest   reflect.Value
	assignFn   func(*ptrDest) error
	targetType reflect.Type
	baseType   reflect.Type
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
			// Note: mysql:"-" fields are NOT skipped for selects — the "-" is
			// only treated specially during inserts. The field maps to its Go
			// name for select column matching. Prefer "noinsert" over "-".
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

				ptrDests[i] = newPtrDestForScanType(f.Type)
			}
		}
		return make([]any, len(columns)), jsonFields, fieldsMap, ptrDests, true, nil
	case isMultiValueElement(indirectType):
		return make([]any, len(columns)), make([]jsonField, 1), nil, nil, false, nil
	default:
		pd := newPtrDestForScanType(t)
		return make([]any, len(columns)), nil, nil, map[int]*ptrDest{0: pd}, false, nil
	}
}

func updateElementPtrs(ref reflect.Value, ptrs *[]any, jsonFields []jsonField, columns []string, fieldsMap map[string][]int, ptrDests map[int]*ptrDest) {
	indirectType := ref.Type()
	indirectRef := ref
	if indirectType.Kind() == reflect.Pointer {
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

func newPtrDestForScanType(fieldType reflect.Type) *ptrDest {
	if fieldType == civilDateType {
		return &ptrDest{
			tempDest:   reflect.New(reflect.PointerTo(timeType)),
			targetType: fieldType,
		}
	}

	pd := &ptrDest{
		tempDest:   reflect.New(reflect.PointerTo(fieldType)),
		targetType: fieldType,
	}

	if baseType, ok := needsUintWorkaround(fieldType); ok {
		pd.tempDest = reflect.New(reflect.PointerTo(interfaceType))
		pd.baseType = baseType
		pd.assignFn = assignUintFromInterface
	}

	return pd
}

func needsUintWorkaround(fieldType reflect.Type) (reflect.Type, bool) {
	baseType := fieldType
	for baseType.Kind() == reflect.Pointer {
		baseType = baseType.Elem()
	}

	switch baseType.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return baseType, true
	default:
		return nil, false
	}
}

func assignUintFromInterface(dest *ptrDest) error {
	holder := dest.tempDest.Elem()
	if !holder.IsValid() || holder.IsNil() {
		dest.finalDest.Elem().Set(reflect.Zero(dest.finalDest.Type().Elem()))
		return nil
	}

	raw := holder.Elem().Interface()
	target := reflect.New(dest.baseType)
	if err := convertAssignRows(target.Interface(), raw); err != nil {
		return err
	}

	if dest.targetType.Kind() == reflect.Pointer {
		dest.finalDest.Elem().Set(target)
	} else {
		dest.finalDest.Elem().Set(target.Elem())
	}

	return nil
}

func (dest *ptrDest) assign() error {
	if dest == nil || !dest.finalDest.IsValid() {
		return nil
	}

	if dest.assignFn != nil {
		return dest.assignFn(dest)
	}

	v := dest.tempDest.Elem()

	if dest.finalDest.Type() == reflect.PointerTo(civilDateType) {
		if !v.IsNil() {
			d := civil.DateOf(v.Elem().Interface().(time.Time))
			dest.finalDest.Elem().Set(reflect.ValueOf(d))
		} else {
			dest.finalDest.Elem().Set(reflect.Zero(civilDateType))
		}
		return nil
	}

	if !v.IsNil() {
		dest.finalDest.Elem().Set(v.Elem())
	} else {
		dest.finalDest.Elem().Set(reflect.Zero(dest.finalDest.Type().Elem()))
	}
	return nil
}
