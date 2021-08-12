package mysql

import (
	"bytes"
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/tinylib/msgp/msgp"
	"golang.org/x/sync/singleflight"
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

var rCtx = context.Background()

var selectSinglelight = new(singleflight.Group)

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// Select selects one or more rows into the
// chan of structs in the destination
func (db *Database) Select(dest interface{}, query string, cache time.Duration, params ...Params) error {
	return db.SelectContext(context.Background(), dest, query, cache, params...)
}

// SelectContext selects one or more rows into the
// chan of structs in the destination
func (db *Database) SelectContext(ctx context.Context, dest interface{}, query string, cache time.Duration, params ...Params) error {
	replacedQuery, mergedParams := ReplaceParams(query, params...)
	if db.die {
		fmt.Println(replacedQuery)
		os.Exit(0)
	}

	refDest, kind, strct, err := checkDest(dest)
	if err != nil {
		return err
	}

	var msgpWriter *msgp.Writer
	var gobEncoder *gob.Encoder

	var msgpEncodable bool
	if cache != 0 {
		_, msgpEncodable = reflect.New(strct).Interface().(msgp.Encodable)
	}

	var start time.Time

	liveGet := func() error {
		start = time.Now()
		var rows *sql.Rows

		b := backoff.NewExponentialBackOff()
		b.MaxElapsedTime = BackoffDefaultMaxElapsedTime
		err := backoff.Retry(func() error {
			var err error
			rows, err = db.Reads.QueryContext(ctx, replacedQuery)
			if err != nil {
				if checkRetryError(err) {
					return err
				} else {
					return backoff.Permanent(err)
				}
			}

			return nil
		}, backoff.WithContext(b, ctx))
		if err != nil {
			return err
		}

		execDuration := time.Since(start)
		start = time.Now()
		db.callLog(replacedQuery, mergedParams, execDuration)

		if err != nil {
			if kind == reflect.Chan {
				refDest.Close()
			}
			return Error{
				Err:           err,
				OriginalQuery: query,
				ReplacedQuery: replacedQuery,
				Params:        mergedParams,
			}
		}

		main := func(ctx context.Context) error {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			if db.Finished != nil {
				defer func() { db.Finished(false, replacedQuery, mergedParams, execDuration, time.Since(start)) }()
			}

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

						// check if field is exported
						if len(f.PkgPath) != 0 {
							continue
						}

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
						case reflect.Ptr:
							switch f.Type.Elem().Kind() {
							case reflect.Map:
								jsonable = true
							case reflect.Struct:
								if f.Type.Elem() == reflect.TypeOf(time.Time{}) {
									break
								}

								if f.Type.Elem().Implements(reflect.Indirect(reflect.ValueOf(new(sql.Scanner))).Type()) {
									break
								}

								jsonable = true
							case reflect.Array, reflect.Slice:
								// if it's a slice, but not a byte slice
								if f.Type.Elem().Elem().Kind() != reflect.Uint8 {
									jsonable = true
								}
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

			var jsonables [][]byte
			if jsonablesCount > 0 {
				jsonables = make([][]byte, jsonablesCount)
			}
		Rows:
			for rows.Next() {
				ran = true

				s := reflect.New(strct).Elem()

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
					return errors.Wrapf(err, "failed to scan rows")
				}

				if jsonablesCount > 0 {
					for _, c := range columns {
						if c == nil || !c.jsonable || jsonables[c.jsonableIndex] == nil {
							continue
						}

						err := json.Unmarshal(jsonables[c.jsonableIndex], s.Field(int(c.structIndex)).Addr().Interface())
						if err != nil {
							return errors.Wrapf(err, "failed to unmarshal %q", jsonables[c.jsonableIndex])
						}
					}
				}

				if cache != 0 {
					switch {
					case msgpEncodable:
						err := s.Addr().Interface().(msgp.Encodable).EncodeMsg(msgpWriter)
						if err != nil {
							return errors.Wrapf(err, "failed to write struct to cache with msgp")
						}
					default:
						err := gobEncoder.EncodeValue(s)
						if err != nil {
							return errors.Wrapf(err, "failed to write struct to cache with gob")
						}
					}
				}

				switch kind {
				case reflect.Chan:
					cases := []reflect.SelectCase{
						{
							Dir:  reflect.SelectRecv,
							Chan: reflect.ValueOf(ctx.Done()),
						},
						{
							Dir:  reflect.SelectSend,
							Chan: refDest,
							Send: s,
						},
					}
					switch index, _, _ := reflect.Select(cases); index {
					case 0:
						cancel()
						return nil
					case 1:
					}
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
			if cache == 0 {
				go func() {
					if err := main(ctx); err != nil {
						panic(err)
					}
				}()
			} else {
				return main(ctx)
			}
		case reflect.Slice, reflect.Struct:
			refDest = refDest.Elem()
			return main(ctx)
		}
		return nil
	}

	cacheGet := func(b []byte) error {
		execDuration := time.Since(start)
		start = time.Now()
		db.callLog("/* cached! */ "+replacedQuery, mergedParams, execDuration)

		main := func(ctx context.Context) error {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			if db.Finished != nil {
				defer func() { db.Finished(true, replacedQuery, mergedParams, execDuration, time.Since(start)) }()
			}

			if kind == reflect.Chan {
				defer refDest.Close()
			}

			var msgpReader *msgp.Reader
			var gobDecoder *gob.Decoder

			switch {
			case msgpEncodable:
				msgpReader = msgp.NewReader(bytes.NewReader(b))
			default:
				gobDecoder = gob.NewDecoder(bytes.NewReader(b))
			}

		Rows:
			for {
				var s reflect.Value

				switch {
				case msgpEncodable:
					s = reflect.New(strct)
					err := s.Interface().(msgp.Decodable).DecodeMsg(msgpReader)
					if err != nil && err.Error() == "EOF" {
						break Rows
					} else if err != nil {
						return errors.Wrapf(err, "failed to decode cached struct with msgp")
					}
					s = s.Elem()
				default:
					s = reflect.New(strct).Elem()
					err := gobDecoder.DecodeValue(s)
					if err == io.EOF {
						break Rows
					} else if err != nil {
						return errors.Wrapf(err, "failed to decode cached struct with gob")
					}
				}

				switch kind {
				case reflect.Chan:
					cases := []reflect.SelectCase{
						{
							Dir:  reflect.SelectRecv,
							Chan: reflect.ValueOf(ctx.Done()),
						},
						{
							Dir:  reflect.SelectSend,
							Chan: refDest,
							Send: s,
						},
					}
					switch index, _, _ := reflect.Select(cases); index {
					case 0:
						cancel()
						return nil
					case 1:
					}
				case reflect.Slice:
					refDest.Set(reflect.Append(refDest, s))
				case reflect.Struct:
					refDest.Set(s)
					break Rows
				}
			}

			return nil
		}

		switch kind {
		case reflect.Chan:
			go func() {
				if err := main(ctx); err != nil {
					panic(err)
				}
			}()
		case reflect.Slice, reflect.Struct:
			refDest = refDest.Elem()
			return main(ctx)
		}
		return nil
	}

	if cache != 0 {
		if db.redis == nil {
			return errors.New("cool-mysql select: cache time given without redis connection for query")
		}

		main := func() error {
			hasher := md5.New()
			gob.NewEncoder(hasher).EncodeValue(reflect.New(strct))
			hasher.Write([]byte(replacedQuery))
			h := base64.RawStdEncoding.EncodeToString(hasher.Sum(nil))

			destFilled := false
			cache, err, _ := selectSinglelight.Do(h, func() (interface{}, error) {
				start = time.Now()
				b, err := db.redis.Get(rCtx, h).Bytes()
				if err == redis.Nil {
					var buf bytes.Buffer
					switch {
					case msgpEncodable:
						msgpWriter = msgp.NewWriter(&buf)
					default:
						gobEncoder = gob.NewEncoder(&buf)
					}

					err = liveGet()
					if err != nil {
						return nil, err
					}
					destFilled = true

					if msgpEncodable {
						msgpWriter.Flush()
					}

					err = db.redis.Set(rCtx, h, buf.Bytes(), cache).Err()
					if err != nil {
						return nil, errors.Wrapf(err, "cool-mysql select: failed to set query cache to redis")
					}

					start = time.Now()

					return buf.Bytes(), nil
				}
				if err != nil {
					return nil, errors.Wrapf(err, "cool-mysql select: failed to get query cache from redis")
				}

				return b, nil
			})
			if err != nil {
				return err
			}

			if !destFilled {
				return cacheGet(cache.([]byte))
			}

			return nil
		}

		if kind == reflect.Chan {
			go func() {
				if err := main(); err != nil {
					panic(err)
				}
			}()
			return nil
		}

		return main()
	}

	// we got this far, so just fill the dest with a normal live get
	return liveGet()
}
