package mysql

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
	"reflect"
	"time"

	"github.com/golang/groupcache"
)

// Select selects one or more rows into the
// chan of structs in the destination
func (db *Database) Select(dest interface{}, query string, cache time.Duration) error {
	if cache == 0 {
		return db.selectRows(dest, query)
	}

	ch, strct, err := checkChanOfStruct(dest)
	if err != nil {
		return err
	}

	ctx := context.Background()
	ctx = context.WithValue(ctx, ctxDB, db)
	ctx = context.WithValue(ctx, ctxStrct, strct)

	var buf []byte
	err = selectGroup.Get(ctx, getCacheKey(query, cache), groupcache.AllocatingByteSliceSink(&buf))
	if err != nil {
		ch.Close()
		return err
	}

	go func() {
		defer ch.Close()

		dec := gob.NewDecoder(bytes.NewReader(buf))
		for {
			s := reflect.New(strct).Elem()
			err = dec.Decode(s.Addr().Interface())
			if err == io.EOF {
				break
			}
			if err != nil {
				panic(err)
			}

			ch.Send(s)
		}
	}()

	return nil
}

// selectRows is the real function responsible for
// writing the rows as structs to the given channel
func (db *Database) selectRows(dest interface{}, query string) error {
	ch, strct, err := checkChanOfStruct(dest)
	if err != nil {
		return err
	}

	rows, err := db.Reads.Query(query)
	if err != nil {
		ch.Close()
		return err
	}

	go func() {
		defer ch.Close()
		defer rows.Close()

		cols, _ := rows.Columns()
		pointers := make([]interface{}, len(cols))

		fieldsPositions := make([]int, len(cols))
	colsLoop:
		for i, c := range cols {
			for j := 0; j < strct.NumField(); j++ {
				f := strct.Field(j)
				if f.Tag.Get("mysql") == c || f.Name == c {
					fieldsPositions[i] = j
					continue colsLoop
				}
				fieldsPositions[i] = -1
			}
		}

		var x interface{}

		for rows.Next() {
			s := reflect.New(strct).Elem()

			for i, j := range fieldsPositions {
				if j != -1 {
					pointers[i] = s.Field(j).Addr().Interface()
				} else {
					pointers[i] = &x
				}
			}
			err = rows.Scan(pointers...)
			if err != nil {
				panic(err)
			}
			ch.Send(s)

			x = nil
		}
	}()

	return nil
}
