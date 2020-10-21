package mysql

import (
	"bytes"
	"context"
	"encoding/gob"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/golang/groupcache"
	"github.com/twmb/murmur3"
)

func getCacheKey(query string, c time.Duration) string {
	// First we need a hash of the query and the arguments
	// so that we can begin our key creation based on a unique mysql call
	m := murmur3.New64()
	m.Write([]byte(query))

	// Really important step: basically we use the first 8 bytes of the hash
	// to get a relatively unique (maybe not but that's okay here) int for this
	// query, and get the remainder of that int/the cache duration (in nanoseconds)...
	r := time.Duration(m.Sum64() % uint64(c.Nanoseconds()))

	// ... so that we have a number of nano seconds to offset our current
	// time by that's between 0 & the cache duration, essentially creating a unique time slot
	// that the cache of this query can invalidate on, with intervals of the
	// original cache duration, once we round it down (or truncate) with the cache duration
	t := time.Now().UTC().Add(r).Truncate(c)

	// then we turn that new offset & truncated time into bytes to make our new final hash with
	b := make([]byte, 0, 32+len(query))
	b = strconv.AppendInt(b, int64(t.UnixNano()), 10)
	b = append(b, ';')
	b = append(b, query...)

	// And build the final hash as bytes for the groupcache key
	return string(b)
}

type ctxValue int

const (
	ctxDB ctxValue = iota
	ctxStrct
)

var selectGroup = groupcache.NewGroup("selectGroup", 512<<20, groupcache.GetterFunc(
	func(ctx context.Context, key string, dest groupcache.Sink) error {
		db := ctx.Value(ctxDB).(*Database)
		ch := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, ctx.Value(ctxStrct).(reflect.Type)), 0)

		err := db.selectRows(ch.Interface(), key[strings.IndexByte(key, ';')+1:])
		if err != nil {
			return err
		}

		var b bytes.Buffer
		enc := gob.NewEncoder(&b)

		for ok := true; ok; {
			var strct reflect.Value
			if strct, ok = ch.Recv(); ok {
				err = enc.EncodeValue(strct)
				if err != nil {
					return err
				}
			}
		}

		err = dest.SetBytes(b.Bytes())
		if err != nil {
			return err
		}

		return nil
	},
))
