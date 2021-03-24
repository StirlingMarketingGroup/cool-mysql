package mysql

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode"

	dynamicstruct "github.com/Ompluscator/dynamic-struct"
	"github.com/fatih/structs"
	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
)

// ErrSourceInvalidType is an error about what types are allowed
var ErrSourceInvalidType = errors.New("source must be a channel of structs, a slice of structs, or a single struct")

func checkSource(source interface{}) (reflect.Value, reflect.Kind, reflect.Type, error) {
	switch source.(type) {
	case Params:
		return reflect.Value{}, 0, nil, nil
	}

	ref := reflect.ValueOf(source)
	kind := ref.Kind()

	switch kind {
	case reflect.Struct:
		return ref, kind, ref.Type(), nil
	case reflect.Slice:
		strct := ref.Type().Elem()
		if strct.Kind() == reflect.Struct {
			return ref, kind, strct, nil
		}
	case reflect.Chan:
		strct := ref.Type().Elem()
		if strct.Kind() == reflect.Struct {
			return ref, kind, strct, nil
		}
	}

	return reflect.Value{}, 0, nil, ErrSourceInvalidType
}

// Insert inserts struct rows from the source as a channel, single struct, or slice of structs
func (db *Database) Insert(insert string, src interface{}) error {
	return db.InsertContextRowComplete(nil, insert, src, nil)
}

// InsertContext inserts struct rows from the source as a channel, single struct, or slice of structs
func (db *Database) InsertContext(ctx context.Context, insert string, src interface{}) error {
	return db.InsertContextRowComplete(ctx, insert, src, nil)
}

type insertColumn struct {
	name             string
	structFieldIndex int
}

func paramToJSON(v interface{}) (interface{}, error) {
	if _, ok := v.(Encodable); ok {
		return v, nil
	}
	if _, ok := v.(time.Time); ok {
		return v, nil
	}
	if _, ok := v.(decimal.Decimal); ok {
		return v, nil
	}

	r := reflect.ValueOf(v)

	switch k := r.Kind(); k {
	case reflect.Ptr:
		return paramToJSON(r.Elem().Interface())
	case reflect.Array, reflect.Map, reflect.Slice, reflect.Struct:
		if k == reflect.Slice && r.Type().Elem().Kind() == reflect.Uint8 {
			return v, nil
		}

		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		return JSON(b), nil
	}

	return v, nil
}

// InsertContextRowComplete inserts struct rows from the source as a channel, single struct, or slice of structs
// rowComplete func is given the start time of processing the row
// for use of things like progress bars, timing the duration it takes to insert the row(s)
func (db *Database) InsertContextRowComplete(ctx context.Context, insert string, source interface{}, rowComplete func(start time.Time)) error {
	reflectValue, reflectKind, reflectStruct, err := checkSource(source)
	if err != nil {
		return err
	}

	var columns []insertColumn

	switch src := source.(type) {
	case Params:
		columns = make([]insertColumn, len(src))
		i := 0
		for c := range src {
			columns[i] = insertColumn{name: c}
			i++
		}
	default:
		switch reflectKind {
		case reflect.Chan, reflect.Slice:
			structFieldCount := reflectStruct.NumField()
			columns = make([]insertColumn, 0, structFieldCount)
			for i := 0; i < structFieldCount; i++ {
				f := reflectStruct.Field(i)

				if !unicode.IsUpper([]rune(f.Name)[0]) {
					continue
				}

				if t, ok := f.Tag.Lookup("mysql"); ok {
					columns = append(columns, insertColumn{t, i})
				} else {
					columns = append(columns, insertColumn{f.Name, i})
				}
			}
		default:
			panic("cool-mysql insert: unhandled source type - how did you get here?")
		}
	}

	onDuplicateKeyUpdateI := strings.Index(insert, "on duplicate key update")
	var onDuplicateKeyUpdate string
	if onDuplicateKeyUpdateI != -1 {
		onDuplicateKeyUpdate = insert[onDuplicateKeyUpdateI:]
		insert = insert[:onDuplicateKeyUpdateI]
	}

	insertBuf := new(bytes.Buffer)
	insertBuf.WriteString(insert)
	insertBuf.WriteByte('(')
	for i, c := range columns {
		if i != 0 {
			insertBuf.WriteByte(',')
		}
		insertBuf.WriteByte('`')
		insertBuf.WriteString(c.name)
		insertBuf.WriteByte('`')
	}
	insertBuf.WriteString(")values")
	baseLen := insertBuf.Len()

	var conn interface {
		Exec(query string, params ...Params) error
	}

	var cancel context.CancelFunc
	if ctx != nil {
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()

		conn, err = db.BeginWritesTx(ctx)
		if err != nil {
			return errors.Wrapf(err, "cool-mysql insert: failed to create transaction")
		}
	} else {
		conn = db
	}

	curRows := 0
	i := 0
	var r reflect.Value
	for ok := true; ok; {
		switch source.(type) {
		case Params:
			if curRows > 0 {
				ok = false
			}
		default:
			switch reflectKind {
			case reflect.Chan:
				r, ok = reflectValue.Recv()
			case reflect.Slice:
				if i >= reflectValue.Len() {
					ok = false
					break
				}

				r, ok = reflectValue.Index(i), true
				i++
			default:
				panic("cool-mysql insert: unhandled source type - how did you get here?")
			}
		}
		if !ok {
			break
		}

		var start time.Time
		if rowComplete != nil {
			start = time.Now()
		}

		preRowLen := insertBuf.Len()

		if curRows != 0 {
			insertBuf.WriteByte(',')
		}
		insertBuf.WriteByte('(')
		for i := 0; i < len(columns); i++ {
			if i != 0 {
				insertBuf.WriteByte(',')
			}

			var p interface{}

			switch src := source.(type) {
			case Params:
				p = src[columns[i].name]
			default:
				switch reflectKind {
				case reflect.Chan, reflect.Slice:
					p = r.Field(columns[i].structFieldIndex).Interface()
				default:
					panic("cool-mysql insert: unhandled source type - how did you get here?")
				}
			}

			p, err = paramToJSON(p)
			if err != nil {
				return errors.Wrapf(err, "failed to convert param to json for value", columns[i].name)
			}

			WriteEncoded(insertBuf, p, true)
		}
		insertBuf.WriteByte(')')

		if insertBuf.Len() > int(float64(db.maxInsertSize+len(onDuplicateKeyUpdate))*0.80) && curRows > 1 {
			buf := insertBuf.Bytes()[preRowLen+1:]
			insertBuf.Truncate(preRowLen)
			if onDuplicateKeyUpdateI != -1 {
				insertBuf.WriteString(onDuplicateKeyUpdate)
			}
			err := conn.Exec(insertBuf.String())
			if err != nil {
				return err
			}

			insertBuf.Truncate(baseLen)
			curRows = 0

			insertBuf.Write(buf)
		}

		curRows++

		if rowComplete != nil {
			rowComplete(start)
		}
	}

	if insertBuf.Len() > baseLen {
		if onDuplicateKeyUpdateI != -1 {
			insertBuf.WriteString(onDuplicateKeyUpdate)
		}
		err := conn.Exec(insertBuf.String())
		if err != nil {
			return err
		}
	}

	if ctx != nil {
		return errors.Wrapf(conn.(*Tx).Commit(), "cool-mysql insert: failed to commit transaction")
	}

	return nil
}

var insertUniquelyTableRegexp = regexp.MustCompile("`.+?`")

// InsertUniquely inserts the structs as rows
// if active versions don't already exist
func (db *Database) InsertUniquely(query string, uniqueColumns []string, active string, args interface{}) error {
	structsErr := fmt.Errorf("args must be a slice of structs")

	// this function only works with a slice of structs
	// that are all the same type
	slice := reflect.ValueOf(args)
	if slice.Kind() != reflect.Slice {
		return structsErr
	}

	// if our slice is empty, then we have nothing to do
	sliceLen := slice.Len()
	if sliceLen == 0 {
		return nil
	}

	colsMap := make(map[string]struct{}, len(uniqueColumns))
	cols := new(strings.Builder)

	// build the query that we'll use to see if active
	// versions of our rows already exist
	q := new(strings.Builder)
	q.WriteString("select distinct")
	for i, c := range uniqueColumns {
		colsMap[c] = struct{}{}

		if i != 0 {
			cols.WriteByte(',')
		}
		cols.WriteByte('`')
		cols.WriteString(c)
		cols.WriteByte('`')
	}
	c := cols.String()
	q.WriteString(c)
	q.WriteString("from")
	q.WriteString(insertUniquelyTableRegexp.FindString(query))
	q.WriteString("where(")
	q.WriteString(c)
	q.WriteString(")in(")

	var structName string
	var fields []*structs.Field
	// params := make([]interface{}, len(uniqueColumns)*sliceLen)
	var k int
	for i := 0; i < sliceLen; i++ {
		iface := slice.Index(i).Interface()
		if !structs.IsStruct(iface) {
			return structsErr
		}
		s := structs.New(iface)
		if i == 0 {
			structName = s.Name()
			fields = s.Fields()
		} else if s.Name() != structName {
			return structsErr
		}

		if slice.Index(i).Kind() != reflect.Struct {
			return fmt.Errorf("")
		}

		if i != 0 {
			q.WriteByte(',')
		}
		q.WriteByte('(')
		for j, c := range uniqueColumns {
			if j != 0 {
				q.WriteByte(',')
			}

			WriteEncoded(q, s.Field(c).Value(), true)
			// params[k] = s.Field(c).Value()
			k++
		}
		q.WriteByte(')')
	}

	q.WriteString(")and ")
	q.WriteString(active)

	uniqueStructBuilder := dynamicstruct.NewStruct()
	for _, f := range fields {
		name := f.Name()
		if _, ok := colsMap[name]; ok {
			var tag string
			if len(f.Tag("mysql")) != 0 {
				tag = f.Tag("mysql")
			} else {
				tag = name
			}
			uniqueStructBuilder.AddField(name, f.Value(), `mysql:"`+tag+`"`)
		}
	}
	uniqueStructType := uniqueStructBuilder.Build()
	uniqueStructs := uniqueStructType.NewSliceOfStructs()

	err := db.Select(uniqueStructs, q.String(), 0) //, ...params )
	if err != nil {
		return errors.Wrapf(err, "failed to execute InsertUniquely's initial select query")
	}

	rowsMap := make(map[string]struct{}, sliceLen)

	uniqueStructsRef := reflect.Indirect(reflect.ValueOf(uniqueStructs))
	for i := 0; i < uniqueStructsRef.Len(); i++ {
		var b bytes.Buffer
		enc := gob.NewEncoder(&b)
		err = enc.Encode(uniqueStructsRef.Index(i).Interface())
		if err != nil {
			return errors.Wrapf(err, "failed to encode InsertUniquely's struct to bytes")
		}

		rowsMap[b.String()] = struct{}{}
	}

	for i := 0; i < sliceLen; i++ {
		// make a new "unique struct" and copy the values
		// from our real struct to this one
		s := uniqueStructType.New()
		copier.Copy(reflect.ValueOf(s).Elem().Addr().Interface(), slice.Index(i).Addr().Interface())

		// convert are unique struct to a byte string so we can
		// lookup this struct in our rows map
		var b bytes.Buffer
		enc := gob.NewEncoder(&b)
		err = enc.Encode(s)
		if err != nil {
			return errors.Wrapf(err, "failed to encode InsertUniquely's struct to bytes")
		}
		k := b.String()

		if _, ok := rowsMap[k]; ok {
			// if the binary value of our unique struct exists
			// in our rows map, then swap this value with the last
			// value and make the slice 1 shorter,
			// removing this value from the slice
			slice.Index(i).Set(slice.Index(sliceLen - 1))
			sliceLen--
			i--
		} else {
			// make sure our newly inserted rows are also
			// not creating non-unique rows
			rowsMap[k] = struct{}{}
		}
	}

	if sliceLen == 0 {
		return nil
	}

	args = slice.Slice(0, sliceLen).Interface()

	return db.Insert(query, args)
}
