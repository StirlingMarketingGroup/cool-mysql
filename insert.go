package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	dynamicstruct "github.com/Ompluscator/dynamic-struct"
	"github.com/fatih/structs"
	"github.com/fatih/structtag"
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

type insertColumn struct {
	name             string
	structFieldIndex int
	omitempty        bool
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
		if r.Elem().IsValid() {
			return paramToJSON(r.Elem().Interface())
		}
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

type Inserter struct {
	db   *Database
	conn commander

	AfterChunkExec func(start time.Time)
	HandleResult   func(sql.Result)
}

func (in *Inserter) SetAfterChunkExec(fn func(start time.Time)) *Inserter {
	in.AfterChunkExec = fn

	return in
}

func (in *Inserter) SetResultHandler(fn func(sql.Result)) *Inserter {
	in.HandleResult = fn

	return in
}

func (in *Inserter) SetExecutor(conn commander) *Inserter {
	in.conn = conn

	return in
}

func (in *Inserter) Insert(insert string, source any) error {
	return in.insert(in.conn, context.Background(), insert, source)
}

func (in *Inserter) InsertContext(ctx context.Context, insert string, source any) error {
	return in.insert(in.conn, ctx, insert, source)
}

// insert inserts struct rows from the source as a channel, single struct, or slice of structs
func (in *Inserter) insert(ex commander, ctx context.Context, insert string, source any) error {
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
		case reflect.Chan, reflect.Slice, reflect.Struct:
			structFieldIndexes := StructFieldIndexes(reflectStruct)
			columns = make([]insertColumn, 0, len(structFieldIndexes))
			j := 0
			for _, i := range structFieldIndexes {
				f := reflectStruct.FieldByIndex(i)

				if f.PkgPath != "" {
					continue
				}

				var t *structtag.Tag
				tag, err := structtag.Parse(string(f.Tag))
				if err == nil {
					t, _ = tag.Get("mysql")
				}

				newCol := insertColumn{f.Name, j, false}
				j++
				if t != nil {
					if t.Name == "-" {
						continue
					}
					if len(t.Name) != 0 {
						newCol.name = t.Name
					}
					newCol.omitempty = t.HasOption("omitempty")
				}

				columns = append(columns, newCol)
			}
		default:
			panic("cool-mysql insert: unhandled source type - how did you get here?")
		}
	}

	onDuplicateKeyUpdateI := strings.Index(strings.ToLower(insert), "on duplicate key update")
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
			case reflect.Struct:
				if i != 0 {
					ok = false
					break
				}

				r, ok = reflectValue, true
				i++
			default:
				panic("cool-mysql insert: unhandled source type - how did you get here?")
			}
		}
		if !ok {
			break
		}

		start := time.Now()

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
				case reflect.Chan, reflect.Slice, reflect.Struct:
					p = r.Field(columns[i].structFieldIndex).Interface()
				default:
					panic("cool-mysql insert: unhandled source type - how did you get here?")
				}
			}

			if columns[i].omitempty && isZero(p) {
				WriteEncoded(insertBuf, Literal("default"), false)
				continue
			}

			p, err = paramToJSON(p)
			if err != nil {
				return errors.Wrapf(err, "failed to convert param to json for value", columns[i].name)
			}

			WriteEncoded(insertBuf, p, true)
		}
		insertBuf.WriteByte(')')

		if insertBuf.Len() > int(float64(in.db.MaxInsertSize.Get()+len(onDuplicateKeyUpdate))*0.80) && curRows > 1 {
			buf := insertBuf.Bytes()[preRowLen+1:]
			insertBuf.Truncate(preRowLen)
			if onDuplicateKeyUpdateI != -1 {
				insertBuf.WriteString(onDuplicateKeyUpdate)
			}
			result, err := in.db.exec(ex, ctx, insertBuf.String())
			if err != nil {
				return err
			}

			if in.HandleResult != nil {
				in.HandleResult(result)
			}

			insertBuf.Truncate(baseLen)
			curRows = 0

			insertBuf.Write(buf)
		}

		curRows++

		if in.AfterChunkExec != nil {
			in.AfterChunkExec(start)
		}
	}

	if insertBuf.Len() > baseLen {
		if onDuplicateKeyUpdateI != -1 {
			insertBuf.WriteString(onDuplicateKeyUpdate)
		}
		result, err := in.db.exec(ex, ctx, insertBuf.String())
		if err != nil {
			return err
		}

		if in.HandleResult != nil {
			in.HandleResult(result)
		}
	}

	return nil
}

var insertUniquelyTableRegexp = regexp.MustCompile("`.+?`")

// InsertUniquely inserts the structs as rows
// if active versions don't already exist
func (in *Inserter) InsertUniquely(insertQuery string, uniqueColumns []string, active string, args interface{}) error {
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
	q.WriteString(insertUniquelyTableRegexp.FindString(insertQuery))
	q.WriteString("where(")
	q.WriteString(c)
	q.WriteString(")in(")

	iface := slice.Index(0).Interface()
	if !structs.IsStruct(iface) {
		return structsErr
	}

	reflectStruct := reflect.TypeOf(iface)
	structFieldIndexes := StructFieldIndexes(reflectStruct)

	type fieldDetail struct {
		name      string
		fieldName string
		skip      bool
		omitempty bool
	}
	fieldDetails := make([]fieldDetail, len(structFieldIndexes))
	fieldDetailsMap := make(map[string]*fieldDetail)

	j := 0
	for _, i := range structFieldIndexes {
		f := reflectStruct.FieldByIndex(i)

		if f.PkgPath != "" {
			fieldDetails[j].skip = true
			continue
		}

		var t *structtag.Tag
		tag, err := structtag.Parse(string(f.Tag))
		if err == nil {
			t, _ = tag.Get("mysql")
		}

		fieldDetails[j].fieldName = f.Name

		if t != nil {
			if t.Name == "-" {
				fieldDetails[j].skip = true
				continue
			}

			fieldDetails[j].omitempty = t.HasOption("omitempty")

			if len(t.Name) != 0 {
				fieldDetails[j].name = t.Name
			}
		}

		if len(fieldDetails[j].name) == 0 {
			fieldDetails[j].name = f.Name
		}

		j++
	}

	for i := range fieldDetails {
		fieldDetailsMap[fieldDetails[i].name] = &fieldDetails[i]
	}

	var structName string
	var fields []*structs.Field
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
		var j int
		for _, u := range uniqueColumns {
			var f *structs.Field

			d, ok := fieldDetailsMap[u]
			if !ok || d.skip {
				return errors.Errorf("column %q doesn't exist in struct or isn't exported or was ignored", c)
			}

			f = s.Field(d.fieldName)

			if j != 0 {
				q.WriteByte(',')
			}

			WriteEncoded(q, f.Value(), true)
			k++
			j++
		}
		q.WriteByte(')')
	}

	q.WriteString(")and ")
	q.WriteString(active)

	uniqueStructBuilder := dynamicstruct.NewStruct()
	for _, f := range fields {
		fieldName := f.Name()
		if _, ok := colsMap[fieldName]; !ok {
			continue
		}
		fd := fieldDetailsMap[fieldName]
		if fd == nil || fd.skip {
			continue
		}
		tag := fd.name
		if fd.omitempty {
			tag += ",omitempty"
		}
		uniqueStructBuilder.AddField(fieldName, f.Value(), `mysql:"`+tag+`"`)
	}
	uniqueStructType := uniqueStructBuilder.Build()
	uniqueStructs := uniqueStructType.NewSliceOfStructs()

	err := query(in.db, in.conn, context.Background(), uniqueStructs, q.String(), 0)
	if err != nil {
		return fmt.Errorf("failed to execute InsertUniquely's initial select query: %w", err)
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

	return in.Insert(insertQuery, args)
}
