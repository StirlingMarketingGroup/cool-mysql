package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	dynamicstruct "github.com/Ompluscator/dynamic-struct"
	"github.com/fatih/structs"
	"github.com/fatih/structtag"
	"github.com/jinzhu/copier"
)

type Inserter struct {
	db   *Database
	conn commander

	AfterChunkExec func(start time.Time)
	AfterRowExec   func(start time.Time)
	HandleResult   func(sql.Result)
}

func (in *Inserter) SetAfterChunkExec(fn func(start time.Time)) *Inserter {
	in.AfterChunkExec = fn

	return in
}

func (in *Inserter) SetAfterRowExec(fn func(start time.Time)) *Inserter {
	in.AfterRowExec = fn

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

var ErrNoColumnNames = fmt.Errorf("no column names given")

func (in *Inserter) insert(ex commander, ctx context.Context, query string, source any) (err error) {
	sourceRef := reflect.Indirect(reflect.ValueOf(source))
	sourceType := sourceRef.Type()

	rowType := sourceType

	multiRow := isMultiRow(sourceType)
	if multiRow {
		rowType = sourceType.Elem()
		if rowType.Kind() == reflect.Ptr {
			rowType = rowType.Elem()
		}

		switch sourceType.Kind() {
		case reflect.Slice, reflect.Array:
			if sourceRef.Len() == 0 {
				return nil
			}
		}
	}

	queryTokens := parseQuery(query)

	columnNames := colNamesFromQuery(queryTokens)
	insertPart := query
	var onDuplicateKeyUpdate string

	for i := range queryTokens {
		if i+3 < len(queryTokens) &&
			queryTokens[i].kind == queryTokenKindWord && strings.EqualFold(queryTokens[i].string, "on") &&
			queryTokens[i+1].kind == queryTokenKindWord && strings.EqualFold(queryTokens[i+1].string, "duplicate") &&
			queryTokens[i+2].kind == queryTokenKindWord && strings.EqualFold(queryTokens[i+2].string, "key") &&
			queryTokens[i+3].kind == queryTokenKindWord && strings.EqualFold(queryTokens[i+3].string, "update") {
			onDuplicateKeyUpdate = insertPart[queryTokens[i].pos:]
			insertPart = insertPart[:queryTokens[i].pos]
			break
		}
	}

	currentRow := sourceRef
	currentRowIndex := 0
	next := func() bool {
		if !multiRow {
			return false
		}

		switch sourceType.Kind() {
		case reflect.Slice, reflect.Array:
			if currentRowIndex >= sourceRef.Len() {
				return false
			}

			currentRow = reflect.Indirect(sourceRef.Index(currentRowIndex))
			currentRowIndex++
			return true
		case reflect.Chan:
			var ok bool
			currentRow, ok = sourceRef.Recv()
			if !ok {
				return false
			}

			currentRow = reflect.Indirect(currentRow)
			return true
		}

		return false
	}
	next()

	var colOpts map[string]insertColOpts
	if len(columnNames) == 0 {
		if typeHasColNames(rowType) {
			switch rowType.Kind() {
			case reflect.Map:
				columnNames = colNamesFromMap(currentRow)
			case reflect.Struct:
				columnNames, colOpts = colNamesFromStruct(rowType)
			}
		}

		s := new(strings.Builder)
		s.WriteByte('(')
		for i, name := range columnNames {
			if i != 0 {
				s.WriteByte(',')
			}
			s.WriteByte('`')
			s.WriteString(name)
			s.WriteByte('`')
		}
		s.WriteByte(')')
		insertPart += s.String()
	}

	if len(columnNames) == 0 {
		err = ErrNoColumnNames
		return
	}

	insertPart += "values"

	insertBuf := bytes.NewBufferString(insertPart)
	rowBuf := new(bytes.Buffer)
	var rowBuffered bool

	resetBuf := func() {
		insertBuf.Truncate(len(insertPart))
		rowBuffered = false
	}

	multiCol := isMultiColumn(rowType)

	buildRow := func(row reflect.Value) string {
		rowBuf.Reset()

		rowBuf.WriteByte('(')

		switch k := row.Kind(); true {
		case !multiCol:
			WriteEncoded(rowBuf, row.Interface(), true)
		case k == reflect.Struct:
			for i, col := range columnNames {
				if i != 0 {
					rowBuf.WriteByte(',')
				}

				v := row.FieldByIndex(colOpts[col].index)
				if colOpts[col].insertDefault && isNil(v.Interface()) {
					rowBuf.WriteString("default")
					continue
				}

				WriteEncoded(rowBuf, v.Interface(), false)
			}
		case k == reflect.Map:
			for i, col := range columnNames {
				if i != 0 {
					rowBuf.WriteByte(',')
				}

				v := row.MapIndex(reflect.ValueOf(col))
				if !v.IsValid() {
					rowBuf.WriteString("default")
					continue
				}

				WriteEncoded(rowBuf, v.Interface(), true)
			}
		case k == reflect.Slice || k == reflect.Array:
			for i := 0; i < row.Len(); i++ {
				if i != 0 {
					rowBuf.WriteByte(',')
				}

				WriteEncoded(rowBuf, row.Index(i).Interface(), true)
			}
		}

		rowBuf.WriteByte(')')
		return rowBuf.String()
	}

	var start time.Time
	chunkStart := time.Now()

	insert := func() error {
		if !rowBuffered {
			return nil
		}

		insertBuf.WriteString(onDuplicateKeyUpdate)

		result, err := in.db.exec(ex, ctx, insertBuf.String())
		if err != nil {
			return err
		}

		if in.AfterChunkExec != nil {
			in.AfterChunkExec(chunkStart)
			chunkStart = time.Now()
		}

		if in.HandleResult != nil {
			in.HandleResult(result)
		}

		resetBuf()
		return nil
	}

	for {
		start = time.Now()

		row := buildRow(currentRow)

		// buffer will be too big with this row, exec first and reset buffer
		if insertBuf.Len()+len(row)+len(onDuplicateKeyUpdate) > int(float64(in.db.MaxInsertSize.Get())*0.80) {
			if err = insert(); err != nil {
				return
			}
		}

		if rowBuffered {
			insertBuf.WriteByte(',')
		}

		insertBuf.WriteString(row)

		rowBuffered = true

		if in.AfterRowExec != nil {
			in.AfterRowExec(start)
		}

		if !next() {
			break
		}
	}

	if err = insert(); err != nil {
		return
	}

	return nil
}

var insertUniquelyTableRegexp = regexp.MustCompile("`.+?`")

// InsertUniquely inserts the structs as rows
// if active versions don't already exist
func (in *Inserter) InsertUniquely(insertQuery string, uniqueColumns []string, active string, args any) error {
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
				return fmt.Errorf("column %q doesn't exist in struct or isn't exported or was ignored", c)
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
			return fmt.Errorf("failed to encode InsertUniquely's struct to bytes: %w", err)
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
			return fmt.Errorf("failed to encode InsertUniquely's struct to bytes: %w", err)
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

func isMultiRow(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Chan:
		return true
	case reflect.Slice, reflect.Array:
		return t.Elem().Kind() != reflect.Uint8
	default:
		return false
	}
}

func isMultiColumn(t reflect.Type) bool {
	if t == timeType {
		return false
	}

	switch t.Kind() {
	case reflect.Map, reflect.Struct:
		return true
	case reflect.Slice, reflect.Array:
		return t.Elem().Kind() != reflect.Uint8
	default:
		return false
	}
}

func typeHasColNames(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Map:
		return t.Key().Kind() == reflect.String
	case reflect.Struct:
		return true
	default:
		return false
	}
}

func colNamesFromMap(v reflect.Value) (columns []string) {
	keys := make([]string, 0, v.Len())
	for _, k := range v.MapKeys() {
		keys = append(keys, k.String())
	}
	return keys
}

type insertColOpts struct {
	index         []int
	insertDefault bool
}

func colNamesFromStruct(t reflect.Type) (columns []string, colOpts map[string]insertColOpts) {
	structFieldIndexes := StructFieldIndexes(t)
	colOpts = make(map[string]insertColOpts, len(structFieldIndexes))

	for _, fieldIndex := range structFieldIndexes {
		f := t.FieldByIndex(fieldIndex)
		if f.PkgPath != "" {
			continue
		}

		column := f.Name
		opts := insertColOpts{
			index: fieldIndex,
		}

		t, _ := structtag.Parse(string(f.Tag))
		if t, _ := t.Get("mysql"); t != nil {
			if t.Name == "-" {
				continue
			}

			if len(t.Name) != 0 {
				column = t.Name
			}

			opts.insertDefault = t.HasOption("insertDefault") || t.HasOption("omitempty")
		}

		columns = append(columns, column)
		colOpts[column] = opts
	}

	return
}

// removes surrounding backticks and unescapes interior ones
func parseColName(s string) string {
	if len(s) < 2 {
		return s
	}

	if s[0] == '`' && s[len(s)-1] == '`' {
		s = s[1 : len(s)-1]
	}

	return strings.Replace(s, "``", "`", -1)
}

func colNamesFromQuery(queryTokens []queryToken) (columns []string) {
	for i, t := range queryTokens {
		// find the first paren
		if t.kind == queryTokenKindParen && t.string == "(" {
			queryTokens = queryTokens[i:]
			for i, t := range queryTokens {
				// if we found an end paren then we are done
				if t.kind == queryTokenKindParen && t.string == ")" {
					return
				}

				if t.kind != queryTokenKindWord && t.kind != queryTokenKindString {
					continue
				}

				// are we the last token or the next token is not a word or string?
				// we only want to push the last name of the "column" iun the case of "table.column"
				if i+i == len(queryTokens) || (queryTokens[i+1].kind != queryTokenKindWord && queryTokens[i+1].kind != queryTokenKindString) {
					col := t.string
					if len(col) != 0 && col[0] == '`' {
						col = parseColName(col)
					}

					columns = append(columns, col)
				}
			}

			break
		}
	}

	return
}
