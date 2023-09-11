package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/fatih/structtag"
)

type Inserter struct {
	db   *Database
	conn commander
	tx   *Tx

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
	return in.insert(context.Background(), insert, source)
}

func (in *Inserter) InsertContext(ctx context.Context, insert string, source any) error {
	return in.insert(ctx, insert, source)
}

var ErrNoColumnNames = fmt.Errorf("no column names given")

func (in *Inserter) insert(ctx context.Context, query string, source any) (err error) {
	sv := reflectUnwrap(reflect.ValueOf(source))
	st := sv.Type()

	rt := st

	multiRow := isMultiRow(st)
	if multiRow {
		rt = reflectUnwrapType(st.Elem())

		switch st.Kind() {
		case reflect.Slice, reflect.Array:
			if sv.Len() == 0 {
				return nil
			}
		}
	}

	queryTokens := parseQuery(query)
	if len(queryTokens) == 1 {
		query = "insert into`" + parseName(query) + "`"
		queryTokens = parseQuery(query)
	}

	insertPart := query
	var onDuplicateKeyUpdate string

	var firstToken *queryToken
	var prevToken *queryToken
DUPE_KEY_SEARCH:
	for _, t := range queryTokens {
		switch true {
		case t.kind == queryTokenKindMisc:
			// skip
		case prevToken == nil && t.kind == queryTokenKindWord && strings.EqualFold(t.string, "on"):
			firstToken = p(t)
			prevToken = p(t)
		case prevToken != nil && strings.EqualFold(prevToken.string, "on") && t.kind == queryTokenKindWord && strings.EqualFold(t.string, "duplicate"):
			prevToken = p(t)
		case prevToken != nil && strings.EqualFold(prevToken.string, "duplicate") && t.kind == queryTokenKindWord && strings.EqualFold(t.string, "key"):
			prevToken = p(t)
		case prevToken != nil && strings.EqualFold(prevToken.string, "key") && t.kind == queryTokenKindWord && strings.EqualFold(t.string, "update"):
			onDuplicateKeyUpdate = query[firstToken.pos:]
			insertPart = query[:firstToken.pos]

			break DUPE_KEY_SEARCH
		default:
			if prevToken != nil {
				break DUPE_KEY_SEARCH
			}
		}
	}

	columnNames := colNamesFromQuery(parseQuery(insertPart))

	currentRow := sv
	currentRowIndex := 0
	next := func() bool {
		if !multiRow {
			return false
		}

		switch st.Kind() {
		case reflect.Slice, reflect.Array:
			if currentRowIndex >= sv.Len() {
				return false
			}

			currentRow = reflectUnwrap(sv.Index(currentRowIndex))
			currentRowIndex++
			return true
		case reflect.Chan:
			var ok bool
			currentRow, ok = sv.Recv()
			if !ok {
				return false
			}

			currentRow = reflectUnwrap(currentRow)
			return true
		}

		return false
	}
	if multiRow && !next() {
		return nil
	}

	var colOpts map[string]insertColOpts
	if len(columnNames) == 0 {
		if typeHasColNames(rt) {
			switch rt.Kind() {
			case reflect.Map:
				columnNames = colNamesFromMap(currentRow)
			case reflect.Struct:
				columnNames, colOpts, _, err = colNamesFromStruct(rt)
				if err != nil {
					return err
				}
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
	} else {
		switch rt.Kind() {
		case reflect.Struct:
			_, colOpts, _, err = colNamesFromStruct(rt)
			if err != nil {
				return err
			}
		}
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

	multiCol := isMultiColumn(rt, in.db.valuerFuncs)

	buildRow := func(row reflect.Value) (string, error) {
		rowBuf.Reset()

		rowBuf.WriteByte('(')

		writeValue := func(r reflect.Value) error {
			r = reflectUnwrap(r)

			if !r.IsValid() {
				rowBuf.WriteString("null")
				return nil
			}

			v := r.Interface()

			b, err := marshal(v, marshalOptJSONSlice, in.db.valuerFuncs)
			if err != nil {
				return fmt.Errorf("failed to marshal value: %w", err)
			}
			rowBuf.Write(b)

			return nil
		}

		switch k := row.Kind(); true {
		case !multiCol:
			writeValue(row)
		case k == reflect.Struct:
			for i, col := range columnNames {
				if i != 0 {
					rowBuf.WriteByte(',')
				}

				f := row.FieldByIndex(colOpts[col].index)
				v := reflectUnwrap(f)

				if colOpts[col].insertDefault {
					pv := v
					if v.Kind() != reflect.Ptr {
						pv = reflect.New(v.Type())
						pv.Elem().Set(v)
					}

					if v, ok := pv.Interface().(Zeroer); ok {
						if pv.IsNil() {
							if _, ok := pv.Type().Elem().MethodByName("IsZero"); ok {
								rowBuf.WriteString("default")
								continue
							}
						}

						if v.IsZero() {
							rowBuf.WriteString("default")
							continue
						}
					}

					if !f.IsValid() || f.IsZero() {
						rowBuf.WriteString("default")
						continue
					}
				}

				writeValue(v)
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

				writeValue(v)
			}
		case k == reflect.Slice || k == reflect.Array:
			for i := 0; i < row.Len(); i++ {
				if i != 0 {
					rowBuf.WriteByte(',')
				}

				writeValue(row.Index(i))
			}
		}

		rowBuf.WriteByte(')')
		return rowBuf.String(), nil
	}

	var start time.Time
	chunkStart := time.Now()

	insert := func() error {
		if !rowBuffered {
			return nil
		}

		insertBuf.WriteString(onDuplicateKeyUpdate)

		result, err := in.db.exec(in.conn, ctx, in.tx, true, insertBuf.String(), marshalOptJSONSlice)
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

		var row string
		row, err = buildRow(currentRow)
		if err != nil {
			return err
		}

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

func colNamesFromStruct(t reflect.Type) (columns []string, colOpts map[string]insertColOpts, colFieldMap map[string]string, err error) {
	structFieldIndexes := StructFieldIndexes(t)
	colOpts = make(map[string]insertColOpts, len(structFieldIndexes))
	colFieldMap = make(map[string]string, len(structFieldIndexes))

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
				column, err = decodeHex(t.Name)
				if err != nil {
					return nil, nil, nil, fmt.Errorf("failed to decode hex in struct tag name %q: %w", t.Name, err)
				}
			}

			opts.insertDefault = t.HasOption("insertDefault") || t.HasOption("omitempty")
		}

		columns = append(columns, column)
		colOpts[column] = opts
		colFieldMap[column] = f.Name
	}

	return
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
						col = parseName(col)
					}

					columns = append(columns, col)
				}
			}

			break
		}
	}

	return
}
