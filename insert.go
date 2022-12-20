package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/fatih/structtag"
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
	return in.insert(context.Background(), insert, source)
}

func (in *Inserter) InsertContext(ctx context.Context, insert string, source any) error {
	return in.insert(ctx, insert, source)
}

var ErrNoColumnNames = fmt.Errorf("no column names given")

func (in *Inserter) insert(ctx context.Context, query string, source any) (err error) {
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
	if multiRow && !next() {
		return nil
	}

	var colOpts map[string]insertColOpts
	if len(columnNames) == 0 {
		if typeHasColNames(rowType) {
			switch rowType.Kind() {
			case reflect.Map:
				columnNames = colNamesFromMap(currentRow)
			case reflect.Struct:
				columnNames, colOpts, _ = colNamesFromStruct(rowType)
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
		switch rowType.Kind() {
		case reflect.Struct:
			_, colOpts, _ = colNamesFromStruct(rowType)
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

	multiCol := isMultiColumn(rowType)

	buildRow := func(row reflect.Value) (string, error) {
		rowBuf.Reset()

		rowBuf.WriteByte('(')

		writeValue := func(r reflect.Value, j *bool) error {
			if j == nil {
				j = new(bool)
				if r.IsValid() && !r.Type().Implements(encoderType) && isMultiColumn(r.Type()) {
					*j = true
				}
			}

			v := r.Interface()

			if *j {
				j, err := json.Marshal(r.Interface())
				if err != nil {
					return fmt.Errorf("failed to json marshal value: %w", err)
				}

				v = json.RawMessage(j)
			}

			b, err := Marshal(v)
			if err != nil {
				return fmt.Errorf("failed to marshal value: %w", err)
			}
			rowBuf.Write(b)

			return nil
		}

		switch k := row.Kind(); true {
		case !multiCol:
			writeValue(row, nil)
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

				writeValue(v, colOpts[col].json)
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

				b, err := Marshal(v.Interface())
				if err != nil {
					return "", fmt.Errorf("failed to marshal value: %w", err)
				}

				rowBuf.Write(b)
			}
		case k == reflect.Slice || k == reflect.Array:
			for i := 0; i < row.Len(); i++ {
				if i != 0 {
					rowBuf.WriteByte(',')
				}

				writeValue(row.Index(i), nil)
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

		result, err := in.db.exec(in.conn, ctx, insertBuf.String())
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

func isMultiRow(t reflect.Type) bool {
	if t.Kind() == reflect.Pointer {
		return isMultiColumn(t.Elem())
	}

	switch t.Kind() {
	case reflect.Chan:
		return true
	case reflect.Slice, reflect.Array:
		switch t.Elem().Kind() {
		case reflect.Uint8, reflect.Interface:
			return false
		default:
			return true
		}
	default:
		return false
	}
}

func isMultiColumn(t reflect.Type) bool {
	if t.Kind() == reflect.Pointer {
		return isMultiColumn(t.Elem())
	}

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
	json          *bool
}

func colNamesFromStruct(t reflect.Type) (columns []string, colOpts map[string]insertColOpts, colFieldMap map[string]string) {
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
				column = t.Name
			}

			opts.insertDefault = t.HasOption("insertDefault") || t.HasOption("omitempty")

			if t.HasOption("json") {
				j := true
				opts.json = &j
			} else if t.HasOption("disableJson") {
				j := false
				opts.json = &j
			}
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
